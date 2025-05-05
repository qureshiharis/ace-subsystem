import os
import time
import pandas as pd
from datetime import datetime, timedelta

from config import TAG_PAIRS, FETCH_INTERVAL, API_KEY
from fetcher import fetch_sensor_data
from detector import detect_anomalies_for_pair
from notifier import alert


import paho.mqtt.client as mqtt

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# MQTT config
MQTT_BROKER = os.getenv("MQTT_BROKER", "192.168.1.219")  # Replace with local IP running the dashboard
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "anomalies")

# Set up MQTT client
mqtt_client = mqtt.Client()
mqtt_connected = False
logger.info(f"MQTT_BROKER, MQTT_PORT: {MQTT_BROKER} {MQTT_PORT}")
try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_start()
    mqtt_connected = True
except Exception as e:
    logger.warning(f"MQTT connection failed: {e}")

def publish_anomaly_row(row):
    import json
    payload = row.to_json()
    if mqtt_connected:
        mqtt_client.publish(MQTT_TOPIC, payload)
        logger.info(f"Published topic: {MQTT_TOPIC}")
        logger.info(f"Published payload: {payload}")
    else:
        logger.info("Skipping MQTT publish since client is not connected.")

BUFFER_HOURS = int(os.getenv("BUFFER_HOURS", 4))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "sensor")


def main():
    while True:
        logger.info("Fetching and processing data...")

        cutoff_time = pd.Timestamp.now(tz="Europe/Stockholm") - timedelta(hours=BUFFER_HOURS)

        for sp_tag, pv_tag in TAG_PAIRS:
            df_sp = fetch_sensor_data(sp_tag, API_KEY, window_minutes=BUFFER_HOURS * 60)
            df_pv = fetch_sensor_data(pv_tag, API_KEY, window_minutes=BUFFER_HOURS * 60)

            if df_sp is not None and df_pv is not None:
                logger.info(f"{sp_tag} -> {df_sp.shape} rows | {pv_tag} -> {df_pv.shape} rows")
              
            else:
                logger.warning(f"Skipping pair ({sp_tag}, {pv_tag}) due to failed fetch (None returned)")
                continue

            if df_sp is not None and df_pv is not None and not df_sp.empty and not df_pv.empty:
                # Rename BEFORE merging
                df_sp.rename(columns={"Value": f"SetPoint_{sp_tag}"}, inplace=True)
                df_pv.rename(columns={"Value": f"Actual_{pv_tag}"}, inplace=True)

                df_sp.sort_values("Timestamp", inplace=True)
                df_pv.sort_values("Timestamp", inplace=True)
                df = pd.merge_asof(
                    df_sp, df_pv, on="Timestamp", direction="nearest", tolerance=pd.Timedelta("1min")
                )
                logger.info(f"Merged {sp_tag} & {pv_tag} → {df.shape} rows")

                df = df.sort_values("Timestamp").interpolate().bfill().ffill()

                df, anomaly_flags = detect_anomalies_for_pair(df, sp_tag, pv_tag)

                output_file = f"{OUTPUT_FILE}_latest_{sp_tag}.csv"
                try:
                    df_existing = pd.read_csv(output_file, parse_dates=["Timestamp"]).sort_values("Timestamp")
                    if not df.empty:
                        df_existing = pd.concat([df_existing.iloc[1:], df.iloc[[-1]]], ignore_index=True)
                except FileNotFoundError:
                    logger.info(f"First time creating output file for {output_file}.")
                    df_existing = df.copy()

                df_existing.drop_duplicates(subset=["Timestamp"], keep="last", inplace=True)
                df_existing = df_existing[df_existing["Timestamp"] >= cutoff_time]
                df_existing.sort_values("Timestamp", inplace=True)
                df_existing.to_csv(output_file, index=False)
                logger.info(f"Data saved to {output_file} with {len(df_existing)} rows")

                if not df_existing.empty:
                    publish_anomaly_row(df_existing.iloc[[-1]])

                if anomaly_flags:
                    logger.warning(f"Anomaly detected for {sp_tag} — triggering alert")
                    alert()
            else:
                logger.warning(f"Skipping {sp_tag} & {pv_tag} due to empty data.")

        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    main()