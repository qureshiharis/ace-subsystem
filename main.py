import os
import time
import pandas as pd
from datetime import datetime, timedelta

from config import TAG_PAIRS, FETCH_INTERVAL, API_KEY
from fetcher import fetch_sensor_data
from detector import detect_anomalies
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
logger.info(f"MQTT_BROKER, MQTT_PORT: {MQTT_BROKER} {MQTT_PORT}")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
mqtt_client.loop_start()

def publish_anomaly_row(row):
    import json
    payload = row.to_json()
    mqtt_client.publish(MQTT_TOPIC, payload)

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "latest_data.csv")
BUFFER_HOURS = int(os.getenv("BUFFER_HOURS", 4))


def main():
    while True:
        logger.info("Fetching and processing data...")

        data_frames = []
        for sp_tag, pv_tag in TAG_PAIRS:
            df_sp = fetch_sensor_data(sp_tag, API_KEY, window_minutes=BUFFER_HOURS * 60)
            df_pv = fetch_sensor_data(pv_tag, API_KEY, window_minutes=BUFFER_HOURS * 60)

            if df_sp is not None and df_pv is not None:
                logger.info(f"{sp_tag} -> {df_sp.shape} rows | {pv_tag} -> {df_pv.shape} rows")
                # continue with merge logic
            else:
                logger.warning(f"Skipping pair ({sp_tag}, {pv_tag}) due to failed fetch (None returned)")
                continue

            if df_sp is not None and df_pv is not None and not df_sp.empty and not df_pv.empty:
                # 🔧 Rename BEFORE merging
                df_sp.rename(columns={"Value": f"SetPoint_{sp_tag}"}, inplace=True)
                df_pv.rename(columns={"Value": f"Actual_{pv_tag}"}, inplace=True)

                df = pd.merge(df_sp, df_pv, on="Timestamp", how="inner")
                logger.info(f"Merged {sp_tag} & {pv_tag} → {df.shape} rows")
                data_frames.append(df)
            else:
                logger.warning(f"Skipping {sp_tag} & {pv_tag} due to empty data.")

        if data_frames:
            df_combined = data_frames[0]
            for df in data_frames[1:]:
                df_combined = pd.merge(df_combined, df, on="Timestamp", how="inner")

            logger.info(f"Total merged shape before preprocessing: {df_combined.shape}")
            df_combined = df_combined.sort_values("Timestamp").interpolate().bfill().ffill()

            # Detect anomalies
            df_combined, anomaly_flags = detect_anomalies(df_combined)

            # Rolling buffer - keep only last BUFFER_HOURS of data
            cutoff_time = pd.Timestamp.now(tz="Europe/Stockholm") - timedelta(hours=BUFFER_HOURS)

            try:
                df_existing = pd.read_csv(OUTPUT_FILE, parse_dates=["Timestamp"])
                df_existing = df_existing[df_existing["Timestamp"] >= cutoff_time]
                df_combined = pd.concat([df_existing, df_combined], ignore_index=True)
                df_combined.drop_duplicates(subset=["Timestamp"], keep="last", inplace=True)
            except FileNotFoundError:
                logger.info("First time creating output file.")

            df_combined = df_combined[df_combined["Timestamp"] >= cutoff_time]
            df_combined.sort_values("Timestamp", inplace=True)

            logger.debug(f"anomalies_detected: {anomaly_flags}")
            # Trigger alerts
            if any(anomaly_flags.values()):
                logger.warning("Anomaly detected — triggering alert")
                alert()

             # Publish only the last row
            last_row = df_combined.iloc[[-1]]
            publish_anomaly_row(last_row)

            df_combined.to_csv(OUTPUT_FILE, index=False)
            logger.info(f"Data saved to {OUTPUT_FILE} with {len(df_combined)} rows")

        else:
            logger.warning("No valid dataframes to combine — skipping write")

        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    main()