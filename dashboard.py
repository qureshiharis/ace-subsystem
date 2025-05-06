import streamlit as st
import pandas as pd
import altair as alt
import os
import glob
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import threading
import time

st.set_page_config(layout="wide")
st.title("IoT Anomaly Monitoring Dashboard")

# Detect available subsystems dynamically based on CSV filenames
csv_files = glob.glob("*latest_*.csv")
subsystems = set(os.path.basename(f).split("_latest_")[0] for f in csv_files)

if not subsystems:
    st.warning("No data files found. Waiting for data...")
    st.stop()

selected_subsystem = st.sidebar.selectbox("Select Subsystem", sorted(subsystems, key=str.lower), format_func=lambda x: x.capitalize())

# Filter files for selected subsystem
subsystem_files = [f for f in csv_files if f.startswith(f"{selected_subsystem}_latest_")]
st.markdown(f"### Subsystem: `{selected_subsystem.capitalize()}`")

live_data = {}

def parse_mqtt_payload(payload):
    try:
        data = json.loads(payload)
        index = next(iter(data["Timestamp"]))
        timestamp = pd.to_datetime(data["Timestamp"][index], unit="ms")

        parsed_rows = []
        for key in data:
            if key == "Timestamp":
                continue
            sensor_prefix = "_".join(key.split("_")[1:-1])
            if sensor_prefix not in live_data:
                live_data[sensor_prefix] = []

            row = {
                "Timestamp": timestamp,
                "TimeOnly": timestamp.strftime("%H:%M:%S"),
            }

            for suffix in ["SetPoint", "Actual", "Error", "Anomaly"]:
                column = f"{suffix}_{sensor_prefix}_CSP"
                if column in data:
                    row[suffix] = data[column][index]

            live_data[sensor_prefix].append(row)
            parsed_rows.append((sensor_prefix, row))

        return parsed_rows
    except Exception as e:
        print(f"Failed to parse MQTT payload: {e}")
        return []

# Display charts for each sensor in the selected subsystem
for file in sorted(subsystem_files):
    try:
        df = pd.read_csv(file, parse_dates=["Timestamp"])
    except Exception as e:
        st.error(f"Failed to read {file}: {e}")
        continue

    sensor_id = os.path.basename(file).split("_latest_")[1].replace(".csv", "").removesuffix("_CSP")
    sensor_prefix = sensor_id

    sp_col = next((col for col in df.columns if col.startswith("SetPoint_")), None)
    pv_col = next((col for col in df.columns if col.startswith("Actual_")), None)
    anom_col = next((col for col in df.columns if col.startswith("Anomaly_")), None)

    if not all([sp_col, pv_col, anom_col]):
        st.warning(f"Missing required columns in {file}")
        continue

    chart_df = df[["Timestamp", sp_col, pv_col, anom_col]].rename(
        columns={sp_col: "SetPoint", pv_col: "Actual", anom_col: "Anomaly"}
    )

    chart_df["TimeOnly"] = chart_df["Timestamp"].dt.strftime("%H:%M:%S")

    if sensor_prefix in live_data:
        live_df = pd.DataFrame(live_data[sensor_prefix])
        chart_df = pd.concat([chart_df, live_df], ignore_index=True).drop_duplicates(subset=["Timestamp"]).sort_values("Timestamp")

    min_val = chart_df[["Actual", "SetPoint"]].min().min()
    max_val = chart_df[["Actual", "SetPoint"]].max().max()
    y_min = min_val - 2
    y_max = max_val + 2

    base = alt.Chart(chart_df).encode(x="Timestamp:T")
    points_actual = base.mark_circle(size=60, color="blue").encode(y="Actual:Q", tooltip=["TimeOnly", "Actual"])
    points_setpoint = base.mark_circle(size=60, color="green").encode(y="SetPoint:Q", tooltip=["TimeOnly", "SetPoint"])

    lines = base.mark_line().encode(
        y=alt.Y("Actual:Q", title="Sensor Value", scale=alt.Scale(domain=[y_min, y_max])),
        color=alt.value("blue"),
        tooltip=["TimeOnly", "Actual"]
    ) + base.mark_line(strokeDash=[4, 4]).encode(
        y=alt.Y("SetPoint:Q", scale=alt.Scale(domain=[y_min, y_max])),
        color=alt.value("green"),
        tooltip=["TimeOnly", "SetPoint"]
    )

    anomalies = base.transform_filter("datum.Anomaly == true").mark_point(size=80).encode(
        y="Actual",
        color=alt.value("red"),
        tooltip=["TimeOnly", "Actual"]
    )

    st.markdown(f"### Sensor: `{sensor_id}`")
    st.altair_chart(lines + anomalies + points_actual + points_setpoint, use_container_width=True)
    st.dataframe(chart_df.tail(5), use_container_width=True)

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe("anomalies/#")

def on_message(client, userdata, msg):
    topic = msg.topic.split("/")[-1]
    print(f"MQTT message received on topic {msg.topic}")
    new_rows = parse_mqtt_payload(msg.payload.decode())
    for sensor_prefix, row in new_rows:
        print(f"Received data for {sensor_prefix}: {row}")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect("localhost", 1883, 60)

def mqtt_thread():
    mqtt_client.loop_forever()

threading.Thread(target=mqtt_thread, daemon=True).start()

st.markdown("---")
st.caption("Dashboard auto-refreshes with MQTT every 5 minutes via separate push")