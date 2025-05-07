import os
import json
import threading
import pandas as pd
import altair as alt
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import paho.mqtt.client as mqtt

def process_payload(payload):

    processed = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            # Handle nested dicts; if multiple keys, flatten with composite keys
            if len(value) == 1:
                inner_value = list(value.values())[0]
                processed[key] = inner_value
            else:
                # Flatten keys with inner dict keys appended
                for inner_key, inner_val in value.items():
                    processed[f"{key}_{inner_key}"] = inner_val
        else:
            processed[key] = value
    return processed

# Global thread lock (use this instead of session_state for thread safety)
data_lock = threading.Lock()

# Configurable MQTT broker settings
MQTT_BROKER = "localhost"        # e.g., "test.mosquitto.org" or your broker address
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

# Set Streamlit page layout and title
st.set_page_config(page_title="Anomaly Dashboard", layout="wide")

st.title("Real-Time Anomaly Detection Dashboard")
st.markdown("""
Welcome to the **ACE Subsystem Monitoring Dashboard**.  
Here you can track sensor activity in real time for the **heating** and **ventilation** subsystems.  
Use the sidebar to switch between subsystems.
""")

# Initialize session state for MQTT and data (run only on first load)
if "mqtt_client" not in st.session_state:
    # Load existing CSV data if available (to avoid duplicating historical data)
    for subsystem in ["heating", "ventilation"]:
        csv_file = f"{subsystem}.csv"
        if os.path.exists(csv_file):
            try:
                df_old = pd.read_csv(csv_file)
                # Ensure expected columns exist and convert to list of dict entries
                if not df_old.empty:
                    # Sort by Timestamp to get the latest entry
                    df_old = df_old.sort_values("Timestamp")
                    # Extend the in-memory list with existing data
                    if subsystem == "heating":
                        st.session_state.setdefault("heating_data", []).extend(df_old.to_dict(orient="records"))
                    else:
                        st.session_state.setdefault("ventilation_data", []).extend(df_old.to_dict(orient="records"))
                    df_old_anomalies = df_old[df_old.get("Anomaly", False) == True]
                    if not df_old_anomalies.empty:
                        anomaly_list = df_old_anomalies.to_dict(orient="records")
                        st.session_state.setdefault(f"{subsystem}_anomalies", []).extend(anomaly_list)
            except Exception as e:
                st.warning(f"Warning: Could not load {csv_file} ({e}). Starting fresh.")
                # If file is corrupted or unreadable, start with empty data

    # Message queue to hold incoming processed messages
    message_queue = []

    # Define MQTT callbacks
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            # Subscribe to all anomaly topics
            client.subscribe("anomalies/#")
        else:
            print(f"Failed to connect, return code {rc}")

    def on_message(client, userdata, msg):
        """Handle incoming MQTT messages for anomalies."""
        try:
            payload = json.loads(msg.payload.decode())
            print(f"[MQTT] Received message on {msg.topic}: {payload}")
            processed_payload = process_payload(payload)
            print(f"[MQTT] Processed payload: {processed_payload}")
            # Ensure thread-safe access to the shared message queue
            with data_lock:
                message_queue.append((msg.topic, processed_payload))
        except Exception as e:
            print(f"Failed to decode or process message: {e}")
            return

    # Set up MQTT client and start background thread loop
    client = mqtt.Client(client_id="streamlit-dashboard")
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    except Exception as e:
        print(f"MQTT connection failed: {e}")
    # Start the network loop in a separate thread so it won't block the Streamlit app
    client.loop_start()

    st.session_state["mqtt_client"] = client
    st.session_state["message_queue"] = message_queue
    # Will be dynamically initialized when a topic is first received

# Sidebar for subsystem selection
subsystem = st.sidebar.selectbox("Select Subsystem", ["heating", "ventilation"], format_func=str.title)

st.sidebar.markdown("---")
st.sidebar.info("""
This dashboard displays live data from MQTT topics.

- **Heating & Ventilation**
- SetPoint vs Actual
- Anomaly markers

Developed for thesis monitoring in real-time.
""")

# Trigger automatic rerun every few seconds to fetch new data (avoids manual refresh or infinite loops)
st_autorefresh(interval=2000, key="auto_refresh")  # refresh every 2 seconds

# Process messages from the queue and update data accordingly
with data_lock:
    message_queue = st.session_state.get("message_queue", [])
    while message_queue:
        topic, data = message_queue.pop(0)
        subsystem_msg = topic.split("/")[-1]  # Dynamically extract subsystem name
        timestamp_raw = data.get("Timestamp", "")
        # Ensure timestamp is numeric (milliseconds since epoch)
        timestamp = int(timestamp_raw) if isinstance(timestamp_raw, (int, float, str)) and str(timestamp_raw).isdigit() else int(pd.Timestamp.now().timestamp() * 1000)
        try:
            time_only = pd.to_datetime(timestamp).strftime("%H:%M:%S")
        except Exception:
            time_only = timestamp  # fallback to raw if parsing fails

        # Group sensor readings by sensor_id
        sensor_data_map = {}
        for key, value in data.items():
            if "_" in key:
                parts = key.split("_")
                if len(parts) >= 3:
                    prefix = parts[0]
                    suffix = parts[-1]
                    sensor_id = "_".join(parts[1:-1])
                    if prefix in ["SetPoint", "Actual", "Error", "Anomaly"]:
                        sensor_entry = sensor_data_map.setdefault(sensor_id, {
                            "Timestamp": timestamp,
                            "TimeOnly": time_only,
                            "Sensor": sensor_id,
                            "SetPoint": None,
                            "Actual": None,
                            "Error": None,
                            "Anomaly": None
                        })
                        if prefix == "SetPoint":
                            sensor_entry["SetPoint"] = float(value) if value is not None else None
                        elif prefix == "Actual":
                            sensor_entry["Actual"] = float(value) if value is not None else None
                        elif prefix == "Error":
                            sensor_entry["Error"] = float(value) if value is not None else None
                        elif prefix == "Anomaly":
                            # Normalize anomaly value to strict boolean from various possible representations
                            if isinstance(value, bool):
                                sensor_entry["Anomaly"] = value
                            elif isinstance(value, (int, float)):
                                sensor_entry["Anomaly"] = bool(value)
                            elif isinstance(value, str):
                                sensor_entry["Anomaly"] = value.strip().lower() in ["true", "1", "yes"]
                            else:
                                sensor_entry["Anomaly"] = False
        print(f"[PARSE] Sensor entries parsed: {sensor_data_map}")

        new_entries = list(sensor_data_map.values())
        if not new_entries:
            continue  # no sensor data parsed

        if subsystem_msg + "_data" not in st.session_state:
            st.session_state[subsystem_msg + "_data"] = []

        target_data = st.session_state[subsystem_msg + "_data"]
        csv_path = f"{subsystem_msg}.csv"
        file_exists = os.path.isfile(csv_path)

        # Limit the number of entries in session state to prevent memory bloat
        # Buffer writes to reduce file I/O overhead
        buffer_df = pd.DataFrame(new_entries)
        try:
            buffer_df.to_csv(csv_path, mode='a', header=not file_exists, index=False)
        except Exception as e:
            print(f"Error writing buffered entries to {csv_path}: {e}")
        file_exists = True  # header written only once
        for entry in new_entries:
            print(f"[WRITE] Writing entry to {csv_path}: {entry}")
            target_data.append(entry)
            # Keep only the latest 1000 entries to limit memory usage
            if len(target_data) > 1000:
                del target_data[0:len(target_data) - 1000]
            print(f"[SESSION] Appended entry to {subsystem_msg}_data: {entry}")
        break

# Safely copy current data under lock for the selected subsystem
with data_lock:
    data_list = list(st.session_state.get(subsystem + "_data", []))

# If no data is available yet, inform the user
if not data_list:
    st.write(f"Waiting for data on **{subsystem}** subsystem...")
else:
    # Create dataframe from the data list
    df = pd.DataFrame(data_list)
    # Ensure Timestamp is datetime for proper plotting
    try:
        df["Timestamp"] = pd.to_datetime(pd.to_numeric(df["Timestamp"], errors="coerce"), unit="ms").dt.tz_localize("UTC").dt.tz_convert("Europe/Stockholm")
    except Exception:
        # If parsing fails (non-standard format), keep as string
        pass
    # Sort by time in case entries are out of order
    df = df.sort_values("Timestamp")
    # Identify unique sensor IDs in the data
    sensors = sorted(df["Sensor"].unique())

    for sensor_id in sensors:
        sensor_df = df[df["Sensor"] == sensor_id]
        sensor_df = sensor_df.sort_values("Timestamp").tail(10)
        latest_row = sensor_df.iloc[-1] if not sensor_df.empty else None
        if latest_row is not None:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Latest SetPoint", f"{latest_row['SetPoint']:.2f}")
            with col2:
                st.metric("Latest Actual", f"{latest_row['Actual']:.2f}")
            with col3:
                st.metric("Anomaly Detected", "Yes" if latest_row["Anomaly"] else "No")
        # Compute tight y-axis domain based on Actual and SetPoint
        y_values = pd.concat([sensor_df["Actual"], sensor_df["SetPoint"]]).dropna()
        y_min = y_values.min() - 2
        y_max = y_values.max() + 2
        y_scale = alt.Scale(domain=[y_min, y_max])
        # Melt sensor_df for unified chart legend and encoding
        sensor_df_melted = sensor_df.melt(
            id_vars=["Timestamp", "Sensor", "Error", "Anomaly"],
            value_vars=["Actual", "SetPoint"],
            var_name="Type",
            value_name="Value"
        )
        base = alt.Chart(sensor_df_melted).encode(
            x=alt.X("Timestamp:T", title="Time", axis=alt.Axis(format="%H:%M:%S")),
            y=alt.Y("Value:Q", title="Value", scale=y_scale),
            color=alt.Color("Type:N", title="Measurement"),
            strokeDash=alt.StrokeDash("Type:N", title="Measurement")
        ).properties(
            width=800
        ).interactive(bind_y=False)

        line_chart = base.mark_line().encode(
            tooltip=["Timestamp:T", "Value:Q", "Type:N", "Error:Q", "Anomaly:O"]
        )

        points = base.mark_point(size=40).encode(
            tooltip=["Timestamp:T", "Value:Q", "Type:N", "Error:Q", "Anomaly:O"]
        )

        anomaly_points = base.transform_filter(
            alt.datum.Anomaly == True
        ).mark_point(color="red", size=75).encode(
            tooltip=["Timestamp:T", "Value:Q", "Error:Q"]
        )

        chart = alt.layer(line_chart, points, anomaly_points)
        st.markdown(f"### 📡 Sensor: `{sensor_id}`")
        st.altair_chart(chart, use_container_width=True)

    st.markdown("## 🔍 Historical Anomalies")
    anomaly_key = f"{subsystem}_anomalies"
    historical_anomalies = st.session_state.get(anomaly_key, [])
    if historical_anomalies:
        df_anomaly = pd.DataFrame(historical_anomalies)
        df_anomaly = df_anomaly.sort_values("Timestamp", ascending=False)
        try:
            df_anomaly["Timestamp"] = pd.to_datetime(pd.to_numeric(df_anomaly["Timestamp"], errors="coerce"), unit="ms").dt.tz_localize("UTC").dt.tz_convert("Europe/Stockholm")
        except Exception:
            pass
        st.dataframe(df_anomaly[["Timestamp", "Sensor", "SetPoint", "Actual", "Error"]].head(20), use_container_width=True)
    else:
        st.write("No historical anomalies recorded yet.")