import os
import json
import threading
import pandas as pd
import altair as alt
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import paho.mqtt.client as mqtt

def process_payload(payload):
    """
    Processes the MQTT payload and returns a cleaned dictionary with flat key-value pairs.
    Supports payloads where values are nested dictionaries with numeric string keys.
    Example output:
    {
        'Timestamp': 1746542991230,
        'SetPoint_1473_04_AS01_VS01_GT101_CSP': 36.2,
        ...
    }
    """
    processed = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            # Attempt to extract the first (and only) value in the inner dict
            try:
                inner_value = next(iter(value.values()))
                processed[key] = inner_value
            except Exception:
                processed[key] = value
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
            except Exception as e:
                st.warning(f"Warning: Could not load {csv_file} ({e}). Starting fresh.")
                # If file is corrupted or unreadable, start with empty data

    # Message queue to hold incoming processed messages
    message_queue = []

    # Define MQTT callbacks
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT broker.")
            # Subscribe to both topics upon connecting
            client.subscribe([("anomalies/heating", 0), ("anomalies/ventilation", 0)])
        else:
            print(f"Failed to connect, return code {rc}")

    def on_message(client, userdata, msg):
        """Handle incoming MQTT messages for anomalies."""
        try:
            payload = json.loads(msg.payload.decode())
            print(f"[MQTT] Received message on {msg.topic}: {payload}")
            processed_payload = process_payload(payload)
            print(f"[MQTT] Processed payload: {processed_payload}")
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
    st.session_state.setdefault("heating_data", [])
    st.session_state.setdefault("ventilation_data", [])

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
        subsystem_msg = "heating" if topic.endswith("/heating") else "ventilation"
        timestamp_raw = data.get("Timestamp", "")
        timestamp = str(timestamp_raw) if timestamp_raw else pd.Timestamp.now().isoformat()
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
                            sensor_entry["Anomaly"] = bool(value) if isinstance(value, bool) else str(value).lower() == "true"
        print(f"[PARSE] Sensor entries parsed: {sensor_data_map}")

        new_entries = list(sensor_data_map.values())
        if not new_entries:
            continue  # no sensor data parsed

        target_data = st.session_state["heating_data"] if subsystem_msg == "heating" else st.session_state["ventilation_data"]
        csv_path = f"{subsystem_msg}.csv"
        file_exists = os.path.isfile(csv_path)

        for entry in new_entries:
            print(f"[WRITE] Writing entry to {csv_path}: {entry}")
            target_data.append(entry)
            print(f"[SESSION] Appended entry to {subsystem_msg}_data: {entry}")
            try:
                pd.DataFrame([entry]).to_csv(csv_path, mode='a', header=not file_exists, index=False)
                file_exists = True  # header written only once
            except Exception as e:
                print(f"Error writing entry to {csv_path}: {e}")

# Safely copy current data under lock for the selected subsystem
with data_lock:
    data_list = list(st.session_state["heating_data"] if subsystem == "heating" else st.session_state["ventilation_data"])

# If no data is available yet, inform the user
if not data_list:
    st.write(f"Waiting for data on **{subsystem}** subsystem...")
else:
    # Create dataframe from the data list
    df = pd.DataFrame(data_list)
    # Ensure Timestamp is datetime for proper plotting
    try:
        df["Timestamp"] = pd.to_datetime(pd.to_numeric(df["Timestamp"], errors="coerce"), unit="ms")
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

        anomaly_points = base.transform_filter(
            alt.datum.Anomaly == True
        ).mark_point(color="red", size=75).encode(
            tooltip=["Timestamp:T", "Value:Q", "Error:Q"]
        )

        chart = alt.layer(line_chart, anomaly_points)
        st.markdown(f"### 📡 Sensor: `{sensor_id}`")
        st.altair_chart(chart, use_container_width=True)