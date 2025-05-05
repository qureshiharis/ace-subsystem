import streamlit as st
import pandas as pd
import altair as alt
import os
import glob
from datetime import datetime

st.set_page_config(layout="wide")
st.title("IoT Anomaly Monitoring Dashboard")

# Detect available subsystems dynamically based on CSV filenames
csv_files = glob.glob("*latest_*.csv")
subsystems = set(os.path.basename(f).split("_latest_")[0] for f in csv_files)

if not subsystems:
    st.warning("No data files found. Waiting for data...")
    st.stop()

selected_subsystem = st.sidebar.selectbox("Select Subsystem", sorted(subsystems))

# Filter files for selected subsystem
subsystem_files = [f for f in csv_files if f.startswith(f"{selected_subsystem}_latest_")]
st.markdown(f"### Subsystem: `{selected_subsystem}`")

# Display charts for each sensor in the selected subsystem
for file in sorted(subsystem_files):
    try:
        df = pd.read_csv(file, parse_dates=["Timestamp"])
    except Exception as e:
        st.error(f"Failed to read {file}: {e}")
        continue

    sensor_id = os.path.basename(file).split("_latest_")[1].replace(".csv", "")
    sp_col = next((col for col in df.columns if col.startswith("SetPoint_")), None)
    pv_col = next((col for col in df.columns if col.startswith("Actual_")), None)
    anom_col = next((col for col in df.columns if col.startswith("Anomaly_")), None)

    if not all([sp_col, pv_col, anom_col]):
        st.warning(f"Missing required columns in {file}")
        continue

    chart_df = df[["Timestamp", sp_col, pv_col, anom_col]].rename(
        columns={sp_col: "SetPoint", pv_col: "Actual", anom_col: "Anomaly"}
    )

    base = alt.Chart(chart_df).encode(x="Timestamp:T")

    lines = base.mark_line().encode(
        y=alt.Y("Actual:Q", title="Sensor Value"),
        color=alt.value("blue"),
        tooltip=["Timestamp", "Actual"]
    ) + base.mark_line(strokeDash=[4, 4]).encode(
        y="SetPoint",
        color=alt.value("green"),
        tooltip=["Timestamp", "SetPoint"]
    )

    anomalies = base.transform_filter("datum.Anomaly == true").mark_point(color="red", size=80).encode(
        y="Actual"
    )

    st.markdown(f"### Sensor: `{sensor_id}`")
    st.altair_chart(lines + anomalies, use_container_width=True)
    st.dataframe(chart_df.tail(5), use_container_width=True)

st.markdown("---")
st.caption("Dashboard auto-refreshes with MQTT every 5 minutes via separate push")
