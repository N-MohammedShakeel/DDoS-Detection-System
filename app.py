# app.py
import streamlit as st
import pandas as pd
from database import init_db, get_recent_logs
from visualization import (
    plot_request_rates, plot_top_ips, plot_prediction_pie
)

st.set_page_config(page_title="DDoS Detection Dashboard", layout="wide")

# Initialize database
init_db()

# --- MONITORING THREAD LOGIC HAS BEEN REMOVED ---
# The monitoring script (monitoring.py) must be run as a separate process.

st.title("DDoS Detection Dashboard")
st.write("Monitoring HTTP requests using a custom-trained model.")

# DDoS Attack Indicator
st.header("DDoS Status")
logs = get_recent_logs(limit=1000)
if logs.empty:
    st.warning("⚠️ No data available. Ensure the Flask server and monitoring script are running.")
elif logs['prediction'].eq(1).any():
    st.error("🚨 DDoS Attack Detected!", icon="🚨")
else:
    st.success("✅ No DDoS Attack Detected", icon="✅")

# Refresh button
if st.button("Refresh Dashboard"):
    st.rerun()

# Visualizations
if not logs.empty:
    st.header("Request Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Request Rates Over Time")
        fig_rates = plot_request_rates(logs)
        st.plotly_chart(fig_rates, use_container_width=True)
    with col2:
        st.subheader("Top IPs by Request Count")
        fig_ips = plot_top_ips(logs)
        st.plotly_chart(fig_ips, use_container_width=True)

    st.header("Prediction Breakdown")
    fig_pie = plot_prediction_pie(logs)
    st.plotly_chart(fig_pie, use_container_width=True)

st.header("Recent Logs")
recent_logs_display = get_recent_logs(limit=50)
if recent_logs_display.empty:
    st.warning("No recent logs to display.")
else:
    st.dataframe(recent_logs_display)

# Instructions
st.sidebar.header("Instructions")
st.sidebar.markdown("""
1.  **Terminal 1:** Run the server:  
    `python flask_server.py`
2.  **Terminal 2:** Run the monitor:  
    `python monitoring.py`
3.  **This App (Terminal 3):** View the dashboard.
4.  **Terminal 4:** Simulate an attack:  
    `python ddos_test.py`
""")