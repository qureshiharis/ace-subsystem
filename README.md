# ACE CyberSafe Subsystem – Raspberry Pi + Avassa Deployment

This repository contains the source code and instructions to deploy a real-time anomaly detection subsystem for smart building environments using **Raspberry Pi**, **Docker**, and **Avassa Edge Orchestrator**. The system supports multiple subsystems (e.g., heating, ventilation) running in parallel on one or more Raspberry Pi devices.

👉 **Docker Image Available**:  
https://hub.docker.com/r/topnot/ace-subsystem

---

## 🛠️ Project Overview

This project allows each Raspberry Pi to:

- Collect sensor data via WebPort API.
- Detect anomalies in real-time using buffered analysis.
- Publish the anomaly information over MQTT.
- Visualize the data via a Streamlit dashboard.

---

## 🔧 Prerequisites

- Raspberry Pi with Raspberry Pi OS installed.
- SSH access to the Pi.
- Docker and Avassa Edge Enforcer installed on the Pi.
- Avassa Control Tower access.
- Code pushed to a container registry (already available at DockerHub).

---

## 📦 Steps to Deploy on a New Raspberry Pi (as a Subsystem)

### 1. Install Docker and Avassa Edge Enforcer on Raspberry Pi

SSH into your Pi and run:

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# (Optional) Add your user to the docker group
sudo usermod -aG docker $USER
```

Then onboard the Pi to your Avassa site using the **Edge Enforcer onboarding command** from Control Tower.

> Make sure to give the host a unique **host ID** and **hostname** to avoid replacement of previous hosts.

---

### 2. Use the Prebuilt Docker Image

The Docker image is already available:

```bash
docker pull topnot/ace-subsystem:v1.0
```

> No need to build locally unless you modify the code.

---

### 3. Create a Site and Application in Avassa

- Create or reuse a site in Control Tower.
- Add the new Raspberry Pi as a **new host** under the same site.
- Create a new **Application** (e.g., `ace-ventilation` or `ace-heating`).

---

### 4. Prepare Your Avassa Application YAML

Here’s a sample application YAML configuration:

```yaml
name: ace-subsystem
services:
  ventilation-subsystem-1:
    image: topnot/ace-subsystem:v1.0
    restart:
      condition: always
    configuration:
      environment:
        MQTT_BROKER: ""  # IP of MQTT broker
        MQTT_TOPIC: "anomalies/ventilation"
        TAG_PAIRS: ""
        API_KEY: "your_api_key_here"
        USE_GPIO: "false"
    access-control:
      outbound-access:
        allow-all: true
deployment:
  placement:
    constraints:
      hostname: your-new-pi-hostname
```

---

### 5. Deploy the Application

- Upload the YAML in the application section of Avassa.
- Deploy it to your new Pi host.
- Watch the logs to verify that the subsystem is running correctly.

---

## 🧪 Testing the MQTT Output

You can test your MQTT output locally:

```bash
mosquitto_sub -h localhost -p 1883 -t 'anomalies/#' -v
```

Make sure the topic matches what's configured in `MQTT_TOPIC`.

---

## ✅ Troubleshooting

- If MQTT fails to connect, the application will continue without crashing.
- If Avassa replaces your previous host, ensure you set a **unique host ID and name** during onboarding.
- If WebPort API calls fail, check that the Raspberry Pi is connected to a working network (e.g., your university Wi-Fi may block some outbound traffic).