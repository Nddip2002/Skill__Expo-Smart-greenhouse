# Smart Greenhouse - Agronomic Vision & SCADA Telemetry Engine

[![Python Version](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Fast Gemini Diagnostic Engine](https://img.shields.io/badge/Gemini_API-Multimodal_Pathology-green.svg)](https://ai.google.dev/)
[![Hardware](https://img.shields.io/badge/Hardware-GoPro_Hero_11_Mini%20%7C%20ESP32--C3-orange.svg)](#hardware-integration)
[![Architecture](https://img.shields.io/badge/Architecture-SCADA_Cockpit_1080p-purple.svg)](#architecture)

Enterprise-grade Greenhouse Capsicum Supervisory Control and Data Acquisition (SCADA) Cockpit with automated agronomic vision, pathological disease classification via Google Gemini API, and continuous microclimate telemetry streaming.

---

## Key Features

1. **Enterprise 1080p SCADA Dashboard (`camera_app.py`)**
   - High-performance web-based cockpit running at `http://localhost:8080`.
   - Real-time video preview with Heads-Up Display (HUD) and optical vegetation index masking (HSV plant isolation).
   - Live 4-channel telemetry time-series charts (Relative Humidity, Temperature, Soil Volumetric Moisture, and Light Lux).
   - Pathology surveillance feed with specimen inspect modal and agronomic intervention directives.

2. **Multimodal Pathology Diagnostic Bridge (`watcher.py`)**
   - Integrates Google Gemini API using structured JSON schema via Pydantic.
   - Intelligent domain validation: Discriminated detection of capsicum specimens vs. non-plant background/railings (`unrelated_images/`).
   - Dynamic API key rotation pool with graceful fallback and 15 RPM pacing to navigate free-tier quotas without dropouts.
   - Offline buffering: Automatically diverts captures to `pending_queue/` during quota limits and replays upon quota renewal.

3. **Open GoPro HERO 11 Mini Protocol (`gopro_usb_stream.py`)**
   - Direct USB-C CDC interface communication using the Open GoPro Protocol.
   - Low-latency MPEG-TS / UDP stream ingestion with automatic keepalive heartbeat.
   - High-resolution raw specimen acquisition and scheduled capture cadence (4 snapshots/hour).

4. **ESP32-C3 Super Mini Telemetry Engine (`esp32_reader.py`)**
   - Direct USB Serial ingestion (COM port @ 115200 baud).
   - Structured parsing for Soil Moisture (ADC), Temperature (°C), Relative Humidity (%), and Illuminance (Lux).
   - Seamless dual-mode fallback: Direct serial link or SQLite sync (`logs/greenhouse.db`).

---

## Directory Structure

```text
├── camera_app.py        # Central 1080p SCADA Dashboard & integrated server
├── esp32_reader.py      # ESP32-C3 serial telemetry parser and logger
├── gopro_usb_stream.py  # Standalone GoPro USB preview stream & snapshot controller
├── watcher.py           # Gemini vision bridge, multi-key pool & SQLite storage
├── test_simulator.py    # Automated test simulator for pipeline verification
├── requirements.txt     # Python dependency specifications
├── .env.example         # Environment template for Gemini API keys
├── incoming_images/     # Raw intake directory for surveillance captures
├── pending_queue/       # Buffer queue for offline/quota-delayed captures
├── processed_images/    # Verified capsicum pathology captures
├── unrelated_images/    # Filtered non-plant or railing captures
└── logs/                # SQLite database (greenhouse.db), CSV & text logs
```

---

## Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/Nddip2002/Skill__Expo-Smart-greenhouse.git
cd Skill__Expo-Smart-greenhouse
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Gemini API Keys
Copy the example environment file and add your Google Gemini API key(s):
```bash
cp .env.example .env
```
Edit `.env`:
```env
GEMINI_API_KEY=your_primary_gemini_api_key_here
GEMINI_API_KEY_2=your_backup_gemini_api_key_here
```

---

## Running the System

### Start the SCADA Cockpit
Launch the unified server (starts GoPro stream, telemetry reader, scheduler, and web UI):
```bash
python camera_app.py
```
Open your browser to: **`http://localhost:8080`**

### Test Pipeline Simulator
To verify the Gemini diagnostic pipeline, key pool rotation, and image sorting without camera hardware:
```bash
python test_simulator.py
```

### ESP32 Telemetry Monitor (Standalone)
```bash
python esp32_reader.py --port COM7
```

---

## Hardware Integration

- **GoPro HERO 11 Mini**: Connect via USB-C with MTP/GoPro Connect mode enabled.
- **ESP32-C3 Super Mini**: Connected via USB Serial (115200 baud) streaming sensor payloads.
