"""
Greenhouse Capsicum Overseer - Enterprise Agronomic Vision & Sensor Analytics Engine.
=====================================================================================
Professional 1080p (1920x1080) Supervisory Control and Data Acquisition (SCADA) Cockpit.
Integrated with GoPro HERO 11 Mini (Open GoPro USB-C Protocol) and ESP32-C3
Super Mini Telemetry Node (COM7 @ 115200 baud).

Architecture:
- View 1: Vision Lab & HSV Optical Calibration Suite
- View 2: Executive SCADA 1080p Operations Cockpit
    [Top Left]     Live Video Stream with HUD & Optical Vegetation Index Masking
    [Top Right]    Surveillance Intake & Pathological Diagnosis Table with Remediation Protocols
    [Bottom 1]     Atmospheric Relative Humidity Dynamics (%)
    [Bottom 2]     Ambient Dry-Bulb Temperature Gradient (°C)
    [Bottom 3]     Substrate Volumetric Moisture Retention (ADC & Ratio)
    [Bottom 4]     Photosynthetically Active Radiation / Illuminance (Lux)
"""

import base64
import json
import mimetypes
import os
import queue
import re
import shutil
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import cv2
import psutil

try:
    import serial
    import serial.tools.list_ports
    HAS_PYSERIAL = True
except ImportError:
    HAS_PYSERIAL = False

# Import Gemini Pathology Diagnostic & Storage Services
from watcher import GeminiBridge, CapsicumDiagnosis, db, validate_image, get_unique_path

# Reconfigure terminal encoding for Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Paths & Network Config
BASE_DIR = Path(__file__).resolve().parent
INCOMING_DIR = BASE_DIR / "incoming_images"
PROCESSED_DIR = BASE_DIR / "processed_images"
UNRELATED_DIR = BASE_DIR / "unrelated_images"
PENDING_DIR = BASE_DIR / "pending_queue"
LOGS_DIR = BASE_DIR / "logs"

for p in [INCOMING_DIR, PROCESSED_DIR, UNRELATED_DIR, PENDING_DIR, LOGS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

DB_PATH = LOGS_DIR / "greenhouse.db"

PORT = 8080
UDP_PORT = 8554
KEEPALIVE_INTERVAL = 2.0


# ============================================================================
# 1. GOPRO HERO 11 MINI USB DRIVER (OPEN GOPRO PROTOCOL)
# ============================================================================
class GoProUSBManager:
    """Manages USB Open GoPro connection, keepalive heartbeat, and MPEG-TS capture."""

    def __init__(self):
        self.camera_ip = None
        self.is_connected = False
        self.is_streaming = False
        self.battery_level = "--%"
        self.current_fps = 0.0
        self.frame_width = 0
        self.frame_height = 0
        self.latest_frame = None
        self.lock = threading.Lock()

        # Thread controls
        self.stop_signal = threading.Event()
        self.keepalive_thread = None
        self.capture_thread = None

        self.detect_camera_ip()

    def detect_camera_ip(self):
        """Scans active network adapters for USB CDC network interface (172.2X.YYY.51)."""
        print("[GoPro Driver] Scanning network interfaces for USB-C connection...", flush=True)
        found_ip = None

        try:
            for iface_name, addrs in psutil.net_if_addrs().items():
                for addr in addrs:
                    if addr.family == socket.AF_INET:
                        ip = addr.address
                        if re.match(r"^172\.(2[0-9])\.\d+\.\d+$", ip):
                            parts = ip.split(".")
                            candidate_ip = f"{parts[0]}.{parts[1]}.{parts[2]}.51"
                            print(f"[GoPro Driver] Probing USB adapter '{iface_name}' at {candidate_ip}...", flush=True)
                            if self.ping_gopro(candidate_ip):
                                found_ip = candidate_ip
                                break
                if found_ip:
                    break
        except Exception as e:
            print(f"[GoPro Driver] Network interface enumeration error: {e}", flush=True)

        if not found_ip:
            candidate = "172.23.171.51"
            if self.ping_gopro(candidate):
                found_ip = candidate

        if found_ip:
            self.camera_ip = found_ip
            self.is_connected = True
            print(f"[GoPro Driver] Camera online and verified at: http://{self.camera_ip}:8080", flush=True)
            self.query_camera_status()
        else:
            print("[GoPro Driver] Camera not detected. Connect via USB-C and power on.", flush=True)
            self.is_connected = False

        return self.camera_ip

    def ping_gopro(self, ip):
        """Dispatches validation probe to GoPro API endpoint."""
        url = f"http://{ip}:8080/gopro/version"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "GreenhouseOverseer/2.0"})
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                return resp.status == 200
        except Exception:
            return False

    def query_camera_status(self):
        """Queries battery charge state and system telemetry."""
        if not self.camera_ip:
            return
        try:
            url = f"http://{self.camera_ip}:8080/gopro/camera/state"
            req = urllib.request.Request(url, headers={"User-Agent": "GreenhouseOverseer/2.0"})
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                status = data.get("status", {})
                if "70" in status:
                    self.battery_level = f"{status['70']}%"
                elif 70 in status:
                    self.battery_level = f"{status[70]}%"
        except Exception:
            pass

    def start_stream(self):
        """Commands camera to start UDP MPEG-TS stream and begins frame ingestion."""
        if self.is_streaming:
            return True

        if not self.camera_ip:
            self.detect_camera_ip()
            if not self.camera_ip:
                print("[GoPro Driver] Cannot initiate stream: Camera IP unresolved.", flush=True)
                return False

        # Reset any lingering stream session
        try:
            stop_url = f"http://{self.camera_ip}:8080/gopro/camera/stream/stop"
            req = urllib.request.Request(stop_url, headers={"User-Agent": "GreenhouseOverseer/2.0"})
            urllib.request.urlopen(req, timeout=1.5)
            time.sleep(0.3)
        except Exception:
            pass

        print(f"[GoPro Driver] Starting stream from http://{self.camera_ip}:8080/gopro/camera/stream/start ...", flush=True)
        try:
            url = f"http://{self.camera_ip}:8080/gopro/camera/stream/start"
            req = urllib.request.Request(url, headers={"User-Agent": "GreenhouseOverseer/2.0"})
            with urllib.request.urlopen(req, timeout=4.0) as resp:
                pass
        except urllib.error.HTTPError as e:
            if e.code == 409:
                print("[GoPro Driver] Stream already initialized on device (HTTP 409). Attaching receiver.", flush=True)
            else:
                print(f"[GoPro Driver] Stream initialization error: {e}", flush=True)
                return False
        except Exception as e:
            print(f"[GoPro Driver] Communication exception: {e}", flush=True)
            return False

        self.stop_signal.clear()
        self.is_streaming = True

        self.keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self.keepalive_thread.start()

        self.capture_thread = threading.Thread(target=self._capture_loop, daemon=True)
        self.capture_thread.start()

        print("[GoPro Driver] Live UDP video ingestion active.", flush=True)
        return True

    def _keepalive_loop(self):
        """Sends periodic UDP keepalive packets to port 8554 to prevent socket timeout."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = b"_GPHD_:0:0:2:0.000000\n"

        while not self.stop_signal.is_set():
            try:
                if self.camera_ip:
                    sock.sendto(msg, (self.camera_ip, UDP_PORT))
            except Exception as e:
                print(f"[GoPro Driver] Keepalive transmission error: {e}", flush=True)
            time.sleep(KEEPALIVE_INTERVAL)
        sock.close()

    def _capture_loop(self):
        """Decodes raw MPEG-TS frames via OpenCV VideoCapture."""
        udp_url = f"udp://0.0.0.0:{UDP_PORT}?overrun_nonfatal=1&fifo_size=50000000"
        print(f"[GoPro Driver] Binding frame receiver to {udp_url} ...", flush=True)

        cap = cv2.VideoCapture(udp_url, cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

        fps_timer = time.time()
        fps_frames = 0

        while not self.stop_signal.is_set():
            ret, frame = cap.read()
            if not ret or frame is None:
                time.sleep(0.01)
                continue

            h, w = frame.shape[:2]
            with self.lock:
                self.latest_frame = frame
                self.frame_width = w
                self.frame_height = h

            fps_frames += 1
            now = time.time()
            if now - fps_timer >= 1.0:
                self.current_fps = round(fps_frames / (now - fps_timer), 1)
                fps_frames = 0
                fps_timer = now

        cap.release()
        print("[GoPro Driver] Frame capture worker terminated.", flush=True)

    def stop_stream(self):
        """Terminates UDP stream and shuts down capture thread."""
        self.stop_signal.set()
        self.is_streaming = False

        if self.camera_ip:
            try:
                url = f"http://{self.camera_ip}:8080/gopro/camera/stream/stop"
                req = urllib.request.Request(url, headers={"User-Agent": "GreenhouseOverseer/2.0"})
                urllib.request.urlopen(req, timeout=2.0)
                print("[GoPro Driver] Dispatched stream stop directive.", flush=True)
            except Exception:
                pass

    def get_latest_jpeg(self, quality=85):
        """Encodes pristine frame into high-density JPEG byte array."""
        with self.lock:
            if self.latest_frame is None:
                return None
            frame = self.latest_frame.copy()

        ret, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
        if ret:
            return buffer.tobytes()
        return None

    def get_raw_frame(self):
        """Returns pristine numpy frame copy."""
        with self.lock:
            if self.latest_frame is not None:
                return self.latest_frame.copy()
        return None


gopro = GoProUSBManager()


# ============================================================================
# 2. ESP32-C3 SERIAL TELEMETRY & DATABASE INTEGRATION ENGINE
# ============================================================================
class SensorTelemetryManager:
    """
    Ingests greenhouse microclimate sensor telemetry from ESP32-C3 Super Mini.
    Supports native serial monitoring on COM7 @ 115200 baud, with automated
    failover to SQLite database synchronization when external readers are active.
    """

    RE_HEADER = re.compile(r"Received from MAC:\s*([0-9A-Fa-f:]+)\s*\[Sender ID:\s*(\d+)\]")
    RE_TEMP = re.compile(r"Temp:\s*([\d\.\-]+)")
    RE_HUMIDITY = re.compile(r"Humidity:\s*([\d\.\-]+)")
    RE_SOIL = re.compile(r"Soil ADC:\s*(\d+)")
    RE_LIGHT = re.compile(r"Light:\s*([\d\.\-]+)")

    def __init__(self, port="COM7", baudrate=115200, db_path=DB_PATH):
        self.port = port
        self.baudrate = baudrate
        self.db_path = db_path
        self.lock = threading.Lock()
        self.history = []  # Ring buffer of recent telemetry packets
        self.latest = None
        self.connection_status = "Scanning"
        self.stop_signal = threading.Event()
        self.thread = None

        self._init_db()
        self._load_recent_from_db()

    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sensor_telemetry (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        mac_address TEXT,
                        sender_id INTEGER,
                        temperature_c REAL,
                        humidity_pct REAL,
                        soil_adc INTEGER,
                        light_lux REAL,
                        raw_payload TEXT
                    );
                """)
                conn.commit()
        except Exception as e:
            print(f"[Telemetry DB] Initialization warning: {e}", flush=True)

    def _load_recent_from_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT timestamp, mac_address, sender_id, temperature_c, humidity_pct, soil_adc, light_lux
                    FROM sensor_telemetry
                    ORDER BY id DESC LIMIT 50
                """)
                rows = cursor.fetchall()
                if rows:
                    for r in reversed(rows):
                        reading = {
                            "timestamp": r[0],
                            "mac": r[1] or "30:ED:A0:65:82:20",
                            "sender_id": r[2] or 1,
                            "temperature": r[3],
                            "humidity": r[4],
                            "soil_adc": r[5],
                            "soil_pct": round(max(0, min(100, (4095 - (r[5] or 2500)) / 25.0)), 1),
                            "light_lux": r[6]
                        }
                        self.history.append(reading)
                    self.latest = self.history[-1]
                    self.connection_status = "Database Sync (Active)"
        except Exception as e:
            print(f"[Telemetry DB] Preload warning: {e}", flush=True)

    def start(self):
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()

    def _worker_loop(self):
        print(f"[Telemetry Engine] Binding sensor listener to {self.port} / SQLite...", flush=True)
        serial_conn = None

        while not self.stop_signal.is_set():
            if HAS_PYSERIAL and serial_conn is None:
                try:
                    serial_conn = serial.Serial(self.port, self.baudrate, timeout=1.0)
                    serial_conn.dtr = True
                    serial_conn.rts = True
                    time.sleep(0.3)
                    serial_conn.reset_input_buffer()
                    self.connection_status = f"Serial Link ({self.port})"
                    print(f"[Telemetry Engine] Connected directly to {self.port} @ {self.baudrate} baud", flush=True)
                except Exception:
                    serial_conn = None
                    self.connection_status = "Database Sync (Reader Active)"

            if serial_conn and serial_conn.is_open:
                try:
                    lines = []
                    start_t = time.time()
                    while time.time() - start_t < 3.0:
                        raw = serial_conn.readline()
                        if raw:
                            clean = raw.decode("utf-8", errors="replace").strip("\r\n")
                            if clean.startswith("----") and lines:
                                break
                            if clean and not clean.startswith("----"):
                                lines.append(clean)
                    
                    if lines:
                        parsed = self._parse_block(lines)
                        if parsed:
                            self._record_reading(parsed, save_to_db=True)
                    time.sleep(0.1)
                except Exception as e:
                    print(f"[Telemetry Engine] Serial error: {e}", flush=True)
                    try:
                        serial_conn.close()
                    except Exception:
                        pass
                    serial_conn = None
                    self.connection_status = "Database Sync"
            else:
                try:
                    with sqlite3.connect(self.db_path) as conn:
                        cursor = conn.execute("""
                            SELECT timestamp, mac_address, sender_id, temperature_c, humidity_pct, soil_adc, light_lux
                            FROM sensor_telemetry
                            ORDER BY id DESC LIMIT 1
                        """)
                        r = cursor.fetchone()
                        if r:
                            ts = r[0]
                            if self.latest is None or self.latest.get("timestamp") != ts:
                                reading = {
                                    "timestamp": ts,
                                    "mac": r[1] or "30:ED:A0:65:82:20",
                                    "sender_id": r[2] or 1,
                                    "temperature": r[3],
                                    "humidity": r[4],
                                    "soil_adc": r[5],
                                    "soil_pct": round(max(0, min(100, (4095 - (r[5] or 2500)) / 25.0)), 1),
                                    "light_lux": r[6]
                                }
                                self._record_reading(reading, save_to_db=False)
                except Exception:
                    pass

                time.sleep(1.5)

        if serial_conn and serial_conn.is_open:
            try:
                serial_conn.close()
            except Exception:
                pass

    def _parse_block(self, lines):
        mac = "30:ED:A0:65:82:20"
        sender_id = 1
        temp = None
        hum = None
        soil = None
        light = None

        found = False
        for l in lines:
            m = self.RE_HEADER.search(l)
            if m:
                mac = m.group(1)
                sender_id = int(m.group(2))
                found = True
            m = self.RE_TEMP.search(l)
            if m:
                temp = float(m.group(1))
                found = True
            m = self.RE_HUMIDITY.search(l)
            if m:
                hum = float(m.group(1))
                found = True
            m = self.RE_SOIL.search(l)
            if m:
                soil = int(m.group(1))
                found = True
            m = self.RE_LIGHT.search(l)
            if m:
                light = float(m.group(1))
                found = True

        if found:
            ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
            soil_pct = round(max(0, min(100, (4095 - (soil or 2500)) / 25.0)), 1)
            return {
                "timestamp": ts,
                "mac": mac,
                "sender_id": sender_id,
                "temperature": temp,
                "humidity": hum,
                "soil_adc": soil,
                "soil_pct": soil_pct,
                "light_lux": light,
                "raw": "\n".join(lines)
            }
        return None

    def _record_reading(self, reading, save_to_db=True):
        with self.lock:
            self.latest = reading
            self.history.append(reading)
            if len(self.history) > 60:
                self.history.pop(0)

        if save_to_db:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        INSERT INTO sensor_telemetry (
                            timestamp, mac_address, sender_id,
                            temperature_c, humidity_pct, soil_adc, light_lux, raw_payload
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        reading["timestamp"], reading["mac"], reading["sender_id"],
                        reading["temperature"], reading["humidity"],
                        reading["soil_adc"], reading["light_lux"], reading.get("raw", "")
                    ))
                    conn.commit()
            except Exception:
                pass

    def get_telemetry(self):
        with self.lock:
            if not self.history:
                now_str = datetime.now().strftime("%H:%M:%S")
                mock = {
                    "timestamp": now_str,
                    "mac": "30:ED:A0:65:82:20",
                    "sender_id": 1,
                    "temperature": 24.5,
                    "humidity": 60.0,
                    "soil_adc": 2400,
                    "soil_pct": 58.0,
                    "light_lux": 850.0
                }
                return {
                    "status": "ok",
                    "connection": self.connection_status,
                    "latest": mock,
                    "history": [mock]
                }

            return {
                "status": "ok",
                "connection": self.connection_status,
                "latest": self.latest,
                "history": list(self.history)
            }


sensor_mgr = SensorTelemetryManager()


# ============================================================================
# 3. AI DIAGNOSTIC ENGINE, SPECIMEN PIPELINE QUEUE & 4 SNAPS/HR SCHEDULER
# ============================================================================
ai_bridge = GeminiBridge()


class ApiAvailabilityChecker:
    """Monitors Gemini AI API health, response latency, and key quota state."""

    def __init__(self, bridge: GeminiBridge):
        self.bridge = bridge
        self.is_available = True
        self.status = "ONLINE"
        self.last_checked_iso = None
        self.latency_ms = 0.0
        self.active_key_masked = "--"
        self.lock = threading.Lock()
        self.stop_signal = threading.Event()
        self._thread = None

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self._thread.start()

    def _monitoring_loop(self):
        self.check_now()
        while not self.stop_signal.is_set():
            # Check every 5 min (300s) when healthy; poll every 90s if quota exhausted to detect recovery (0 token cost via models.get)
            interval = 90.0 if not self.is_available else 300.0
            self.stop_signal.wait(interval)
            if self.stop_signal.is_set():
                break
            self.check_now()

    def check_now(self) -> bool:
        from google import genai
        from google.genai import types
        from watcher import PRIMARY_MODEL

        active_key = self.bridge.key_pool.get_active_key()
        if not active_key:
            # If all keys were exhausted, attempt a reset to see if daily/hourly quota renewed
            self.bridge.key_pool.reset_exhaustion()
            active_key = self.bridge.key_pool.get_active_key()

        if not active_key:
            with self.lock:
                self.is_available = False
                self.status = "QUOTA_EXHAUSTED"
                self.active_key_masked = "NONE"
                self.last_checked_iso = datetime.now().strftime("%H:%M:%S")
            return False

        masked = active_key[:7] + "..." + active_key[-4:]
        t0 = time.time()
        try:
            client = genai.Client(api_key=active_key, http_options=types.HttpOptions(timeout=10000))
            # Zero-token model metadata probe (does NOT burn generation tokens)
            client.models.get(model=PRIMARY_MODEL)
            latency = (time.time() - t0) * 1000.0
            with self.lock:
                self.is_available = True
                self.status = "ONLINE"
                self.latency_ms = round(latency, 1)
                self.active_key_masked = masked
                self.last_checked_iso = datetime.now().strftime("%H:%M:%S")
            return True
        except Exception as e:
            err_str = str(e)
            with self.lock:
                self.last_checked_iso = datetime.now().strftime("%H:%M:%S")
                if self.bridge._is_quota_error(err_str):
                    next_key = self.bridge.key_pool.mark_exhausted(active_key, err_str)
                    if next_key:
                        self.status = "KEY_ROTATED"
                        self.is_available = True
                    else:
                        self.status = "QUOTA_EXHAUSTED"
                        self.is_available = False
                else:
                    self.status = "OFFLINE"
                    self.is_available = False
            return self.is_available

    def get_status_dict(self):
        with self.lock:
            avail_keys = len([k for k in self.bridge.key_pool.keys if k not in self.bridge.key_pool.exhausted_keys])
            return {
                "status": self.status,
                "is_available": self.is_available,
                "latency_ms": self.latency_ms,
                "active_key": self.active_key_masked,
                "keys_available": avail_keys,
                "total_keys": len(self.bridge.key_pool.keys),
                "last_checked": self.last_checked_iso or "--:--:--"
            }


class SpecimenQueueManager:
    """Asynchronous specimen processing queue with API availability gating."""

    def __init__(self, bridge: GeminiBridge, api_checker: ApiAvailabilityChecker):
        self.bridge = bridge
        self.api_checker = api_checker
        self.q = queue.Queue()
        self.stop_signal = threading.Event()
        self.processed_count = 0
        self.lock = threading.Lock()
        self._worker_thread = None

    def start(self):
        if self._worker_thread is None:
            self._enqueue_existing_files()
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self._worker_thread.start()

    def _enqueue_existing_files(self):
        for p in sorted(INCOMING_DIR.glob("*.jp*g")) + sorted(INCOMING_DIR.glob("*.png")):
            self.enqueue(p)
        for p in sorted(PENDING_DIR.glob("*.jp*g")) + sorted(PENDING_DIR.glob("*.png")):
            self.enqueue(p, is_pending=True)

    def enqueue(self, filepath: Path, is_pending: bool = False):
        if not filepath.exists():
            return
        self.q.put((filepath, is_pending))
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [SPECIMEN QUEUE] Enqueued: '{filepath.name}' (Queue backlog: {self.q.qsize()})", flush=True)

    def _worker_loop(self):
        while not self.stop_signal.is_set():
            try:
                filepath, is_pending = self.q.get(timeout=1.5)
            except queue.Empty:
                continue

            try:
                self._process_specimen(filepath, is_pending)
            except Exception as e:
                print(f"[Queue Worker Error] Processing {filepath.name}: {e}", flush=True)
            finally:
                self.q.task_done()

    def _process_specimen(self, filepath: Path, is_pending: bool):
        if not filepath.exists():
            return

        filename = filepath.name

        # Check API availability before making call
        if not self.api_checker.is_available:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [API GATE] Gemini API currently {self.api_checker.status}. Holding '{filename}' in pending_queue/...", flush=True)
            if not is_pending:
                dest = get_unique_path(PENDING_DIR, filepath)
                shutil.move(str(filepath), str(dest))
                pending_diag = CapsicumDiagnosis(
                    is_capsicum_plant=True,
                    plant_part="unrelated",
                    health_status="pending_quota",
                    primary_condition="Awaiting API Availability / Quota Reset",
                    confidence_score=0.0,
                    severity_level="low",
                    visual_symptoms=["Analysis enqueued: holding in pending_queue until AI API is reachable"],
                    recommended_action="Continuous microclimate monitoring active. Evaluation will process automatically.",
                    quarantine_required=False
                )
                db.insert_diagnosis(filename, str(dest), "pending_queue", pending_diag)
            time.sleep(4.0)
            return

        # Validate image integrity
        if not validate_image(filepath):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [SPECIMEN QUEUE] Unreadable image '{filename}' -> routing to unrelated_images/", flush=True)
            dest = get_unique_path(UNRELATED_DIR, filepath)
            shutil.move(str(filepath), str(dest))
            return

        # Call AI analysis
        success, diagnosis, err_type = self.bridge.analyze(filepath)

        if success and diagnosis:
            if not diagnosis.is_capsicum_plant or diagnosis.health_status == "unrelated":
                dest = get_unique_path(UNRELATED_DIR, filepath)
                shutil.move(str(filepath), str(dest))
                folder = "unrelated_images"
            else:
                dest = get_unique_path(PROCESSED_DIR, filepath)
                shutil.move(str(filepath), str(dest))
                folder = "processed_images"

            if is_pending:
                db.update_pending_diagnosis(filename, str(dest), folder, diagnosis)
            else:
                db.insert_diagnosis(filename, str(dest), folder, diagnosis)

            with self.lock:
                self.processed_count += 1

            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DIAGNOSIS COMPLETE] '{filename}': {diagnosis.health_status.upper()} - {diagnosis.primary_condition} ({diagnosis.confidence_score*100:.1f}%) -> {folder}/", flush=True)

        elif err_type == "ALL_KEYS_EXHAUSTED":
            self.api_checker.check_now()
            if not is_pending:
                dest = get_unique_path(PENDING_DIR, filepath)
                shutil.move(str(filepath), str(dest))
                pending_diag = CapsicumDiagnosis(
                    is_capsicum_plant=True,
                    plant_part="unrelated",
                    health_status="pending_quota",
                    primary_condition="Awaiting API Quota / Re-evaluation",
                    confidence_score=0.0,
                    severity_level="low",
                    visual_symptoms=["Analysis postponed due to API quota exhaustion"],
                    recommended_action="Provide additional GEMINI_API_KEY in .env or await daily quota reset.",
                    quarantine_required=False
                )
                db.insert_diagnosis(filename, str(dest), "pending_queue", pending_diag)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [QUOTA EXHAUSTED] Moved '{filename}' -> pending_queue/.", flush=True)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ANALYSIS FAULT] Transient failure for '{filename}'.", flush=True)

    def get_stats(self):
        pending_files = len(list(PENDING_DIR.glob("*.jp*g"))) + len(list(PENDING_DIR.glob("*.png")))
        return {
            "queue_depth": self.q.qsize(),
            "processed_count": self.processed_count,
            "pending_disk_count": pending_files
        }


class ScheduledSnapshotEngine:
    """
    Executes real automatic snapshots at 4 snaps per hour (every 15 minutes = 900s).
    Captures high-resolution frames directly from the live GoPro HERO 11 Mini stream
    and pushes them into the asynchronous specimen pipeline queue.
    """

    def __init__(self, gopro_mgr: GoProUSBManager, queue_mgr: SpecimenQueueManager, snaps_per_hour: int = 4):
        self.gopro = gopro_mgr
        self.queue_mgr = queue_mgr
        self.snaps_per_hour = snaps_per_hour
        self.interval_seconds = int(3600 / snaps_per_hour)  # 900 seconds
        self.last_snapshot_time = None
        self.next_snapshot_target = time.time() + self.interval_seconds
        self.total_snaps_taken = 0
        self.stop_signal = threading.Event()
        self._thread = None

    def start(self):
        if self._thread is None:
            self._align_next_target()
            self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._thread.start()
            print(f"[Snapshot Engine] Real 4 snaps/hour schedule active (900s interval). Next capture in {self.get_seconds_remaining()}s.", flush=True)

    def _align_next_target(self):
        now = datetime.now()
        minute = now.minute
        second = now.second
        next_quarter = ((minute // 15) + 1) * 15
        delta_seconds = (next_quarter - minute) * 60 - second
        if delta_seconds <= 10:
            delta_seconds += 900
        self.next_snapshot_target = time.time() + delta_seconds

    def _scheduler_loop(self):
        while not self.stop_signal.is_set():
            now = time.time()
            if now >= self.next_snapshot_target:
                self.trigger_real_snapshot(trigger_source="AUTO_4_PER_HR")
                self._align_next_target()
            self.stop_signal.wait(1.0)

    def trigger_real_snapshot(self, trigger_source: str = "MANUAL") -> Optional[Path]:
        ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"gopro11_{ts_str}.jpg"
        target_path = INCOMING_DIR / filename

        raw_frame = None
        if self.gopro.is_streaming:
            raw_frame = self.gopro.get_raw_frame()

        if raw_frame is not None:
            cv2.imwrite(str(target_path), raw_frame, [cv2.IMWRITE_JPEG_QUALITY, 95])
        else:
            jpeg_bytes = self.gopro.get_latest_jpeg(quality=95)
            if jpeg_bytes:
                with open(target_path, "wb") as f:
                    f.write(jpeg_bytes)
            else:
                print(f"[Snapshot Engine] Warning: No active camera frame available for {filename}", flush=True)
                return None

        size_bytes = os.path.getsize(target_path)
        self.total_snaps_taken += 1
        self.last_snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [{trigger_source}] Real specimen acquired from GoPro: '{filename}' ({size_bytes} bytes). Enqueueing for AI analysis...", flush=True)
        self.queue_mgr.enqueue(target_path)
        return target_path

    def get_seconds_remaining(self) -> int:
        remaining = int(self.next_snapshot_target - time.time())
        return max(0, remaining)

    def get_status_dict(self):
        rem = self.get_seconds_remaining()
        mins, secs = divmod(rem, 60)
        return {
            "snaps_per_hour": self.snaps_per_hour,
            "interval_seconds": self.interval_seconds,
            "seconds_remaining": rem,
            "countdown_str": f"{mins}m {secs:02d}s",
            "last_snapshot": self.last_snapshot_time or "--",
            "total_taken": self.total_snaps_taken
        }


api_checker = ApiAvailabilityChecker(ai_bridge)
queue_mgr = SpecimenQueueManager(ai_bridge, api_checker)
snapshot_engine = ScheduledSnapshotEngine(gopro, queue_mgr, snaps_per_hour=4)


# ============================================================================
# 3. PROFESSIONAL ENTERPRISE SCADA WEB COCKPIT (1080P FIT, ZERO EMOJIS)
# ============================================================================
HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Greenhouse Overseer SCADA Cockpit - Optical Phenotyping & Environmental Telemetry</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root, [data-theme="light"] {
      --bg-void: #f1f5f9;
      --bg-panel: #ffffff;
      --bg-surface: #f8fafc;
      --border-subtle: #e2e8f0;
      --border-strong: #cbd5e1;
      --header-bg: rgba(255, 255, 255, 0.95);
      --strip-bg: rgba(255, 255, 255, 0.95);
      --chip-bg: #f1f5f9;
      --chip-hover: #e2e8f0;
      --table-header-bg: #f1f5f9;
      --table-row-hover: #f8fafc;
      --table-border: #f1f5f9;
      --primary: #059669;
      --primary-glow: rgba(5, 150, 105, 0.2);
      --accent-cyan: #0284c7;
      --accent-amber: #d97706;
      --accent-rose: #e11d48;
      --accent-violet: #7c3aed;
      --text-high: #0f172a;
      --text-medium: #475569;
      --text-low: #64748b;
      --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      --font-mono: 'JetBrains Mono', monospace;
      --modal-bg: #ffffff;
      --modal-subbox: #f8fafc;
      --modal-remedy-bg: #f0fdf4;
      --modal-remedy-border: #bbf7d0;
      --modal-remedy-text: #065f46;
      --hud-chip-bg: rgba(255, 255, 255, 0.94);
      --hud-chip-text: #0f172a;
      --hud-chip-border: #cbd5e1;
      --tag-unrelated-bg: #f1f5f9;
      --tag-unrelated-color: #475569;
      --tag-unrelated-border: #e2e8f0;
      --card-meta-b: #1e293b;
      --badge-bg: #f8fafc;
      --grid-pattern-color: rgba(15, 23, 42, 0.035);
      --box-shadow-panel: 0 2px 8px rgba(0, 0, 0, 0.04);
    }

    [data-theme="dark"] {
      --bg-void: #090d16;
      --bg-panel: #0f172a;
      --bg-surface: #1e293b;
      --border-subtle: #1e293b;
      --border-strong: #334155;
      --header-bg: rgba(15, 23, 42, 0.94);
      --strip-bg: rgba(15, 23, 42, 0.94);
      --chip-bg: #1e293b;
      --chip-hover: #334155;
      --table-header-bg: #1e293b;
      --table-row-hover: #162032;
      --table-border: #1e293b;
      --primary: #10b981;
      --primary-glow: rgba(16, 185, 129, 0.25);
      --accent-cyan: #38bdf8;
      --accent-amber: #fbbf24;
      --accent-rose: #fb7185;
      --accent-violet: #a78bfa;
      --text-high: #f8fafc;
      --text-medium: #cbd5e1;
      --text-low: #94a3b8;
      --modal-bg: #0f172a;
      --modal-subbox: #1e293b;
      --modal-remedy-bg: rgba(16, 185, 129, 0.12);
      --modal-remedy-border: rgba(16, 185, 129, 0.35);
      --modal-remedy-text: #6ee7b7;
      --hud-chip-bg: rgba(15, 23, 42, 0.88);
      --hud-chip-text: #f8fafc;
      --hud-chip-border: #334155;
      --tag-unrelated-bg: #1e293b;
      --tag-unrelated-color: #94a3b8;
      --tag-unrelated-border: #334155;
      --card-meta-b: #f1f5f9;
      --badge-bg: #1e293b;
      --grid-pattern-color: rgba(255, 255, 255, 0.03);
      --box-shadow-panel: 0 4px 16px rgba(0, 0, 0, 0.25);
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      background-color: var(--bg-void);
      background-image: 
        radial-gradient(circle at 12% 10%, rgba(5, 150, 105, 0.06) 0%, transparent 40%),
        radial-gradient(circle at 88% 90%, rgba(2, 132, 199, 0.06) 0%, transparent 40%),
        linear-gradient(var(--grid-pattern-color) 1px, transparent 1px),
        linear-gradient(90deg, var(--grid-pattern-color) 1px, transparent 1px);
      background-size: 100% 100%, 100% 100%, 28px 28px, 28px 28px;
      color: var(--text-high);
      font-family: var(--font-sans);
      height: 100vh;
      width: 100vw;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      user-select: none;
      transition: background-color 0.2s ease, color 0.2s ease;
    }

    /* Enterprise SCADA Header */
    header {
      background: var(--header-bg);
      backdrop-filter: blur(20px);
      border-bottom: 1px solid var(--border-subtle);
      padding: 0.5rem 1.4rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-shrink: 0;
      z-index: 100;
      transition: max-height 0.25s cubic-bezier(0.16, 1, 0.3, 1), 
                  opacity 0.2s ease, 
                  padding 0.25s ease, 
                  transform 0.25s cubic-bezier(0.16, 1, 0.3, 1),
                  background 0.2s ease,
                  border-color 0.2s ease;
      max-height: 56px;
      overflow: hidden;
      box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
    }

    header.header-collapsed {
      max-height: 0;
      opacity: 0;
      padding-top: 0;
      padding-bottom: 0;
      border-bottom: none;
      pointer-events: none;
      transform: translateY(-100%);
    }

    .btn-topbar-toggle {
      display: flex;
      align-items: center;
      gap: 0.35rem;
      background: var(--chip-bg);
      border: 1px solid var(--border-subtle);
      color: var(--text-medium);
      padding: 0.3rem 0.65rem;
      border-radius: 4px;
      font-family: var(--font-mono);
      font-size: 0.68rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .btn-topbar-toggle:hover {
      background: var(--chip-hover);
      color: var(--text-high);
      border-color: var(--border-strong);
    }

    /* Floating Reveal Pill when Header is Hidden */
    .floating-header-btn {
      position: fixed;
      top: 0;
      left: 50%;
      transform: translate(-50%, -100%);
      background: var(--header-bg);
      backdrop-filter: blur(12px);
      border: 1px solid var(--border-strong);
      border-top: none;
      border-radius: 0 0 6px 6px;
      color: var(--text-medium);
      padding: 0.25rem 0.95rem;
      font-family: var(--font-mono);
      font-size: 0.65rem;
      font-weight: 600;
      letter-spacing: 0.04em;
      display: flex;
      align-items: center;
      gap: 0.45rem;
      cursor: pointer;
      z-index: 500;
      opacity: 0;
      pointer-events: none;
      transition: transform 0.22s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.2s ease, background 0.15s ease, color 0.15s ease;
      box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
    }
    .floating-header-btn:hover {
      background: var(--bg-surface);
      color: var(--primary);
      border-color: var(--primary);
    }
    .floating-header-btn.visible {
      transform: translate(-50%, 0);
      opacity: 0.96;
      pointer-events: auto;
    }

    .brand-cluster {
      display: flex;
      align-items: center;
      gap: 0.85rem;
    }

    .brand-symbol {
      width: 32px;
      height: 32px;
      background: linear-gradient(135deg, rgba(2, 132, 199, 0.12), rgba(5, 150, 105, 0.18));
      border: 1px solid var(--primary);
      border-radius: 6px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--primary);
      box-shadow: 0 0 10px rgba(5, 150, 105, 0.15);
    }

    .brand-meta h1 {
      font-size: 0.95rem;
      font-weight: 700;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      color: var(--text-high);
    }
    .brand-meta p {
      font-size: 0.68rem;
      color: var(--text-low);
      font-family: var(--font-mono);
      letter-spacing: 0.02em;
    }

    /* Central Cockpit Navigation Tabs */
    .cockpit-nav-bar {
      display: flex;
      gap: 0.4rem;
      background: var(--chip-bg);
      padding: 0.2rem;
      border-radius: 8px;
      border: 1px solid var(--border-subtle);
      transition: background 0.2s ease, border-color 0.2s ease;
    }

    .cockpit-tab-btn {
      display: flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.35rem 0.85rem;
      background: transparent;
      border: 1px solid transparent;
      border-radius: 6px;
      cursor: pointer;
      color: var(--text-medium);
      font-size: 0.76rem;
      font-family: var(--font-sans);
      font-weight: 600;
      letter-spacing: 0.02em;
      transition: all 0.15s ease;
    }
    .cockpit-tab-btn:hover {
      color: var(--text-high);
      background: var(--chip-hover);
    }
    .cockpit-tab-btn.active {
      background: var(--bg-panel);
      border-color: var(--border-strong);
      color: var(--primary);
      box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
    }
    .tab-index {
      width: 18px;
      height: 18px;
      border-radius: 4px;
      background: var(--chip-hover);
      font-family: var(--font-mono);
      font-size: 0.7rem;
      font-weight: 700;
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--text-medium);
    }
    .cockpit-tab-btn.active .tab-index {
      background: var(--primary);
      color: #ffffff;
    }

    /* Header Telemetry Badges */
    .telemetry-badges {
      display: flex;
      align-items: center;
      gap: 0.65rem;
    }

    .badge-pill {
      display: flex;
      align-items: center;
      gap: 0.45rem;
      background: var(--badge-bg);
      border: 1px solid var(--border-subtle);
      color: var(--text-medium);
      padding: 0.3rem 0.65rem;
      border-radius: 4px;
      font-family: var(--font-mono);
      font-size: 0.7rem;
      font-weight: 600;
      transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
    }
    .badge-pill.status-green {
      border-color: rgba(16, 185, 129, 0.35);
      color: var(--primary);
      background: rgba(16, 185, 129, 0.1);
    }
    .badge-pill.status-amber {
      border-color: rgba(245, 158, 11, 0.35);
      color: var(--accent-amber);
      background: rgba(245, 158, 11, 0.1);
    }
    .badge-pill.status-blue {
      border-color: rgba(56, 189, 248, 0.35);
      color: var(--accent-cyan);
      background: rgba(56, 189, 248, 0.1);
    }
    .badge-pill.status-rose {
      border-color: rgba(244, 63, 94, 0.35);
      color: var(--accent-rose);
      background: rgba(244, 63, 94, 0.1);
    }
    .indicator-node {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: currentColor;
      box-shadow: 0 0 6px currentColor;
    }

    /* Main Viewport Workspace */
    .cockpit-viewport {
      flex: 1;
      padding: 0.75rem 1.25rem 0.9rem 1.25rem;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    /* ========================================================================
       1080P EXECUTIVE COCKPIT (EXACT 6-BOX SPECIFICATION)
       ======================================================================== */
    #cockpitView1080 {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      height: 100%;
      overflow: hidden;
    }

    /* Top Row: Stream (Left) + Pathology Surveillance Table (Right) */
    .cockpit-upper-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.75rem;
      flex: 1.45;
      min-height: 0;
    }

    /* Box 1 (Top Left): Live Stream */
    .stream-module {
      background: #020617;
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      position: relative;
      overflow: hidden;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: var(--box-shadow-panel);
    }

    #canvasPreview, #canvasPreviewV1 {
      width: 100%;
      height: 100%;
      object-fit: contain;
      display: block;
    }

    #goproImg, #webcamVideo {
      display: none;
    }

    .stream-scada-hud {
      position: absolute;
      inset: 0;
      pointer-events: none;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      padding: 0.85rem;
    }
    .hud-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
    }
    .hud-metric-chip {
      background: var(--hud-chip-bg);
      backdrop-filter: blur(8px);
      border: 1px solid var(--hud-chip-border);
      color: var(--hud-chip-text);
      padding: 0.25rem 0.55rem;
      border-radius: 4px;
      font-family: var(--font-mono);
      font-size: 0.68rem;
      display: flex;
      gap: 0.45rem;
      box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
    }

    .reticle-bracket {
      position: absolute;
      width: 18px;
      height: 18px;
      border-color: var(--primary);
      border-style: solid;
      pointer-events: none;
      opacity: 0.75;
    }
    .reticle-bracket.tl { top: 12px; left: 12px; border-width: 2px 0 0 2px; }
    .reticle-bracket.tr { top: 12px; right: 12px; border-width: 2px 2px 0 0; }
    .reticle-bracket.bl { bottom: 48px; left: 12px; border-width: 0 0 2px 2px; }
    .reticle-bracket.br { bottom: 48px; right: 12px; border-width: 0 2px 2px 0; }

    .stream-control-strip {
      position: absolute;
      bottom: 0.65rem;
      left: 0.65rem;
      right: 0.65rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: var(--strip-bg);
      backdrop-filter: blur(12px);
      border: 1px solid var(--border-strong);
      border-radius: 6px;
      padding: 0.35rem 0.75rem;
      pointer-events: auto;
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.15);
    }

    .mode-switch-group {
      display: flex;
      gap: 0.35rem;
    }
    .chip-btn {
      background: var(--chip-bg);
      border: 1px solid var(--border-subtle);
      color: var(--text-medium);
      padding: 0.28rem 0.6rem;
      border-radius: 4px;
      font-size: 0.7rem;
      font-family: var(--font-mono);
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .chip-btn:hover {
      background: var(--chip-hover);
      color: var(--text-high);
    }
    .chip-btn.active {
      background: var(--primary-glow);
      border-color: var(--primary);
      color: var(--primary);
    }

    .btn-action-primary {
      display: flex;
      align-items: center;
      gap: 0.4rem;
      background: linear-gradient(135deg, #059669, #0284c7);
      color: #ffffff;
      border: none;
      padding: 0.35rem 0.85rem;
      border-radius: 5px;
      font-size: 0.72rem;
      font-weight: 700;
      font-family: var(--font-sans);
      letter-spacing: 0.03em;
      cursor: pointer;
      transition: opacity 0.15s ease;
      box-shadow: 0 2px 6px rgba(5, 150, 105, 0.25);
    }
    .btn-action-primary:hover { opacity: 0.92; }

    .shutter-screen {
      position: absolute;
      inset: 0;
      background: #ffffff;
      opacity: 0;
      pointer-events: none;
      transition: opacity 0.06s ease-out;
    }
    .shutter-screen.active { opacity: 0.8; }

    /* Box 2 (Top Right): Processed Images Table with Diagnoses & Solutions */
    .diagnoses-module {
      background: var(--bg-panel);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      box-shadow: var(--box-shadow-panel);
    }

    .module-toolbar {
      padding: 0.55rem 0.9rem;
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-shrink: 0;
    }
    .module-toolbar h2 {
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--text-high);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .table-container {
      flex: 1;
      overflow-y: auto;
      overflow-x: hidden;
      background: var(--bg-panel);
    }

    .scada-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.72rem;
      text-align: left;
    }
    .scada-table th {
      background: var(--table-header-bg);
      color: var(--text-low);
      font-family: var(--font-mono);
      font-weight: 700;
      font-size: 0.65rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      padding: 0.45rem 0.75rem;
      position: sticky;
      top: 0;
      z-index: 10;
      border-bottom: 1px solid var(--border-subtle);
    }
    .scada-table td {
      padding: 0.45rem 0.75rem;
      border-bottom: 1px solid var(--table-border);
      vertical-align: middle;
      color: var(--text-high);
    }
    .scada-table tr:hover {
      background: var(--table-row-hover);
    }

    .specimen-thumb {
      width: 44px;
      height: 33px;
      border-radius: 3px;
      object-fit: cover;
      border: 1px solid var(--border-strong);
      cursor: pointer;
      transition: border-color 0.15s ease;
    }
    .specimen-thumb:hover {
      border-color: var(--accent-cyan);
    }

    .tag-status {
      display: inline-block;
      padding: 0.15rem 0.4rem;
      border-radius: 3px;
      font-family: var(--font-mono);
      font-size: 0.62rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .tag-status.diseased {
      background: rgba(244, 63, 94, 0.15);
      color: var(--accent-rose);
      border: 1px solid rgba(244, 63, 94, 0.35);
    }
    .tag-status.healthy {
      background: rgba(16, 185, 129, 0.15);
      color: var(--primary);
      border: 1px solid rgba(16, 185, 129, 0.35);
    }
    .tag-status.unrelated {
      background: var(--tag-unrelated-bg);
      color: var(--tag-unrelated-color);
      border: 1px solid var(--tag-unrelated-border);
    }

    .remediation-directive {
      color: var(--text-medium);
      font-size: 0.7rem;
      line-height: 1.35;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    /* Bottom Row: 4 Detailed SCADA Charts */
    .cockpit-lower-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 0.75rem;
      flex: 1.05;
      min-height: 0;
    }

    .telemetry-card {
      background: var(--bg-panel);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      padding: 0.65rem 0.85rem;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      position: relative;
      box-shadow: var(--box-shadow-panel);
    }

    .card-top-stat {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 0.25rem;
      flex-shrink: 0;
    }
    .channel-title {
      font-size: 0.68rem;
      font-family: var(--font-mono);
      font-weight: 700;
      color: var(--text-low);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .channel-kpi {
      font-family: var(--font-mono);
      font-size: 1.25rem;
      font-weight: 800;
      color: var(--text-high);
      display: flex;
      align-items: baseline;
      gap: 0.25rem;
    }
    .channel-kpi .unit {
      font-size: 0.72rem;
      color: var(--text-medium);
      font-weight: 500;
    }

    .channel-meta-sub {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.64rem;
      font-family: var(--font-mono);
      color: var(--text-low);
      padding-bottom: 0.25rem;
      border-bottom: 1px solid var(--table-border);
      margin-bottom: 0.35rem;
      flex-shrink: 0;
    }
    .channel-meta-sub span b {
      color: var(--card-meta-b);
    }

    .chart-viewport-box {
      flex: 1;
      width: 100%;
      min-height: 0;
      position: relative;
    }

    /* ========================================================================
       OPTICAL CALIBRATION SUITE (VIEW 1)
       ======================================================================== */
    #cockpitViewLab {
      display: none;
      grid-template-columns: 1fr 380px;
      gap: 1rem;
      height: 100%;
      overflow: hidden;
    }

    .lab-stage {
      display: flex;
      flex-direction: column;
      height: 100%;
    }
    .lab-controls-panel {
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      height: 100%;
    }

    .control-block {
      background: var(--bg-panel);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      padding: 0.85rem;
      box-shadow: var(--box-shadow-panel);
    }
    .block-header {
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-bottom: 0.65rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      color: var(--text-high);
    }

    .slider-row {
      display: flex;
      flex-direction: column;
      gap: 0.2rem;
      margin-bottom: 0.55rem;
    }
    .slider-text {
      display: flex;
      justify-content: space-between;
      font-size: 0.7rem;
      color: var(--text-medium);
      font-family: var(--font-mono);
    }
    .slider-text .val {
      color: var(--text-high);
      font-weight: 600;
    }
    input[type=range] {
      -webkit-appearance: none;
      width: 100%;
      height: 4px;
      background: var(--border-strong);
      border-radius: 2px;
      outline: none;
    }
    input[type=range]::-webkit-slider-thumb {
      -webkit-appearance: none;
      width: 12px;
      height: 12px;
      background: var(--primary);
      border-radius: 50%;
      cursor: pointer;
    }

    .hue-spectrum-rail {
      position: relative;
      height: 10px;
      border-radius: 4px;
      background: linear-gradient(90deg, #ff0000 0%, #ffff00 16.6%, #00ff00 33.3%, #00ffff 50%, #0000ff 66.6%, #ff00ff 83.3%, #ff0000 100%);
      margin: 0.25rem 0 0.55rem 0;
      overflow: hidden;
    }
    .hue-active-gate {
      position: absolute;
      top: 0; bottom: 0;
      border: 1.5px solid var(--text-high);
      box-shadow: 0 0 6px rgba(0, 0, 0, 0.4);
      border-radius: 3px;
      pointer-events: none;
    }

    /* Modal Inspection Frame */
    #specimenModal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.72);
      backdrop-filter: blur(12px);
      z-index: 1000;
      align-items: center;
      justify-content: center;
      padding: 2rem;
    }
    #specimenModal.active { display: flex; }
    .modal-frame {
      background: var(--modal-bg);
      border: 1px solid var(--border-strong);
      border-radius: 8px;
      max-width: 860px;
      width: 100%;
      display: grid;
      grid-template-columns: 1fr 1fr;
      overflow: hidden;
      box-shadow: 0 24px 60px rgba(0, 0, 0, 0.45);
    }
    .modal-symptoms-box {
      background: var(--modal-subbox);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 0.85rem;
      margin-bottom: 1rem;
    }
    .modal-remediation-box {
      background: var(--modal-remedy-bg);
      border: 1px solid var(--modal-remedy-border);
      border-radius: 6px;
      padding: 0.85rem;
    }
    .modal-close-btn {
      margin-top: 1.5rem;
      padding: 0.6rem;
      background: var(--chip-bg);
      border: 1px solid var(--border-strong);
      color: var(--text-high);
      border-radius: 4px;
      cursor: pointer;
      font-family: var(--font-sans);
      font-size: 0.76rem;
      font-weight: 600;
      transition: all 0.15s ease;
    }
    .modal-close-btn:hover {
      background: var(--chip-hover);
    }

    /* Toast */
    #toastLog {
      position: fixed;
      bottom: 1.25rem;
      right: 1.25rem;
      background: var(--bg-panel);
      border: 1px solid var(--primary);
      color: var(--text-high);
      padding: 0.65rem 1.15rem;
      border-radius: 6px;
      font-family: var(--font-mono);
      font-size: 0.76rem;
      transform: translateY(80px);
      opacity: 0;
      transition: all 0.2s ease;
      z-index: 2000;
      box-shadow: 0 4px 14px rgba(0, 0, 0, 0.25);
    }
    #toastLog.active {
      transform: translateY(0);
      opacity: 1;
    }
  </style>
</head>
<body>

  <!-- Enterprise SCADA Header -->
  <header>
    <div class="brand-cluster">
      <div class="brand-symbol">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>
        </svg>
      </div>
      <div class="brand-meta">
        <h1>Greenhouse Overseer SCADA</h1>
        <p>Optical Phenotyping & Microclimate Telemetry Architecture</p>
      </div>
    </div>

    <!-- Mode Selector Tabs -->
    <div class="cockpit-nav-bar">
      <button id="tabNav1" class="cockpit-tab-btn" onclick="setCockpitView(1)" title="Optical Calibration & Threshold Sliders (Key: 1)">
        <span class="tab-index">1</span>
        <span>Optical Calibration Lab</span>
      </button>

      <button id="tabNav2" class="cockpit-tab-btn active" onclick="setCockpitView(2)" title="1080p SCADA Supervisory Cockpit (Key: 2)">
        <span class="tab-index">2</span>
        <span>1080p Operations Cockpit</span>
      </button>
    </div>

    <div class="telemetry-badges">
      <div class="badge-pill status-blue" id="pillBattery">
        <span class="indicator-node"></span>
        <span id="txtBattery">BATTERY: --%</span>
      </div>

      <div class="badge-pill status-amber" id="pillGateway">
        <span class="indicator-node"></span>
        <span id="txtGateway">ESP32: COM7</span>
      </div>

      <div class="badge-pill status-green">
        <span class="indicator-node"></span>
        <span id="txtStreamFps">STREAM 30.0 FPS</span>
      </div>

      <!-- Live AI API Availability Badge -->
      <div class="badge-pill status-green" id="pillAiApi" title="Gemini AI Diagnostic Engine & Key Rotation Status">
        <span class="indicator-node" id="nodeAiApi"></span>
        <span id="txtAiApi">AI API: ONLINE</span>
      </div>

      <!-- Specimen Pipeline Queue Badge -->
      <div class="badge-pill" id="pillQueue" title="Asynchronous Specimen Ingestion & Diagnostic Pipeline Queue">
        <span class="indicator-node" style="background: #0284c7;"></span>
        <span id="txtQueue">QUEUE: 0 PENDING</span>
      </div>

      <!-- Real 4 Snaps / Hour Schedule Countdown -->
      <div class="badge-pill status-blue" id="pillNextSnap" title="Automated Real Specimen Acquisition Schedule (4 Snaps / Hour = 15m Cadence)">
        <span class="indicator-node" style="background: #d97706;"></span>
        <span id="txtNextSnap">NEXT SNAP: 15m 00s (4/HR)</span>
      </div>

      <!-- Theme Switcher (Dark / Light Mode) -->
      <button id="btnThemeToggle" class="btn-topbar-toggle" onclick="toggleTheme()" title="Toggle Dark / Light Theme (Shortcut: T)">
        <svg id="iconSun" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="display:none;">
          <circle cx="12" cy="12" r="5"></circle>
          <line x1="12" y1="1" x2="12" y2="3"></line>
          <line x1="12" y1="21" x2="12" y2="23"></line>
          <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line>
          <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line>
          <line x1="1" y1="12" x2="3" y2="12"></line>
          <line x1="21" y1="12" x2="23" y2="12"></line>
          <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line>
          <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line>
        </svg>
        <svg id="iconMoon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>
        </svg>
        <span id="txtTheme">DARK [T]</span>
      </button>

      <button id="btnHideTopBar" class="btn-topbar-toggle" onclick="toggleTopBar()" title="Hide Top Navigation Bar (Shortcut: H)">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <polyline points="18 15 12 9 6 15"></polyline>
        </svg>
        <span>HIDE BAR [H]</span>
      </button>
    </div>
  </header>

  <!-- Floating Reveal Button when Header is Hidden -->
  <button id="floatingShowHeaderBtn" class="floating-header-btn" onclick="toggleTopBar()" title="Reveal Top Navigation Bar (Shortcut: H | Theme: T)">
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="6 9 12 15 18 9"></polyline>
    </svg>
    <span>SHOW HEADER [H]</span>
  </button>

  <!-- Main Viewport Workspace -->
  <div class="cockpit-viewport">

    <!-- ========================================================================
         VIEW 2: 1080P OPERATIONS COCKPIT (THE EXACT 6-BOX SPECIFICATION)
         ======================================================================== -->
    <div id="cockpitView1080">
      
      <!-- Upper Grid: Live Stream (Left) + Pathology Table (Right) -->
      <div class="cockpit-upper-grid">
        
        <!-- Box 1 (Top Left): Live Stream -->
        <div class="stream-module">
          <img id="goproImg" crossOrigin="anonymous" src="/video_feed" alt="Video Stream">
          <video id="webcamVideo" autoplay playsinline muted></video>

          <canvas id="canvasPreview"></canvas>
          <div class="shutter-screen" id="shutterFx"></div>

          <!-- Technical Corner Brackets -->
          <div class="reticle-bracket tl"></div>
          <div class="reticle-bracket tr"></div>
          <div class="reticle-bracket bl"></div>
          <div class="reticle-bracket br"></div>

          <!-- SCADA HUD Overlay -->
          <div class="stream-scada-hud">
            <div class="hud-header">
              <div class="hud-metric-chip">
                <span style="color: var(--text-low);">FEED:</span>
                <span id="lblActiveShader" style="color: var(--primary);">EXG SPECTRAL HIGHLIGHT</span>
              </div>
              <div class="hud-metric-chip">
                <span style="color: var(--text-low);">CADENCE:</span>
                <span style="color: var(--accent-amber); font-weight: 700;" id="lblHudCadence">4 SNAPS/HR [AUTO]</span>
              </div>
              <div class="hud-metric-chip">
                <span style="color: var(--text-low);">CANOPY:</span>
                <span id="lblCanopyCoverage" style="color: var(--accent-cyan);">0.0%</span>
              </div>
              <div class="hud-metric-chip">
                <span style="color: var(--text-low);">STREAM:</span>
                <span id="lblFps" style="color: var(--primary);">30.0 FPS</span>
              </div>
            </div>
          </div>

          <!-- Bottom Control Bar -->
          <div class="stream-control-strip">
            <div class="mode-switch-group">
              <button class="chip-btn active" onclick="setFilterMode('highlight')">HIGHLIGHT</button>
              <button class="chip-btn" onclick="setFilterMode('isolate')">ISOLATION</button>
              <button class="chip-btn" onclick="setFilterMode('contour')">CONTOUR</button>
              <button class="chip-btn" onclick="setFilterMode('raw')">RAW SENSOR</button>
            </div>
            <button class="btn-action-primary" onclick="triggerSnapshot()">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/></svg>
              <span>CAPTURE SPECIMEN</span>
            </button>
          </div>
        </div>

        <!-- Box 2 (Top Right): Processed Images Table with Diagnoses & Solutions -->
        <div class="diagnoses-module">
          <div class="module-toolbar">
            <h2>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
              <span>Pathology Surveillance & AI Diagnostic Intake</span>
            </h2>
            <div style="display: flex; gap: 0.5rem; align-items: center;">
              <span id="lblDiagTotal" style="font-family: var(--font-mono); font-size: 0.68rem; color: var(--accent-cyan); font-weight: 700;">14 SPECIMENS LOGGED</span>
              <button onclick="fetchDiagnoses()" style="background: #ffffff; border: 1px solid var(--border-strong); color: var(--text-high); font-size: 0.65rem; font-family: var(--font-mono); font-weight: 600; padding: 0.2rem 0.55rem; border-radius: 4px; cursor: pointer; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">REFRESH</button>
            </div>
          </div>

          <div class="table-container">
            <table class="scada-table">
              <thead>
                <tr>
                  <th style="width: 55px;">Specimen</th>
                  <th style="width: 85px;">Timestamp</th>
                  <th style="width: 130px;">Pathology Classification</th>
                  <th style="width: 80px;">Status</th>
                  <th>Remediation Protocol</th>
                </tr>
              </thead>
              <tbody id="tbodyDiagnoses">
                <tr>
                  <td colspan="5" style="text-align: center; color: var(--text-low); padding: 2.5rem; font-family: var(--font-mono);">
                    INITIALIZING SURVEILLANCE DATABASE INTAKE...
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

      </div>

      <!-- Lower Grid: 4 Dedicated Detailed Telemetry Charts -->
      <div class="cockpit-lower-grid">

        <!-- Box 1 (Bottom Left): Relative Humidity -->
        <div class="telemetry-card">
          <div class="card-top-stat">
            <span class="channel-title">Relative Humidity</span>
            <div class="channel-kpi">
              <span id="txtValHumidity">60.2</span>
              <span class="unit">% RH</span>
            </div>
          </div>
          <div class="channel-meta-sub">
            <span>TARGET: <b>55.0% - 75.0%</b></span>
            <span>VPD: <b id="txtVpd">1.05 kPa</b></span>
          </div>
          <div class="chart-viewport-box">
            <canvas id="canvasHumidityChart"></canvas>
          </div>
        </div>

        <!-- Box 2 (Bottom Mid-Left): Temperature -->
        <div class="telemetry-card">
          <div class="card-top-stat">
            <span class="channel-title">Ambient Dry Bulb</span>
            <div class="channel-kpi">
              <span id="txtValTemperature">24.5</span>
              <span class="unit">°C</span>
            </div>
          </div>
          <div class="channel-meta-sub">
            <span>TARGET: <b>21.0°C - 28.0°C</b></span>
            <span>GRADIENT: <b id="txtTempTrend">+0.2°C/hr</b></span>
          </div>
          <div class="chart-viewport-box">
            <canvas id="canvasTemperatureChart"></canvas>
          </div>
        </div>

        <!-- Box 3 (Bottom Mid-Right): Soil Moisture -->
        <div class="telemetry-card">
          <div class="card-top-stat">
            <span class="channel-title">Substrate Volumetric</span>
            <div class="channel-kpi">
              <span id="txtValSoil">2191</span>
              <span class="unit">ADC</span>
            </div>
          </div>
          <div class="channel-meta-sub">
            <span>MOISTURE INDEX: <b id="txtSoilPct">76.2%</b></span>
            <span>CAPACITY: <b>OPTIMAL</b></span>
          </div>
          <div class="chart-viewport-box">
            <canvas id="canvasSoilChart"></canvas>
          </div>
        </div>

        <!-- Box 4 (Bottom Right): Light Intensity -->
        <div class="telemetry-card">
          <div class="card-top-stat">
            <span class="channel-title">Solar Radiance / PAR</span>
            <div class="channel-kpi">
              <span id="txtValLight">1120</span>
              <span class="unit">Lux</span>
            </div>
          </div>
          <div class="channel-meta-sub">
            <span>EST. PPFD: <b id="txtPpfd">~22.4 µmol</b></span>
            <span>PHOTOPERIOD: <b>ACTIVE</b></span>
          </div>
          <div class="chart-viewport-box">
            <canvas id="canvasLightChart"></canvas>
          </div>
        </div>

      </div>

    </div>

    <!-- ========================================================================
         VIEW 1: OPTICAL CALIBRATION LAB (TOGGLEABLE)
         ======================================================================== -->
    <div id="cockpitViewLab">
      <div class="lab-stage">
        <div class="stream-module" style="height: 100%;">
          <canvas id="canvasPreviewV1"></canvas>
        </div>
      </div>

      <div class="lab-controls-panel">
        <div class="control-block">
          <div class="block-header">
            <span>HSV Spectral Wavelength Window</span>
            <span style="font-family: var(--font-mono); font-size: 0.65rem; color: var(--primary);" id="lblHueRange">52° - 168°</span>
          </div>

          <div class="hue-spectrum-rail">
            <div id="boxHueGate" class="hue-active-gate" style="left: 14.4%; width: 32.2%;"></div>
          </div>

          <div class="slider-row">
            <div class="slider-text"><span>HUE LOWER BOUND (YELLOW-GREEN)</span><span class="val" id="valHMin">52°</span></div>
            <input type="range" id="sliderHMin" min="30" max="110" value="52">
          </div>
          <div class="slider-row">
            <div class="slider-text"><span>HUE UPPER BOUND (DEEP GREEN)</span><span class="val" id="valHMax">168°</span></div>
            <input type="range" id="sliderHMax" min="110" max="210" value="168">
          </div>
          <div class="slider-row">
            <div class="slider-text"><span>SATURATION LOWER CUTOFF</span><span class="val" id="valSMin">14%</span></div>
            <input type="range" id="sliderSMin" min="2" max="60" value="14">
          </div>
          <div class="slider-row">
            <div class="slider-text"><span>LUMINANCE / VALUE FLOOR</span><span class="val" id="valVMin">10%</span></div>
            <input type="range" id="sliderVMin" min="2" max="50" value="10">
          </div>
          <div class="slider-row">
            <div class="slider-text"><span>EXG DIFFERENTIAL WEIGHT</span><span class="val" id="valExgW">20%</span></div>
            <input type="range" id="sliderExgW" min="0" max="100" value="20" title="0% = Pure HSV Spectral Isolation">
          </div>
          <div class="slider-row">
            <div class="slider-text"><span>OVERLAY OPACITY</span><span class="val" id="valOp">55%</span></div>
            <input type="range" id="sliderOp" min="10" max="100" value="55">
          </div>
        </div>

        <div class="control-block">
          <div class="block-header"><span>Calibration Presets</span></div>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.4rem;">
            <button class="chip-btn active" onclick="applyPreset('standard')">STANDARD CANOPY</button>
            <button class="chip-btn" onclick="applyPreset('shoots')">JUVENILE / LIME</button>
            <button class="chip-btn" onclick="applyPreset('shade')">CANOPY SHADE</button>
            <button class="chip-btn" onclick="applyPreset('wide')">WIDE ADAPTIVE</button>
          </div>
        </div>

        <div class="control-block">
          <div class="block-header"><span>Isolation Matrix Background</span></div>
          <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.4rem;">
            <button class="chip-btn active" onclick="setIsolateBg('black')">ZERO BLACK</button>
            <button class="chip-btn" onclick="setIsolateBg('gray')">MONOCHROME</button>
            <button class="chip-btn" onclick="setIsolateBg('white')">HIGH-KEY</button>
          </div>
        </div>
      </div>
    </div>

  </div>

  <!-- Detailed Specimen Modal -->
  <div id="specimenModal" onclick="closeSpecimenModal(event)">
    <div class="modal-frame" onclick="event.stopPropagation()">
      <div style="background: #000; display: flex; align-items: center; justify-content: center; padding: 1.25rem; border-right: 1px solid var(--border-subtle);">
        <img id="modalSpecimenImg" src="" alt="Specimen Inspection" style="max-width: 100%; max-height: 440px; border-radius: 4px;">
      </div>
      <div style="padding: 1.5rem; display: flex; flex-direction: column; justify-content: space-between;">
        <div>
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.8rem;">
            <span id="modalSpecimenStatus" class="tag-status diseased">DISEASED</span>
            <span id="modalSpecimenTime" style="font-family: var(--font-mono); font-size: 0.72rem; color: var(--text-low);">2026-10-20 18:19:25</span>
          </div>
          <h3 id="modalSpecimenCondition" style="font-size: 1.25rem; font-weight: 700; color: var(--text-high); margin-bottom: 0.35rem;">Powdery Mildew</h3>
          <p id="modalSpecimenConf" style="font-family: var(--font-mono); font-size: 0.78rem; color: var(--accent-cyan); margin-bottom: 1.25rem;">CONFIDENCE: 94.0%</p>

          <div class="modal-symptoms-box">
            <div style="font-size: 0.68rem; font-family: var(--font-mono); color: var(--text-low); text-transform: uppercase; font-weight: 700; margin-bottom: 0.3rem;">Symptomology Profile:</div>
            <div id="modalSpecimenSymptoms" style="font-size: 0.78rem; line-height: 1.45; color: var(--text-medium);">Localized mycelial superficial colonies and chlorosis.</div>
          </div>

          <div class="modal-remediation-box">
            <div style="font-size: 0.68rem; font-family: var(--font-mono); color: #047857; text-transform: uppercase; font-weight: 700; margin-bottom: 0.3rem;">Agronomic Remediation Protocol:</div>
            <div id="modalSpecimenAction" style="font-size: 0.78rem; line-height: 1.45; color: #065f46; font-weight: 500;">Apply systemic triazole or sulfur foliar treatment. Modulate air ventilation.</div>
          </div>
        </div>

        <button class="modal-close-btn" onclick="document.getElementById('specimenModal').classList.remove('active')">CLOSE SPECIMEN DOSSIER</button>
      </div>
    </div>
  </div>

  <div id="toastLog">Notification Log</div>

  <script>
    // --- SCADA View Controller ---
    let currentViewMode = 2; // Default: 1080p SCADA Operations Cockpit

    function setCockpitView(mode) {
      currentViewMode = mode;
      const v1 = document.getElementById('cockpitViewLab');
      const v2 = document.getElementById('cockpitView1080');
      const t1 = document.getElementById('tabNav1');
      const t2 = document.getElementById('tabNav2');

      if (mode === 1) {
        v1.style.display = 'grid';
        v2.style.display = 'none';
        t1.classList.add('active');
        t2.classList.remove('active');
      } else {
        v1.style.display = 'none';
        v2.style.display = 'flex';
        t1.classList.remove('active');
        t2.classList.add('active');
        fetchDiagnoses();
      }
    }

    // --- Top Navigation Bar Visibility Toggle ---
    let isHeaderHidden = false;

    function toggleTopBar(forceState) {
      const header = document.querySelector('header');
      const floatBtn = document.getElementById('floatingShowHeaderBtn');
      
      if (typeof forceState === 'boolean') {
        isHeaderHidden = forceState;
      } else {
        isHeaderHidden = !isHeaderHidden;
      }

      if (isHeaderHidden) {
        header.classList.add('header-collapsed');
        floatBtn.classList.add('visible');
        showToast('Top Navigation Bar Hidden [Press H to restore]');
      } else {
        header.classList.remove('header-collapsed');
        floatBtn.classList.remove('visible');
        showToast('Top Navigation Bar Restored');
      }

      try {
        localStorage.setItem('scada_header_hidden', isHeaderHidden ? 'true' : 'false');
      } catch (e) {}

      // Trigger resize for Chart.js and video canvas to re-fit expanded 1080p area
      setTimeout(() => {
        window.dispatchEvent(new Event('resize'));
      }, 260);
    }

    // --- Theme Controller (Dark Mode & Light Mode) ---
    let currentTheme = 'dark';

    function applyTheme(theme) {
      currentTheme = theme;
      document.documentElement.setAttribute('data-theme', theme);
      const iconSun = document.getElementById('iconSun');
      const iconMoon = document.getElementById('iconMoon');
      const txtTheme = document.getElementById('txtTheme');

      if (theme === 'dark') {
        if (iconSun) iconSun.style.display = 'inline-block';
        if (iconMoon) iconMoon.style.display = 'none';
        if (txtTheme) txtTheme.innerText = 'LIGHT [T]';
      } else {
        if (iconSun) iconSun.style.display = 'none';
        if (iconMoon) iconMoon.style.display = 'inline-block';
        if (txtTheme) txtTheme.innerText = 'DARK [T]';
      }

      updateChartsTheme(theme === 'dark');

      try {
        localStorage.setItem('scada_theme', theme);
      } catch (e) {}
    }

    function toggleTheme() {
      const nextTheme = currentTheme === 'dark' ? 'light' : 'dark';
      applyTheme(nextTheme);
      showToast(`SCADA Theme: ${nextTheme.toUpperCase()} MODE ACTIVE`);
    }

    function updateChartsTheme(isDark) {
      const gridColor = isDark ? 'rgba(255, 255, 255, 0.07)' : 'rgba(0, 0, 0, 0.05)';
      const tickColor = isDark ? '#94a3b8' : '#64748b';
      [chartHumidity, chartTemperature, chartSoil, chartLight].forEach(chart => {
        if (!chart) return;
        if (chart.options && chart.options.scales) {
          if (chart.options.scales.x) {
            chart.options.scales.x.grid.color = gridColor;
            chart.options.scales.x.ticks.color = tickColor;
          }
          if (chart.options.scales.y) {
            chart.options.scales.y.grid.color = gridColor;
            chart.options.scales.y.ticks.color = tickColor;
          }
        }
        chart.update('none');
      });
    }

    window.addEventListener('keydown', (e) => {
      if (e.target.tagName === 'INPUT') return;
      if (e.key === '1') setCockpitView(1);
      if (e.key === '2') setCockpitView(2);
      if (e.key === 'h' || e.key === 'H') toggleTopBar();
      if (e.key === 't' || e.key === 'T') toggleTheme();
      if (e.key === 'Escape' && isHeaderHidden) toggleTopBar(false);
    });

    // --- Stream & Processing Pipeline ---
    const goproImg = document.getElementById('goproImg');
    const canvas = document.getElementById('canvasPreview');
    const ctx = canvas.getContext('2d', { willReadFrequently: true });
    const canvasV1 = document.getElementById('canvasPreviewV1');
    const ctxV1 = canvasV1 ? canvasV1.getContext('2d') : null;

    const lblFps = document.getElementById('lblFps');
    const txtStreamFps = document.getElementById('txtStreamFps');
    const lblCanopyCoverage = document.getElementById('lblCanopyCoverage');
    const lblActiveShader = document.getElementById('lblActiveShader');
    const shutterFx = document.getElementById('shutterFx');
    const toastLog = document.getElementById('toastLog');

    // Optical Threshold Sliders
    const sliderHMin = document.getElementById('sliderHMin');
    const sliderHMax = document.getElementById('sliderHMax');
    const valHMin = document.getElementById('valHMin');
    const valHMax = document.getElementById('valHMax');
    const lblHueRange = document.getElementById('lblHueRange');
    const boxHueGate = document.getElementById('boxHueGate');

    const sliderSMin = document.getElementById('sliderSMin');
    const valSMin = document.getElementById('valSMin');
    const sliderVMin = document.getElementById('sliderVMin');
    const valVMin = document.getElementById('valVMin');
    const sliderExgW = document.getElementById('sliderExgW');
    const valExgW = document.getElementById('valExgW');
    const sliderOp = document.getElementById('sliderOp');
    const valOp = document.getElementById('valOp');

    let currentFilterMode = 'highlight'; // 'highlight', 'isolate', 'contour', 'raw'
    let currentIsolateBg = 'black';
    let lastFrameTime = performance.now();
    let frameCount = 0;
    let fps = 0;
    let animationId = null;

    function setFilterMode(mode) {
      currentFilterMode = mode;
      lblActiveShader.innerText = mode === 'highlight' ? 'EXG SPECTRAL HIGHLIGHT' :
                                 mode === 'isolate' ? 'FOLIAGE ISOLATION MATRIX' :
                                 mode === 'contour' ? 'CANOPY CONTOUR ISOLINES' : 'UNPROCESSED SENSOR';
      document.querySelectorAll('.stream-control-strip .chip-btn').forEach(b => {
        b.classList.toggle('active', b.innerText.toLowerCase().includes(mode));
      });
    }

    function setIsolateBg(bg) {
      currentIsolateBg = bg;
    }

    function applyPreset(preset) {
      if (preset === 'standard') {
        sliderHMin.value = 52; sliderHMax.value = 168; sliderSMin.value = 14; sliderVMin.value = 10; sliderExgW.value = 20;
      } else if (preset === 'shoots') {
        sliderHMin.value = 42; sliderHMax.value = 145; sliderSMin.value = 10; sliderVMin.value = 14; sliderExgW.value = 10;
      } else if (preset === 'shade') {
        sliderHMin.value = 58; sliderHMax.value = 180; sliderSMin.value = 12; sliderVMin.value = 6; sliderExgW.value = 15;
      } else if (preset === 'wide') {
        sliderHMin.value = 36; sliderHMax.value = 195; sliderSMin.value = 6; sliderVMin.value = 5; sliderExgW.value = 0;
      }
      updateSpectrumGate();
    }

    function initStream() {
      goproImg.src = '/video_feed?' + Date.now();
      goproImg.onload = () => {
        txtStreamFps.innerText = 'GOPRO 11 USB: 30.0 FPS';
        if (!animationId) animationId = requestAnimationFrame(processVideoFrame);
      };
      goproImg.onerror = () => {
        txtStreamFps.innerText = 'RECONNECTING STREAM...';
      };
    }

    function processVideoFrame(now) {
      let sourceElem = goproImg;
      let sWidth = goproImg.naturalWidth || 1280;
      let sHeight = goproImg.naturalHeight || 960;

      if (sourceElem && sWidth > 0 && sHeight > 0) {
        if (canvas.width !== sWidth || canvas.height !== sHeight) {
          canvas.width = sWidth;
          canvas.height = sHeight;
        }

        ctx.drawImage(sourceElem, 0, 0, canvas.width, canvas.height);

        if (currentFilterMode !== 'raw') {
          const imgData = ctx.getImageData(0, 0, canvas.width, canvas.height);
          const data = imgData.data;
          const totalPixels = canvas.width * canvas.height;

          const uHMin = parseFloat(sliderHMin.value);
          const uHMax = parseFloat(sliderHMax.value);
          const uSMin = parseFloat(sliderSMin.value) / 100.0;
          const uVMin = parseFloat(sliderVMin.value) / 100.0;
          const uExgW = parseFloat(sliderExgW.value) / 100.0;
          const opacity = parseFloat(sliderOp.value) / 100.0;

          let leafCount = 0;

          for (let i = 0; i < data.length; i += 4) {
            const r = data[i];
            const g = data[i + 1];
            const b = data[i + 2];

            const maxC = Math.max(r, g, b);
            const minC = Math.min(r, g, b);
            const delta = maxC - minC;

            // Value / Luminance Gate
            const val = maxC / 255.0;
            if (val < uVMin) {
              applyBackgroundPixel(data, i, currentFilterMode, currentIsolateBg);
              continue;
            }

            // Saturation Gate
            const sat = maxC === 0 ? 0 : delta / maxC;
            if (sat < uSMin) {
              applyBackgroundPixel(data, i, currentFilterMode, currentIsolateBg);
              continue;
            }

            // Hue Gate
            let hue = 0;
            if (delta > 0) {
              if (maxC === g) hue = 60 * (((b - r) / delta) + 2);
              else if (maxC === r) hue = 60 * (((g - b) / delta) + (g < b ? 6 : 0));
              else hue = 60 * (((r - g) / delta) + 4);
            }

            if (hue < uHMin || hue > uHMax) {
              applyBackgroundPixel(data, i, currentFilterMode, currentIsolateBg);
              continue;
            }

            // ExG Weight
            if (uExgW > 0.05) {
              const exg = 2.0 * g - r - b;
              if (exg < (-25.0 * (1.0 - uExgW))) {
                applyBackgroundPixel(data, i, currentFilterMode, currentIsolateBg);
                continue;
              }
            }

            leafCount++;
            if (currentFilterMode === 'highlight') {
              data[i] = Math.min(255, r * (1 - opacity) + 16 * opacity);
              data[i + 1] = Math.min(255, g * (1 - opacity) + 185 * opacity);
              data[i + 2] = Math.min(255, b * (1 - opacity) + 129 * opacity);
            }
          }

          ctx.putImageData(imgData, 0, 0);
          const cov = ((leafCount / totalPixels) * 100).toFixed(1);
          lblCanopyCoverage.innerText = `${cov}%`;
        } else {
          lblCanopyCoverage.innerText = 'RAW SENSOR';
        }

        if (currentViewMode === 1 && canvasV1) {
          if (canvasV1.width !== canvas.width || canvasV1.height !== canvas.height) {
            canvasV1.width = canvas.width;
            canvasV1.height = canvas.height;
          }
          ctxV1.drawImage(canvas, 0, 0);
        }
      }

      frameCount++;
      if (now - lastFrameTime >= 1000) {
        fps = frameCount;
        frameCount = 0;
        lastFrameTime = now;
        lblFps.innerText = `${fps}.0 FPS`;
      }
      animationId = requestAnimationFrame(processVideoFrame);
    }

    function applyBackgroundPixel(data, i, mode, style) {
      if (mode === 'isolate') {
        if (style === 'black') {
          data[i] = 0; data[i + 1] = 0; data[i + 2] = 0;
        } else if (style === 'white') {
          data[i] = 255; data[i + 1] = 255; data[i + 2] = 255;
        } else {
          const gray = (data[i] * 0.299 + data[i + 1] * 0.587 + data[i + 2] * 0.114) * 0.25;
          data[i] = gray; data[i + 1] = gray; data[i + 2] = gray;
        }
      }
    }

    function updateSpectrumGate() {
      const hMin = parseFloat(sliderHMin.value);
      const hMax = parseFloat(sliderHMax.value);
      valHMin.innerText = `${hMin}°`;
      valHMax.innerText = `${hMax}°`;
      lblHueRange.innerText = `${hMin}° - ${hMax}°`;
      valSMin.innerText = `${sliderSMin.value}%`;
      valVMin.innerText = `${sliderVMin.value}%`;
      valExgW.innerText = `${sliderExgW.value}%`;
      valOp.innerText = `${sliderOp.value}%`;

      const leftPct = (hMin / 360.0) * 100.0;
      const widthPct = ((hMax - hMin) / 360.0) * 100.0;
      boxHueGate.style.left = `${leftPct.toFixed(1)}%`;
      boxHueGate.style.width = `${Math.max(2, widthPct).toFixed(1)}%`;
    }
    sliderHMin.addEventListener('input', updateSpectrumGate);
    sliderHMax.addEventListener('input', updateSpectrumGate);
    sliderSMin.addEventListener('input', updateSpectrumGate);
    sliderVMin.addEventListener('input', updateSpectrumGate);
    sliderExgW.addEventListener('input', updateSpectrumGate);
    sliderOp.addEventListener('input', updateSpectrumGate);

    // Snapshot
    function showToast(msg) {
      toastLog.innerText = msg;
      toastLog.classList.add('active');
      setTimeout(() => toastLog.classList.remove('active'), 3200);
    }

    async function triggerSnapshot() {
      shutterFx.classList.add('active');
      setTimeout(() => shutterFx.classList.remove('active'), 80);

      const imageData = canvas.toDataURL('image/jpeg', 0.95);
      const now = new Date();
      const ts = now.toISOString().replace(/[-:T]/g, '').slice(0, 15);
      const filename = `specimen_${ts}.jpg`;

      try {
        const resp = await fetch('/api/capture', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ filename, use_native_raw: true, image_data: imageData })
        });
        const res = await resp.json();
        if (res.success) {
          showToast(`Specimen enqueued for AI analysis: ${filename}`);
          setTimeout(fetchDiagnoses, 2500);
        }
      } catch (err) {
        showToast('Capture dispatch fault');
      }
    }

    // --- Processed Images Table with Diagnoses & Solutions ---
    let specimenRecordCache = [];

    async function fetchDiagnoses() {
      try {
        const resp = await fetch('/api/diagnoses');
        const data = await resp.json();
        specimenRecordCache = data;

        document.getElementById('lblDiagTotal').innerText = `${data.length} SPECIMENS LOGGED`;
        const tbody = document.getElementById('tbodyDiagnoses');

        if (!data || data.length === 0) {
          tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:#64748b; padding:2rem; font-family:var(--font-mono);">ZERO SURVEILLANCE SPECIMENS RECORDED</td></tr>';
          return;
        }

        tbody.innerHTML = data.map((item, idx) => {
          const statusCls = item.health_status === 'diseased' ? 'diseased' :
                            item.health_status === 'healthy' ? 'healthy' : 'unrelated';
          const timeFormatted = item.timestamp || '--:--:--';
          const confPercent = Math.round((item.confidence_score || 0) * 100);

          return `
            <tr>
              <td>
                <img class="specimen-thumb" src="${item.image_url}" alt="Specimen" onclick="openSpecimenModal(${idx})" onerror="this.src='/video_feed'">
              </td>
              <td style="font-family: var(--font-mono); color: var(--text-medium); font-size: 0.68rem;">
                ${timeFormatted}
              </td>
              <td>
                <div style="font-weight: 700; color: var(--text-high); font-size: 0.74rem;">${item.primary_condition || 'Undetermined'}</div>
                <div style="font-family: var(--font-mono); font-size: 0.62rem; color: var(--text-low);">Confidence: ${confPercent}% | ${item.severity_level || 'Standard'}</div>
              </td>
              <td>
                <span class="tag-status ${statusCls}">${item.health_status}</span>
              </td>
              <td>
                <div class="remediation-directive" title="${item.recommended_action}">
                  ${item.recommended_action || 'Continuous sensory monitoring active.'}
                </div>
              </td>
            </tr>
          `;
        }).join('');

      } catch (err) {
        console.warn('Diagnoses fetch fault:', err);
      }
    }

    function openSpecimenModal(idx) {
      const item = specimenRecordCache[idx];
      if (!item) return;

      document.getElementById('modalSpecimenImg').src = item.image_url;
      document.getElementById('modalSpecimenCondition').innerText = item.primary_condition || 'Undetermined';
      document.getElementById('modalSpecimenTime').innerText = `TIMESTAMP: ${item.timestamp || ''}`;
      document.getElementById('modalSpecimenConf').innerText = `CONFIDENCE SCORE: ${Math.round(item.confidence_score * 100)}% | SEVERITY: ${(item.severity_level || 'NOMINAL').toUpperCase()}`;
      document.getElementById('modalSpecimenSymptoms').innerText = item.visual_symptoms || 'No anomalous visual lesions noted.';
      document.getElementById('modalSpecimenAction').innerText = item.recommended_action || 'Routine baseline observation protocol.';

      const st = document.getElementById('modalSpecimenStatus');
      st.className = `tag-status ${item.health_status}`;
      st.innerText = item.health_status.toUpperCase();

      document.getElementById('specimenModal').classList.add('active');
    }

    function closeSpecimenModal(e) {
      document.getElementById('specimenModal').classList.remove('active');
    }

    // --- Detailed SCADA Charts (4 Channels) ---
    let chartHumidity = null;
    let chartTemperature = null;
    let chartSoil = null;
    let chartLight = null;

    function initTechnicalCharts() {
      const gridConfig = { color: 'rgba(0, 0, 0, 0.05)', drawBorder: false };
      const tickConfig = { color: '#64748b', font: { family: 'JetBrains Mono', size: 9 }, maxTicksLimit: 5 };
      const tooltipConfig = (channelLabel, unitStr) => ({
        backgroundColor: 'rgba(15, 23, 42, 0.94)',
        titleColor: '#ffffff',
        bodyColor: '#ffffff',
        titleFont: { family: 'JetBrains Mono' },
        bodyFont: { family: 'JetBrains Mono' },
        padding: 8,
        cornerRadius: 4,
        callbacks: { label: ctx => `${channelLabel}: ${ctx.parsed.y} ${unitStr}` }
      });

      // 1. Humidity (Cyan Axis)
      const ctxH = document.getElementById('canvasHumidityChart').getContext('2d');
      chartHumidity = new Chart(ctxH, {
        type: 'line',
        data: {
          labels: [],
          datasets: [{
            data: [],
            borderColor: '#0284c7',
            backgroundColor: 'rgba(2, 132, 199, 0.08)',
            borderWidth: 2,
            pointRadius: 1,
            fill: true,
            tension: 0.3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: tooltipConfig('Relative Humidity', '%')
          },
          scales: {
            x: { grid: gridConfig, ticks: tickConfig },
            y: { grid: gridConfig, ticks: { ...tickConfig, callback: v => v + '%' } }
          }
        }
      });

      // 2. Temperature (Emerald Axis)
      const ctxT = document.getElementById('canvasTemperatureChart').getContext('2d');
      chartTemperature = new Chart(ctxT, {
        type: 'line',
        data: {
          labels: [],
          datasets: [{
            data: [],
            borderColor: '#059669',
            backgroundColor: 'rgba(5, 150, 105, 0.08)',
            borderWidth: 2,
            pointRadius: 1,
            fill: true,
            tension: 0.3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: tooltipConfig('Dry Bulb Temperature', '°C')
          },
          scales: {
            x: { grid: gridConfig, ticks: tickConfig },
            y: { grid: gridConfig, ticks: { ...tickConfig, callback: v => v + '°C' } }
          }
        }
      });

      // 3. Soil Moisture (Forest Green)
      const ctxS = document.getElementById('canvasSoilChart').getContext('2d');
      chartSoil = new Chart(ctxS, {
        type: 'line',
        data: {
          labels: [],
          datasets: [{
            data: [],
            borderColor: '#16a34a',
            backgroundColor: 'rgba(22, 163, 74, 0.08)',
            borderWidth: 2,
            pointRadius: 1,
            fill: true,
            tension: 0.3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: tooltipConfig('Substrate Raw ADC', 'counts')
          },
          scales: {
            x: { grid: gridConfig, ticks: tickConfig },
            y: { grid: gridConfig, ticks: { ...tickConfig, callback: v => v + ' ADC' } }
          }
        }
      });

      // 4. Light Radiance (Solar Amber)
      const ctxL = document.getElementById('canvasLightChart').getContext('2d');
      chartLight = new Chart(ctxL, {
        type: 'line',
        data: {
          labels: [],
          datasets: [{
            data: [],
            borderColor: '#d97706',
            backgroundColor: 'rgba(217, 119, 6, 0.08)',
            borderWidth: 2,
            pointRadius: 1,
            fill: true,
            tension: 0.3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: tooltipConfig('Illuminance', 'Lux')
          },
          scales: {
            x: { grid: gridConfig, ticks: tickConfig },
            y: { grid: gridConfig, ticks: { ...tickConfig, callback: v => v + ' lx' } }
          }
        }
      });
    }

    async function pollTelemetryChannels() {
      try {
        const resp = await fetch('/api/sensor_telemetry');
        const data = await resp.json();
        if (!data || data.status !== 'ok') return;

        const latest = data.latest;
        const history = data.history || [];

        if (latest) {
          if (latest.humidity != null) {
            document.getElementById('txtValHumidity').innerText = Number(latest.humidity).toFixed(1);
            // Calculate Vapor Pressure Deficit (VPD in kPa)
            const T = Number(latest.temperature || 24.5);
            const RH = Number(latest.humidity);
            const VPsat = 0.61078 * Math.exp((17.27 * T) / (T + 237.3));
            const VPact = VPsat * (RH / 100.0);
            const vpd = Math.max(0, VPsat - VPact);
            document.getElementById('txtVpd').innerText = `${vpd.toFixed(2)} kPa`;
          }
          if (latest.temperature != null) {
            document.getElementById('txtValTemperature').innerText = Number(latest.temperature).toFixed(1);
          }
          if (latest.soil_adc != null) {
            document.getElementById('txtValSoil').innerText = latest.soil_adc;
            document.getElementById('txtSoilPct').innerText = `${latest.soil_pct || 76.2}%`;
          }
          if (latest.light_lux != null) {
            const lux = Number(latest.light_lux);
            document.getElementById('txtValLight').innerText = lux.toFixed(0);
            const estPpfd = (lux * 0.0185).toFixed(1);
            document.getElementById('txtPpfd').innerText = `~${estPpfd} µmol`;
          }

          if (data.connection) {
            document.getElementById('txtGateway').innerText = `ESP32: ${data.connection.includes('Live') ? 'COM7 LIVE' : 'SYNCED'}`;
          }
        }

        if (history.length > 0) {
          const labels = history.map(h => (h.timestamp || '').split(' ')[1] || '');
          const hums = history.map(h => h.humidity);
          const temps = history.map(h => h.temperature);
          const soils = history.map(h => h.soil_adc);
          const lights = history.map(h => h.light_lux);

          if (chartHumidity) {
            chartHumidity.data.labels = labels;
            chartHumidity.data.datasets[0].data = hums;
            chartHumidity.update('none');
          }
          if (chartTemperature) {
            chartTemperature.data.labels = labels;
            chartTemperature.data.datasets[0].data = temps;
            chartTemperature.update('none');
          }
          if (chartSoil) {
            chartSoil.data.labels = labels;
            chartSoil.data.datasets[0].data = soils;
            chartSoil.update('none');
          }
          if (chartLight) {
            chartLight.data.labels = labels;
            chartLight.data.datasets[0].data = lights;
            chartLight.update('none');
          }
        }
      } catch (err) {
        console.warn('Telemetry poll error:', err);
      }
    }

    async function pollSystemHeartbeat() {
      try {
        const resp = await fetch('/api/status');
        const data = await resp.json();
        if (data.gopro) {
          const bEl = document.getElementById('txtBattery');
          if (bEl) bEl.innerText = `BATTERY: ${data.gopro.battery || '--%'}`;
        }

        // Live AI API Health & Key Quota Monitoring
        if (data.ai_engine) {
          const pillAi = document.getElementById('pillAiApi');
          const txtAi = document.getElementById('txtAiApi');
          if (pillAi && txtAi) {
            pillAi.classList.remove('status-green', 'status-amber', 'status-rose', 'status-blue');
            if (data.ai_engine.status === 'ONLINE') {
              pillAi.classList.add('status-green');
              txtAi.innerText = `AI API: ONLINE (${data.ai_engine.latency_ms}ms)`;
            } else if (data.ai_engine.status === 'KEY_ROTATED') {
              pillAi.classList.add('status-amber');
              txtAi.innerText = `AI API: ROTATED (${data.ai_engine.keys_available}/${data.ai_engine.total_keys})`;
            } else if (data.ai_engine.status === 'QUOTA_EXHAUSTED') {
              pillAi.classList.add('status-amber');
              txtAi.innerText = `AI API: QUOTA LIMIT`;
            } else {
              pillAi.classList.add('status-rose');
              txtAi.innerText = `AI API: OFFLINE`;
            }
          }
        }

        // Asynchronous Pipeline Queue Monitoring
        if (data.queue) {
          const pillQueue = document.getElementById('pillQueue');
          const txtQueue = document.getElementById('txtQueue');
          if (pillQueue && txtQueue) {
            const backlog = (data.queue.queue_depth || 0) + (data.queue.pending_disk_count || 0);
            txtQueue.innerText = `QUEUE: ${backlog} PENDING`;
            pillQueue.classList.remove('status-green', 'status-amber', 'status-rose', 'status-blue');
            if (backlog > 0) {
              pillQueue.classList.add('status-amber');
            } else {
              pillQueue.classList.add('status-blue');
            }
          }
        }

        // Real 4 Snaps / Hour Schedule Countdown
        if (data.scheduler) {
          const txtNextSnap = document.getElementById('txtNextSnap');
          if (txtNextSnap) {
            txtNextSnap.innerText = `NEXT SNAP: ${data.scheduler.countdown_str} (4/HR)`;
          }
        }
      } catch (e) {}
    }

    // SCADA Initialization
    window.addEventListener('load', () => {
      try {
        const savedTheme = localStorage.getItem('scada_theme') || 'dark';
        applyTheme(savedTheme);
      } catch (e) {
        applyTheme('dark');
      }

      initStream();
      updateSpectrumGate();
      initTechnicalCharts();
      fetchDiagnoses();

      try {
        if (localStorage.getItem('scada_header_hidden') === 'true') {
          toggleTopBar(true);
        }
      } catch (e) {}

      setInterval(pollTelemetryChannels, 1500);
      setInterval(pollSystemHeartbeat, 1000);
      setInterval(fetchDiagnoses, 8000);
    });
  </script>
</body>
</html>
"""


# ============================================================================
# 4. HTTP REQUEST HANDLER WITH IMAGE STREAMING & DIAGNOSES API
# ============================================================================
class CameraOverseerHandler(BaseHTTPRequestHandler):
    """SCADA HTTP Server Engine."""

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path

        if path == "/" or path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_CONTENT.encode("utf-8"))

        elif path.startswith("/video_feed"):
            if not gopro.is_streaming:
                gopro.start_stream()

            self.send_response(200)
            self.send_header("Content-Type", "multipart/x-mixed-replace; boundary=frame")
            self.send_header("Cache-Control", "no-cache, private")
            self.send_header("Pragma", "no-cache")
            self.end_headers()

            while gopro.is_streaming:
                jpeg_bytes = gopro.get_latest_jpeg(quality=85)
                if jpeg_bytes is None:
                    time.sleep(0.02)
                    continue

                try:
                    self.wfile.write(b"--frame\r\n")
                    self.wfile.write(b"Content-Type: image/jpeg\r\n\r\n")
                    self.wfile.write(jpeg_bytes)
                    self.wfile.write(b"\r\n")
                    time.sleep(0.03)  # ~30 fps cap
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    break

        elif path == "/api/status":
            incoming_count = len(list(INCOMING_DIR.glob("*.*")))
            gopro.query_camera_status()
            data = {
                "status": "online",
                "incoming_count": incoming_count,
                "gopro": {
                    "camera_ip": gopro.camera_ip,
                    "is_connected": gopro.is_connected,
                    "is_streaming": gopro.is_streaming,
                    "battery": gopro.battery_level,
                    "fps": gopro.current_fps,
                    "width": gopro.frame_width,
                    "height": gopro.frame_height
                },
                "ai_engine": api_checker.get_status_dict(),
                "queue": queue_mgr.get_stats(),
                "scheduler": snapshot_engine.get_status_dict()
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))

        elif path == "/api/sensor_telemetry" or path == "/api/sensors":
            telemetry = sensor_mgr.get_telemetry()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(telemetry).encode("utf-8"))

        elif path == "/api/diagnoses":
            diagnoses_list = []
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.execute("""
                        SELECT id, timestamp, filename, destination_folder, health_status,
                               primary_condition, confidence_score, severity_level,
                               visual_symptoms, recommended_action, quarantine_required
                        FROM diagnoses
                        ORDER BY id DESC LIMIT 50
                    """)
                    for r in cursor.fetchall():
                        filename = r[2]
                        dest_folder = r[3] or "processed_images"
                        if "pending" in dest_folder:
                            prefix = "/images/pending/"
                        elif "processed" in dest_folder:
                            prefix = "/images/processed/"
                        else:
                            prefix = "/images/unrelated/"
                        diagnoses_list.append({
                            "id": r[0],
                            "timestamp": r[1],
                            "filename": filename,
                            "image_url": prefix + urllib.parse.quote(filename),
                            "health_status": r[4],
                            "primary_condition": r[5],
                            "confidence_score": round(r[6] or 0.0, 2),
                            "severity_level": r[7],
                            "visual_symptoms": r[8],
                            "recommended_action": r[9],
                            "quarantine_required": bool(r[10])
                        })
            except Exception as e:
                print(f"[API Diagnoses] Query error: {e}", flush=True)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(diagnoses_list).encode("utf-8"))

        elif path.startswith("/images/processed/"):
            filename = urllib.parse.unquote(path[len("/images/processed/"):])
            filepath = PROCESSED_DIR / filename
            self._serve_file(filepath)

        elif path.startswith("/images/pending/"):
            filename = urllib.parse.unquote(path[len("/images/pending/"):])
            filepath = PENDING_DIR / filename
            self._serve_file(filepath)

        elif path.startswith("/images/unrelated/"):
            filename = urllib.parse.unquote(path[len("/images/unrelated/"):])
            filepath = UNRELATED_DIR / filename
            self._serve_file(filepath)

        else:
            self.send_response(404)
            self.end_headers()

    def _serve_file(self, filepath: Path):
        if filepath.exists() and filepath.is_file():
            mime_type, _ = mimetypes.guess_type(str(filepath))
            mime_type = mime_type or "image/jpeg"
            try:
                with open(filepath, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", mime_type)
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                self.wfile.write(content)
                return
            except Exception:
                pass
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path == "/api/capture":
            content_length = int(self.headers.get("Content-Length", 0))
            post_body = self.rfile.read(content_length)
            try:
                payload = json.loads(post_body.decode("utf-8"))
                filename = payload.get("filename", "")
                use_native_raw = payload.get("use_native_raw", False)
                image_data = payload.get("image_data", "")

                if not filename:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"gopro11_{ts}.jpg"

                target_path = INCOMING_DIR / filename

                if use_native_raw and gopro.is_streaming:
                    raw_frame = gopro.get_raw_frame()
                    if raw_frame is not None:
                        cv2.imwrite(str(target_path), raw_frame, [cv2.IMWRITE_JPEG_QUALITY, 95])
                        size_bytes = os.path.getsize(target_path)
                        queue_mgr.enqueue(target_path)
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [RAW ACQUISITION] Specimen '{filename}' ({size_bytes} bytes) -> incoming_images/ (enqueued)", flush=True)
                        resp = {"success": True, "filename": filename, "size_bytes": size_bytes, "path": str(target_path)}
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps(resp).encode("utf-8"))
                        return

                if "," in image_data:
                    base64_str = image_data.split(",")[1]
                else:
                    base64_str = image_data

                image_bytes = base64.b64decode(base64_str)
                with open(target_path, "wb") as f:
                    f.write(image_bytes)

                queue_mgr.enqueue(target_path)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [OPTICAL ACQUISITION] Specimen '{filename}' ({len(image_bytes)} bytes) -> incoming_images/ (enqueued)", flush=True)

                resp = {"success": True, "filename": filename, "size_bytes": len(image_bytes), "path": str(target_path)}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode("utf-8"))

            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode("utf-8"))

        elif self.path.startswith("/api/gopro_control"):
            action = "start" if "action=start" in self.path else "stop"
            if action == "start":
                success = gopro.start_stream()
            else:
                gopro.stop_stream()
                success = True

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"success": success, "action": action}).encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()


# ============================================================================
# 5. SERVER LAUNCH ENGINE
# ============================================================================
def start_server():
    gopro.start_stream()
    sensor_mgr.start()
    api_checker.start()
    queue_mgr.start()
    snapshot_engine.start()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), CameraOverseerHandler)
    url = f"http://localhost:{PORT}"
    border = "=" * 70
    print(f"\n{border}")
    print("[SCADA ENGINE] GREENHOUSE OVERSEER ENTERPRISE COCKPIT")
    print(f"{border}")
    print(f" * SCADA Endpoint    : {url}")
    print(f" * Display Standard  : 1080p (1920x1080) Full-Screen Specification")
    print(f" * Optical Driver    : GoPro HERO 11 Mini (USB CDC @ {gopro.camera_ip or 'Auto-Detect'})")
    print(f" * Telemetry Engine  : ESP32-C3 Super Mini (COM7 @ 115200 baud)")
    print(f" * Scheduled Cadence : 4 Snaps / Hour (every 15 min / 900s interval)")
    print(f" * Pipeline Queue    : Asynchronous Worker (incoming_images/ -> pending_queue/ -> processed_images/)")
    print(f" * AI API Engine     : Gemini Availability Probe & Automatic Key Pool Rotation")
    print(f" * Terminate Command : Press Ctrl+C in this console")
    print(f"{border}\n")

    threading.Timer(1.2, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SCADA Engine] Graceful shutdown dispatched...")
    finally:
        snapshot_engine.stop_signal.set()
        queue_mgr.stop_signal.set()
        api_checker.stop_signal.set()
        gopro.stop_stream()
        server.shutdown()


if __name__ == "__main__":
    start_server()
