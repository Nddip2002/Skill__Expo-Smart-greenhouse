"""
ESP32-C3 Super Mini Serial Telemetry Reader
============================================
Communicates with ESP32-C3 Super Mini over USB Serial (default: COM7 @ 115200 baud).
Parses incoming greenhouse sensor telemetry packets (Temperature, Humidity, Soil ADC, Light Lux),
displays formatted live telemetry, logs to CSV/JSONL, and records to SQLite (logs/greenhouse.db).

Usage:
    python esp32_reader.py                  # Standard mode (COM7, 115200)
    python esp32_reader.py --port COM7      # Explicit port
    python esp32_reader.py --raw            # Stream raw serial lines
    python esp32_reader.py --list-ports     # List all available serial ports
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import serial
    import serial.tools.list_ports
except ImportError:
    print("[ERROR] pyserial is required. Install via: pip install pyserial")
    sys.exit(1)

# Ensure UTF-8 output on Windows consoles
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

# Paths
BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
CSV_LOG_PATH = LOGS_DIR / "sensor_telemetry.csv"
JSON_LOG_PATH = LOGS_DIR / "sensor_telemetry.jsonl"
DB_PATH = LOGS_DIR / "greenhouse.db"


@dataclass
class SensorReading:
    timestamp: str
    mac_address: str
    sender_id: int
    temperature_c: Optional[float]
    humidity_pct: Optional[float]
    soil_adc: Optional[int]
    light_lux: Optional[float]
    raw_payload: str

    def to_dict(self):
        return asdict(self)


class TelemetryDatabase:
    """Manages SQLite storage for ESP32 sensor telemetry."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_telemetry(timestamp);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_sender ON sensor_telemetry(sender_id);")
            conn.commit()

    def save_reading(self, reading: SensorReading):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO sensor_telemetry (
                        timestamp, mac_address, sender_id,
                        temperature_c, humidity_pct, soil_adc,
                        light_lux, raw_payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    reading.timestamp,
                    reading.mac_address,
                    reading.sender_id,
                    reading.temperature_c,
                    reading.humidity_pct,
                    reading.soil_adc,
                    reading.light_lux,
                    reading.raw_payload
                ))
                conn.commit()
        except Exception as e:
            print(f"[DB ERROR] Could not save reading: {e}", file=sys.stderr)


class PacketParser:
    """
    Parses multi-line sensor packet blocks transmitted by ESP32-C3.
    Format example:
        ----------------------------------------
        Received from MAC: 30:ED:A0:65:82:20 [Sender ID: 1]
        Temp:       24.70 °C
        Humidity:   62.10 %
        Soil ADC:   2417
        Light:      842.0 Lux
        ----------------------------------------
    """

    RE_HEADER = re.compile(r"Received from MAC:\s*([0-9A-Fa-f:]+)\s*\[Sender ID:\s*(\d+)\]")
    RE_TEMP = re.compile(r"Temp:\s*([\d\.\-]+)")
    RE_HUMIDITY = re.compile(r"Humidity:\s*([\d\.\-]+)")
    RE_SOIL = re.compile(r"Soil ADC:\s*(\d+)")
    RE_LIGHT = re.compile(r"Light:\s*([\d\.\-]+)")

    def __init__(self):
        self.buffer = []

    def feed_line(self, line: str) -> Optional[SensorReading]:
        clean_line = line.strip()

        # Check for packet delimiter
        if clean_line.startswith("----"):
            if self.buffer:
                packet = self._parse_block(self.buffer)
                self.buffer = []
                return packet
            return None

        if clean_line:
            self.buffer.append(clean_line)

        # Also support JSON strings if sent by alternate firmware
        if clean_line.startswith("{") and clean_line.endswith("}"):
            try:
                data = json.loads(clean_line)
                ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
                return SensorReading(
                    timestamp=ts,
                    mac_address=data.get("mac", "UNKNOWN"),
                    sender_id=int(data.get("sender_id", data.get("id", 0))),
                    temperature_c=float(data["temp"]) if "temp" in data else None,
                    humidity_pct=float(data["humidity"]) if "humidity" in data else None,
                    soil_adc=int(data["soil"]) if "soil" in data else None,
                    light_lux=float(data["light"]) if "light" in data else None,
                    raw_payload=clean_line,
                )
            except Exception:
                pass

        return None

    def _parse_block(self, lines: list[str]) -> Optional[SensorReading]:
        raw_text = "\n".join(lines)
        mac = "UNKNOWN"
        sender_id = 0
        temp = None
        hum = None
        soil = None
        light = None

        found_any = False
        for line in lines:
            m_header = self.RE_HEADER.search(line)
            if m_header:
                mac = m_header.group(1)
                sender_id = int(m_header.group(2))
                found_any = True

            m_temp = self.RE_TEMP.search(line)
            if m_temp:
                try:
                    temp = float(m_temp.group(1))
                    found_any = True
                except ValueError:
                    pass

            m_hum = self.RE_HUMIDITY.search(line)
            if m_hum:
                try:
                    hum = float(m_hum.group(1))
                    found_any = True
                except ValueError:
                    pass

            m_soil = self.RE_SOIL.search(line)
            if m_soil:
                try:
                    soil = int(m_soil.group(1))
                    found_any = True
                except ValueError:
                    pass

            m_light = self.RE_LIGHT.search(line)
            if m_light:
                try:
                    light = float(m_light.group(1))
                    found_any = True
                except ValueError:
                    pass

        if found_any:
            ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
            return SensorReading(
                timestamp=ts,
                mac_address=mac,
                sender_id=sender_id,
                temperature_c=temp,
                humidity_pct=hum,
                soil_adc=soil,
                light_lux=light,
                raw_payload=raw_text,
            )
        return None


class ESP32Reader:
    """Manages serial connection and continuous reading from ESP32-C3 Super Mini."""

    def __init__(self, port: str = "COM7", baudrate: int = 115200, timeout: float = 1.0):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial_conn: Optional[serial.Serial] = None
        self.parser = PacketParser()

    def connect(self) -> bool:
        """Opens the serial port with DTR/RTS set for ESP32 CDC."""
        try:
            print(f"[*] Opening serial connection on {self.port} at {self.baudrate} baud...")
            self.serial_conn = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.timeout,
            )
            # ESP32-C3 native USB CDC requires DTR & RTS asserted
            self.serial_conn.dtr = True
            self.serial_conn.rts = True
            # Short sleep to allow CDC interface to stabilize
            time.sleep(0.3)
            self.serial_conn.reset_input_buffer()
            print(f"[+] Connected to {self.port} successfully!")
            return True
        except serial.SerialException as e:
            print(f"[!] Serial error connecting to {self.port}: {e}")
            return False
        except Exception as e:
            print(f"[!] Unexpected error opening {self.port}: {e}")
            return False

    def close(self):
        """Closes the serial connection."""
        if self.serial_conn and self.serial_conn.is_open:
            try:
                self.serial_conn.close()
                print(f"[*] Closed connection to {self.port}.")
            except Exception:
                pass
            self.serial_conn = None

    def read_lines(self) -> Generator[str, None, None]:
        """Yields clean raw lines received over serial."""
        while self.serial_conn and self.serial_conn.is_open:
            try:
                raw_bytes = self.serial_conn.readline()
                if raw_bytes:
                    line = raw_bytes.decode("utf-8", errors="replace").strip("\r\n")
                    if line:
                        yield line
            except serial.SerialException as e:
                print(f"[!] Connection lost on {self.port}: {e}")
                break
            except Exception as e:
                print(f"[!] Read error: {e}")
                time.sleep(0.1)

    def read_readings(self) -> Generator[SensorReading, None, None]:
        """Yields parsed SensorReading objects."""
        for line in self.read_lines():
            reading = self.parser.feed_line(line)
            if reading:
                yield reading


def log_reading_to_csv(reading: SensorReading, filepath: Path = CSV_LOG_PATH):
    """Appends a reading to CSV."""
    is_new = not filepath.exists() or filepath.stat().st_size == 0
    with open(filepath, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow([
                "timestamp", "mac_address", "sender_id",
                "temperature_c", "humidity_pct", "soil_adc", "light_lux"
            ])
        writer.writerow([
            reading.timestamp,
            reading.mac_address,
            reading.sender_id,
            reading.temperature_c if reading.temperature_c is not None else "",
            reading.humidity_pct if reading.humidity_pct is not None else "",
            reading.soil_adc if reading.soil_adc is not None else "",
            reading.light_lux if reading.light_lux is not None else "",
        ])


def log_reading_to_json(reading: SensorReading, filepath: Path = JSON_LOG_PATH):
    """Appends a reading as a line to JSONL."""
    with open(filepath, mode="a", encoding="utf-8") as f:
        f.write(json.dumps(reading.to_dict()) + "\n")


def print_formatted_reading(reading: SensorReading):
    """Displays a clean visual telemetry card in terminal."""
    temp_str = f"{reading.temperature_c:.2f} °C" if reading.temperature_c is not None else "N/A"
    hum_str = f"{reading.humidity_pct:.1f} %" if reading.humidity_pct is not None else "N/A"
    soil_str = f"{reading.soil_adc}" if reading.soil_adc is not None else "N/A"
    light_str = f"{reading.light_lux:.1f} Lux" if reading.light_lux is not None else "N/A"

    print("\n" + "=" * 54, flush=True)
    print(f" [SENSOR NODE {reading.sender_id}] MAC: {reading.mac_address} | {reading.timestamp}", flush=True)
    print("=" * 54, flush=True)
    print(f"  * Temperature     : {temp_str}", flush=True)
    print(f"  * Air Humidity    : {hum_str}", flush=True)
    print(f"  * Soil Moisture   : {soil_str} ADC", flush=True)
    print(f"  * Ambient Light   : {light_str}", flush=True)
    print("-" * 54, flush=True)


def list_ports():
    """Prints all detected serial ports on the system."""
    ports = serial.tools.list_ports.comports()
    if not ports:
        print("[!] No active serial COM ports detected.", flush=True)
        return
    print(f"Detected {len(ports)} Serial Port(s):", flush=True)
    for p in ports:
        print(f"  - {p.device:<8} : {p.description}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Read telemetry data from ESP32-C3 Super Mini.")
    parser.add_argument("--port", default="COM7", help="Serial port to connect (default: COM7)")
    parser.add_argument("--baud", type=int, default=115200, help="Baud rate (default: 115200)")
    parser.add_argument("--raw", action="store_true", help="Print raw incoming lines without parsing")
    parser.add_argument("--no-db", action="store_true", help="Disable SQLite database logging")
    parser.add_argument("--no-csv", action="store_true", help="Disable CSV logging")
    parser.add_argument("--list-ports", action="store_true", help="List detected COM ports and exit")
    args = parser.parse_args()

    if args.list_ports:
        list_ports()
        return

    print("=" * 64, flush=True)
    print("  ESP32-C3 SUPER MINI - GREENHOUSE TELEMETRY MONITOR", flush=True)
    print(f"  Target Port : {args.port} @ {args.baud} baud", flush=True)
    print(f"  Database    : {DB_PATH if not args.no_db else 'Disabled'}", flush=True)
    print(f"  CSV Log     : {CSV_LOG_PATH if not args.no_csv else 'Disabled'}", flush=True)
    print("  Press Ctrl+C to terminate.", flush=True)
    print("=" * 64 + "\n", flush=True)

    db = None if args.no_db else TelemetryDatabase(DB_PATH)
    reader = ESP32Reader(port=args.port, baudrate=args.baud)

    # Reconnection loop
    while True:
        if not reader.connect():
            print("[*] Retrying connection in 3 seconds... (Press Ctrl+C to stop)", flush=True)
            try:
                time.sleep(3.0)
            except KeyboardInterrupt:
                break
            continue

        try:
            if args.raw:
                print(f"[*] Streaming raw data from {args.port}...\n", flush=True)
                for line in reader.read_lines():
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {line}", flush=True)
            else:
                print(f"[*] Listening for sensor packets on {args.port}...\n", flush=True)
                for reading in reader.read_readings():
                    print_formatted_reading(reading)

                    # Save to CSV
                    if not args.no_csv:
                        log_reading_to_csv(reading)

                    # Save to SQLite
                    if db:
                        db.save_reading(reading)

                    # Save to JSONL
                    log_reading_to_json(reading)

        except KeyboardInterrupt:
            print("\n[*] Stopping reader by user request (Ctrl+C)...", flush=True)
            reader.close()
            break
        except Exception as e:
            print(f"[!] Error during read stream: {e}", flush=True)
            reader.close()
            print("[*] Reconnecting in 2 seconds...", flush=True)
            time.sleep(2.0)


if __name__ == "__main__":
    main()
