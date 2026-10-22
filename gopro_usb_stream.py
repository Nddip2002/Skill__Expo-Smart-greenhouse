"""
GoPro HERO 11 Mini - USB Web Streaming Server
============================================
Interfaces directly with GoPro Hero 11 Mini over USB-C (Open GoPro Protocol).
- Auto-detects GoPro USB Network Interface (172.2x.xxx.51)
- Starts UDP preview stream (1280x960 H.264)
- Keeps connection alive with periodic UDP keepalive packets
- Streams live video via HTTP MJPEG to web browsers (/video_feed)
- Saves full-res snapshots to incoming_images/ for downstream AI processing
"""

import json
import os
import re
import socket
import sys
import threading
import time
import urllib.request
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import cv2
import psutil

# Terminal encoding for Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Paths & Settings
BASE_DIR = Path(__file__).resolve().parent
INCOMING_DIR = BASE_DIR / "incoming_images"
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

STREAM_PORT = 5005
UDP_PORT = 8554
KEEPALIVE_INTERVAL = 2.0  # seconds


class GoProUSBManager:
    """Manages the USB Open GoPro session, keepalive loop, and frame capture."""

    def __init__(self):
        self.camera_ip = None
        self.is_connected = False
        self.is_streaming = False
        self.battery_level = "N/A"
        self.current_fps = 0.0
        self.frame_width = 0
        self.frame_height = 0
        self.latest_frame = None
        self.lock = threading.Lock()

        # Thread controls
        self.stop_signal = threading.Event()
        self.keepalive_thread = None
        self.capture_thread = None

        # Auto-detect camera IP
        self.detect_camera_ip()

    def detect_camera_ip(self):
        """Find the GoPro USB IP address from active network interfaces (172.2X.YYY.51)."""
        print("[GoPro USB] Scanning network interfaces for GoPro USB connection...", flush=True)
        found_ip = None

        try:
            # Check all network interfaces
            for iface_name, addrs in psutil.net_if_addrs().items():
                for addr in addrs:
                    if addr.family == socket.AF_INET:
                        ip = addr.address
                        # GoPro assigns 172.20-29.xxx.52 to host, camera is .51
                        if re.match(r"^172\.(2[0-9])\.\d+\.\d+$", ip):
                            parts = ip.split(".")
                            candidate_camera_ip = f"{parts[0]}.{parts[1]}.{parts[2]}.51"
                            print(f"[GoPro USB] Detected USB adapter '{iface_name}' at {ip}. Checking camera at {candidate_camera_ip}...", flush=True)
                            if self.ping_gopro(candidate_camera_ip):
                                found_ip = candidate_camera_ip
                                break
                if found_ip:
                    break
        except Exception as e:
            print(f"[GoPro USB] Error scanning interfaces: {e}", flush=True)

        if not found_ip:
            # Fallback check common IP
            default_candidate = "172.23.171.51"
            if self.ping_gopro(default_candidate):
                found_ip = default_candidate

        if found_ip:
            self.camera_ip = found_ip
            self.is_connected = True
            print(f"[GoPro USB] Camera verified online at: http://{self.camera_ip}:8080", flush=True)
            self.query_camera_status()
        else:
            print("[GoPro USB] GoPro not detected yet. Connect via USB-C and ensure camera is powered on.", flush=True)
            self.is_connected = False

        return self.camera_ip

    def ping_gopro(self, ip):
        """Send quick test request to verify GoPro API responsiveness."""
        url = f"http://{ip}:8080/gopro/version"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                return resp.status == 200
        except Exception:
            return False

    def query_camera_status(self):
        """Query camera state (battery, etc.) over USB."""
        if not self.camera_ip:
            return
        try:
            url = f"http://{self.camera_ip}:8080/gopro/camera/state"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                status = data.get("status", {})
                # Status 70 is internal battery percentage in Hero 11
                if "70" in status:
                    self.battery_level = f"{status['70']}%"
                elif 70 in status:
                    self.battery_level = f"{status[70]}%"
        except Exception as e:
            pass

    def start_stream(self):
        """Command GoPro to start UDP video stream and begin frame capture."""
        if self.is_streaming:
            print("[GoPro USB] Stream already running.", flush=True)
            return True

        if not self.camera_ip:
            self.detect_camera_ip()
            if not self.camera_ip:
                print("[GoPro USB] Cannot start stream: Camera IP not found.", flush=True)
                return False

        print(f"[GoPro USB] Requesting stream start from http://{self.camera_ip}:8080/gopro/camera/stream/start ...", flush=True)
        try:
            url = f"http://{self.camera_ip}:8080/gopro/camera/stream/start"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=4.0) as resp:
                if resp.status != 200:
                    print(f"[GoPro USB] Stream start returned HTTP {resp.status}", flush=True)
                    return False
        except Exception as e:
            print(f"[GoPro USB] Error calling stream/start: {e}", flush=True)
            return False

        self.stop_signal.clear()
        self.is_streaming = True

        # Launch UDP Keepalive thread
        self.keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self.keepalive_thread.start()

        # Launch Video Capture thread
        self.capture_thread = threading.Thread(target=self._capture_loop, daemon=True)
        self.capture_thread.start()

        print("[GoPro USB] Stream initialized successfully!", flush=True)
        return True

    def _keepalive_loop(self):
        """Periodically send UDP keepalive packets to GoPro to prevent stream timeout."""
        payload = b"_GPHD_:0:0:2:0.000000\n"
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        while not self.stop_signal.is_set():
            try:
                sock.sendto(payload, (self.camera_ip, UDP_PORT))
            except Exception as e:
                print(f"[GoPro Keepalive Error]: {e}", flush=True)
            self.stop_signal.wait(KEEPALIVE_INTERVAL)

        sock.close()

    def _capture_loop(self):
        """Read UDP stream using OpenCV and store latest frame."""
        stream_url = f"udp://0.0.0.0:{UDP_PORT}?overrun_nonfatal=1&fifo_size=50000000"
        print(f"[GoPro USB] Opening video receiver on {stream_url} ...", flush=True)

        cap = cv2.VideoCapture(stream_url, cv2.CAP_FFMPEG)
        if not cap.isOpened():
            print("[GoPro USB] Failed to open OpenCV VideoCapture for UDP stream.", flush=True)
            self.is_streaming = False
            return

        fps_counter = 0
        fps_start_time = time.time()

        while not self.stop_signal.is_set():
            ret, frame = cap.read()
            if not ret or frame is None:
                time.sleep(0.01)
                continue

            with self.lock:
                self.latest_frame = frame.copy()
                self.frame_height, self.frame_width = frame.shape[:2]

            fps_counter += 1
            elapsed = time.time() - fps_start_time
            if elapsed >= 1.0:
                self.current_fps = round(fps_counter / elapsed, 1)
                fps_counter = 0
                fps_start_time = time.time()

        cap.release()
        print("[GoPro USB] Capture loop stopped.", flush=True)

    def stop_stream(self):
        """Stop capture, cancel keepalive, and instruct camera to halt stream."""
        if not self.is_streaming:
            return

        print("[GoPro USB] Stopping stream...", flush=True)
        self.stop_signal.set()
        self.is_streaming = False

        if self.camera_ip:
            try:
                url = f"http://{self.camera_ip}:8080/gopro/camera/stream/stop"
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    pass
            except Exception:
                pass

        if self.capture_thread and self.capture_thread.is_alive():
            self.capture_thread.join(timeout=1.0)
        if self.keepalive_thread and self.keepalive_thread.is_alive():
            self.keepalive_thread.join(timeout=1.0)

        print("[GoPro USB] Stream stopped cleanly.", flush=True)

    def get_latest_jpeg(self, quality=85):
        """Encode current frame as JPEG bytes."""
        with self.lock:
            if self.latest_frame is None:
                return None
            frame = self.latest_frame.copy()

        ret, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
        if ret:
            return buffer.tobytes()
        return None

    def capture_snapshot(self, filename=None):
        """Save clean full-resolution snapshot directly to incoming_images/."""
        with self.lock:
            if self.latest_frame is None:
                return None, "No active frame received from camera yet"
            frame = self.latest_frame.copy()

        if not filename:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"gopro11_snap_{ts}.jpg"

        target_path = INCOMING_DIR / filename
        cv2.imwrite(str(target_path), frame, [cv2.IMWRITE_JPEG_QUALITY, 95])
        return str(target_path), filename


# Global Manager Instance
gopro_mgr = GoProUSBManager()


# Modern Glassmorphic Web Interface
HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GoPro Hero 11 Mini // USB Live Stream Overseer</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-dark: #080c12;
      --bg-card: rgba(15, 23, 36, 0.75);
      --bg-glass: rgba(20, 31, 48, 0.65);
      --border-glass: rgba(255, 255, 255, 0.08);
      --border-glow: rgba(0, 229, 255, 0.25);
      --accent-blue: #00d2ff;
      --accent-cyan: #06b6d4;
      --accent-green: #10b981;
      --accent-amber: #f59e0b;
      --accent-red: #ef4444;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --font-ui: 'Outfit', sans-serif;
      --font-mono: 'JetBrains Mono', monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background-color: var(--bg-dark);
      background-image: 
        radial-gradient(circle at 10% 20%, rgba(0, 210, 255, 0.06) 0%, transparent 45%),
        radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.05) 0%, transparent 45%);
      color: var(--text-main);
      font-family: var(--font-ui);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }

    header {
      background: rgba(12, 19, 30, 0.85);
      backdrop-filter: blur(16px);
      border-bottom: 1px solid var(--border-glass);
      padding: 1rem 2rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 100;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 0.9rem;
    }

    .brand-icon {
      width: 40px;
      height: 40px;
      background: linear-gradient(135deg, #00d2ff, #0070f3);
      border-radius: 10px;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 0 15px rgba(0, 210, 255, 0.35);
      font-size: 1.25rem;
    }

    .brand-titles h1 {
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: -0.01em;
      background: linear-gradient(90deg, #ffffff, #cbd5e1);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }

    .brand-titles p {
      font-size: 0.72rem;
      color: var(--text-muted);
      font-family: var(--font-mono);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .top-badges {
      display: flex;
      align-items: center;
      gap: 1rem;
    }

    .badge {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid var(--border-glass);
      padding: 0.4rem 0.85rem;
      border-radius: 20px;
      font-size: 0.75rem;
      font-family: var(--font-mono);
    }

    .badge-green { border-color: rgba(16, 185, 129, 0.3); color: var(--accent-green); }
    .badge-blue { border-color: rgba(0, 210, 255, 0.3); color: var(--accent-blue); }

    .pulse-dot {
      width: 8px;
      height: 8px;
      background: currentColor;
      border-radius: 50%;
      box-shadow: 0 0 8px currentColor;
      animation: pulse 1.6s infinite;
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.3; transform: scale(0.8); }
    }

    main {
      flex: 1;
      display: grid;
      grid-template-columns: 1fr 340px;
      gap: 1.5rem;
      padding: 1.5rem 2rem;
      max-width: 1600px;
      margin: 0 auto;
      width: 100%;
    }

    .stream-panel {
      display: flex;
      flex-direction: column;
      gap: 1rem;
    }

    .viewport-box {
      background: #000;
      border: 1px solid var(--border-glass);
      border-radius: 16px;
      position: relative;
      overflow: hidden;
      aspect-ratio: 4 / 3;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.7);
    }

    #streamImg {
      width: 100%;
      height: 100%;
      object-fit: contain;
      display: block;
    }

    .hud-overlay {
      position: absolute;
      inset: 0;
      pointer-events: none;
      padding: 1.25rem;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .hud-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .hud-tag {
      background: rgba(0, 0, 0, 0.65);
      backdrop-filter: blur(8px);
      border: 1px solid rgba(255, 255, 255, 0.1);
      padding: 0.35rem 0.75rem;
      border-radius: 6px;
      font-family: var(--font-mono);
      font-size: 0.75rem;
      color: #fff;
    }

    .hud-rec {
      display: flex;
      align-items: center;
      gap: 0.4rem;
      color: #ef4444;
      font-weight: 700;
    }

    .rec-circle {
      width: 8px;
      height: 8px;
      background: #ef4444;
      border-radius: 50%;
      box-shadow: 0 0 6px #ef4444;
      animation: pulse 1s infinite;
    }

    .sidebar {
      display: flex;
      flex-direction: column;
      gap: 1.2rem;
    }

    .card {
      background: var(--bg-card);
      backdrop-filter: blur(12px);
      border: 1px solid var(--border-glass);
      border-radius: 14px;
      padding: 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.9rem;
    }

    .card h3 {
      font-size: 0.85rem;
      text-transform: uppercase;
      font-family: var(--font-mono);
      color: var(--text-muted);
      letter-spacing: 0.05em;
      border-bottom: 1px solid var(--border-glass);
      padding-bottom: 0.5rem;
    }

    .stat-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.85rem;
    }

    .stat-label { color: var(--text-muted); }
    .stat-value { font-family: var(--font-mono); font-weight: 600; color: #fff; }

    .btn {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.6rem;
      padding: 0.85rem 1.25rem;
      border-radius: 10px;
      border: none;
      font-family: var(--font-ui);
      font-weight: 600;
      font-size: 0.92rem;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    .btn-primary {
      background: linear-gradient(135deg, #00d2ff, #0070f3);
      color: #fff;
      box-shadow: 0 4px 15px rgba(0, 210, 255, 0.35);
    }

    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(0, 210, 255, 0.5);
    }

    .btn-danger {
      background: rgba(239, 68, 68, 0.15);
      border: 1px solid rgba(239, 68, 68, 0.3);
      color: #ef4444;
    }

    .btn-danger:hover {
      background: rgba(239, 68, 68, 0.25);
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.06);
      border: 1px solid var(--border-glass);
      color: var(--text-main);
    }

    .btn-secondary:hover {
      background: rgba(255, 255, 255, 0.1);
    }

    #snapToast {
      display: none;
      background: rgba(16, 185, 129, 0.2);
      border: 1px solid rgba(16, 185, 129, 0.4);
      color: var(--accent-green);
      padding: 0.65rem 0.9rem;
      border-radius: 8px;
      font-size: 0.8rem;
      font-family: var(--font-mono);
      animation: fadeIn 0.3s ease;
    }

    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(-5px); }
      to { opacity: 1; transform: translateY(0); }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="brand-icon">📹</div>
      <div class="brand-titles">
        <h1>GoPro HERO 11 Mini // USB Live Stream</h1>
        <p>Direct USB-C Wired Stream &bull; Open GoPro Pipeline</p>
      </div>
    </div>
    <div class="top-badges">
      <div class="badge badge-blue">
        <span class="pulse-dot"></span>
        <span id="badgeUsb">USB: <span id="cameraIpVal">Checking...</span></span>
      </div>
      <div class="badge badge-green">
        <span class="pulse-dot"></span>
        <span id="badgeBattery">BATTERY: --%</span>
      </div>
    </div>
  </header>

  <main>
    <div class="stream-panel">
      <div class="viewport-box">
        <img id="streamImg" src="/video_feed" alt="GoPro Live Feed">
        <div class="hud-overlay">
          <div class="hud-header">
            <div class="hud-tag hud-rec">
              <span class="rec-circle"></span> LIVE USB FEED
            </div>
            <div class="hud-tag" id="hudRes">1280 &times; 960</div>
          </div>
          <div class="hud-tag" style="align-self: flex-start;" id="hudFps">FPS: 0.0</div>
        </div>
      </div>
    </div>

    <div class="sidebar">
      <div class="card">
        <h3>Camera Telemetry</h3>
        <div class="stat-row">
          <span class="stat-label">Device Model</span>
          <span class="stat-value">HERO 11 Mini</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Connection</span>
          <span class="stat-value" id="valConn">USB Wired</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Target IP</span>
          <span class="stat-value" id="valIp">--</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Resolution</span>
          <span class="stat-value" id="valRes">1280x960</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Stream Rate</span>
          <span class="stat-value" id="valFps">0.0 fps</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Battery</span>
          <span class="stat-value" id="valBattery">--%</span>
        </div>
      </div>

      <div class="card">
        <h3>Stream Controls</h3>
        <button class="btn btn-primary" id="btnSnap" onclick="takeSnapshot()">
          📸 Capture Snapshot
        </button>
        <button class="btn btn-secondary" id="btnToggle" onclick="toggleStream()">
          ⏹ Pause / Resume Stream
        </button>
        <div id="snapToast">Snapshot saved to incoming_images/</div>
      </div>

      <div class="card">
        <h3>Pipeline Integration</h3>
        <p style="font-size: 0.8rem; color: var(--text-muted); line-height: 1.4;">
          Snapshots taken from this GoPro stream are automatically written directly to:
          <code style="display:block; margin-top:0.4rem; padding:0.4rem; background:rgba(0,0,0,0.4); border-radius:4px; font-family:var(--font-mono); color:var(--accent-blue);">incoming_images/</code>
        </p>
        <p style="font-size: 0.75rem; color: var(--text-muted); margin-top: 0.4rem;">
          Shortcut: Press <b>Spacebar</b> anywhere to capture a snapshot.
        </p>
      </div>
    </div>
  </main>

  <script>
    let isStreaming = true;

    async function updateStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        
        document.getElementById('cameraIpVal').textContent = data.camera_ip || 'Disconnected';
        document.getElementById('valIp').textContent = data.camera_ip || '--';
        document.getElementById('badgeBattery').textContent = 'BATTERY: ' + (data.battery || '--%');
        document.getElementById('valBattery').textContent = data.battery || '--%';
        document.getElementById('hudFps').textContent = 'FPS: ' + data.fps;
        document.getElementById('valFps').textContent = data.fps + ' fps';
        
        if (data.width > 0 && data.height > 0) {
          const resStr = data.width + ' × ' + data.height;
          document.getElementById('hudRes').textContent = resStr;
          document.getElementById('valRes').textContent = resStr;
        }

        isStreaming = data.is_streaming;
        document.getElementById('btnToggle').textContent = isStreaming ? '⏹ Stop Stream' : '▶ Start Stream';
      } catch (e) {
        console.error('Status fetch error:', e);
      }
    }

    async function takeSnapshot() {
      const btn = document.getElementById('btnSnap');
      btn.disabled = true;
      btn.textContent = '⏳ Saving...';
      
      try {
        const res = await fetch('/api/snapshot', { method: 'POST' });
        const data = await res.json();
        if (data.success) {
          const toast = document.getElementById('snapToast');
          toast.textContent = '📸 Saved: ' + data.filename;
          toast.style.display = 'block';
          setTimeout(() => { toast.style.display = 'none'; }, 3500);
        } else {
          alert('Snapshot error: ' + (data.error || 'Unknown'));
        }
      } catch (err) {
        alert('Snapshot request failed: ' + err);
      } finally {
        btn.disabled = false;
        btn.textContent = '📸 Capture Snapshot';
      }
    }

    async function toggleStream() {
      const action = isStreaming ? 'stop' : 'start';
      await fetch('/api/control?action=' + action, { method: 'POST' });
      const img = document.getElementById('streamImg');
      if (action === 'start') {
        img.src = '/video_feed?' + Date.now();
      }
      setTimeout(updateStatus, 500);
    }

    // Spacebar shortcut
    window.addEventListener('keydown', (e) => {
      if (e.code === 'Space' && e.target.tagName !== 'INPUT') {
        e.preventDefault();
        takeSnapshot();
      }
    });

    setInterval(updateStatus, 1500);
    window.addEventListener('load', updateStatus);
  </script>
</body>
</html>
"""


class GoProStreamHandler(BaseHTTPRequestHandler):
    """Serves the MJPEG stream, web dashboard, and API endpoints."""

    def log_message(self, format, *args):
        pass  # Suppress noisy HTTP logs

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))

        elif self.path.startswith("/video_feed"):
            # Multi-part MJPEG video stream
            self.send_response(200)
            self.send_header("Content-Type", "multipart/x-mixed-replace; boundary=frame")
            self.send_header("Cache-Control", "no-cache, private")
            self.send_header("Pragma", "no-cache")
            self.end_headers()

            while gopro_mgr.is_streaming:
                jpeg_bytes = gopro_mgr.get_latest_jpeg(quality=80)
                if jpeg_bytes is None:
                    time.sleep(0.02)
                    continue

                try:
                    self.wfile.write(b"--frame\r\n")
                    self.wfile.write(b"Content-Type: image/jpeg\r\n\r\n")
                    self.wfile.write(jpeg_bytes)
                    self.wfile.write(b"\r\n")
                    time.sleep(0.03)  # ~30 fps cap
                except (BrokenPipeError, ConnectionResetError):
                    break

        elif self.path == "/api/status":
            gopro_mgr.query_camera_status()
            data = {
                "camera_ip": gopro_mgr.camera_ip,
                "is_connected": gopro_mgr.is_connected,
                "is_streaming": gopro_mgr.is_streaming,
                "battery": gopro_mgr.battery_level,
                "fps": gopro_mgr.current_fps,
                "width": gopro_mgr.frame_width,
                "height": gopro_mgr.frame_height
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/api/snapshot":
            path, filename = gopro_mgr.capture_snapshot()
            if path:
                resp = {"success": True, "filename": filename, "path": path}
                status_code = 200
            else:
                resp = {"success": False, "error": filename}
                status_code = 400

            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(resp).encode("utf-8"))

        elif self.path.startswith("/api/control"):
            action = "start" if "action=start" in self.path else "stop"
            if action == "start":
                success = gopro_mgr.start_stream()
            else:
                gopro_mgr.stop_stream()
                success = True

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"success": success, "action": action}).encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()


def run():
    print("=" * 64)
    print("  GoPro HERO 11 Mini - USB Web Stream Server")
    print("=" * 64)

    # Initialize stream
    success = gopro_mgr.start_stream()
    if not success:
        print("[Warning] Could not start stream immediately. Retrying in background...")

    server = ThreadingHTTPServer(("0.0.0.0", STREAM_PORT), GoProStreamHandler)
    web_url = f"http://localhost:{STREAM_PORT}"
    print(f"\n * Web Streaming UI : {web_url}")
    print(f" * MJPEG Video Feed : {web_url}/video_feed")
    print(f" * Snapshots Target : {INCOMING_DIR.name}/")
    print(" * Press Ctrl+C in terminal to stop server\n")
    print("=" * 64)

    # Open browser automatically
    threading.Timer(1.2, lambda: webbrowser.open(web_url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        gopro_mgr.stop_stream()
        server.shutdown()


if __name__ == "__main__":
    run()
