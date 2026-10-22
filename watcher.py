"""
Greenhouse Capsicum Overseer - Automation Bridge, Multi-Key Pool & Database Engine.
Monitors incoming_images/ for railing camera captures, manages multi-key Gemini API rotation,
queues files in pending_queue/ during quota exhaustion, sorts into processed_images/ or unrelated_images/,
and commits structured records to SQLite database (logs/greenhouse.db), CSV, and text log.
"""

import csv
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

# Reconfigure terminal encoding for Windows
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

from dotenv import load_dotenv
from PIL import Image
from pydantic import BaseModel, Field
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

# --- 1. DIRECTORY STRUCTURE & CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

INCOMING_DIR = BASE_DIR / "incoming_images"
PROCESSED_DIR = BASE_DIR / "processed_images"
UNRELATED_DIR = BASE_DIR / "unrelated_images"
PENDING_DIR = BASE_DIR / "pending_queue"
LOGS_DIR = BASE_DIR / "logs"

DB_PATH = LOGS_DIR / "greenhouse.db"
TEXT_LOG_PATH = LOGS_DIR / "greenhouse_log.txt"
CSV_LOG_PATH = LOGS_DIR / "diagnoses.csv"

# Models (high-throughput flash models)
PRIMARY_MODEL = "gemini-3.5-flash-lite"
FALLBACK_MODEL = "gemini-3.8-flash"

# Free tier rate limit pacing: 15 RPM = 4.0s minimum interval
MIN_REQUEST_INTERVAL_SECONDS = 4.0
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png"}


def ensure_directories() -> None:
    """Creates the managed directories if they do not exist."""
    for folder in [INCOMING_DIR, PROCESSED_DIR, UNRELATED_DIR, PENDING_DIR, LOGS_DIR]:
        folder.mkdir(parents=True, exist_ok=True)


def load_api_keys() -> List[str]:
    """
    Loads all configured Gemini API keys from environment or .env file.
    Supports GEMINI_API_KEYS (comma-separated), GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc.,
    and the standard GEMINI_API_KEY.
    """
    keys = []
    # 1. Comma-separated list
    multi = os.environ.get("GEMINI_API_KEYS", "").strip()
    if multi:
        for k in multi.split(","):
            clean_k = k.strip()
            if clean_k and clean_k not in keys:
                keys.append(clean_k)

    # 2. Numbered keys: GEMINI_API_KEY_1, GEMINI_API_KEY_2, ...
    for i in range(1, 10):
        k = os.environ.get(f"GEMINI_API_KEY_{i}", "").strip()
        if k and k not in keys:
            keys.append(k)

    # 3. Default single key
    single = os.environ.get("GEMINI_API_KEY", "").strip()
    if single and single not in keys:
        keys.append(single)

    return keys


# --- 2. STRUCTURED RESPONSE SCHEMA ---
class CapsicumDiagnosis(BaseModel):
    """Pydantic model for structured greenhouse capsicum diagnostics."""
    is_capsicum_plant: bool = Field(
        ...,
        description="True if image clearly depicts a capsicum plant, leaf, flower, or fruit. False if unrelated."
    )
    plant_part: Literal["leaf", "fruit", "stem", "flower", "whole_plant", "unrelated"] = Field(
        ...,
        description="Observed plant structure or 'unrelated' for non-crop background."
    )
    health_status: Literal["healthy", "diseased", "pest_damage", "nutrient_deficiency", "abiotic_stress", "unrelated", "pending_quota"] = Field(
        ...,
        description="Crop health condition, 'unrelated', or 'pending_quota'."
    )
    primary_condition: str = Field(
        ...,
        description="Name of pathology or status (e.g. 'Bacterial Spot', 'Blossom End Rot', 'Powdery Mildew', 'Healthy', 'Unrelated Non-Plant Subject')."
    )
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    severity_level: Literal["none", "low", "moderate", "severe", "critical", "unrelated"] = Field(...)
    visual_symptoms: List[str] = Field(default_factory=list)
    recommended_action: str = Field(...)
    quarantine_required: bool = Field(...)


# --- 3. SYSTEM PROMPT ---
CAPSICUM_SYSTEM_PROMPT = """
You are an expert greenhouse agronomic pathologist monitoring a commercial capsicum (bell pepper / chili) crop via railing cameras.
First, determine if the image actually shows a capsicum plant, leaf, fruit, flower, or stem.
- If the image is unrelated (empty railing, walkway, roof, human, tool, blur, or non-plant object), set `is_capsicum_plant = False`, `plant_part = "unrelated"`, `health_status = "unrelated"`, `primary_condition = "Unrelated Non-Plant Subject"`, and `severity_level = "unrelated"`.
- If it is a capsicum plant, diagnose condition: Bacterial Spot (Xanthomonas), Anthracnose, Powdery Mildew, Blossom End Rot, Spider Mites/Thrips, viral stunting (TMV/TSWV), or confirm healthy vigor.
Respond strictly in JSON matching the schema.
""".strip()


# --- 4. SQLITE DATABASE ENGINE ---
class DatabaseEngine:
    """Manages SQLite persistent storage for greenhouse surveillance records."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        ensure_directories()
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS diagnoses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    archived_path TEXT NOT NULL,
                    destination_folder TEXT NOT NULL,
                    is_capsicum_plant BOOLEAN NOT NULL,
                    plant_part TEXT NOT NULL,
                    health_status TEXT NOT NULL,
                    primary_condition TEXT NOT NULL,
                    confidence_score REAL NOT NULL,
                    severity_level TEXT NOT NULL,
                    visual_symptoms TEXT NOT NULL,
                    recommended_action TEXT NOT NULL,
                    quarantine_required BOOLEAN NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_diagnoses_status ON diagnoses(health_status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_diagnoses_condition ON diagnoses(primary_condition);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_diagnoses_quarantine ON diagnoses(quarantine_required);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_diagnoses_folder ON diagnoses(destination_folder);")
            conn.commit()

    def insert_diagnosis(self, filename: str, archived_path: str, dest_folder: str, diagnosis: CapsicumDiagnosis) -> int:
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        symptoms_str = "; ".join(diagnosis.visual_symptoms) if diagnosis.visual_symptoms else "None"

        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO diagnoses (
                    timestamp, filename, archived_path, destination_folder,
                    is_capsicum_plant, plant_part, health_status, primary_condition,
                    confidence_score, severity_level, visual_symptoms,
                    recommended_action, quarantine_required
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                ts, filename, archived_path, dest_folder,
                1 if diagnosis.is_capsicum_plant else 0,
                diagnosis.plant_part,
                diagnosis.health_status,
                diagnosis.primary_condition,
                round(diagnosis.confidence_score, 4),
                diagnosis.severity_level,
                symptoms_str,
                diagnosis.recommended_action,
                1 if diagnosis.quarantine_required else 0
            ))
            conn.commit()
            return cursor.lastrowid

    def update_pending_diagnosis(self, filename: str, archived_path: str, dest_folder: str, diagnosis: CapsicumDiagnosis) -> bool:
        """Updates a previously queued record when it is successfully evaluated."""
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        symptoms_str = "; ".join(diagnosis.visual_symptoms) if diagnosis.visual_symptoms else "None"

        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE diagnoses SET
                    timestamp = ?,
                    archived_path = ?,
                    destination_folder = ?,
                    is_capsicum_plant = ?,
                    plant_part = ?,
                    health_status = ?,
                    primary_condition = ?,
                    confidence_score = ?,
                    severity_level = ?,
                    visual_symptoms = ?,
                    recommended_action = ?,
                    quarantine_required = ?
                WHERE filename = ? AND destination_folder = 'pending_queue';
            """, (
                ts, archived_path, dest_folder,
                1 if diagnosis.is_capsicum_plant else 0,
                diagnosis.plant_part,
                diagnosis.health_status,
                diagnosis.primary_condition,
                round(diagnosis.confidence_score, 4),
                diagnosis.severity_level,
                symptoms_str,
                diagnosis.recommended_action,
                1 if diagnosis.quarantine_required else 0,
                filename
            ))
            conn.commit()
            return cursor.rowcount > 0

    def get_summary(self) -> Dict[str, Any]:
        with self.get_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM diagnoses").fetchone()[0]
            by_status = dict(conn.execute("SELECT health_status, COUNT(*) FROM diagnoses GROUP BY health_status").fetchall())
            quarantines = conn.execute("SELECT COUNT(*) FROM diagnoses WHERE quarantine_required = 1").fetchone()[0]
            pending_count = conn.execute("SELECT COUNT(*) FROM diagnoses WHERE destination_folder = 'pending_queue'").fetchone()[0]
            latest = conn.execute("SELECT * FROM diagnoses ORDER BY id DESC LIMIT 5").fetchall()
        return {
            "total_records": total,
            "by_status": by_status,
            "quarantine_alerts": quarantines,
            "pending_in_db": pending_count,
            "latest": [dict(r) for r in latest]
        }


db = DatabaseEngine()


# --- 5. LOGGING SERVICE ---
CSV_FIELDS = [
    "timestamp", "filename", "is_capsicum_plant", "plant_part", "health_status",
    "primary_condition", "confidence_score", "severity_level", "visual_symptoms",
    "recommended_action", "quarantine_required"
]


class LoggerService:
    def __init__(self):
        ensure_directories()
        self._init_csv()

    def _init_csv(self):
        if not CSV_LOG_PATH.exists() or CSV_LOG_PATH.stat().st_size == 0:
            with open(CSV_LOG_PATH, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writeheader()

    def log(self, message: str, level: str = "INFO"):
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{ts}] [{level.upper()}] {message}"
        try:
            print(entry, flush=True)
        except UnicodeEncodeError:
            print(entry.encode("ascii", errors="replace").decode("ascii"), flush=True)
        with open(TEXT_LOG_PATH, mode="a", encoding="utf-8") as f:
            f.write(entry + "\n")

    def record_diagnosis(self, filename: str, archived_path: str, dest_folder: str, diagnosis: CapsicumDiagnosis, is_update: bool = False):
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        symptoms = "; ".join(diagnosis.visual_symptoms) if diagnosis.visual_symptoms else "None"
        
        # 1. Update or Insert SQLite Database
        if is_update:
            updated = db.update_pending_diagnosis(filename, archived_path, dest_folder, diagnosis)
            if not updated:
                db.insert_diagnosis(filename, archived_path, dest_folder, diagnosis)
        else:
            db.insert_diagnosis(filename, archived_path, dest_folder, diagnosis)

        # 2. Console & text log
        status_tag = "[QUARANTINE ALERT]" if diagnosis.quarantine_required else (
            "[PENDING QUEUE]" if dest_folder == "pending_queue" else (
                "[UNRELATED]" if not diagnosis.is_capsicum_plant else (
                    "[ELEVATED THREAT]" if diagnosis.severity_level in ("severe", "moderate") else "[NORMAL]"
                )
            )
        )
        self.log(
            f"DIAGNOSIS {status_tag} for '{filename}':\n"
            f"   * Status: {diagnosis.health_status.upper()} | Condition: {diagnosis.primary_condition}\n"
            f"   * Confidence: {diagnosis.confidence_score * 100:.1f}% | Folder: {dest_folder}\n"
            f"   * Action: {diagnosis.recommended_action}",
            level="INFO"
        )

        # 3. Append to CSV
        with open(CSV_LOG_PATH, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writerow({
                "timestamp": ts,
                "filename": filename,
                "is_capsicum_plant": diagnosis.is_capsicum_plant,
                "plant_part": diagnosis.plant_part,
                "health_status": diagnosis.health_status,
                "primary_condition": diagnosis.primary_condition,
                "confidence_score": round(diagnosis.confidence_score, 4),
                "severity_level": diagnosis.severity_level,
                "visual_symptoms": symptoms,
                "recommended_action": diagnosis.recommended_action,
                "quarantine_required": diagnosis.quarantine_required,
            })


logger = LoggerService()


# --- 6. MULTI-KEY POOL & GEMINI API BRIDGE ---
class ApiKeyPool:
    """Manages a pool of Gemini API keys with automatic failover and exhaustion tracking."""

    def __init__(self, keys: List[str]):
        self.keys = [k for k in keys if k and k != "your_gemini_api_key_here"]
        self.current_idx = 0
        self.exhausted_keys: Set[str] = set()

    def has_keys(self) -> bool:
        return len(self.keys) > 0

    def get_active_key(self) -> Optional[str]:
        """Returns the current active, non-exhausted key from the pool, or None if all exhausted."""
        available = [k for k in self.keys if k not in self.exhausted_keys]
        if not available:
            return None
        # Return currently selected or cycle to next available
        active = self.keys[self.current_idx % len(self.keys)]
        if active in self.exhausted_keys:
            # Find next available
            for idx, k in enumerate(self.keys):
                if k not in self.exhausted_keys:
                    self.current_idx = idx
                    return k
        return active

    def mark_exhausted(self, key: str, error_detail: str = "") -> Optional[str]:
        """Marks a key as exhausted and rotates to the next available key."""
        self.exhausted_keys.add(key)
        key_num = self.keys.index(key) + 1 if key in self.keys else "?"
        logger.log(f"[API KEY #{key_num} EXHAUSTED] Quota limit reached ({error_detail[:60]}...).", level="WARNING")

        available = [k for k in self.keys if k not in self.exhausted_keys]
        if available:
            next_key = available[0]
            self.current_idx = self.keys.index(next_key)
            logger.log(f"[KEY ROTATION] Seamlessly switched to API Key #{self.current_idx + 1} ({len(available)} available in pool).", level="SUCCESS")
            return next_key
        else:
            logger.log("[ALL API KEYS EXHAUSTED] All configured API keys have exhausted their daily quota!", level="ERROR")
            return None

    def reset_exhaustion(self):
        """Resets exhausted keys pool (e.g. for a new day or manual retry)."""
        self.exhausted_keys.clear()


class GeminiBridge:
    def __init__(self):
        self.last_call_time = 0.0
        self.key_pool = ApiKeyPool(load_api_keys())

        if self.key_pool.has_keys():
            logger.log(f"API Key Pool initialized with {len(self.key_pool.keys)} key(s). Active Key: #1", level="SUCCESS")
        else:
            logger.log("No valid GEMINI_API_KEY found! Captures will queue in pending_queue/.", level="WARNING")

    def _wait_pacing(self):
        elapsed = time.time() - self.last_call_time
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            wait = MIN_REQUEST_INTERVAL_SECONDS - elapsed
            logger.log(f"Pacing {wait:.2f}s for 15 RPM free tier limit...", level="INFO")
            time.sleep(wait)
        self.last_call_time = time.time()

    def _is_quota_error(self, err_msg: str) -> bool:
        low = err_msg.lower()
        return "429" in err_msg or "resource_exhausted" in low or "quota" in low or "exhausted" in low

    def analyze(self, path: Path) -> Tuple[bool, Optional[CapsicumDiagnosis], Optional[str]]:
        """
        Attempts evaluation using the active API key.
        Rotates automatically on 429 quota exhaustion.
        Returns:
            (success: bool, diagnosis: Optional[CapsicumDiagnosis], error_type: Optional[str])
            error_type can be 'ALL_KEYS_EXHAUSTED' or other.
        """
        self._wait_pacing()

        active_key = self.key_pool.get_active_key()
        if not active_key:
            return False, None, "ALL_KEYS_EXHAUSTED"

        from google import genai
        from google.genai import types

        ext = path.suffix.lower()
        mime = "image/png" if ext == ".png" else "image/jpeg"

        with open(path, "rb") as f:
            data = f.read()

        part = types.Part.from_bytes(data=data, mime_type=mime)
        prompt = (
            "Analyze this greenhouse surveillance capture. "
            "Determine if it is a capsicum plant. If yes, evaluate health, identify condition, "
            "and recommend agronomic remedies. Return JSON adhering to schema."
        )

        config = types.GenerateContentConfig(
            system_instruction=CAPSICUM_SYSTEM_PROMPT,
            response_mime_type="application/json",
            response_schema=CapsicumDiagnosis,
            temperature=0.2,
        )

        # Loop through keys if exhaustion occurs
        while active_key:
            client = genai.Client(api_key=active_key, http_options=types.HttpOptions(timeout=45000))
            for model in [PRIMARY_MODEL, FALLBACK_MODEL]:
                try:
                    res = client.models.generate_content(
                        model=model,
                        contents=[part, prompt],
                        config=config,
                    )
                    clean_json = re.sub(r"^```(?:json)?\s*|\s*```$", "", (res.text or "").strip(), flags=re.MULTILINE)
                    diagnosis = CapsicumDiagnosis.model_validate_json(clean_json)
                    return True, diagnosis, None
                except Exception as model_err:
                    err_str = str(model_err)
                    if self._is_quota_error(err_str):
                        # Rotate key in pool!
                        active_key = self.key_pool.mark_exhausted(active_key, err_str)
                        break  # Break inner model loop, retry with next key
                    logger.log(f"Model {model} note: {err_str[:80]}", level="WARNING")
                    continue
            else:
                # If neither model succeeded and it wasn't quota, return failure
                break

        if not self.key_pool.get_active_key():
            return False, None, "ALL_KEYS_EXHAUSTED"

        return False, None, "API_CALL_FAILED"


# --- 7. FOLDER WATCHER & FILE ROUTING ---
def validate_image(filepath: Path) -> bool:
    try:
        with Image.open(filepath) as img:
            img.verify()
        with Image.open(filepath) as img:
            img.load()
        return True
    except Exception:
        return False


def get_unique_path(dest_dir: Path, source_path: Path) -> Path:
    dest = dest_dir / source_path.name
    if not dest.exists():
        return dest
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dest_dir / f"{source_path.stem}_{ts}{source_path.suffix}"


class GreenhouseWatcher:
    def __init__(self):
        ensure_directories()
        self.bridge = GeminiBridge()
        self.active_files: Set[str] = set()

    def process_file(self, filepath: Path, is_from_pending: bool = False) -> bool:
        if not filepath.exists() or filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
            return False

        abs_key = str(filepath.resolve())
        if abs_key in self.active_files:
            return False

        self.active_files.add(abs_key)
        try:
            filename = filepath.name
            logger.log(f"Processing capture: '{filename}'", level="INFO")

            # Validate integrity
            if not validate_image(filepath):
                logger.log(f"Unreadable/corrupt image '{filename}' -> routing to unrelated_images/", level="WARNING")
                dest = get_unique_path(UNRELATED_DIR, filepath)
                shutil.move(str(filepath), str(dest))
                return False

            success, diagnosis, err_type = self.bridge.analyze(filepath)

            if success and diagnosis:
                # Route to unrelated_images if not a capsicum plant capture
                if not diagnosis.is_capsicum_plant or diagnosis.health_status == "unrelated":
                    dest = get_unique_path(UNRELATED_DIR, filepath)
                    shutil.move(str(filepath), str(dest))
                    logger.record_diagnosis(filename, str(dest), "unrelated_images", diagnosis, is_update=is_from_pending)
                    logger.log(f"Archived '{filename}' -> unrelated_images/", level="INFO")
                else:
                    dest = get_unique_path(PROCESSED_DIR, filepath)
                    shutil.move(str(filepath), str(dest))
                    logger.record_diagnosis(filename, str(dest), "processed_images", diagnosis, is_update=is_from_pending)
                    logger.log(f"Archived '{filename}' -> processed_images/", level="SUCCESS")
                return True

            elif err_type == "ALL_KEYS_EXHAUSTED":
                # Safely hold photo in pending_queue/ without losing it or guessing
                if not is_from_pending:
                    dest = get_unique_path(PENDING_DIR, filepath)
                    shutil.move(str(filepath), str(dest))
                    pending_diag = CapsicumDiagnosis(
                        is_capsicum_plant=True,
                        plant_part="unknown" if "unknown" in CapsicumDiagnosis.__fields__["plant_part"].annotation.__args__ else "unrelated",
                        health_status="pending_quota",
                        primary_condition="Awaiting API Quota / Re-evaluation",
                        confidence_score=0.0,
                        severity_level="low",
                        visual_symptoms=["Analysis postponed due to API quota exhaustion"],
                        recommended_action="Provide additional GEMINI_API_KEY in .env or await daily quota reset.",
                        quarantine_required=False
                    )
                    logger.record_diagnosis(filename, str(dest), "pending_queue", pending_diag, is_update=False)
                    logger.log(f"⚠️ [QUOTA EXHAUSTED] Safely moved '{filename}' -> pending_queue/. Will re-evaluate once keys reset.", level="WARNING")
                return False
            else:
                logger.log(f"Evaluation failed for '{filename}'. Keeping in incoming queue for retry.", level="WARNING")
                return False

        finally:
            self.active_files.discard(abs_key)

    def scan_pending_queue(self) -> int:
        """Processes any backlogged photos in pending_queue/ if an active key is now available."""
        if not self.bridge.key_pool.get_active_key():
            return 0
        count = 0
        for f in sorted(PENDING_DIR.iterdir()):
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS:
                if self.process_file(f, is_from_pending=True):
                    count += 1
        return count

    def scan_incoming(self) -> int:
        count = 0
        for f in sorted(INCOMING_DIR.iterdir()):
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS:
                if self.process_file(f, is_from_pending=False):
                    count += 1
        return count


class FileHandler(FileSystemEventHandler):
    def __init__(self, watcher: GreenhouseWatcher):
        self.watcher = watcher

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            p = Path(event.src_path)
            time.sleep(1.0)
            self.watcher.process_file(p)


def show_database_summary():
    """Prints a structured summary of records stored in SQLite database."""
    summary = db.get_summary()
    print("\n" + "=" * 65)
    print("🌱 GREENHOUSE CAPSICUM SURVEILLANCE DATABASE (SQLite)")
    print("=" * 65)
    print(f"• Database Path      : {DB_PATH}")
    print(f"• Total Evaluations  : {summary['total_records']}")
    print(f"• Quarantine Alerts  : {summary['quarantine_alerts']}")
    print(f"• In Pending Queue   : {summary['pending_in_db']}")
    print("• By Health Status   :")
    for status, count in summary["by_status"].items():
        print(f"   * {status.ljust(20)}: {count}")

    if summary["latest"]:
        print("\n• Recent Database Entries:")
        for r in summary["latest"]:
            print(f"   [#{r['id']}] {r['timestamp']} | {r['filename']} | {r['primary_condition']} ({r['health_status']})")
    print("=" * 65 + "\n")


def run_watcher():
    ensure_directories()
    watcher = GreenhouseWatcher()
    keys_loaded = len(watcher.bridge.key_pool.keys)

    logger.log("=" * 65, level="INFO")
    logger.log("Greenhouse Capsicum Overseer (Multi-Key Pool & Pending Queue)", level="INFO")
    logger.log(f"Database         : {DB_PATH}", level="INFO")
    logger.log(f"Keys Loaded      : {keys_loaded} key(s) in active rotation pool", level="INFO")
    logger.log(f"Incoming Folder  : {INCOMING_DIR}", level="INFO")
    logger.log(f"Pending Queue    : {PENDING_DIR}", level="INFO")
    logger.log(f"Processed Folder : {PROCESSED_DIR}", level="INFO")
    logger.log(f"Unrelated Folder : {UNRELATED_DIR}", level="INFO")
    logger.log("=" * 65, level="INFO")

    # Step 1: Drain pending_queue/ first if keys are available
    pending_drained = watcher.scan_pending_queue()
    if pending_drained > 0:
        logger.log(f"Processed {pending_drained} backlogged capture(s) from pending_queue/.", level="INFO")

    # Step 2: Scan incoming
    incoming_processed = watcher.scan_incoming()
    if incoming_processed > 0:
        logger.log(f"Processed {incoming_processed} new capture(s) from incoming_images/.", level="INFO")

    observer = Observer()
    observer.schedule(FileHandler(watcher), path=str(INCOMING_DIR), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(2.0)
            watcher.scan_pending_queue()
            watcher.scan_incoming()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--db":
        show_database_summary()
    elif len(sys.argv) > 1 and sys.argv[1] == "--once":
        w = GreenhouseWatcher()
        p = w.scan_pending_queue()
        i = w.scan_incoming()
        print(f"Processed {p} from pending_queue, {i} from incoming_images.")
        show_database_summary()
    else:
        run_watcher()
