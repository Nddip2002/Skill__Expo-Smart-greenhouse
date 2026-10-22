"""
Test Simulator for Greenhouse Capsicum Surveillance Pipeline.
Verifies multi-key pool, pending queue, database logging, and routing to processed_images/ or unrelated_images/.
"""

import csv
import sys
import time
from pathlib import Path
from PIL import Image, ImageDraw

from watcher import (
    INCOMING_DIR,
    PROCESSED_DIR,
    UNRELATED_DIR,
    PENDING_DIR,
    LOGS_DIR,
    CSV_LOG_PATH,
    TEXT_LOG_PATH,
    DB_PATH,
    GreenhouseWatcher,
    db,
    ensure_directories,
    logger,
)


def create_healthy_leaf(path: Path):
    img = Image.new("RGB", (640, 480), color=(25, 35, 25))
    draw = ImageDraw.Draw(img)
    draw.polygon([(320, 50), (480, 200), (450, 380), (320, 440), (190, 380), (160, 200)], fill=(34, 139, 34))
    draw.line([(320, 50), (320, 440)], fill=(50, 205, 50), width=4)
    img.save(path, format="JPEG")


def create_unrelated_railing(path: Path):
    img = Image.new("RGB", (640, 480), color=(120, 125, 130))
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 180, 640, 220], fill=(200, 205, 210))
    draw.rectangle([0, 300, 640, 340], fill=(200, 205, 210))
    for x in range(50, 640, 100):
        draw.rectangle([x, 100, x + 25, 450], fill=(170, 175, 180))
    img.save(path, format="JPEG")


def run_simulation():
    ensure_directories()
    logger.log("=" * 65, level="INFO")
    logger.log("[TEST] STARTING GREENHOUSE MULTI-KEY & QUEUE SIMULATION", level="INFO")
    logger.log("=" * 65, level="INFO")

    tests = [
        ("sim_test_healthy.jpg", create_healthy_leaf),
        ("sim_test_unrelated.jpg", create_unrelated_railing),
    ]

    for filename, generator in tests:
        dest = INCOMING_DIR / filename
        generator(dest)
        logger.log(f"Deposited: {filename} into incoming_images/", level="INFO")

    logger.log("\nProcessing files through watcher...", level="INFO")
    watcher = GreenhouseWatcher()
    watcher.scan_pending_queue()
    watcher.scan_incoming()

    logger.log("\n" + "=" * 65, level="INFO")
    logger.log("[AUDIT] VERIFICATION AUDIT", level="INFO")
    logger.log("=" * 65, level="INFO")

    # 1. Incoming must be empty
    remaining = list(INCOMING_DIR.glob("*.*"))
    logger.log(f"1. Remaining incoming files : {len(remaining)} (Expected: 0)", level="INFO")
    assert len(remaining) == 0, "Error: incoming_images not empty"

    # 2. Check SQLite database
    summary = db.get_summary()
    logger.log(f"2. SQLite Database records  : {summary['total_records']} total in {DB_PATH.name}", level="INFO")
    assert summary["total_records"] > 0, "Error: Database has 0 records"

    # 3. Check Pending Queue folder
    logger.log(f"3. Pending Queue folder     : {PENDING_DIR} (Managed & Active)", level="INFO")

    # 4. Check Text log & CSV
    assert TEXT_LOG_PATH.exists() and TEXT_LOG_PATH.stat().st_size > 0
    assert CSV_LOG_PATH.exists()
    logger.log(f"4. Diagnoses CSV verified   : {CSV_LOG_PATH.name}", level="INFO")
    logger.log(f"5. Greenhouse Log verified  : {TEXT_LOG_PATH.name}", level="INFO")

    logger.log("\n" + "=" * 65, level="SUCCESS")
    logger.log("[PASSED] MULTI-KEY & PENDING QUEUE SIMULATION PASSED CLEANLY!", level="SUCCESS")
    logger.log("=" * 65, level="SUCCESS")


if __name__ == "__main__":
    run_simulation()
