import gzip
import json
import os
import sqlite3
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_DATA = BASE_DIR / "local_data"
DB_PATH = LOCAL_DATA / "app.db"
S3_PATH = LOCAL_DATA / "s3"


def get_connection():
    LOCAL_DATA.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id INTEGER PRIMARY KEY,
            location TEXT NOT NULL,
            city TEXT,
            country TEXT,
            longitude REAL,
            latitude REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            location_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            parameter TEXT NOT NULL,
            value REAL NOT NULL,
            unit TEXT NOT NULL,

            PRIMARY KEY (
                location_id,
                timestamp,
                parameter
            )
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_measurements_time
        ON measurements(timestamp)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_measurements_location_time
        ON measurements(location_id, timestamp)
    """)

    conn.commit()

    return conn


def save_location(location):
    conn = get_connection()

    conn.execute("""
        INSERT INTO locations (
            location_id,
            location,
            city,
            country,
            longitude,
            latitude
        )
        VALUES (?, ?, ?, ?, ?, ?)

        ON CONFLICT(location_id)
        DO UPDATE SET
            location = excluded.location,
            city = excluded.city,
            country = excluded.country,
            longitude = excluded.longitude,
            latitude = excluded.latitude
    """, (
        location["location_id"],
        location["location"],
        location["city"],
        location["country"],
        location["longitude"],
        location["latitude"],
    ))

    conn.commit()
    conn.close()


def save_measurement(measurement):
    conn = get_connection()

    cursor = conn.execute("""
        INSERT OR IGNORE INTO measurements (
            location_id,
            timestamp,
            parameter,
            value,
            unit
        )
        VALUES (?, ?, ?, ?, ?)
    """, (
        measurement["location_id"],
        measurement["timestamp"],
        measurement["parameter"],
        measurement["value"],
        measurement["unit"],
    ))

    inserted = cursor.rowcount == 1

    conn.commit()
    conn.close()

    return inserted


def archive_measurements(measurements):
    """
    Local equivalent of S3.

    One JSON Lines file per day.
    """

    for measurement in measurements:

        timestamp = datetime.fromisoformat(
            measurement["timestamp"].replace("Z", "+00:00")
        )

        day_path = (
            Path("local_data")
            / "s3"
            / "measurements"
            / f"year={timestamp.year:04d}"
            / f"month={timestamp.month:02d}"
            / f"day={timestamp.day:02d}"
        )

        day_path.mkdir(parents=True, exist_ok=True)

        file_path = day_path / "measurements.jsonl"

#        with gzip.open(file_path, "at", encoding="utf-8") as f:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(measurement) + "\n")