"""Read side of the local stand-in storage (SQLite), matching the writer's storage.py."""
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("LOCAL_DB_PATH", BASE_DIR / "local_data" / "app.db"))

WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "3"))
# "now"    -> window ends at the current time (live traffic)
# "latest" -> window ends at the newest stored measurement (for when replaying old data)
WINDOW_REFERENCE = "latest"#os.environ.get("WINDOW_REFERENCE", "now")

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"  # same format the writer stores; sorts as text


def get_connection():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. Run the writer first (or set LOCAL_DB_PATH)."
        )
    conn = sqlite3.connect(DB_PATH, timeout=10)  # wait if the writer holds a lock
    conn.row_factory = sqlite3.Row
    return conn


def window(conn):
    """Return (as_of, cutoff) as strings in the stored timestamp format."""
    ref = datetime.now(timezone.utc)
    if WINDOW_REFERENCE == "latest":
        row = conn.execute("SELECT MAX(timestamp) AS t FROM measurements").fetchone()
        if row["t"]:
            ref = datetime.strptime(row["t"], TS_FORMAT).replace(tzinfo=timezone.utc)
    cutoff = ref - timedelta(hours=WINDOW_HOURS)
    return ref.strftime(TS_FORMAT), cutoff.strftime(TS_FORMAT)


def expire_old(conn, cutoff):
    """Remove measurements that fell out of the window (the archive keeps them)."""
    cur = conn.execute("DELETE FROM measurements WHERE timestamp < ?", (cutoff,))
    conn.commit()
    return cur.rowcount


def _stat(row):
    return {"avg": round(row["avg"], 2), "count": row["n"], "unit": row["unit"]}


def city_stats(conn, cutoff, city=None):
    """{city: {parameter: stat}}, optionally for a single city."""
    sql = """SELECT l.city AS city, m.parameter AS parameter, MIN(m.unit) AS unit,
                    AVG(m.value) AS avg, COUNT(*) AS n
             FROM measurements m JOIN locations l ON l.location_id = m.location_id
             WHERE m.timestamp >= ?"""
    args = [cutoff]
    if city is not None:
        sql += " AND l.city = ?"
        args.append(city)
    sql += " GROUP BY l.city, m.parameter ORDER BY l.city"
    out = {}
    for r in conn.execute(sql, args):
        out.setdefault(r["city"], {})[r["parameter"]] = _stat(r)
    return out


def location_stats(conn, cutoff, location_id=None):
    """{location_id: {parameter: stat}}, optionally for a single location."""
    sql = """SELECT location_id, parameter, MIN(unit) AS unit, AVG(value) AS avg, COUNT(*) AS n
             FROM measurements WHERE timestamp >= ?"""
    args = [cutoff]
    if location_id is not None:
        sql += " AND location_id = ?"
        args.append(location_id)
    sql += " GROUP BY location_id, parameter"
    out = {}
    for r in conn.execute(sql, args):
        out.setdefault(r["location_id"], {})[r["parameter"]] = _stat(r)
    return out


def list_locations(conn):
    return [dict(r) for r in conn.execute("SELECT * FROM locations ORDER BY location_id")]


def get_location(conn, location_id):
    row = conn.execute("SELECT * FROM locations WHERE location_id = ?", (location_id,)).fetchone()
    return dict(row) if row else None