"""
serves the last-N-hours averages from the SQLite data
written by writer/handler.py, and removes measurements older than the window.

    GET /cities               (every city with its own averages = all of Belgium)
    GET /cities/{city}
    GET /locations            (?active=true hides locations without recent data)
    GET /locations/{location_id}
"""
import json
import logging
import sqlite3
from urllib.parse import unquote

from .storage import (
    WINDOW_HOURS,
    city_stats,
    expire_old,
    get_connection,
    get_location,
    list_locations,
    location_stats,
    window,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _response(status, body):
    # CORS is added by API Gateway (deployed) or local_server.py (local), not here.
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    route = event.get("routeKey", "")
    path = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}

    try:
        conn = get_connection()
    except FileNotFoundError as e:
        return _response(503, {"error": str(e)})

    try:
        as_of, cutoff = window(conn)

        try:
            removed = expire_old(conn, cutoff)
            if removed:
                logger.info("Expired %d old measurements", removed)
        except sqlite3.Error:
            logger.exception("Expiry failed; serving the window anyway")

        if route == "GET /cities":
            result = {"cities": [
                {"city": c, "measurements": s} for c, s in city_stats(conn, cutoff).items()
            ]}

        elif route == "GET /cities/{city}":
            city = unquote(path.get("city", ""))
            stats = city_stats(conn, cutoff, city).get(city)
            if stats is None:
                return _response(404, {"error": f"No recent data for city '{city}'"})
            result = {"city": city, "measurements": stats}

        elif route == "GET /locations":
            active_only = query.get("active", "").lower() == "true"
            stats = location_stats(conn, cutoff)
            locations = []
            for loc in list_locations(conn):
                s = stats.get(loc["location_id"], {})
                if active_only and not s:
                    continue
                locations.append({**loc, "measurements": s})
            result = {"locations": locations}

        elif route == "GET /locations/{location_id}":
            raw_id = path.get("location_id", "")
            loc = get_location(conn, int(raw_id)) if raw_id.isdigit() else None
            if loc is None:
                return _response(404, {"error": f"Unknown location '{raw_id}'"})
            s = location_stats(conn, cutoff, loc["location_id"]).get(loc["location_id"], {})
            result = {**loc, "measurements": s}

        else:
            return _response(404, {"error": f"Unknown route '{route}'"})

        return _response(200, {"window_hours": WINDOW_HOURS, "as_of_utc": as_of, **result})

    except sqlite3.OperationalError as e:
        logger.exception("Database not ready")
        return _response(503, {"error": f"Database not ready ({e}). Has the writer stored anything yet?"})
    finally:
        conn.close()