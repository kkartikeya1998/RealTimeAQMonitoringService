"""
READER Lambda (Function URL, payload format v2)

Endpoints
---------
GET /cities          -> pollutant averages + locations for every city
GET /cities/<name>   -> the same for one city (case-insensitive)

"Average" = mean over a rolling 3-hour window, per city and pollutant,
computed from running totals (total_sum / count) in the precomputes table.

How the window is kept
----------------------
Every request first EVICTS measurements older than the window:
  - expired rows are found through the time-index GSI (oldest first)
  - each chunk is deleted AND subtracted from the totals in ONE transaction,
    so measurements and precomputes can never drift apart
Then the (cheap) totals are read and averaged.

Tables
------
measurements : PK = location_id (N), SK = "<timestamp>#<parameter>",
               gsi_pk = "all" (GSI time-index: gsi_pk + SK)
precomputes  : PK = city_key (lowercased city), SK = parameter
locations    : PK = location_id, GSI city-index on "city"
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import unquote

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# "now"    -> window ends at the current time (live traffic)
# "latest" -> window ends at the newest stored measurement (replaying old data)
WINDOW_REFERENCE = os.environ.get("WINDOW_REFERENCE", "now")
WINDOW = timedelta(hours=3)

PRIMARY_POLLUTANTS = ["pm25", "pm10", "so2", "no2", "co", "o3", "bc"]
SECONDARY_POLLUTANTS = ["pm1", "pm4", "co2", "no", "nox", "ch4", "ufp"]
# Hardcoded list from OpenAQ website. Used to build the per-parameter summaries
# returned by the API; a parameter missing here is never returned.
PARAMETERS = PRIMARY_POLLUTANTS + SECONDARY_POLLUTANTS

CITY_INDEX = "city-index"   # GSI on the locations table
TIME_INDEX = "time-index"   # GSI on the measurements table: gsi_pk="all", SK=<ts>#<parameter>
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"  # fixed width, so string order == time order

SAFETY_MS = 10_000  # stop evicting when this much Lambda time is left
CHUNK = 50          # deletes per transaction (DynamoDB limit: 100 items incl. the update)

dynamodb = boto3.resource("dynamodb")
measurements = dynamodb.Table("aws-kumar-measurements")  # PK=location_id (N), SK=<ts>#<parameter>
precomputes = dynamodb.Table("aws-kumar-precomputes")    # PK=lowercased city, SK=parameter
locations = dynamodb.Table("aws-kumar-locations")        # PK=location_id, GSI city-index

# Low-level client + serializer: transactions need typed values ({"S": ...}, {"N": ...})
ddb_client = boto3.client("dynamodb")
_ser = TypeSerializer()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _paginate(fn, **kwargs):
    """Yield all items from a paginated DynamoDB operation."""
    while True:
        resp = fn(**kwargs)
        yield from resp["Items"]
        if "LastEvaluatedKey" not in resp:
            return
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def _json_default(o):
    """Convert DynamoDB Decimal values to JSON-compatible numbers."""
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    raise TypeError(f"Cannot serialise {type(o)}")


def _response(status, body):
    """Build the Function URL response."""
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=_json_default),
    }


# ---------------------------------------------------------------------------
# Rolling window: find the cutoff, evict expired measurements
# ---------------------------------------------------------------------------

def compute_cutoff():
    """Return the cutoff timestamp string (window start), or None if there is no data."""
    if WINDOW_REFERENCE == "now":
        reference = datetime.now(timezone.utc)
    else:
        # Newest measurement overall: one query on the time-ordered GSI.
        items = measurements.query(
            IndexName=TIME_INDEX,
            KeyConditionExpression=Key("gsi_pk").eq("all"),
            ScanIndexForward=False,
            Limit=1,
        )["Items"]
        if not items:
            return None
        reference = datetime.strptime(
            items[0]["SK"].split("#")[0], TS_FORMAT
        ).replace(tzinfo=timezone.utc)
    return (reference - WINDOW).strftime(TS_FORMAT)


def _expired_pages(cutoff):
    """Yield small pages of expired items (oldest first) so the time budget is checked often."""
    kwargs = dict(
        IndexName=TIME_INDEX,
        KeyConditionExpression=Key("gsi_pk").eq("all") & Key("SK").lt(cutoff),
        # The GSI must project these attributes (projection type ALL is simplest)
        ProjectionExpression="PK, SK, city_key, #p, #v",
        ExpressionAttributeNames={"#p": "parameter", "#v": "value"},
        Limit=500,
    )
    while True:
        resp = measurements.query(**kwargs)
        if resp["Items"]:
            yield resp["Items"]
        if "LastEvaluatedKey" not in resp:
            return
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def _evict_group(city_key, parameter, items):
    """
    Delete expired measurements and subtract them from the city's totals in ONE
    transaction: either all of it happens or none of it does.
    All items must belong to the same (city, parameter) = one precomputes row.
    """
    total = sum(i["value"] for i in items)
    ddb_client.transact_write_items(TransactItems=[
        # Conditional deletes: if anyone already removed an item, the whole
        # transaction is cancelled, so nothing is ever subtracted twice.
        *[{"Delete": {
            "TableName": measurements.name,
            "Key": {"PK": _ser.serialize(i["PK"]), "SK": {"S": i["SK"]}},
            "ConditionExpression": "attribute_exists(PK)",
        }} for i in items],
        # One decrement for the whole chunk.
        # (Reset measurements and precomputes together; otherwise this would
        #  create a negative total on a missing row.)
        {"Update": {
            "TableName": precomputes.name,
            "Key": {"PK": {"S": city_key}, "SK": {"S": parameter}},
            "UpdateExpression": "ADD total_sum :s, #c :n",
            "ExpressionAttributeNames": {"#c": "count"},
            "ExpressionAttributeValues": {
                ":s": _ser.serialize(-total),
                ":n": {"N": str(-len(items))},
            },
        }},
    ])


def evict_expired(cutoff, context):
    """Remove everything older than the cutoff, within the Lambda time budget."""
    deleted = 0
    timed_out = False

    for page in _expired_pages(cutoff):
        # Group by the precomputes row each item contributes to
        
        groups = defaultdict(list)
        for item in page:
            groups[(item["city_key"], item["parameter"])].append(item)

        for (city_key, parameter), items in groups.items():
            for n in range(0, len(items), CHUNK):
                if context.get_remaining_time_in_millis() < SAFETY_MS:
                    timed_out = True
                    print(f"evict cutoff={cutoff} deleted={deleted} timed_out={timed_out}")
                    return
                chunk = items[n:n + CHUNK]
                try:
                    _evict_group(city_key, parameter, chunk)
                    deleted += len(chunk)
                except ClientError as e:
                    # Another request evicted some of these first, or the writer
                    # is touching the same totals row. Nothing was changed; the
                    # items stay in the index and are retried on the next request.
                    if e.response["Error"]["Code"] != "TransactionCanceledException":
                        raise

    print(f"evict cutoff={cutoff} deleted={deleted} timed_out={timed_out}")


# ---------------------------------------------------------------------------
# Building responses
# ---------------------------------------------------------------------------

def summarize(items):
    """Build parameter averages, counts, and units for one city (items = its precomputes rows)."""
    by_param = {i["SK"]: i for i in items}  # SK is the parameter
    out = {}
    for p in PARAMETERS:
        item = by_param.get(p)
        # Attributes may not exist yet on a fresh table, so default them
        count = int(item.get("count", 0)) if item else 0
        total = item.get("total_sum", Decimal(0)) if item else Decimal(0)
        out[p] = {
            "average": float(total / count) if count > 0 else None,  # null when no data
            "count": max(count, 0),
            "unit": item.get("unit") if item else None,
        }
    return out


def location_view(l):
    """Return the location fields exposed by the API."""
    return {
        "location_id": l["location_id"],
        "name": l.get("location"),
        "latitude": l.get("latitude"),
        "longitude": l.get("longitude"),
    }


def all_cities():
    """Return summaries and locations for all cities."""
    cities = {}  # city_key -> {"name", "locations", "items"}

    # POC: full scan of the locations table (~800 small items). Fine at this size;
    # in production, cache this or keep a per-city summary item.
    for l in _paginate(locations.scan):
        if l.get("city"):
            c = cities.setdefault(
                l["city"].strip().lower(),
                {"name": l["city"], "locations": [], "items": []},
            )
            c["locations"].append(location_view(l))

    # POC: scan the precomputed totals (cities x parameters rows), grouped by city (PK).
    for i in _paginate(precomputes.scan):
        c = cities.setdefault(
            i["PK"], {"name": i.get("city", i["PK"]), "locations": [], "items": []}
        )
        c["items"].append(i)

    return {
        c["name"]: {"parameters": summarize(c["items"]), "locations": c["locations"]}
        for c in cities.values()
    }


def one_city(name):
    """Return a city's summary, or None if it is unknown."""
    city_key = name.strip().lower()  # case-insensitive match

    # One Query returns every pollutant row of the city (PK = city_key).
    items = list(_paginate(precomputes.query, KeyConditionExpression=Key("PK").eq(city_key)))

    if items:
        display = items[0].get("city", city_key)  # original casing for the response
    else:
        # No totals rows (e.g. after a reset). The city may still be known from the
        # locations table: then it is a real city without data (empty summary below),
        # as opposed to a city that does not exist at all (None -> 404).
        # POC: a scan is fine at ~800 items.
        display = None
        for l in _paginate(locations.scan):
            if l.get("city") and l["city"].strip().lower() == city_key:
                display = l["city"]
                break
        if display is None:
            return None  # truly unknown city

    locs = [
        location_view(l)
        for l in _paginate(
            locations.query,
            IndexName=CITY_INDEX,
            KeyConditionExpression=Key("city").eq(display),
        )
    ]
    # summarize([]) gives average=None, count=0 for every parameter (known city, no data)
    return {"city": display, "parameters": summarize(items), "locations": locs}


# ---------------------------------------------------------------------------
# Handler (Lambda Function URL, payload format v2)
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    # Route the Function URL path.
    path = event.get("rawPath", "").rstrip("/")  # "/cities" or "/cities/Brussels"

    if path == "/cities":
        name = None
    elif path.startswith("/cities/"):
        name = unquote(path[len("/cities/"):])
    else:
        # Checked before any table work, so stray requests (e.g. /favicon.ico) stay cheap
        return _response(404, {"error": "Not found"})

    # POC: eviction runs on every request, so the window is always fresh.
    cutoff = compute_cutoff()
    if cutoff:
        evict_expired(cutoff, context)

    if name is None:
        return _response(200, all_cities())

    result = one_city(name)
    if result is None:
        return _response(404, {"error": f"Unknown city: {name}"})
    return _response(200, result)