"""
WRITER Lambda (SQS -> DynamoDB)

Triggered by SQS (messages originate from SNS, hence the double-JSON body).
For every OpenAQ measurement it:
  1. Stores the raw measurement            -> measurements table
  2. Adds it to the city/parameter totals  -> precomputes table
     (1 and 2 happen in ONE transaction, so they can never disagree)
  3. Upserts the location metadata         -> locations table

Tables
------
measurements : PK = location_id (N), SK = "<timestamp>#<parameter>"
               gsi_pk = "all" puts every row in the time-index GSI, which the
               reader uses to find expired rows for eviction.
precomputes  : PK = city_key (lowercased city), SK = parameter
               attributes: total_sum, count, unit, city (display name)
               -> one Query on PK returns all pollutants of a city.
               -> many locations of one city all feed the same rows.
locations    : PK = location_id, GSI city-index on "city"

Idempotency: the measurement key (location, timestamp, parameter) is unique,
so a redelivered SQS message fails the Put's condition, the whole transaction
is cancelled, and the totals are NOT incremented twice.
"""

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MEASUREMENTS_TABLE = "aws-kumar-measurements"
PRECOMPUTES_TABLE = "aws-kumar-precomputes"

dynamodb = boto3.resource("dynamodb")
locations_table = dynamodb.Table("aws-kumar-locations")

# The low-level client is needed for transactions; it expects typed values
# ({"S": ...}, {"N": ...}), which the serializer produces from Python values.
ddb_client = boto3.client("dynamodb")
_ser = TypeSerializer()

# Fixed-width UTC format: lexicographic order == chronological order.
# The reader relies on this when it compares timestamps as strings.
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_message(record):
    """Turn one SQS record into (location_data, measurement_data)."""
    # SQS body -> SNS envelope -> actual OpenAQ payload
    body = json.loads(record["body"])
    data = json.loads(body["Message"])

    location_id = data["locationId"]

    # Normalise to a fixed-width UTC string (see TS_FORMAT above)
    timestamp = (
        datetime.fromisoformat(data["date"]["utc"])
        .replace(tzinfo=timezone.utc)
        .strftime(TS_FORMAT)
    )

    coordinates = data.get("coordinates") or {}

    # DynamoDB rejects floats, so numbers go through str() into Decimal.
    location_data = {
        "location_id": location_id,
        "location": data.get("location"),
        "city": data.get("city"),
        "longitude": Decimal(str(coordinates["longitude"]))
        if coordinates.get("longitude") is not None
        else None,
        "latitude": Decimal(str(coordinates["latitude"]))
        if coordinates.get("latitude") is not None
        else None,
    }

    measurement_data = {
        "location_id": location_id,
        "timestamp": timestamp,
        "parameter": data["parameter"],
        "value": Decimal(str(data["value"])),
        "unit": data["unit"],
    }

    return location_data, measurement_data


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def save_measurement(m, city):
    """
    Atomically store a measurement and add it to its city/parameter totals.
    Duplicates are silently ignored (nothing is written at all).
    """
    city_key = city.strip().lower()  # case-insensitive city identity

    item = {
        "PK": m["location_id"],
        "SK": f"{m['timestamp']}#{m['parameter']}",
        "gsi_pk": "all",             # constant key -> time-index GSI (eviction scan)
        "parameter": m["parameter"],
        "location_id": m["location_id"],
        "city_key": city_key,        # lets eviction find the right totals row
        "value": m["value"],
        "unit": m["unit"],
    }

    try:
        ddb_client.transact_write_items(
            TransactItems=[
                # (1) The raw measurement. Fails if it already exists -> dedupe.
                {
                    "Put": {
                        "TableName": MEASUREMENTS_TABLE,
                        "Item": {k: _ser.serialize(v) for k, v in item.items()},
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                },
                # (2) Running totals for the city+parameter. ADD creates the
                #     attributes on first use. average = total_sum / count.
                {
                    "Update": {
                        "TableName": PRECOMPUTES_TABLE,
                        "Key": {
                            "PK": {"S": city_key},
                            "SK": {"S": m["parameter"]},
                        },
                        "UpdateExpression": (
                            "SET city = :city, #u = :unit "
                            "ADD total_sum :v, #c :one"
                        ),
                        # "count" and "unit" are DynamoDB reserved words
                        "ExpressionAttributeNames": {
                            "#c": "count",
                            "#u": "unit",
                        },
                        "ExpressionAttributeValues": {
                            ":city": {"S": city},  # original casing, for display
                            ":unit": {"S": m["unit"]},
                            ":v": _ser.serialize(m["value"]),
                            ":one": {"N": "1"},
                        },
                    }
                },
            ]
        )
    except ClientError as e:
        # Reasons are listed in the same order as TransactItems; index 0 is the Put.
        reasons = e.response.get("CancellationReasons", [])
        if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
            logger.info("Duplicate measurement, ignored")
            return
        raise  # anything else (throttling, conflict, ...) -> SQS retries the message


# ---------------------------------------------------------------------------
# Handler (SQS event source, partial batch response)
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    failures = []

    for record in event["Records"]:
        message_id = record["messageId"]

        try:
            location_data, measurement_data = parse_message(record)

            # Totals are per city, so a measurement without a city cannot be used
            city = location_data["city"]
            if not city:
                logger.warning("Skipping message %s: no city", message_id)
                continue

            save_measurement(measurement_data, city)
            locations_table.put_item(Item=location_data)

        except Exception:
            logger.exception("Failed processing message %s", message_id)
            # Only this message is retried; the rest of the batch is acknowledged
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}