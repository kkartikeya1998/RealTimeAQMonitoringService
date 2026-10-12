import json
import logging
from datetime import datetime, timezone

from .storage import (
    save_location,
    save_measurement,
    archive_measurements,
)


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_message(record):
    """
    Parse the SQS message and return a tuple of (location, measurement) information.
    """

    body = json.loads(record["body"])

    data = json.loads(body["Message"])


    timestamp = datetime.fromisoformat(
        data["date"]["utc"]
    ).replace(tzinfo=timezone.utc)

    # use .get() so a missing city/country/coordinates no longer fails the whole message
    coordinates = data.get("coordinates") or {}

    location = {
        "location_id": data["locationId"],
        "location": data["location"],
        "city": data.get("city"),
        "country": data.get("country"),
        "longitude": coordinates.get("longitude"),
        "latitude": coordinates.get("latitude"),
    }

    measurement = {
        "location_id": data["locationId"],
        "timestamp": timestamp.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),
        "parameter": data["parameter"],
        "value": float(data["value"]),
        "unit": data["unit"],
    }

    return location, measurement


def lambda_handler(event, context):

    failures = []

    measurements_to_archive = []

    for record in event["Records"]:

        message_id = record["messageId"]

        try:
            location, measurement = parse_message(record)


            save_location(location)

            inserted = save_measurement(measurement)


            if inserted:
                measurements_to_archive.append(measurement)
                logger.info(
                    "Stored measurement: location=%s parameter=%s",
                    measurement["location_id"],
                    measurement["parameter"],
                )
            else:
                logger.info(
                    "Duplicate measurement: %s",
                    message_id,
                )

        except Exception:
            logger.exception(
                "Failed processing message %s",
                message_id,
            )

            failures.append({
                "itemIdentifier": message_id
            })


    if measurements_to_archive:
        archive_measurements(measurements_to_archive)

    return {
        "batchItemFailures": failures
    }