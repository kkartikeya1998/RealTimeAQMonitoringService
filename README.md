# Real-Time Air-Quality Monitoring

A serverless application that ingests OpenAQ-style air-quality measurements, maintains rolling 3-hour city averages, and serves them through a JSON API and interactive dashboard.

The repository contains two implementations of the same event-driven pipeline:

- **`cloud_simulation/`** — local simulation using SQLite and file-based storage
- **`cloud_deployment/`** — AWS deployment using Lambda, SQS, DynamoDB, and a Lambda Function URL

## Architecture

```text
OpenAQ
     |
     v
    SQS
     |
     v
Writer Lambda --> DynamoDB
     |
     v
Reader Lambda --> JSON API
     |
     v
Static dashboard
```

The writer stores each raw measurement and updates city-level running totals atomically. The reader keeps only measurements within the rolling window, removes expired data, and computes averages from precomputed totals rather than scanning raw measurements on every request.

## Local Simulation

`cloud_simulation/` mirrors the cloud workflow without AWS dependencies:

| Cloud component | Local replacement |
|---|---|
| DynamoDB | SQLite |
| S3 archive (only conceptual, implementation missing) | Date-partitioned JSONL files |
| API Gateway / Function URL | Python HTTP server |
| SQS event | Local JSON event files |

The writer consumes SQS-shaped events containing embedded SNS messages, normalizes measurements, writes them to SQLite, and archives newly inserted records as JSON Lines. The reader exposes city- and location-level rolling averages through a local HTTP API.

Measurement deduplication is handled by a composite key of `(location_id, timestamp, parameter)`, preventing duplicate events from being stored or archived twice.

## AWS Deployment

`cloud_deployment/` implements the production-oriented version:

- **Writer Lambda:** triggered by SQS, parses SNS-wrapped OpenAQ measurements, stores raw records, updates city/pollutant totals, and upserts location metadata.
- **Reader Lambda:** exposed through a Function URL, evicts expired measurements, maintains rolling-window consistency, and serves city aggregates.
- **DynamoDB:** stores measurements, locations, and precomputed city totals.
- **Dashboard:** static HTML/JavaScript client with city tables, a Leaflet map, and automatic refresh.

### Data model

| Table | Keys | Purpose |
|---|---|---|
| `measurements` | `location_id` / `<timestamp>#<parameter>` | Raw measurement records |
| `precomputes` | Lowercased city / pollutant | Running totals used for fast averages |
| `locations` | `location_id` | Sensor location metadata |

The measurements table includes a time-index GSI so the reader can efficiently find and evict measurements outside the 3-hour window.

### Consistency and idempotency

- Raw measurement insertion and running-total updates occur in one DynamoDB transaction.
- Duplicate SQS deliveries fail a conditional write and are safely ignored.
- Expired measurements are deleted and subtracted from totals in the same transaction, preventing averages and raw data from drifting apart.
- SQS partial-batch responses ensure only failed messages are retried.

## API

The deployed reader exposes:

```text
GET /cities
GET /cities/{city}
```

City names are case-insensitive. Responses include per-pollutant averages, measurement counts, units, and associated sensor locations.

The local simulation additionally exposes:

```text
GET /cities
GET /cities/{city}
GET /locations
GET /locations/{location_id}
```

## Dashboard

The dashboard is a dependency-free static web client for the deployed API. It displays:

- Rolling pollutant averages by city
- Measurement counts and units
- A Leaflet map with city-level status indicators
- Per-city detail views
- Automatic refresh every 30 seconds

## Project Structure

```text
cloud_simulation/
├── local_server.py       # Local API server
├── writer/               # Event ingestion and local persistence
├── reader/               # Query API and SQLite access
├── scripts/              # Event conversion and DB inspection
├── tests/                # Writer tests
├── events/               # Sample SQS events
└── web/                  # Local dashboard

cloud_deployment/
├── writer.py             # SQS-to-DynamoDB writer Lambda
├── reader.py             # DynamoDB reader Lambda
└── dashboard.html        # Deployed dashboard
```

## Design Notes

- The rolling window is fixed at 3 hours.
- The local reader can operate in either live mode or replay mode, where the window is anchored to the newest stored measurement.
- The deployed reader defaults to live mode.
- The `/cities` endpoint uses table scans for locations and precomputed totals; this is appropriate for the current implementation but would require caching or a revised access pattern at production scale.


## Finished Product

![alt text](https://github.com/kkartikeya1998/RealTimeAQMonitoringService/tree/main/LAQM_Snapshot.png "Dashboard Snapshot")
