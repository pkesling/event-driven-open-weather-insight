# Weather Insight - An event-driven open weather data platform and demo  
*A modern, event-driven data engineering demo built for exploration, forecasting, and intelligent ride planning.*

## Overview

Weather Insight is a small but complete modern data-engineering platform.  It is, admittedly, a bit overengineered for
 the current set of goals, but it serves as a good example of a modern data engineering tech stack. It ingests
 environmental and meteorological data from multiple open APIs, publishes the data to Kafka as event streams,
 applies minimal bronze-layer validation, and stores curated hourly records in a PostgreSQL warehouse.

The project exists for three purposes:

1. **Personal use** — 
      - gain experience on specific data engineering technologies (where I used proprietary or less popular tools in my work experience)
      - a foundation for learning and implementing an intelligent cycling recommendation AI agent (being an avid cyclist):
        - “Should I ride east or west to avoid headwinds?”
        - “Is the air quality too poor for a long workout?”
        - “What time window minimizes my chance of riding in rain?”
        - “What’s the heat index and should I carry extra water?”
        - "What should I wear to be comfortable on the bike?"

2. **Professional demonstration** — a compact, albeit, overengineered, example of:
   - Event-driven ingestion via Apache Kafka  
   - Declarative transforms and warehouse modeling  
   - Extensible, pluggable sink architecture  
   - Proper handling of dimensional reference data  
   - Airflow DAG orchestration  
   - SQLAlchemy ORM modeling  
   - SCD-2 ready warehouse structures  
   - Clean separation of concerns  

3. **Explore and leverage AI for development** — gain understanding and experience using the latest gen AI tools:
   - Treat AI as a coding partner during design and development
   - Leverage AI to:
     - Write unit tests
     - Generate initial doc strings
     - Optimize code adhering to Python best practices
     - Check for PEP8 adherence
     - Summarize and explain new tools, libraries, concepts, etc.
     - Compare/Contrast approaches (design patterns) to use throughout development
     - Review code
   - See AI_USAGE.md for a list of AI tools used, as well as observations on the state of AI with respect to data
     platform development

The goal is to build an approachable platform that still reflects modern best practices while remaining small enough
to iterate on quickly.

---

## High-Level Architecture

```
                  ┌──────────────────────────────┐
                  │ External APIs                │
                  │  - OpenAQ.org                │
                  │  - api.weather.gov           │
                  │  - etc.                      │
                  └─────────────┬────────────────┘
                                |
                                v
                     ┌─────────────────────┐
                     │  Airflow Ingestion  │
                     │ (Python @task DAGs) │
                     └───────┬─────────────┘
                             |
                             v
                     ┌─────────────────────┐
                     │ Apache Kafka Topics │
                     │  raw.openaq.*       │
                     │  raw.weather.*      │
                     │  etc.               │
                     └───────┬─────────────┘
                             |
                             v
                  ┌──────────────────────────────┐
                  │  Sink Containers             │
                  │   (generic Kafka→warehouse)  │
                  └─────────────┬────────────────┘
                                |
                                v
                   ┌────────────────────────────┐
                   │ PostgreSQL Warehouse       │
                   │  stg.* (bronze)            │
                   │  ref.* (dimensions)        │
                   │  marts.*                   │
                   └────────────────────────────┘
```

---

## Goals
- Build an extensible, event-driven data pipeline using industry-standard tools (Airflow, Kafka, Postgres, dbt, SQLAlchemy).
- Ingest and normalize real-time weather and air quality data (current + forecasts) from multiple providers (OpenAQ, NWS, AirNow, Open-Meteo).
- Standardize records to a common schema with bronze/staging validation and UTC normalization for time-safe joins.
- Curate hourly fact tables and SCD-ready dimensions in PostgreSQL, organized into stg.\*, ref.\*, and marts.\* layers.
- Provide reusable sink and client templates to add new sources quickly with minimal boilerplate.
- Enable downstream analytics and agents (e.g., “Ride Advisor”) via dbt models that blend weather, AQI, and cycling heuristics.

---

# Project Structure

```
weather_insight/
│
├── src/weather_insight/
│   ├── clients/             # API clients - openaq_client, weather_client, etc.
|   ├── dags/                # Airflow DAGs (.py with @task)
│   ├── db/                  # SQLAlchemy engine/session + sqlalchemy models and DB operations for each respective API client
│   │   └── models/          # SQLAlchemy ORM models
│   ├── dbt/                 # dbt project for marts
│   ├── sinks/               # generic base sink + per-source sinks (e.g., read from Kafka, write to Postgres)
│   ├── templates/           # collection of client, dag, db models, sinks, unit test, etc., templates for adding new sources 
│   └── util/                # shared functions, common app-wide logging config, etc.
├── tests/                   # unit tests
├── docker-compose.yml       # docker-compose stack definition
├── .env.example             # template env values
├── AI_USAGE.md              # notes on AI tools used and observations on AI state with respect to data platform development
├── pyproject.toml           # Python project metadata 
├── README.md                # this file
├── uv.lock                  # uv lockfile for reproducible environments
└── SOURCES.md               # list of data sources with attribution

```

---

# Setup & Running

## 1. Clone and configure environment variables

```
cp .env.example .env
```

Edit `.env` to configure run-time values.

Notes: 
- The default values should work out-of-the-box with Docker Desktop to quickly bring up a demo environment, with a few exceptions:
  - `OPENAQ_API_KEY` must be set.  This should be your personal OpenAQ API key obtained from openaq.org.
  - `HOME_LAT` and `HOME_LON` should be set to your location.
- *IMPORTANT security notes*
  - DO NOT commit `.env` to source control.
  - DO NOT expose sensitive values (e.g., API keys) in `.env` in a production environment.  Read these from a secure secrets store.
  - DO NOT hardcode sensitive values in your code.  Use environment variables instead.
  - DO NOT use the default PLAINTEXT Kafka security protocol outside a personal demo environment.  This is not secure.
  - DO change all passwords to be unique, following security best practices.
  - DO change the `AIRFLOW_WEBSERVER_SECRET_KEY` to a unique, secret value.


## 2. Local development setup (optional but recommended)

```
uv venv .venv  # or python3 -m venv .venv
source .venv/bin/activate
uv sync --group dev  # or: uv pip install -e ".[dev]"
```

*Running tests locally avoids bringing up Docker during quick iterations.*

Unit tests:
```
PYTHONPATH=src python3 -m pytest
```

## 3. Start the full stack

```
docker-compose up --build
```

Services started:

- PostgreSQL Warehouse (`postgres-warehouse`)
- PostgreSQL Airflow application DB (`postgres-airflow`)
- Kafka Broker (`kafka`)
- Kafka UI (`kafka-ui`)
- HTTPS reverse proxy (`caddy`) terminating TLS for the local UIs
- Airflow
  - `airflow-init` (idempotent initialization service that creates the `airflow` DB, if needed)
  - `airflow-webserver`  
  - `airflow-scheduler` 
  - `airflow-triggerer`
- Sink containers (OpenAQ, AirNow, Weather, Open-Meteo)
  - `openaq-sink`
  - `airnow-sink`
  - `weather-sink`
  - `open-meteo-sink`

### HTTPS for local UIs (Airflow, Kafka UI)
- A lightweight Caddy reverse proxy now terminates TLS locally and proxies to `airflow-webserver:8080` and `kafka-ui:8080`.
- Caddy config lives at `infra/caddy/Caddyfile` (checked in).
- Browse the secure endpoints at `https://airflow.localhost` and `https://kafka-ui.localhost` after `docker-compose up --build`.
- Caddy uses its own internal CA; to eliminate browser warnings, trust the CA cert on your host:
  ```bash
  # Export the CA cert from the running container
  mkdir -p ./local/certs
  docker cp caddy:/data/caddy/pki/authorities/local/root.crt ./local/certs/root.crt

  # macOS (creates a trusted certificate in System keychain; requires sudo GUI prompt)
  sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ./local/certs/root.crt

  # Linux (example for Debian/Ubuntu)
  sudo cp ./local/certs/root.crt /usr/local/share/ca-certificates/caddy-local.crt
  sudo update-ca-certificates
  ```
- The original HTTP ports (`8080` for Airflow, `9000` for Kafka UI) remain exposed for convenience; prefer the HTTPS endpoints when possible.

## BikeAgent integration (warehouse-backed Open-Meteo feed)
- Airflow DAG `open_meteo_ingest` pulls Open-Meteo weather + air quality for `HOME_LAT`/`HOME_LON` and publishes to Kafka topic `raw.open_meteo.weather_record`.
- Sink `open_meteo_postgres_sink` consumes that topic and upserts into `stg.open_meteo_weather` and `stg.open_meteo_air`.
- BikeAgent (or any client) can skip live Open-Meteo API calls and read from the warehouse using the drop-in adapter:
  ```python
  from weather_insight.integrations.bike_agent import OpenMeteoWarehouseAdapter

  adapter = OpenMeteoWarehouseAdapter.from_url(
      "postgresql+psycopg://warehouse:warehouse@localhost:5442/warehouse"
  )
  rows = adapter.fetch_weather_and_air(latitude=43.0, longitude=-89.0, hours_ahead=6)
  ```
  Rows include both weather and air-quality values (with units) ordered by start time so the agent can hydrate its local models without hitting external APIs.


## 4. Run ingestion DAGs

Inside Airflow:

- `openaq_ingest` - ingests OpenAQ data from the API and publishes to Kafka
- `airnow_air_quality_forecast_ingest` - ingests AirNow AQ forecasts and publishes to Kafka
- `weather_forecast_ingest` - ingests National Weather Service hourly forecasts and publishes to Kafka 
- `open_meteo_ingest` - ingests Open-Meteo weather + air quality and publishes to Kafka
- `dbt_transform_core` - runs dbt transforms across sources (Great Expectations disabled)

All ingest DAGs publish normalized envelope messages into Kafka.

## 5. Warehouse sinks

Long-running sink containers:

- `openaq_postgres_sink` → consumes `raw.openaq.latest_by_location`
- `airnow_postgres_sink` → consumes `raw.airnow.air_quality_forecast`
- `weather_postgres_sink` → consumes `raw.weather.hourly_forecast`
- `open_meteo_postgres_sink` → consumes `raw.open_meteo.weather_record`

Both use the shared abstract sink runner.

Each sink publishes messages that failed to be processed to a DLQ topic:
- `openaq_postgres_sink` → publishes `raw.openaq.latest_by_location.dlq`
- `airnow_postgres_sink` → publishes `raw.airnow.air_quality_forecast.dlq`
- `weather_postgres_sink` → publishes `raw.weather.hourly_forecast.dlq`
- `open_meteo_postgres_sink` → publishes `raw.open_meteo.weather_record.dlq`

# Adding a New Data Source

Adding a new source should be a small task—not a forked rewrite.

To add a new source:

### 1. Write a small API client  
Place it in `src/weather_insight/clients/your_source_client.py`.  
Output a **Python dict envelope**.

### 2. Write an Airflow ingestion DAG  
Create `src/weather_insight/dags/your_source_ingest_dag.py`.

Your DAG should:

- fetch raw API data,
- normalize timestamps as UTC,
- build a single “envelope” dict,
- publish to a dedicated Kafka topic.

### 3. Define SQLAlchemy models  
Add new ORM models in `src/weather_insight/db/models/your_source.py`.

Use the same pattern as OpenAQ & Weather:

- surrogate key (`*_id_sk`)
- original source ID
- SCD-2 columns (`effective_start_at_dtz`, etc.)
- unique constraint on `(source_id, is_current)`

### 4. Add "ops" functions  
Add `ensure_*` functions in `src/weather_insight/db/ops_your_source.py`.

These should:

- Upsert into `ref.*` dimension tables.
- Upsert or insert into `stg.*` tables.

### 5. Create a per-source sink module  
Create `src/weather_insight/sinks/your_source_postgres_sink.py` with a class that inherits `BasePostgresSink` and overrides `handle_event()`:

```python
class YourSourcePostgresSink(BasePostgresSink):
    def __init__(self) -> None:
        cfg = SinkConfig(
            bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            group_id="your_source_sink",  # you may want to read this from an env var
            topic="raw.your_source.full_envelope", # you may want to read this from an env var
            dead_letter_topic="raw.your_source.full_envelope.dlq", # you may want to read this from an env var
        )
        super().__init__(config=cfg, logger=logger)

    def handle_event(self, payload: Dict[str, Any]) -> None:
        record = build_your_source_payload(payload)
        with SessionLocal() as session:
            ensure_your_dimensions(session, record["dimensions"])
            ensure_your_measurements(session, record["facts"])
            session.commit()


def main() -> int:
    return YourSourcePostgresSink().run()
```

The base sink provides the Kafka consumer lifecycle, poll loop, JSON decoding, offset commits, DLQ routing, and graceful shutdown. Your subclass just focuses on translating payloads into DB upserts.

### 6. Wire it into Docker / deployment  
Add the new sink service to `docker-compose.yml` (mirroring the existing sinks) so it runs continuously and consumes your topic.

### 7. Write your unit tests
Boring, but important.

```
PYTHONPATH=src python3 -m pytest tests/your_source
```

# Configuration & Deployment

**Env vars (core)**: see `.env.example`. Key ones: `HOME_LAT`, `HOME_LON`, `OPENAQ_API_KEY`, Kafka topics (`KAFKA_TOPIC_*`), DB creds (`WAREHOUSE_DB_*`, `AIRFLOW_DB_*`), Airflow secrets (`AIRFLOW_WEBSERVER_SECRET_KEY`).

**Open-Meteo config**: optional overrides for `OPEN_METEO_WEATHER_URL`, `OPEN_METEO_AIR_URL`, `OPEN_METEO_TIMEOUT_SEC`, `OPEN_METEO_RETRIES`, `OPEN_METEO_RETRY_BACKOFF`, `OPEN_METEO_FORECAST_HOURS`, and Kafka topics `KAFKA_TOPIC_OPEN_METEO` / `KAFKA_TOPIC_OPEN_METEO_DLT`.

**GX_STRICT**: controls GX strictness (default strict). Set `GX_STRICT=0` only in dev/test if you need GX to fall back when `project_config` isn’t supported or DSN is absent.

**Deployment**: `docker-compose up --build` brings up the full stack (Airflow, Kafka, Postgres, sinks). Adjust `.env` for your environment/secrets before running. For production, harden Kafka security, rotate credentials, and consider building sink images with preinstalled deps instead of pip-installing on startup.

---

# Future Work

- Build vector-indexed semantic search on historical weather forecasts.
- Train a small LLM or prompt-engineered agent for ride recommendations.
- Add Grafana (or similar) dashboards for:
  - Kafka lag
  - warehouse row counts
  - ingestion latency
