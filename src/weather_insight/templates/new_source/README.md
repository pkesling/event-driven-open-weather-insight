# New Source Skeleton (cookiecutter-style)

This folder provides minimal stubs you can copy when adding a new source:

- `clients/your_source_client.py` – API client that returns normalized event dicts.
- `dags/your_source_ingest_dag.py` – Airflow DAG to pull from the API and publish to Kafka.
- `db/models/your_source.py` – SQLAlchemy models for staging/dim tables.
- `db/ops_your_source.py` – Upsert helpers for your models.
- `sinks/your_source_postgres_sink.py` – Kafka→Postgres sink using `BasePostgresSink`.
- `dbt/models/staging/stg_your_source.sql` – dbt staging view for your raw/stg table.
- `dbt/models/marts/your_source/fct_your_source.sql` – example mart view.
- `tests/your_source/` – placeholders for unit tests.

Search-and-replace `your_source` with your source name, set env vars (topics, DSN), and wire the sink into docker-compose.
