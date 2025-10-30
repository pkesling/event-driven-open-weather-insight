import importlib
from unittest.mock import MagicMock


def _reload(module_path: str):
    module = importlib.import_module(module_path)
    return importlib.reload(module)


def test_openaq_ingest_dag_structure():
    dag_module = importlib.import_module("weather_insight.dags.openaq_ingest_dag")
    dag = dag_module.openaq_nearest_location_latest()
    assert dag.dag_id == "openaq_ingest"
    assert len(dag.tasks) == 2  # fetch + publish


def test_weather_forecast_ingest_dag_structure():
    dag_module = importlib.import_module("weather_insight.dags.weather_forecast_ingest_dag")
    dag = dag_module.weather_forecast_ingest()
    assert dag.dag_id == "weather_forecast_ingest"
    assert len(dag.tasks) == 2


def test_open_meteo_ingest_dag_structure():
    dag_module = importlib.import_module("weather_insight.dags.open_meteo_ingest_dag")
    dag = dag_module.open_meteo_ingest()
    assert dag.dag_id == "open_meteo_ingest"
    assert len(dag.tasks) == 2


def test_airnow_forecast_ingest_dag_structure():
    dag_module = importlib.import_module("weather_insight.dags.airnow_forecast_ingest_dag")
    dag = dag_module.airnow_forecast_ingest()
    assert dag.dag_id == "airnow_air_quality_forecast_ingest"
    assert len(dag.tasks) == 2


def test_dbt_transform_core_dag_structure():
    dag_module = importlib.import_module("weather_insight.dags.dbt_transform_core_dag")
    dag = dag_module.dbt_transform_core()
    assert dag.dag_id == "dbt_transform_core"
    assert len(dag.tasks) == 1


def test_openaq_fetch_task_uses_client(monkeypatch):
    monkeypatch.setenv("HOME_LAT", "43.0")
    monkeypatch.setenv("HOME_LON", "-89.0")
    monkeypatch.setenv("HOME_RADIUS_M", "1000")

    dag_module = _reload("weather_insight.dags.openaq_ingest_dag")
    fake_client = MagicMock()
    fake_client.fetch_nearest_location_latest.return_value = {"location": {}, "sensors": []}
    monkeypatch.setattr(
        dag_module,
        "make_openaq_client_from_env",
        lambda: fake_client,
    )
    dag = dag_module.openaq_nearest_location_latest()
    fetch_task = dag.task_dict["t_fetch"]
    envelope = fetch_task.python_callable()

    fake_client.fetch_nearest_location_latest.assert_called_once_with(
        43.0, -89.0, radius_m=1000.0
    )
    assert envelope == {"location": {}, "sensors": []}


def test_airnow_fetch_task_uses_client(monkeypatch):
    monkeypatch.setenv("HOME_LAT", "43.0")
    monkeypatch.setenv("HOME_LON", "-89.0")

    dag_module = _reload("weather_insight.dags.airnow_forecast_ingest_dag")
    fake_client = MagicMock()
    fake_client.get_daily_forecast_by_lat_lon.return_value = [
        {
            "event_id": "airnow:madison:2024-01-01:2024-01-02",
            "source": "airnow",
            "reporting_area": "Madison",
        }
    ]
    monkeypatch.setattr(
        dag_module,
        "make_airnow_client_from_env",
        lambda: fake_client,
    )
    dag = dag_module.airnow_forecast_ingest()
    fetch_task = dag.task_dict["t_fetch_forecast_events"]
    events = fetch_task.python_callable()

    fake_client.get_daily_forecast_by_lat_lon.assert_called_once_with(lat=43.0, lon=-89.0)
    assert len(events) == 1
    assert events[0]["source"] == "airnow"


def test_weather_fetch_task_uses_client(monkeypatch):
    monkeypatch.setenv("HOME_LAT", "43.0")
    monkeypatch.setenv("HOME_LON", "-89.0")

    dag_module = _reload("weather_insight.dags.weather_forecast_ingest_dag")
    fake_client = MagicMock()
    fake_client.get_forecast_urls.return_value = {
        "forecast_hourly": "http://forecast",
        "grid_id": "MKX",
        "grid_x": 10,
        "grid_y": 20,
        "office": "MKX",
    }
    fake_client.fetch_hourly_forecast.return_value = {"properties": {}}
    fake_client.build_forecast_events.return_value = [
        {
            "event_id": "1",
            "event_type": "nws.hourly_forecast",
            "source": "api.weather.gov",
            "office": "MKX",
            "grid_id": "MKX",
            "grid_x": 10,
            "grid_y": 20,
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-01T01:00:00Z",
            "is_daytime": True,
            "temperature": 70,
            "temperature_unit": "F",
            "relative_humidity": 50,
            "relative_humidity_unit": "%",
            "dewpoint": 55,
            "dewpoint_unit": "F",
            "wind_speed": 5,
            "wind_speed_unit": "mph",
            "wind_direction": "NW",
            "wind_gust": 8,
            "wind_gust_unit": "mph",
            "probability_of_precipitation": 10,
            "probability_of_precipitation_unit": "%",
            "short_forecast": "Sunny",
            "detailed_forecast": "Clear skies",
            "ingested_at_dtz": "2024-01-01T00:00:00Z",
        }
    ]
    monkeypatch.setattr(
        dag_module,
        "make_weather_gov_client_from_env",
        lambda: fake_client,
    )
    dag = dag_module.weather_forecast_ingest()
    fetch_task = dag.task_dict["t_fetch_forecast_events"]
    events = fetch_task.python_callable()

    fake_client.get_forecast_urls.assert_called_once_with(43.0, -89.0)
    fake_client.fetch_hourly_forecast.assert_called_once_with("http://forecast")
    fake_client.build_forecast_events.assert_called_once()
    event = events[0]
    assert event["event_id"] == "1"
    assert event["event_type"] == "nws.hourly_forecast"
    assert event["source"] == "api.weather.gov"


def test_open_meteo_fetch_task_uses_client(monkeypatch):
    monkeypatch.setenv("HOME_LAT", "43.0")
    monkeypatch.setenv("HOME_LON", "-89.0")
    monkeypatch.setenv("HOME_TZ", "UTC")

    dag_module = _reload("weather_insight.dags.open_meteo_ingest_dag")
    fake_client = MagicMock()
    fake_client.fetch_weather.return_value = {"hourly": {"time": []}, "hourly_units": {}}
    fake_client.fetch_air_quality.return_value = {"hourly": {"time": []}, "hourly_units": {}}
    fake_client.build_weather_events.return_value = [
        {
            "event_id": "w",
            "data_kind": "weather",
            "record_type": "forecast",
            "record_frequency_min": 60,
            "open_meteo_start_time": "2024-01-01T00:00:00Z",
            "open_meteo_end_time": "2024-01-01T01:00:00Z",
            "latitude": 43.0,
            "longitude": -89.0,
        }
    ]
    fake_client.build_air_quality_events.return_value = [
        {
            "event_id": "a",
            "data_kind": "air_quality",
            "record_type": "forecast",
            "record_frequency_min": 60,
            "open_meteo_start_time": "2024-01-01T00:00:00Z",
            "open_meteo_end_time": "2024-01-01T01:00:00Z",
            "latitude": 43.0,
            "longitude": -89.0,
        }
    ]

    monkeypatch.setattr(
        dag_module,
        "make_open_meteo_client_from_env",
        lambda: fake_client,
    )

    dag = dag_module.open_meteo_ingest()
    fetch_task = dag.task_dict["t_fetch_events"]
    events = fetch_task.python_callable()

    fake_client.fetch_weather.assert_called_once_with(43.0, -89.0, timezone="UTC", forecast_hours=48)
    fake_client.fetch_air_quality.assert_called_once()
    assert events["weather"][0]["data_kind"] == "weather"
    assert events["air_quality"][0]["data_kind"] == "air_quality"
