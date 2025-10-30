from datetime import timezone

from weather_insight.sinks import open_meteo_sink as sink


def test_build_weather_record_parses_times_and_defaults():
    event = {
        "event_id": None,
        "data_kind": "weather",
        "record_type": "forecast",
        "record_frequency_min": 60,
        "open_meteo_start_time": "2024-01-01T00:00:00Z",
        "open_meteo_end_time": "2024-01-01T01:00:00Z",
        "latitude": 1.0,
        "longitude": 2.0,
        "temperature": 10.0,
        "temperature_unit": "C",
        "rel_humidity": 75.0,
        "rel_humidity_unit": "%",
        "wind_speed": 5.0,
        "wind_speed_unit": "kmh",
        "wind_direction": 180.0,
        "wind_direction_unit": "deg",
        "ingested_at_dtz": "2024-01-01T00:00:00Z",
    }

    rec = sink.build_weather_record_from_event(event)
    assert rec["event_id"].startswith("open-meteo:weather")
    assert rec["open_meteo_start_time"].tzinfo == timezone.utc
    assert rec["temperature"] == 10.0
    assert rec["rel_humidity_unit"] == "%"


def test_build_air_record_parses_times_and_defaults():
    event = {
        "event_id": None,
        "data_kind": "air_quality",
        "record_type": "forecast",
        "record_frequency_min": 60,
        "open_meteo_start_time": "2024-01-01T00:00:00Z",
        "open_meteo_end_time": "2024-01-01T01:00:00Z",
        "latitude": 1.0,
        "longitude": 2.0,
        "pm2_5": 5.0,
        "pm2_5_unit": "ug/m3",
        "pm10": 7.0,
        "pm10_unit": "ug/m3",
        "us_aqi": 20,
        "us_aqi_unit": "idx",
        "ingested_at_dtz": "2024-01-01T00:00:00Z",
    }

    rec = sink.build_air_quality_record_from_event(event)
    assert rec["event_id"].startswith("open-meteo:air_quality")
    assert rec["open_meteo_start_time"].tzinfo == timezone.utc
    assert rec["pm10"] == 7.0
