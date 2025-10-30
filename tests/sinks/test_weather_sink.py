from datetime import datetime, timezone

from weather_insight.sinks import weather_postgres_sink as sink


def test_build_forecast_record_from_event_casts_fields():
    event = {
        "event_id": "abc",
        "event_type": "evt",
        "source": "api",
        "office": "MKX",
        "grid_id": "MKX",
        "grid_x": 1,
        "grid_y": 2,
        "start_time": "2024-01-01T00:00:00Z",
        "end_time": "2024-01-01T01:00:00Z",
        "is_daytime": True,
        "temperature": 70,
        "temperature_min": 65,
        "temperature_max": 75,
        "temperature_unit": "degF",
        "temperature_trend": None,
        "relative_humidity": 50,
        "relative_humidity_min": 45,
        "relative_humidity_max": 55,
        "relative_humidity_unit": "%",
        "dewpoint": 40,
        "dewpoint_min": 38,
        "dewpoint_max": 42,
        "dewpoint_unit": "degF",
        "wind_speed": 5,
        "wind_speed_min": 3,
        "wind_speed_max": 7,
        "wind_speed_unit": "mph",
        "wind_direction": "N",
        "wind_gust": 10,
        "wind_gust_min": 8,
        "wind_gust_max": 12,
        "wind_gust_unit": "mph",
        "probability_of_precipitation": 20,
        "probability_of_precipitation_min": 10,
        "probability_of_precipitation_max": 30,
        "probability_of_precipitation_unit": "%",
        "short_forecast": "Sunny",
        "detailed_forecast": "Sunny and warm",
        "ingested_at_dtz": "2024-01-01T00:00:00Z",
    }

    forecast = sink.build_forecast_record_from_event(event)

    assert forecast["event_id"] == "abc"
    assert forecast["start_time"].tzinfo == timezone.utc
    assert forecast["probability_of_precipitation"] == 20
