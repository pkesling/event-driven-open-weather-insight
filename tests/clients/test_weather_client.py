from datetime import datetime, timezone

import requests

from weather_insight.clients import weather_client as wc


def make_client():
    config = wc.WeatherGovClientConfig()
    session = requests.Session()
    return wc.WeatherGovClient(config=config, session=session)


def test_flatten_quantitative_dict_handles_valid_payload():
    payload = {"value": 10, "minValue": 5, "maxValue": 12, "unitCode": "degC"}
    flattened = wc.WeatherGovClient.flatten_quantitative_dict("windSpeed", payload)
    assert flattened == {
        "wind_speed": 10,
        "wind_speed_min": 5,
        "wind_speed_max": 12,
        "wind_speed_unit": "degC",
    }


def test_flatten_quantitative_dict_handles_invalid_payload():
    assert wc.WeatherGovClient.flatten_quantitative_dict("windSpeed", None) == {}


def test_build_forecast_events_includes_quantitative_fields():
    ingest_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    meta = {
        "grid_id": "MKX",
        "grid_x": 10,
        "grid_y": 20,
        "office": "MKX",
    }
    raw = {
        "properties": {
            "periods": [
                {
                    "startTime": "2024-01-01T00:00:00+00:00",
                    "endTime": "2024-01-01T01:00:00+00:00",
                    "isDaytime": True,
                    "temperatureTrend": "rising",
                    "shortForecast": "Clear",
                    "detailedForecast": "Clear skies",
                    "temperature": {"value": 70, "minValue": 68, "maxValue": 72, "unitCode": "degF"},
                    "probabilityOfPrecipitation": {"value": 10, "minValue": 5, "maxValue": 20, "unitCode": "%"},
                    "relativeHumidity": {"value": 55, "minValue": 50, "maxValue": 60, "unitCode": "%"},
                    "dewpoint": {"value": 50, "minValue": 48, "maxValue": 52, "unitCode": "degF"},
                    "windSpeed": {"value": 5, "minValue": 3, "maxValue": 7, "unitCode": "mph"},
                    "windGust": {"value": 8, "minValue": 6, "maxValue": 10, "unitCode": "mph"},
                }
            ]
        }
    }

    client = make_client()
    events = client.build_forecast_events(meta, raw, ingest_dt=ingest_dt)

    assert len(events) == 1
    event = events[0]
    assert event["event_id"].startswith("MKX:10,20:")
    assert event["temperature"] == 70
    assert event["wind_speed_unit"] == "mph"
    assert event["ingested_at_dtz"] == "2024-01-01T00:00:00Z"
