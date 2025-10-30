from weather_insight.models.events import (
    AirNowForecastEvent,
    WeatherHourlyForecastEvent,
    validate_airnow_events,
    validate_weather_events,
)


def test_validate_airnow_events_returns_clean_dicts():
    events = validate_airnow_events(
        [
            {
                "event_id": "airnow:madison:2024-01-01:2024-01-02",
                "source": "airnow",
                "reporting_area": "Madison",
            }
        ]
    )
    assert events[0]["event_id"].startswith("airnow:")
    assert events[0]["source"] == "airnow"


def test_validate_weather_events_requires_required_fields():
    events = validate_weather_events(
        [
            {
                "event_id": "evt-1",
                "event_type": "nws.hourly_forecast",
                "source": "api.weather.gov",
            }
        ]
    )
    assert events[0]["event_type"] == "nws.hourly_forecast"
    assert events[0]["source"] == "api.weather.gov"


def test_pydantic_models_ignore_extra_fields():
    model = WeatherHourlyForecastEvent.model_validate(
        {
            "event_id": "evt-1",
            "event_type": "nws.hourly_forecast",
            "source": "api.weather.gov",
            "extra": "ignored",
        }
    )
    assert not hasattr(model, "extra")
