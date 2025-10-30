from datetime import datetime, timezone
from types import SimpleNamespace

from weather_insight.integrations.bike_agent.open_meteo_adapter import (
    OpenMeteoWarehouseAdapter,
)


def test_merge_weather_and_air_combines_matching_rows():
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)

    weather_rows = [
        SimpleNamespace(
            open_meteo_start_time=start_time,
            open_meteo_end_time=start_time,
            record_type="forecast",
            latitude=1.0,
            longitude=2.0,
            temperature=10.0,
            temperature_unit="C",
            rel_humidity=70.0,
            rel_humidity_unit="%",
            dew_point=5.0,
            dew_point_unit="C",
            apparent_temperature=9.0,
            apparent_temperature_unit="C",
            precipitation_prob=10.0,
            precipitation_prob_unit="%",
            precipitation=0.0,
            precipitation_unit="mm",
            cloud_cover=20.0,
            cloud_cover_unit="%",
            wind_speed=5.0,
            wind_speed_unit="kmh",
            wind_gusts=9.0,
            wind_gusts_unit="kmh",
            wind_direction=180.0,
            wind_direction_unit="deg",
            is_day=True,
        )
    ]
    air_rows = [
        SimpleNamespace(
            open_meteo_start_time=start_time,
            pm2_5=5.0,
            pm2_5_unit="ug/m3",
            pm10=7.0,
            pm10_unit="ug/m3",
            us_aqi=20.0,
            us_aqi_unit="idx",
            ozone=40.0,
            ozone_unit="ppb",
            uv_index=1.0,
            uv_index_unit="idx",
        )
    ]

    merged = OpenMeteoWarehouseAdapter._merge_weather_and_air(weather_rows, air_rows)
    assert len(merged) == 1
    assert merged[0]["temperature"] == 10.0
    assert merged[0]["pm2_5"] == 5.0
    assert merged[0]["us_aqi_unit"] == "idx"
