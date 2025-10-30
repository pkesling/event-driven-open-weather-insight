"""Model package for weather_insight."""
from .base import Base
from .openaq import (
    DimOpenAQLocation,
    DimOpenAQSensor,
    DimOpenAQLicense,
    OpenAQLocationLicenseBridge,
    StgOpenaqLatestMeasurements,
)
from .weather import StgWeatherHourlyForecast
from .airnow import StgAirnowAirQualityForecast
from .open_meteo import StgOpenMeteoAir, StgOpenMeteoWeather

__all__ = [
    "Base",
    "DimOpenAQLocation",
    "DimOpenAQSensor",
    "DimOpenAQLicense",
    "OpenAQLocationLicenseBridge",
    "StgOpenaqLatestMeasurements",
    "StgWeatherHourlyForecast",
    "StgAirnowAirQualityForecast",
    "StgOpenMeteoWeather",
    "StgOpenMeteoAir",
]
