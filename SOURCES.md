# Data Sources & Usage Notes

- **OpenAQ (api.openaq.org/v3)**  
  - Public air-quality measurements.  
  - Respects source-provided license metadata; attribution required per payload.  
  - Provide your own `OPENAQ_API_KEY` where applicable.

- **AirNow (airnowapi.org)**  
  - Air quality forecasts/observations; data is preliminary and must be attributed.  
  - Do not alter AQI values or categories; clearly label as AirNow data.  
  - Requires `AIRNOW_API_KEY`.

- **weather.gov (api.weather.gov)**
  - Hourly forecasts; follows NWS API usage policy.
  - Use a descriptive User-Agent; default provided, override with `WEATHERGOV_USER_AGENT`.

- **Open-Meteo (open-meteo.com)**
  - Free weather and air-quality forecasts used for BikeAgent and warehouse demos.
  - No API key required, but please cache locally and avoid excessive polling.
  - Respect unit parameters (e.g., `temperature_unit`, `wind_speed_unit`) to reduce duplicate requests.

If you add a new source, note its usage terms and required attribution here.
