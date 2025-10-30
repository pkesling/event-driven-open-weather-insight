# Future Considerations
This is far from perfect, but it demonstrates the basic idea.  There are a few notable gaps that may
be addressed in the future:
- The `mart.fct_air_weather_forecast` matview doesn't join on location.  It works now because I'm 
  only measuring a single location.  The challenge is that each of the three data sources represent
  locations in different ways.  So a canonical location dimension needs to be created so that all sources can link to the
  same physical location and therefore be joined on.
- It only supports a single location out-of-the-box.  The input latitude/longitude could be extended
  to support multiple locations.

