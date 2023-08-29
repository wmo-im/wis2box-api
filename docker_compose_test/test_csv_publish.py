import requests
import sys

url = 'http://localhost:4343/oapi/processes/wis2box-csv-process/execution'

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

data = {
  "inputs": {
    "data": "wsi_series,wsi_issuer,wsi_issue_number,wsi_local,wmo_block_number,wmo_station_number,station_type,year,month,day,hour,minute,latitude,longitude,station_height_above_msl,barometer_height_above_msl,station_pressure,msl_pressure,geopotential_height,thermometer_height,air_temperature,dewpoint_temperature,relative_humidity,method_of_ground_state_measurement,ground_state,method_of_snow_depth_measurement,snow_depth,precipitation_intensity,anemometer_height,time_period_of_wind,wind_direction,wind_speed,maximum_wind_gust_direction_10_minutes,maximum_wind_gust_speed_10_minutes,maximum_wind_gust_direction_1_hour,maximum_wind_gust_speed_1_hour,maximum_wind_gust_direction_3_hours,maximum_wind_gust_speed_3_hours,rain_sensor_height,total_precipitation_1_hour,total_precipitation_3_hours,total_precipitation_6_hours,total_precipitation_12_hours,total_precipitation_24_hours\n0,20000,0,15015,15,15,1,2022,3,31,0,0,47.77706163,23.94046026,503,504.43,100940,10104,1448,5,298.15,294.55,80.4,3,1,1,0,0.004,10,-10,30,3,30,5,40,9,20,11,2,4.7,5.3,7.9,9.5,11.4", # noqa
    "channel": "csv/test",
    "notify": False,
    "template": "aws_mappings.json"
  }
}

expected_response = {
  "result": "success",
  "messages transformed": 1,
  "messages published": 0,
  "data_items": [
    {
      "data": "QlVGUgABgAQAABYAAAAAAAAAAAJuHgAH5gMfAAAAAAALAAABgMGWx2AAAVMABOIAAAMTUwMTUAAAAAAAAAAAAAAB4H//////////////////////////+vzG+ABpHZUm5gfCNGEap///////////////////////////+duD8v/////+CZAB9P/3R3cw+h////////////////////////////////////////////////8wiAAX//////////Af////gP////////////////////////////////////+AyP//////////////////////////+J/YPAPff2DwGT4goBadMCgN3//////////////////////////////////////////A+j/f/AMH/QDZ/oBQf0AYH6AHP////////////////////////////////////////////////////////////////////////////////////8A3Nzc3", # noqa
      "filename": "WIGOS_0-20000-0-15015_20220331T000000.bufr4"
    }
  ],
  "errors": [],
  "warnings": []
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200 and response.json() == expected_response:
    print("API call successful. Response matches the expected result.")
else:
    print("API call failed or response doesn't match the expected result.")
    print(f"Status code: {response.status_code}")
    print(f"Response:\n {response.json()}")
    sys.exit(1)
