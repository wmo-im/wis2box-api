import requests
import sys

url = 'http://localhost:4343/oapi/processes/wis2box-synop2bufr/execution'

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

data = {
    "inputs": {
        "channel": "synop/test",
        "year": 2023,
        "month": 1,
        "notify": True,
        "data": "AAXX 19064 64400 36/// /0000 10102 20072 30068 40182 53001 333 20056 91003 555 10302 91018=" # noqa
    }
}

expected_response = {
    'result': 'success',
    'messages transformed': 1,
    'messages published': 1,
    'data_items': [
        {
            'data': 'QlVGUgABgAQAABYAAAAAAAAAAAJuHgAH5wETBgAAAAALAAABgMGWx2AAAVMABOIAAANjQ0MDAAAAAAAAAAAAAAAIDIGxoaGBgAAAAAAAAAAAAAAAAAAAAPzimYBA/78kmTlBBUCDB///////////////////////////+dUnxn1P///////////26vbYOl////////////////////////////////////////////////////////////////AR////gJH///+T/x/+R/yf////////////7///v9f/////////////////////////////////+J/b/gAff2/4Dz/X/////////////////////////////////////7+kAH//v6QANnH//////AAf/wAF+j//////////////v0f//////f//+/R/+////////////////////fo//////////////////3+oAP///////////////////8A3Nzc3', # noqa
            'filename': 'WIGOS_0-20000-0-64400_20230119T060000.bufr4',
            'meta': {
                'id': 'WIGOS_0-20000-0-64400_20230119T060000',
                'wigos_station_identifier': '0-20000-0-64400',
                'data_date': '2023-01-19T06:00:00',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [11.8817, -4.8045]
                  }
              },
            'channel': 'synop/test',
            'EventName': 'DataPublishRequest'
        }
      ],
    'errors': [],
    'warnings': []
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200 and response.json() == expected_response:
    print("API call successful. Response matches the expected result.")
else:
    print("API call failed or response doesn't match the expected result.")
    print(f"Status code: {response.status_code}")
    print(f"Response:\n {response.json()}")
    sys.exit(1)
