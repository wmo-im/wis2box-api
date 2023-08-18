import requests
import sys

url = 'http://localhost:4343/oapi/processes/wis2box-synop-process/execution'

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
    "result": "success",
    "messages transformed": 1,
    "messages published": 1,
    "files": [
        "http://localhost/data/2023-01-19/wis/synop/test/WIGOS_0-20000-0-64400_20230119T060000.bufr4" # noqa
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
