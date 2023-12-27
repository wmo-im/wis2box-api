import json
import requests
import sys

import paho.mqtt.client as mqtt

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
            'channel': 'synop/test'
        }
      ],
    'errors': [],
    'warnings': []
}


def validate_message(message, expected_response):
    """Validate the received message against the expected message

    :param message: received message
    :param expected_message: expected message
    """
    print(f"Received message on topic: {message.topic}")
    # decode message payload as json
    message = json.loads(message.payload.decode())
    # create the expected message
    expected_message = {
        'EventName': 'DataPublishRequest',
        'channel': expected_response['data_items'][0]['channel'],
        'filename': expected_response['data_items'][0]['filename'],
        'data': expected_response['data_items'][0]['data'],
        'meta': expected_response['data_items'][0]['meta']
    }
    # compare the received message with the expected message
    is_equal = True
    if message['EventName'] != expected_message['EventName']:
        print(f"Expected EventName: {expected_message['EventName']}")
        print(f"Received EventName: {message['EventName']}")
        is_equal = False
    if message['channel'] != expected_message['channel']:
        print(f"Expected channel: {expected_message['channel']}")
        print(f"Received channel: {message['channel']}")
        is_equal = False
    if message['filename'] != expected_message['filename']:
        print(f"Expected filename: {expected_message['filename']}")
        print(f"Received filename: {message['filename']}")
        is_equal = False
    if message['data'] != expected_message['data']:
        print(f"Expected data: {expected_message['data']}")
        print(f"Received data: {message['data']}")
        is_equal = False
    if message['meta'] != expected_message['meta']:
        print(f"Expected meta: {expected_message['meta']}")
        print(f"Received meta: {message['meta']}")
        is_equal = False
    if is_equal is False:
        print("Received message doesn't match the expected message.")
        sys.exit(1)
    else:
        print("Received message matches the expected message.")


# start mqtt client
client = mqtt.Client()

# user credentials wis2box:wis2box
client.username_pw_set('wis2box', 'wis2box')
# connect to the broker
client.connect('localhost', 5883, 60)

# subscribe to the topic
client.subscribe('wis2box/#')

# define callback function for received messages
client.on_message = lambda client, userdata, message: validate_message(message, expected_response) # noqa

# start the loop
client.loop_start()

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200 and response.json() == expected_response:
    print("API call successful. Response matches the expected result.")
else:
    print("API call failed or response doesn't match the expected result.")
    print(f"Status code: {response.status_code}")
    print(f"Response:\n {response.json()}")
    sys.exit(1)

# stop the loop
client.loop_stop()
