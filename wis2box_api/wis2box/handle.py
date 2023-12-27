###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import base64
import json
import logging

import paho.mqtt.publish as publish

from enum import Enum

from wis2box_api.wis2box.env import BROKER_HOST
from wis2box_api.wis2box.env import BROKER_PORT
from wis2box_api.wis2box.env import BROKER_USERNAME
from wis2box_api.wis2box.env import BROKER_PASSWORD

LOGGER = logging.getLogger(__name__)

DATA_OBJECT_MIMETYPES = {
    'bufr4': 'application/x-bufr',
    'grib2': 'application/x-grib2',
    'grib': 'application/x-grib',
    'cap': 'application/cap+xml',
    'geojson': 'application/json'
}


def handle_error(error):
    """Handle errors

    :param e: exception

    :returns: mimetype, outputs
    """

    mimetype = 'application/json'
    errors = []
    errors.append(f'{error}')
    outputs = {
        'result': 'failure',
        'errors': errors,
        'warnings': []
    }
    return mimetype, outputs


class SecureHashAlgorithms(Enum):
    SHA512 = 'sha512'
    MD5 = 'md5'


class DataHandler():

    def __init__(self, channel, notify):
        # remove leading and trailing slashes
        channel = channel.strip('/')

        self._notify = notify
        self._channel = channel.replace('origin/a/wis2/', '')

    def process_items(self, output_items: []):
        """Process output_items, store and publish them

        :param output_items: list of output-items from the transform

        :returns: 'application/json'
        """

        LOGGER.info(f'Processing {len(output_items)} output-items')

        data_items = []

        mimetype = 'application/json'
        errors = []
        warnings = []
        data = []
        result = 'failure'

        record_nr = 0
        data_converted = 0
        data_published = 0
        # iterate over the output_items
        # each record contains either a key from DATA_OBJECT_MIMETYPES or errors and warnings # noqa
        for record in output_items:
            # extract the data from the record
            if any(key in record for key in DATA_OBJECT_MIMETYPES):
                data_items.append(record)
                data_converted += 1

            # extract the errors and warnings from the record
            if 'errors' in record:
                for error in record['errors']:
                    errors.append(error)
            if 'warnings' in record:
                for warning in record['warnings']:
                    warnings.append(warning)
            record_nr += 1

        for item in data_items:
            wsi = None
            if 'wigos_station_identifier' in item['_meta']['properties']:
                wsi = item['_meta']['properties']['wigos_station_identifier']
            identifier = item['_meta']['id']
            data_date = item['_meta']['properties']['datetime']
            if 'result' in item['_meta']:
                if item['_meta']['result']['code'] != 1:
                    msg = item['_meta']['result']['message']
                    LOGGER.error(f'Transform returned {msg} for wsi={wsi}')
                    continue

            for fmt, the_data in item.items():
                if fmt in ['_meta', 'errors', 'warnings']:
                    continue

                if fmt not in DATA_OBJECT_MIMETYPES:
                    LOGGER.error(f'Unknown format {fmt}')
                    continue
                elif the_data is None:
                    if wsi:
                        errors.append(f'No data returned WSI={wsi} and timestamp={data_date}') # noqa
                    else:
                        errors.append(f'No data returned for WSI=(no WSI found) and timestamp={data_date}') # noqa
                    continue

                filename = f'{identifier}.{fmt}'
                meta = {
                        'id': identifier,
                        'wigos_station_identifier': wsi,
                        'data_date': data_date.isoformat(),
                        'geometry': item['_meta']['geometry'],
                }
                data.append(
                    {
                        'data': base64.b64encode(the_data).decode(),
                        'filename': filename,
                        'meta': meta,
                        'channel': self._channel
                    })
                if self._notify:
                    # send the last entry in the data list as a notification
                    result = self.send_data_publish_request(data[-1])
                    if result != 'success':
                        errors.append(f'{result}')
                    else:
                        # TODO check if the notification was successful
                        data_published += 1

        if data_converted > 0 and errors == [] and warnings == []:
            result = 'success'
        elif data_converted == 0:
            result = 'failure'
        else:
            result = 'partial success'

        outputs = {
            'result': result,
            'messages transformed': data_converted,
            'messages published': data_published,
            'data_items': data,
            'errors': errors,
            'warnings': warnings
        }

        return mimetype, outputs

    def send_data_publish_request(self, data_item: dict):
        """Send DataPublishRequest

        :param data: data_item

        :returns: 'success' or error message
        """

        try:
            # create the message out of the data_item
            msg = {
                'EventName': 'DataPublishRequest',
                'channel': data_item['channel'],
                'data': data_item['data'],
                'filename': data_item['filename'],
                'meta': data_item['meta']
            }
            # publish notification on internal broker
            private_auth = {
                'username': BROKER_USERNAME,
                'password': BROKER_PASSWORD
            }
            publish.single(topic='wis2box/requests',
                           payload=json.dumps(msg),
                           qos=1,
                           retain=False,
                           hostname=BROKER_HOST,
                           port=int(BROKER_PORT),
                           auth=private_auth)
            LOGGER.debug('DataPublishRequest published')
        except Exception as e:
            return f'Error publishing message: msg={msg}, error={e}'

        return 'success'
