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
import hashlib
import uuid

import paho.mqtt.publish as publish

from urllib.parse import urlparse

from datetime import datetime as dt

from enum import Enum

from wis2box_api.wis2box.env import BROKER_PUBLIC
from wis2box_api.wis2box.env import BROKER_HOST
from wis2box_api.wis2box.env import BROKER_PORT
from wis2box_api.wis2box.env import BROKER_USERNAME
from wis2box_api.wis2box.env import BROKER_PASSWORD

from wis2box_api.wis2box.env import STORAGE_TYPE
from wis2box_api.wis2box.env import STORAGE_PUBLIC
from wis2box_api.wis2box.env import STORAGE_PUBLIC_URL

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
    errors.append(f"{error}")
    outputs = {
        "result": 'failure',
        "errors": errors,
        "warnings": []
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
        self._channel = channel

        if STORAGE_TYPE in ['S3', 'minio', 's3', 'MINIO', 'MinIO']:
            from wis2box_api.wis2box.minio import MinIOStorage
            self._storage = MinIOStorage(name=STORAGE_PUBLIC)
        else:
            LOGGER.error(f'Unknown storage type: {STORAGE_TYPE}')
            raise Exception(f'Unknown storage type: {STORAGE_TYPE}')

    def _generate_checksum(self, bytes, algorithm: SecureHashAlgorithms) -> str:  # noqa
        """
        Generate a checksum of message file
        :param algorithm: secure hash algorithm (md5, sha512)
        :returns: `tuple` of hexdigest and length
        """

        sh = getattr(hashlib, algorithm)()
        sh.update(bytes)
        return base64.b64encode(sh.digest()).decode()

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
                        errors.append(f'No data returned WSI={wsi} and data_date={data_date}') # noqa
                    else:
                        errors.append(f'No data returned for WSI=(no WSI found) and data_date={data_date}') # noqa
                    continue

                filename = f'{identifier}.{fmt}'
                if not self._notify:
                    try:
                        data.append(
                            {
                                'data': base64.b64encode(the_data).decode(),
                                'filename': filename
                            })
                    except Exception as e:
                        LOGGER.error(e)
                        return handle_error(e)
                else:
                    yyyymmdd = data_date.strftime('%Y-%m-%d')
                    storage_path = f'{yyyymmdd}/wis/{self._channel}/{identifier}.{fmt}'  # noqa   
                    storage_url = f'{STORAGE_PUBLIC_URL}/{storage_path}'
                    try:
                        self._storage.put(data=the_data, identifier=storage_path) # noqa
                        data.append(
                            {
                                'file_url': storage_url,
                                'filename': filename
                            })
                    except Exception as e:
                        LOGGER.error(e)
                        return handle_error(e)

                    notify_result = 'unknown'
                    try:
                        checksum_type = SecureHashAlgorithms.SHA512.value
                        checksum_value = self._generate_checksum(the_data, checksum_type) # noqa
                        notify_result = self._publish_wis2_message(
                            storage_url=storage_url,
                            checksum_type=checksum_type,
                            checksum_value=checksum_value,
                            data_length=len(the_data),
                            content_type=DATA_OBJECT_MIMETYPES[fmt],
                            identifier=identifier,
                            data_date_iso=data_date.isoformat(),
                            geometry=item['_meta']['geometry'],
                            wsi=wsi)
                    except Exception as e:
                        LOGGER.error(e)
                        errors.append(f"error hashing: {e}")
                    if notify_result == 'success':
                        data_published += 1
                    else:
                        errors.append(f"error publishing WIS2-notification: {notify_result}") # noqa

        if data_converted > 0 and errors == [] and warnings == []:
            result = 'success'
        elif data_converted == 0:
            result = 'failure'
        else:
            result = 'partial success'

        outputs = {
            'result': result,
            "messages transformed": data_converted,
            "messages published": data_published,
            "data_items": data,
            "errors": errors,
            "warnings": warnings
        }

        return mimetype, outputs

    def _publish_wis2_message(self,
                              storage_url: str,
                              checksum_type: str,
                              checksum_value: str,
                              data_length: int,
                              content_type: str,
                              identifier: str,
                              data_date_iso: str,
                              geometry: dict,
                              wsi: str = None) -> str:
        """Publish a WIS2 message

        :param storage_url: url to the stored file
        :param checksum_type: type of the checksum
        :param checksum_value: value of the checksum
        :param data_length: length of the file
        :param content_type: content type of the file
        :param identifier: identifier of the file
        :param data_date_iso: date of the file in iso-format
        :param geometry: geometry of the file
        :param wsi: wigos station identifier

        :returns: status of the publish
        """

        try:
            msg = {
                'id': str(uuid.uuid4()),
                'type': 'Feature',
                'version': 'v04',
                'geometry': geometry,
                'properties': {
                    'data_id': f'wis2/{self._channel}/{identifier}',
                    'datetime': data_date_iso,
                    'pubtime': dt.now().isoformat(),
                    'integrity': {
                        'method': checksum_type,
                        'value': checksum_value
                    },
                    'wigos_station_identifier': wsi
                },
                'links': [
                    {
                        'rel': 'canonical',
                        'type': content_type,
                        'href': storage_url,
                        'length': data_length
                    },
                    {
                        'rel': 'via',
                        'type': 'text/html',
                        'href': f'https://oscar.wmo.int/surface/#/search/station/stationReportDetails/{wsi}' # noqa
                    }
                ]
            }
        except Exception as e:
            LOGGER.error(e)
            return f"Error creating message: {e}"
        LOGGER.debug(msg)

        try:
            topic = f'origin/a/wis2/{self._channel}'
            LOGGER.info(f"Publishing to {topic} on {BROKER_PUBLIC}")
            # parse public broker url
            broker_public = urlparse(BROKER_PUBLIC)
            public_auth = {
                'username': broker_public.username,
                'password': broker_public.password
            }
            broker_port = broker_public.port
            if broker_port is None:
                if broker_public.scheme == 'mqtts':
                    broker_port = 8883
                else:
                    broker_port = 1883
            # publish notification on public broker
            publish.single(topic=f'origin/a/wis2/{topic}',
                           payload=json.dumps(msg),
                           qos=1,
                           retain=False,
                           hostname=broker_public.hostname,
                           port=broker_port,
                           auth=public_auth)

            # publish notification on internal broker
            private_auth = {
                'username': BROKER_USERNAME,
                'password': BROKER_PASSWORD
            }
            publish.single(topic='wis2box/notifications',
                           payload=json.dumps(msg),
                           qos=1,
                           retain=False,
                           hostname=BROKER_HOST,
                           port=int(BROKER_PORT),
                           auth=private_auth)
            LOGGER.debug("Message successfully published")
        except Exception as e:
            return f"Error publishing message: {e}"

        return "success"
