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

from datetime import datetime as dt
import hashlib
import io
import json
import logging
from minio import Minio
import os
import paho.mqtt.publish as publish

import uuid

from urllib.parse import urlparse

LOGGER = logging.getLogger(__name__)

# Get broker connection details
BROKER_USERNAME = os.environ.get('WIS2BOX_BROKER_USERNAME')
BROKER_PASSWORD = os.environ.get('WIS2BOX_BROKER_PASSWORD')
BROKER_HOST = os.environ.get('WIS2BOX_BROKER_HOST')
BROKER_PORT = os.environ.get('WIS2BOX_BROKER_PORT')
BROKER_PUBLIC = os.environ.get('WIS2BOX_BROKER_PUBLIC').rstrip('/')

STORAGE_SOURCE = os.environ.get('WIS2BOX_STORAGE_SOURCE')
STORAGE_USERNAME = os.environ.get('WIS2BOX_STORAGE_USERNAME')
STORAGE_PASSWORD = os.environ.get('WIS2BOX_STORAGE_PASSWORD')
STORAGE_PUBLIC = os.environ.get('WIS2BOX_STORAGE_PUBLIC')

STORAGE_PUBLIC_URL = f"{os.environ.get('WIS2BOX_URL')}/data"


class WIS2Publish():

    def __init__(self):
        self.notify = True
        self.minio_client = self._minio_client()

    def _minio_client(self):
        is_secure = False
        urlparsed = urlparse(STORAGE_SOURCE)
        if STORAGE_SOURCE.startswith('https://'):
            is_secure = True
        client = Minio(endpoint=urlparsed.netloc,
                       access_key=STORAGE_USERNAME,
                       secret_key=STORAGE_PASSWORD,
                       secure=is_secure)
        return client

    def process_bufr(self, output_items: [], channel: str, notify: bool):
        """Process bufr messages, store and publish them

        :param output_items: list of bufr messages
        :param channel: string with channel name
        :param notify: boolean to publish to broker or not

        :returns: 'application/json'
        """

        LOGGER.info(f'Processing {len(output_items)} bufr messages')

        client = self._minio_client()

        mimetype = 'application/json'
        errors = []
        warnings = []
        bufr = []
        urls = []
        result = 'failure'

        record_nr = 0
        data_converted = 0
        data_published = 0
        # iterate over the bufr_generator
        # each record contains either a bufr4 message or errors/warnings
        for record in output_items:
            if 'bufr4' in record:
                bufr.append(record)
                data_converted += 1
            elif 'errors' not in record and 'warnings' not in record:
                errors.append(f"Internal error for record-nr: {record_nr}")
            else:
                for error in record['errors']:
                    errors.append(error)
                for warning in record['warnings']:
                    warnings.append(warning)
            record_nr += 1

        for item in bufr:
            wsi = item['_meta']['properties']['wigos_station_identifier']
            identifier = item['_meta']['id']
            data_date = item['_meta']['properties']['datetime']
            if 'result' in item['_meta']:
                if item['_meta']['result']['code'] != 1:
                    msg = item['_meta']['result']['message']
                    LOGGER.error(f'Transform returned {msg} for wsi={wsi}')
                    continue

            for fmt, the_data in item.items():
                if fmt != "bufr4":
                    continue
                yyyymmdd = data_date.strftime('%Y-%m-%d')
                storage_path = f'{yyyymmdd}/wis/{channel}/{identifier}.{fmt}'  # noqa   
                storage_url = f'{STORAGE_PUBLIC_URL}/{storage_path}'
                try:
                    client.put_object(
                        bucket_name=STORAGE_PUBLIC,
                        object_name=storage_path,
                        data=io.BytesIO(the_data), length=-1,
                        part_size=10 * 1024 * 1024)
                    urls.append(storage_url)
                except Exception as e:
                    return self._handle_error(e)

                if notify:
                    try:
                        hash256_value = hashlib.sha256(the_data).hexdigest()
                    except Exception as e:
                        LOGGER.error(e)
                        errors.append(f"error hashing: {e}")
                    status = self._publish_wis2_message(
                        channel=channel,
                        storage_url=storage_url,
                        hash256_value=hash256_value,
                        data_length=len(the_data),
                        content_type='application/x-bufr',
                        identifier=identifier,
                        data_date_iso=data_date.isoformat(),
                        geometry=item['_meta']['geometry'],
                        wsi=wsi
                    )
                    if status == 'success':
                        data_published += 1
                    else:
                        errors.append(status)

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
            "files": urls,
            "errors": errors,
            "warnings": warnings
        }

        return mimetype, outputs

    def _publish_wis2_message(self,
                              channel: str,
                              storage_url: str,
                              hash256_value: str,
                              data_length: int,
                              content_type: str,
                              identifier: str,
                              data_date_iso: str,
                              geometry: dict,
                              wsi: str = None) -> str:
        """Publish a WIS2 message

        :param channel: channel name
        :param storage_url: url to the stored file
        :param hash256_value: sha256 hash of the file
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
                    'data_id': f'wis2/{channel}/{identifier}',
                    'datetime': data_date_iso,
                    'pubtime': dt.now().isoformat(),
                    'integrity': {
                        'method': 'sha256',
                        'value': hash256_value,
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
            LOGGER.info(f"Publishing to {BROKER_PUBLIC}{channel}")
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
            publish.single(topic=f'origin/a/wis2/{channel}',
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
            LOGGER.debug(f"Message successfully published to {BROKER_PUBLIC}{channel}") # noqa
        except Exception as e:
            return f"Error publishing message: {e}"

        return f"success"

    def handle_error(self, e):
        """Handle errors

        :param e: exception

        :returns: mimetype, outputs
        """

        LOGGER.error(e)
        mimetype = 'application/json'
        errors = []
        errors.append(f"{e}")
        outputs = {
            "result": 'failure',
            "errors": errors,
            "warnings": []
        }
        return mimetype, outputs
