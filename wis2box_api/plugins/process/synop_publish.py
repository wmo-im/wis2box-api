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
import csv
from datetime import datetime as dt
import hashlib
import io
import json
import logging
from minio import Minio
import os
import paho.mqtt.publish as publish
from pygeoapi.process.base import BaseProcessor
import requests
import uuid

from synop2bufr import transform
from urllib.parse import urlparse

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'x-wmo:wis2box-synop-process',
    'title': 'Process and publish FM-12 SYNOP',
    'description': 'Converts the posted data to BUFR and publishes to specified topic',  # noqa
    'keywords': [],
    'links': [],
    'inputs': {
        'channel': {
            'title': {'en': 'Channel'},
            'description': {'en': 'Channel / topic to publish on'},
            'schema': {'type': 'string', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        },
        "data": {
            "title": "FM 12-SYNOP",
            "description": "Input FM 12-SYNOP bulletin to convert to BUFR.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [],
        },
        "year": {
            "title": "Year",
            "description": "Year (UTC) corresponding to FM 12-SYNOP bulletin",
            "schema": {"type": "integer"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": []
        },
        "month": {
            "title": "Month",
            "description": "Month (UTC) corresponding to FM 12-SYNOP bulletin",
            "schema": {"type": "integer"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": []
        }
    },
    'outputs': {
        'path': {
            'title': {'en': 'FeatureCollection'},
            'description': {
                'en': 'A GeoJSON FeatureCollection of the '
                'stations with their status'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            "channel": "synop/test",
            "year": 2023,
            "month": 1,
            "data": "AAXX 19064 68399 36/// /0000 10102 20072 30068 40182 53001 333 20056 91003 555 10302 91018=",  # noqa
        }
    }
}

# Get broker connection details
BROKER_USERNAME = os.environ.get('WIS2BOX_BROKER_USERNAME')
BROKER_PASSWORD = os.environ.get('WIS2BOX_BROKER_PASSWORD')
BROKER_HOST = os.environ.get('WIS2BOX_BROKER_HOST')
BROKER_PORT = os.environ.get('WIS2BOX_BROKER_PORT')
BROKER_PUBLIC = os.environ.get('WIS2BOX_BROKER_PUBLIC').rstrip('/')

DOCKER_API_URL = os.environ.get('WIS2BOX_DOCKER_API_URL')
STORAGE_SOURCE = os.environ.get('WIS2BOX_STORAGE_SOURCE')
STORAGE_USERNAME = os.environ.get('WIS2BOX_STORAGE_USERNAME')
STORAGE_PASSWORD = os.environ.get('WIS2BOX_STORAGE_PASSWORD')
STORAGE_PUBLIC = os.environ.get('WIS2BOX_STORAGE_PUBLIC')

# API details
API_URL = os.environ.get('WIS2BOX_API_URL').rstrip('/')
LOGGER.debug(API_URL)


class SynopProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: pygeoapi.process.synop-form.submit
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        mimetype = 'application/json'
        errors = []
        bufr = []
        urls = []
        result = 'failure'

        # First setup MinIO client
        try:
            is_secure = False
            urlparsed = urlparse(STORAGE_SOURCE)
            if STORAGE_SOURCE.startswith('https://'):
                is_secure = True
            client = Minio(endpoint=urlparsed.netloc,
                           access_key=STORAGE_USERNAME,
                           secret_key=STORAGE_PASSWORD,
                           secure=is_secure)
        except Exception as e:
            msg = f"Error connecting to MinIO: {e}"
            return self._handle_error(msg)

        # Second get list of all stations in CSV
        try:
            metadata = self._load_stations()
        except Exception as e:
            msg = f"Error loading stations: {e}"
            return self._handle_error(msg)

        LOGGER.debug("Metadata fetched")
        LOGGER.debug(metadata)
        synop_converted = 0
        # Now call synop to BUFR
        try:
            fm12 = data['data']
            year = data['year']
            month = data['month']
            if 'channel' not in data:
                data['channel'] = 'synop/test'
                channel = data['channel']
            else:
                channel = data['channel']
            bufr_generator = transform(data=fm12,
                                       metadata=metadata,
                                       year=year,
                                       month=month)

            # transform returns a generator, we need to iterate over
            # and add to single output object
            # TODO inspect result and handle errors
            for result in bufr_generator:
                bufr.append(result)
                synop_converted += 1
                LOGGER.info(f"result: {result}")

        except Exception as e:
            LOGGER.error(e)
            errors.append(f"Error converting to BUFR: {e}")

        # mqtt connection details
        auth = {'username': BROKER_USERNAME, 'password': BROKER_PASSWORD}

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
                if fmt == "_meta":
                    continue
                storage_url = f'{STORAGE_SOURCE}/{STORAGE_PUBLIC}/{channel}/{identifier}.{fmt}'  # noqa
                storage_path = f'{channel}/{identifier}.{fmt}'
                client.put_object(
                    bucket_name=STORAGE_PUBLIC,
                    object_name=storage_path,
                    data=io.BytesIO(the_data), length=-1,
                    part_size=10 * 1024 * 1024)

                # TODO ask Dave, how could this not be bufr4?
                if fmt == 'bufr4':
                    try:
                        hash_method = 'sha256'
                        hash_value = hashlib.sha256(the_data).hexdigest()
                    except Exception as e:
                        LOGGER.error(e)
                        errors.append(f"error hashing: {e}")

                    try:
                        msg = {
                            'id': str(uuid.uuid4()),
                            'type': 'Feature',
                            'version': 'v04',
                            'geometry': item['_meta']['geometry'],
                            'properties': {
                                'data_id': identifier,
                                'datetime': data_date.isoformat(),
                                'pubtime': dt.now().isoformat(),
                                'integrity': {
                                    'method': hash_method,
                                    'value': hash_value,
                                },
                                'wigos_station_identifier': wsi
                            },
                            'links': [
                                {
                                    'rel': 'canonical',
                                    'type': 'application/x-bufr',
                                    'href': storage_url,
                                    'length': len(the_data)
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
                        errors.append(f"error creating message: {e}")

                    LOGGER.debug(msg)

                    try:
                        publish.single(topic=f'{BROKER_PUBLIC}/{channel}',
                                       payload=json.dumps(msg), qos=1,
                                       retain=False, hostname=BROKER_HOST,
                                       port=int(BROKER_PORT), auth=auth)
                        LOGGER.debug(f"Message successfully published to {BROKER_PUBLIC}{channel}") # noqa
                        urls.append(storage_url)
                    except Exception as e:
                        LOGGER.error("Error publishing")
                        LOGGER.error(json.dumps(auth, indent=2))
                        LOGGER.error(BROKER_HOST)
                        LOGGER.error(BROKER_PORT)
                        LOGGER.error(e)
                        errors.append(f"{e}")

        if synop_converted > 0 and errors == []:
            result = 'success'
        elif synop_converted == 0:
            errors.append("No SYNOP messages converted")
            result = 'failure'
        else:
            result = 'partial success'
            errors.append("Some errors occurred")

        outputs = {
            'result': result,
            "messages transformed": synop_converted,
            "messages published": len(urls),
            "files": urls,
            "errors": errors
        }

        return mimetype, outputs

    def _load_topics(self):
        # attempt to load topics
        topics_url = f"{API_URL}/collections/topics/items"  # noqa
        return

    def _load_stations(self):

        stations_url = f"{API_URL}/collections/stations/items"  # noqa

        LOGGER.debug(stations_url)

        r = requests.get(stations_url, params={'f': 'json'}).json()
        csv_output = []
        for station in r['features']:
            wsi = station['properties']['wigos_station_identifier']
            tsi = wsi.split("-")[3]
            obj = {
                'station_name': station['properties']['name'],
                'wigos_station_identifier': wsi,
                'traditional_station_identifier': tsi,
                'facility_type': station['properties']['facility_type'],
                'latitude': station['geometry']['coordinates'][1],
                'longitude': station['geometry']['coordinates'][0],
                'elevation': station['geometry']['coordinates'][2],
                'territory_name': station['properties']['territory_name'],
                'wmo_region': station['properties']['wmo_region'],
                'barometer_height': None
            }
            csv_output.append(obj)

        string_buffer = io.StringIO()
        csv_writer = csv.DictWriter(string_buffer, fieldnames=csv_output[0].keys())  # noqa
        csv_writer.writeheader()
        csv_writer.writerows(csv_output)
        csv_string = string_buffer.getvalue()
        csv_string = csv_string.replace("\r\n", "\n")  # noqa make sure *nix line endings
        string_buffer.close()

        return csv_string

    def __repr__(self):
        return '<submit> {}'.format(self.name)

    def _handle_error(self, e):
        LOGGER.error(e)
        mimetype = 'application/json'
        errors = []
        errors.append(f"{e}")
        outputs = {
            'result': 'failure',
            "errors": errors
        }
        return mimetype, outputs
