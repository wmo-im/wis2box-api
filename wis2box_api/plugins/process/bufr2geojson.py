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
import logging

import requests

from pygeoapi.process.base import BaseProcessor
from bufr2geojson import transform as as_geojson

from wis2box_api.wis2box.env import STORAGE_PUBLIC_URL, STORAGE_SOURCE

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'bufr2geojson',
    'title': 'Convert BUFR to geoJSON',  # noqa
    'description': 'Download bufr from URL and convert file-content to geoJSON',  # noqa
    'keywords': [],
    'links': [],
    'jobControlOptions': ['async-execute'],
    'inputs': {
        'data_url': {
            'title': 'data_url',
            'description': 'URL to the BUFR file',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
            'default': None
        },
        'data': {
            'title': 'data',
            'description': 'UTF-8 string of base64 encoded bytes to be converted to geoJSON',  # noqa
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
            'default': None
        },
    },
    'outputs': {
        'path': {
            'title': {'en': 'FeatureCollection'},
            'description': {
                'en': 'A GeoJSON FeatureCollection of the converted data'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'data_url': 'https://zmdwis2box.mgee.gov.zm/data/2023-07-27/wis/zmb/zambia_met_service/data/core/weather/surface-based-observations/synop/WIGOS_0-894-2-ZimbaSS_20230727T125400.bufr4', # noqa
        }
    }
}


class Bufr2geojsonProcessor(BaseProcessor):

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

        LOGGER.debug('Execute process')

        items = []
        input_bytes = None
        error = ''
        try:
            if 'data_url' in data:
                data_url = data['data_url']
                # replace the public URL with the internal minio URL
                LOGGER.debug('Replacing STORAGE_PUBLIC_URL with internal storage URL')  # noqa
                # replace the public URL with the internal storage URL
                data_url = data_url.replace(STORAGE_PUBLIC_URL, f'{STORAGE_SOURCE}/wis2box-public')  # noqa
                LOGGER.debug(f'Executing bufr2geojson on: {data_url}')
                # read the data from the URL
                result = requests.get(data_url)
                # raise an exception if the status code is not 200
                result.raise_for_status()
                # get the bytes from the response
                input_bytes = result.content
            elif 'data' in data:
                base64_encoded_data = data['data']
                LOGGER.debug(f'Executing bufr2geojson on: {base64_encoded_data}')  # noqa
                # Convert the encoded data string to bytes
                encoded_data_bytes = base64_encoded_data.encode('utf-8')
                # Decode base64 encoded data
                input_bytes = base64.b64decode(encoded_data_bytes)
            else:
                raise Exception('No data or data_url provided')
            LOGGER.debug('Generating GeoJSON features')
            
            #extract input filename from data_url
            input_filename = data_url.split('/')[-1]
            generator = as_geojson(input_bytes)
            # iterate over the generator
            # add the features to a list
            error = ''
            for collection in generator:
                for id, item in collection.items():
                    LOGGER.info(f'Processing item: {id}')
                    LOGGER.info(f'item: {item}')
                    if id != 'geojson':
                        continue
                    try:
                        my_props = {}
                        my_props['name'] = item['properties']['observedProperty']
                        my_props['value'] = item['properties']['result']['value']
                        my_props['units'] = item['properties']['result']['units']
                        my_props['phenomenonTime'] = item['properties']['phenomenonTime']
                        my_props['host'] = item['properties']['host'] if 'host' in item['properties'] else None # noqa
                        LOGGER.info(f"keys in item['properties']: {item['properties'].keys()}")
                        # attempt to extract the reportIdentifier from the parameter
                        # otherwise use the data_url
                        report_id = 'reportId not found'
                        if 'parameter' in item['properties'] and 'reportIdentifier' in item['properties']['parameter']: # noqa
                            report_id = item['properties']['parameter']['reportIdentifier']	
                        elif data_url:
                            report_id = data_url.split('/')[-1].split('.')[0]
                        my_item = {
                            'id': item['id'],
                            'type': item['type'],
                            'geometry': item['geometry'],
                            'reportId': report_id,
                            'properties': my_props,
                        }
                        items.append(my_item)
                    except Exception as e:
                        msg = f"Error processing item={item['id']}: {e}; "
                        LOGGER.error(msg)
                        error += str(msg)
        except Exception as e:
            LOGGER.error(e)
            error += str(e)

        LOGGER.info(f'Number of features found: {len(items)}')

        mimetype = 'application/json'
        outputs = {
            'items': items,
            'error': error
        }
        return mimetype, outputs
