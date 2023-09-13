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

import logging
import requests

from pygeoapi.process.base import BaseProcessor

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-oscar2feature',
    'title': 'Query OSCAR and return station metadata for wis2box',
    'description': 'Query OSCAR and return station metadata for wis2box',
    'keywords': [],
    'links': [],
    'inputs': {
        'wigos_station_identifier': {
            'title': {'en': 'WIGOS Station Identifier'},
            'description': {'en': 'WIGOS Station Identifier'},
            'schema': {'type': 'string', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        }
    },
    'outputs': {
        'path': {
            'title': {'en': 'StationFeature'},
            'description': {
                'en': 'StationFeature for wis2box'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'wigos_station_identifier': '0-20000-0-10393',	# noqa
        },
    },
}

OSCAR_STATION_URL = 'https://oscar.wmo.int/surface/#/search/station/stationReportDetails/' # noqa


class Oscar2FeatureProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: pygeoapi.process.synop-form.submit
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def handle_error(self, err):
        """
        Handle error

        :param err: error message

        :returns: json error response
        """

        mimetype = 'application/json'
        outputs = {
            'error': err
        }
        return mimetype, outputs

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        LOGGER.debug('Execute process')

        wmo_regions = {
            'Africa': 'I',
            'Asia': 'II',
            'South America': 'III',
            'North America, Central America and the Caribbean': 'IV',
            'South-West Pacific': 'V',
            'Europe': 'VI',
            'Antartica': 'VII'
        }

        wis2box_station = {}
        wigos_id = data.get('wigos_station_identifier')
        path = '/surface/rest/api/search/station?wigosId='+wigos_id
        page_path = f'{path}'
        headers = {
            'User-Agent': 'wis2box-api: https://github.com/wmo-im/wis2box-api'
        }
        res = requests.get('http://oscar.wmo.int'+page_path, headers=headers)
        if res.status_code != 200:
            return self.handle_error(f'OSCAR request failed: {res.status_code}') # noqa

        json_data = res.json()
        oscar_result = None
        if 'stationSearchResults' in json_data:
            for result in json_data['stationSearchResults']:
                oscar_result = result

        if oscar_result is None:
            return self.handle_error(f'Station with WIGOS identifier {wigos_id} not found in OSCAR') # noqa
        else:
            wis2box_station = {
                'id': oscar_result['wigosId'],
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [
                        oscar_result['longitude'],
                        oscar_result['latitude'],
                        oscar_result['elevation']
                    ]
                },
                'properties': {
                    'name': oscar_result['name'],
                    'wigos_station_identifier': oscar_result['wigosId'],
                    'facility_type': oscar_result['stationTypeName'],
                    'territory_name': oscar_result['territory'],
                    'barometer_height': None,
                    'wmo_region': wmo_regions[oscar_result['region']],
                    'url': OSCAR_STATION_URL+oscar_result['wigosId'],
                    'topics': [],
                    'status': oscar_result['stationDeclaredStatusCode'],
                    'id': oscar_result['wigosId']
                }
            }

        mimetype = 'application/json'
        outputs = {
            'feature': wis2box_station
        }
        return mimetype, outputs
