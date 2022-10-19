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

from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import os
import logging
import requests

from pygeoapi.util import yaml_load, url_join
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


LOGGER = logging.getLogger(__name__)

with open(os.getenv('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
    CONFIG = yaml_load(fh)


PROCESS_DEF = {
    'version': '0.1.0',
    'id': 'station-info',
    'title': 'Station Information',
    'description': 'Returns the station feature collection with'
    ' counted observations by station',
    'keywords': [],
    'links': [],
    'inputs': {
        'collection': {
            'title': {'en': 'Collection identifier'},
            'description': {'en': 'Collection identifier'},
            'keywords': {'en': ['collection', 'topic', 'dataset']},
            'schema': {
                'type': 'string',
                'default': None,
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
        },
        'wigos_station_identifier': {
            'title': {'en': 'WIGOS Station Identifier'},
            'schema': {
                'type': 'array',
                'minItems': 1,
                'items': {'type': 'string'},
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
        },
        'days': {
            'title': {'en': 'Days'},
            'schema': {'type': 'number', 'default': 1},
            'minOccurs': 0,
            'maxOccurs': 1,
        },
        'years': {
            'title': {'en': 'Years'},
            'schema': {'type': 'number', 'default': 0},
            'minOccurs': 0,
            'maxOccurs': 1,
        },
    },
    'outputs': {
        'path': {
            'title': {'en': 'FeatureCollection'},
            'description': {
                'en': 'A geoJSON FeatureCollection of the '
                'stations with their status'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json',
            },
        }
    },
    'example': {
        'inputs': {
            'collection': 'mwi.mwi_met_centre.data.core.weather.surface-based-observations.SYNOP',  # noqa
        }
    },
}


class StationInfoProcessor(BaseProcessor):
    """Station Info Processor"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.station_info.StationInfoProcessor
        """
        super().__init__(processor_def, PROCESS_DEF)

        host = os.environ['WIS2BOX_API_BACKEND_URL']
        self.es = Elasticsearch(host)

        if not self.es.ping():
            msg = 'Cannot connect to Elasticsearch'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        LOGGER.debug('Checking Elasticsearch version')
        version = float(self.es.info()['version']['number'][:3])
        if version < 7:
            msg = 'Elasticsearch version below 7 not supported ({})'.format(
                version
            )
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

    def execute(self, data):
        """
        Execute River Runner Process
        :param data: processor arguments
        :returns: 'application/json'
        """
        mimetype = 'application/json'
        outputs = {
            'id': 'path',
            'code': 'success',
            'value': None,
        }

        try:
            collection_id = data['collection']
            index = collection_id.lower()
        except KeyError:
            msg = 'Collection id required'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        wigos_station_identifiers = data.get('wigos_station_identifier', [])
        if isinstance(wigos_station_identifiers, list) is False:
            msg = 'wigos_station_identifier must be an array'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        days = data.get('days', 1) + (data.get('years', 0) * 365)
        date_offset = (datetime.utcnow() - timedelta(days=days)).isoformat()

        if CONFIG['resources'].get('stations') is None:
            msg = 'stations collection does not exist'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        fc = self._load_stations(wigos_station_identifiers)
        if None in fc['features']:
            msg = 'Invalid WIGOS station identifier provided'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)
        else:
            outputs['value'] = fc

        query_core = {
            'bool': {
                'filter': [
                    {
                        'range': {
                            'properties.resultTime.raw': {'gte': date_offset}
                        }
                    }
                ]
            }
        }
        query_agg = {
            'each': {
                'terms': {
                    'field': 'properties.wigos_station_identifier.raw',
                    'size': 64000
                },
                'aggs': {
                    'count': {
                        'terms': {
                            'field': 'reportId.raw',
                            'size': 64000
                        }
                    }
                }
            }
        }
        query = {'size': 0, 'query': query_core, 'aggs': query_agg}

        response = self.es.search(index=index, body=query)
        response_buckets = response['aggregations']['each']['buckets']

        hits = {b['key']: len(b['count']['buckets']) for b in response_buckets}

        for station in outputs['value']['features']:
            station['properties']['num_obs'] = hits.get(station['id'], 0)

        return mimetype, outputs

    def _load_stations(self, wigos_station_identifiers: list = []):
        fc = {'type': 'FeatureCollection', 'features': []}
        stations_url = url_join(
            os.getenv('WIS2BOX_DOCKER_API_URL'), 'collections/stations/items'
        )

        if wigos_station_identifiers != []:

            for wsi in wigos_station_identifiers:
                r = requests.get(f'{stations_url}/{wsi}')
                if r.ok:
                    fc['features'].append(r.json())
                else:
                    fc['features'].append(None)

        else:
            r = requests.get(
                stations_url, params={'resulttype': 'hits'}
            ).json()

            fc = requests.get(
                stations_url, params={'limit': r['numberMatched']}
            ).json()

        return fc

    def __repr__(self):
        return '<StationInfoProcessor> {}'.format(self.name)
