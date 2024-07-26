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

import json
import os
import logging
import requests

from pygeoapi.util import yaml_load
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from wis2box_api.wis2box.env import WIS2BOX_DOCKER_API_URL

LOGGER = logging.getLogger(__name__)

with open(os.getenv('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
    CONFIG = yaml_load(fh)


PROCESS_DEF = {
    'version': '0.1.0',
    'id': 'dataset-info',
    'title': 'Dataset Information',
    'description': 'Returns information collected about the dataset, such as notifications and errors in the last 24 hours',
    'keywords': [],
    'links': [],
    'inputs': {
        'collection': {
            'title': {'en': 'Collection identifier'},
            'description': {'en': 'Collection identifier'},
            'keywords': {'en': ['collection', 'topic', 'dataset']},
            'schema': {
                'type': 'string',
                'default': None
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None  # TODO how to use?
        }
    },
    'outputs': {
        'dataset_info': {
            'title': {'en': 'Dataset Info'},
            'description': {
                'en': 'Dataset info in JSON format'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'collection': 'urn:x-wmo:md:mw-mw_met_centre:surface-weather-observations'  # noqa
        }
    }
}


class DatasetInfoProcessor(BaseProcessor):
    """Station Info Processor"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: wis2box_api.plugins.process.dataset_info.DatasetInfoProcessor
        """

        super().__init__(processor_def, PROCESS_DEF)

        host = os.environ['WIS2BOX_API_BACKEND_URL']

        self.es = Elasticsearch(host)

        if not self.es.ping():
            msg = 'Cannot connect to Elasticsearch'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        mimetype = 'application/json'

        dataset_info = {
            'status': 'Unknown',
            'msg_last_24hrs': None,
            'errors_last_24hours': None
        }

        try:
            collection_id = data['collection']
            topic = 'notfound'
            # get the topic from the collection
            url = f'{WIS2BOX_DOCKER_API_URL}/collections/discovery-metadata/items/{collection_id}?f=json' # noqa
            response = requests.get(url)
            if response.status_code == 200 and 'wmo:topicHierarchy' in response.json()['properties']:  # noqa
                topic = response.json()['properties']['wmo:topicHierarchy']
            else:
                LOGGER.error(f'Error getting topic for collection {collection_id}') # noqa
                raise ProcessorExecuteError('Error getting topic for collection') # noqa
        except KeyError:
            msg = 'Collection id required'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        # define date offset
        days = data.get('days', 1) + (data.get('years', 0) * 365)
        _time_delta = timedelta(days=days, minutes=59, seconds=59)
        date_min_24hrs = (datetime.utcnow() - _time_delta).isoformat()

        # prepare es query
        query_core = {
            'bool': {
                'filter': [
                    {"range": {"properties.pubtime": {"gte": date_min_24hrs}}}
                ],
                'must': [
                    {"wildcard": {"properties.data_id.keyword": f"*{topic.replace('origin/a/wis2/','')}*"}} # noqa
                ]
            }
        }
        query_agg = {
            'each': {
                'terms': {
                    'field': 'properties.data_id.keyword',
                    'size': 64000
                },
                'aggs': {
                    'count': {
                        'terms': {'field': 'properties.data_id.keyword', 'size': 64000} # noqa
                    }
                }
            }
        }
        query = {'size': 0, 'query': query_core, 'aggs': query_agg}
        response = self.es.search(index='messages', **query)

        # get count result from elastic search query response
        dataset_info['msg_last_24hrs'] = response['hits']['total']['value']

        # query loki for errors
        loki_base_url = "http://loki:3100"
        query = '{container_name="wis2box-management"} |~ "ERROR"'
        now_utc = datetime.now(datetime.UTC)
        current_time = now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time = now_utc - timedelta(hours=24).strftime('%Y-%m-%dT%H:%M:%SZ') # noqa
        end_time = current_time

        result = self.query_loki(loki_base_url, query, start_time, end_time)
        if result:
            dataset_info['errors_last_24hours'] = len(result['data']['result'])
        else:
            dataset_info['errors_last_24hours'] = 0
        outputs = {
            'dataset_info': dataset_info
        }
        return mimetype, outputs

    def query_loki(base_url, query, start_time, end_time):
        endpoint = f"{base_url}/loki/api/v1/query_range"
        payload = {
            "query": query,
            "start": start_time,
            "end": end_time
        }
        headers = {
            "Accept": "application/json",
            "X-Scope-OrgID": "1"  # Adjust as per your Loki setup
        }
        try:
            response = requests.get(endpoint, params=payload, headers=headers)
            if response.status_code == 200:
                try:
                    json_response = response.json()
                    return json_response
                except json.JSONDecodeError:
                    LOGGER.error("Loki returned a 200 status, but response is not valid JSON") # noqa
            else:
                LOGGER.error(f"Received non-200 status code. Response text: {response.text[:500]}") # noqa
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request Exception: {e}")
            return None

    def __repr__(self):
        return '<DatasetInfoProcessor> {}'.format(self.name)
