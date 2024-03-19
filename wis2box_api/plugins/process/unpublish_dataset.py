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

import paho.mqtt.publish as publish

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from wis2box_api.wis2box.env import BROKER_HOST
from wis2box_api.wis2box.env import BROKER_PORT
from wis2box_api.wis2box.env import BROKER_USERNAME
from wis2box_api.wis2box.env import BROKER_PASSWORD


LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'unpublish_dataset',
    'title': 'Unpublish dataset',
    'description': 'Remove metadata and data-mappings in backend and send notification to unpublish metadata', # noqa
    'keywords': [],
    'links': [],
    'inputs': {
        'metadata_id': {
            'title': {'en': 'metadata record identifier'},
            'description': {'en': 'metadata record identifier'},
            'schema': {'type': 'string', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        }
    },
    'outputs': {
        'path': {
            'title': {'en': 'status'},
            'description': {
                'en': 'status of update'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs':
        {
            'dataset_id': "urn:wmo:md:test-wis-node2:surface-based-observations.synop", # noqa
        }
    }
}


class UnpublishDatasetProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: wis2box_api.plugins.process.publish_dataset
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        LOGGER.debug('Execute process')

        try:
            metadata_id = data['metadata_id']
        except KeyError:
            msg = 'Missing required parameter: metadata_id'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        try:
            # publish notification on internal broker
            private_auth = {
                'username': BROKER_USERNAME,
                'password': BROKER_PASSWORD
            }
            publish.single(topic=f'wis2box/dataset/unpublication/{metadata_id}', # noqa
                           payload=None,
                           qos=1,
                           retain=False,
                           hostname=BROKER_HOST,
                           port=int(BROKER_PORT),
                           auth=private_auth)
            LOGGER.debug('DataPublishRequest published')
        except Exception as e:
            return f'Error publishing message: msg={msg}, error={e}'

        status = 'success'
        mimetype = 'application/json'
        outputs = {
            'status': status
        }
        return mimetype, outputs
