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

from pygeoapi.process.base import BaseProcessor
from synop2bufr import transform

from wis2box_api.wis2box.publish import WIS2Publish
from wis2box_api.wis2box.publish import handle_error

from wis2box_api.wis2box.station import Stations

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-synop-process',
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
        "notify": {
            "title": "Notify",
            "description": "Enable WIS2 notifications",
            "schema": {"type": "boolean"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "default": True
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
        'result': {
            'title': 'WIS2Publish result',
            'description': 'WIS2Publish result',
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
            "notify": True,
            "data": "AAXX 19064 64400 36/// /0000 10102 20072 30068 40182 53001 333 20056 91003 555 10302 91018=" # noqa
        }
    }
}


class SynopPublishProcessor(BaseProcessor):

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

        LOGGER.info('Executing process {}'.format(self.name))

        try:
            channel = data['channel']
            notify = data['notify']
            # initialize the WIS2Publish object
            wis2_publish = WIS2Publish(channel,notify)
        except Exception as err:
            return handle_error({err})
        
        # initialize the Stations object at execute
        # stations might have been updated since the process was initialized
        stations = Stations()
        # get the station metadata as a CSV string
        metadata = stations.get_csv_string()
        
        # Now call synop to BUFR
        try:
            fm12 = data['data']
            year = data['year']
            month = data['month']
            # run the transform
            bufr_generator = transform(data=fm12,
                                       metadata=metadata,
                                       year=year,
                                       month=month)
        except Exception as err:
            return wis2_publish.handle_error(f'synop2bufr raised Exception: {err}') # noqa

        output_items = []
        for item in bufr_generator:
            output_items.append(item)

        LOGGER.debug(f'synop2bufr-transform returned {len(output_items)} items') # noqa

        return wis2_publish.process_items(output_items)

    def __repr__(self):
        return '<submit> {}'.format(self.name)
