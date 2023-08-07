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
from bufr2geojson import transform as as_geojson

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-bufr2geojson',
    'title': 'Convert BUFR to geoJSON',  # noqa
    'description': 'Download bufr from URL and convert file-content to geoJSON',  # noqa
    'keywords': [],
    'links': [],
    'inputs': {
        "data_url": {
            "title": "data_url (required)",
            "description": "URL to the BUFR file",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [],
        }
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
            "data_url": "https://zmdwis2box.mgee.gov.zm/data/2023-07-27/wis/zmb/zambia_met_service/data/core/weather/surface-based-observations/synop/WIGOS_0-894-2-ZimbaSS_20230727T125400.bufr4",
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
        data_url = data.get('data_url')

        LOGGER.info(f"Executing bufr2geojson on: {data_url}")

        # read the data from the URL
        input_bytes = requests.get(data_url).content
        LOGGER.debug('Generating GeoJSON features')
        generator = as_geojson(input_bytes, serialize=False)
        
        # iterate over the generator
        # add the features to a list
        items = []
        for collection in generator:
            for id, item in collection.items():
                if 'geojson' in item:
                    items.append(item['geojson'])
        LOGGER.info(f"Number of features found: {len(items)}")

        mimetype = 'application/json'
        outputs = {
            'items': items
        }
        return mimetype, outputs