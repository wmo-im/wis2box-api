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

from osgeo import ogr
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

wmo_ra_geojson = '/data/wmo-ra.geojson'

PROCESS_DEF = {
    'version': '0.1.0',
    'id': 'wmo-get-ra',
    'title': {
        'en': 'Get WMO RA by geometry'
    },
    'description': {
        'en': 'Get WMO RA by geometry'
    },
    'keywords': ['wmo', 'ra'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://github.com/OGCMetOceanDWG/wmo-ra',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'geometry': {
            'title': 'Geometry (WKT)',
            'description': 'Geometry (WKT)',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['wmo', 'ra']
        }
    },
    'outputs': {
        'result': {
            'title': 'WMO RA result',
            'description': 'WMO RA result',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'geometry': 'POINT(-79 43)',
        }
    }
}


class WMORAProcessor(BaseProcessor):
    """WMO RA Processor"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: wis2box_api.plugins.process.wmo_ra.WMORAProcessor
        """

        super().__init__(processor_def, PROCESS_DEF)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        mimetype = 'application/json'

        outputs = {
            'wmo-ra': []
        }

        geometry = data.get('geometry')

        if geometry is None:
            msg = 'Missing WKT geometry'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        driver = ogr.GetDriverByName("GeoJSON")
        dataSource = driver.Open(wmo_ra_geojson, 0)
        layer = dataSource.GetLayer()

        geometry = ogr.CreateGeometryFromWkt(data['geometry'])
        LOGGER.debug(f'Geometry: {geometry}')

        if geometry is None:
            msg = 'Invalid WKT geometry'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        for feature in layer:
            if geometry.Intersects(feature.GetGeometryRef()):
                outputs['wmo-ra'].append(feature.GetField("roman_num"))

        return mimetype, outputs

    def __repr__(self):
        return '<StationInfoProcessor> {}'.format(self.name)
