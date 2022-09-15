# =================================================================
#
# Authors: David Berry (dberry@wmo.int)
#
# Copyright (c) 2022 WMO
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import numpy as np
from pyproj import Geod
import requests

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


LOGGER = logging.getLogger(__name__)

g = Geod(ellps="WGS84")

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'tc-wind-quadrants',
    'title': {
        'en': 'Tropical cyclone wind quandrants',
    },
    'description': {
        'en': 'Process to extract tropical cyclone wind quadrants and to transform to usable geoJSONs',
    },
    'keywords': ['wind speed threshold','tropical cyclone','quadrant'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'storm': {
            'title': 'storm',
            'description': 'The name of the tropical cyclone',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['tropical cyclone','name']
        },
        'forecast-time': {
            'title': 'forecast-time',
            'description': 'Time that the forecast was made available (result time)',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['resulTime']
        },
        'phenomenon-time': {
            'title': 'phenomenon-time',
            'description': 'Time that the forecast is applicable for (phenomenonTime)',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['phenomenonTime']
        }
    },
    'outputs': {},
    'example': {
        'inputs': {
            'storm': 'MUIFA-16W',
            'forecast-time': '2022-09-15T18:00:00Z',
            'phenomenon-time': '2022-09-16T00:00:00Z',
        }
    }
}


class TCWindQuadrants(BaseProcessor):
    """Hello World Processor example"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.tropical_cyclone_quadrants.TCWindQuadrants
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        # check we have data
        if data.get('storm',None) is None:
            raise ProcessorExecuteError('Tropical cyclone name required')
        mimetype = 'application/json'
        # dict to store features to return
        collection = {
            "type": "FeatureCollection",
            "features": list()
        }
        # API endpoint
        api = "http://3.71.32.201/oapi/collections"
        # collection
        collection = "eue.ecmwf.data.core.weather.global-analysis-prediction.forecast"
        # build URL
        url = f'{api}/{collection}/items'
        # now filters, filtering on time currently broken due to the way
        # phenomenonTime and resultTime are set.
        query_parameters = {
            'wigos_station_identifier': data['storm'],
            'limit': 10000,
            'name': 'effective_radius_with_respect_to_wind_speeds_above_threshold'
        }
        collection = list()
        # get data
        response = requests.get(url, params=query_parameters).json()
        # now filter on time and convert polygons
        for feature in response['features']:
            if (feature['properties']['phenomenonTime'] == data['forecast-time']) & \
                    (feature['properties']['resultTime'] == data['phenomenon-time']):
                collection.append(self.extract_polygon(feature))

        return mimetype, collection

    def extract_polygon(self, feature):
        keep = ("centre", "generating_application", "storm_identifier", "long_storm_name",
                "technique_for_making_up_initial_perturbations", "ensemble_member_number", "ensemble_forecast_type",
                "meteorological_attribute_significance")

        radius = feature['properties']['value']
        parameters = feature['properties']['metadata']
        lon = feature['geometry']['coordinates'][0]
        lat = feature['geometry']['coordinates'][1]

        # get bearing
        bearing = None
        for parameter in parameters:
            if parameter["name"] == "bearing_or_azimuth":
                bearing = parameter["value"]
        assert (bearing is not None)
        if bearing[1] == 0:
            bearing[1] = 360
        # get wind speed
        wind_speed = None
        for parameter in parameters:
            if parameter["name"] == "wind_speed_threshold":
                wind_speed = parameter["value"]
                units = parameter["units"]
        assert (wind_speed is not None)

        # drop unwanted / used parameters
        feature['properties']['parameters'] = list()
        for parameter in parameters:
            if parameter['name'] in keep:
                feature['properties']['parameters'].append(parameter)
        del feature['properties']['metadata']

        x = list(map(lambda b: g.fwd(lon, lat, b, radius)[0:2], np.arange(bearing[0], bearing[1] + 2.5, 2.5)))
        x.insert(0, (lon, lat))
        x.append((lon, lat))
        feature['geometry']['type'] = "Polygon"
        feature['geometry']['coordinates'] = list()
        feature['geometry']['coordinates'].insert(0, x)
        feature['properties']['name'] = "wind_speed_threshold"
        feature['properties']['value'] = wind_speed
        feature['properties']['units'] = units

        return feature

    def __repr__(self):
        return '<TCWindQuadrants> {}'.format(self.name)