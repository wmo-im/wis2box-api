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

import io
import logging

import requests

from wis2box_api.wis2box.env import WIS2BOX_DOCKER_API_URL

LOGGER = logging.getLogger(__name__)


class Stations():

    def __init__(self):
        self.stations = {}
        self._load_stations()

    def get_geometry(self, wsi: str) -> dict:
        """
        Get geometry from wsi

        :param wsi: WIGOS Station identifier

        :returns: `dict`, of geometryF
        """

        if wsi in self.stations:
            return self.stations[wsi]['geometry']
        else:
            return None

    def get_valid_wsi(self, wsi, tsi=None) -> str:
        """
        Validates and returns WSI

        :param wsi: WIGOS Station identifier
        :param tsi: Traditional Station identifier

        :returns: `str`, of valid wsi or `None`
        """

        if wsi in self.stations:
            return wsi
        elif tsi is not None:
            for station in self.stations:
                # check if the tsi is contain in the string
                if tsi in station['properties']['wigos_station_identifier']:
                    return station['properties']['wigos_station_identifier']
        return None

    def check_valid_wsi(self, wsi: str) -> bool:
        """
        Validates and returns WSI

        :param wsi: WIGOS Station identifier
        :param tsi: Traditional Station identifier

        :returns: `str`, of valid wsi or `None`
        """

        if wsi in self.stations:
            return True
        else:
            return False

    def get_station(self, wsi: str) -> dict:
        """
        Get station from wsi

        :param wsi: WIGOS Station identifier

        :returns: `dict`, of station
        """

        if wsi in self.stations:
            return self.stations[wsi]
        else:
            return None

    def get_csv_string(self) -> str:
        """Load stations into csv-string

        :returns: csv_string: csv string with station data
        """

        LOGGER.info('Loading stations into csv-string')

        csv_output = []
        for station in self.stations.values():
            wsi = station['properties']['wigos_station_identifier']
            tsi = wsi.split("-")[3]
            barometer_height = None
            if 'barometer_height' in station['properties']:
                barometer_height = station['properties']['barometer_height']
            else:
                barometer_height = station['geometry']['coordinates'][2] + 1.25
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
                'barometer_height': barometer_height
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

    def _load_stations(self):
        """Load stations from API

        :returns: None
        """

        LOGGER.info("Loading stations from API")

        stations_url = f"{WIS2BOX_DOCKER_API_URL}/collections/stations/items"
        LOGGER.info(stations_url)

        r = requests.get(stations_url, params={'f': 'json'}).json()
        if 'features' not in r:
            LOGGER.error("No features in response")
            raise Exception(f"No features in response from {stations_url}")
        elif len(r['features']) == 0:
            LOGGER.error("No features in response")
            raise Exception(f"No features in response from {stations_url}")
        else:
            for feature in r['features']:
                wsi = feature['properties']['wigos_station_identifier']
                self.stations[wsi] = feature
        LOGGER.info(f"Loaded {len(self.stations.keys())} stations from API")
