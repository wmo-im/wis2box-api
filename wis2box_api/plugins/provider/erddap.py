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


# feature provider for ERDDAP integrations
#
# sample configuration
#
# providers:
#     -   type: feature
#         name: pygeoapi.provider.erddap.ERDDAPProvider
#         data: http://osmc.noaa.gov/erddap/tabledap/OSMC_Points
#         id_field: id
#         time_field: time
#         options:
#             query: "?"
#             filters: "&parameter=\"SLP\"&platform!=\"C-MAN%20WEATHER%20STATIONS\"&platform!=\"TIDE GAUGE STATIONS (GENERIC)\""   # noqa
#             max_age_hours: 12


from datetime import datetime, timedelta, timezone
import json
import logging

import requests

from pygeoapi.provider.base import BaseProvider

LOGGER = logging.getLogger(__name__)


class ERDDAPProvider(BaseProvider):
    def __init__(self, provider_def):
        super().__init__(provider_def)

    def query(self, startindex=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None,
              filterq=None, **kwargs):

        url = self.data

        LOGGER.debug('Setting query filters')
        query = self.options.get('query', '')
        filters = self.options.get('filters', '')

        url = f'{url}.geoJson{query}{filters}'

        timefilter = ''

        max_age_hours = self.options.get('max_age_hours')
        if max_age_hours is not None:
            LOGGER.debug('Setting time filter')
            currenttime = datetime.now(timezone.utc)
            mintime = currenttime - timedelta(hours=max_age_hours)
            mintime = mintime.strftime('%Y-%m-%dT%H:%M:%SZ')
            timefilter = f'&time>={mintime}'

        url = f'{url}{timefilter}'

        LOGGER.debug(f'Fetching data from {url}')
        data = json.loads(requests.get(url).text)['features'][startindex:limit]

        # add id to each feature as this is required by pygeoapi
        for idx in range(len(data)):
            data[idx]['id'] = idx

        return {
            'type': 'FeatureCollection',
            'features': data
        }
