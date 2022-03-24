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
from pygeoapi.provider.base import BaseProvider
import json, requests
from datetime import datetime, timedelta, timezone


class ERDDAPProvider(BaseProvider):
    def __init__(self, provider_def):
        super().__init__(provider_def)

    def query(self, startindex=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None,
              filterq=None, **kwargs):
        url = self.data
        query = self.options["query"] if self.options["query"] is not None else ""  # noqa
        filters = self.options["filters"] if self.options["filters"] is not None else ""  # noqa
        url = f"{url}.geoJson{query}{filters}"
        timefilter = ""
        if "maxAgeHours" in self.options:
            maxAge = self.options["maxAgeHours"]
            if maxAge is not None:
                currenttime = datetime.now(timezone.utc)
                mintime = currenttime - \
                          timedelta(hours=maxAge)
                mintime = mintime.strftime("%Y-%m-%dT%H:%M:%SZ")
                timefilter = f"&time>={mintime}"
        url = f"{url}{timefilter}"
        data = json.loads(requests.get(url).text)
        # add id to each feature as this is required by pygeoapi
        for idx in range( len(data["features"])):
            data["features"][idx]["id"] = idx
        return data