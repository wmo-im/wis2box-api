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