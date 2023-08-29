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

default_mappings = {}

default_mappings['aws_mappings.json'] = {
    "inputShortDelayedDescriptorReplicationFactor": [],
    "inputDelayedDescriptorReplicationFactor": [1, 1],
    "inputExtendedDelayedDescriptorReplicationFactor": [],
    "number_header_rows": 1,
    "column_names_row": 1,
    "delimiter": ",",
    "quoting": "QUOTE_NONE",
    "header": [
        {"eccodes_key": "edition", "value": "const:4"},
        {"eccodes_key": "masterTableNumber", "value": "const:0"},
        {"eccodes_key": "bufrHeaderCentre", "value": "const:0"},
        {"eccodes_key": "bufrHeaderSubCentre", "value": "const:0"},
        {"eccodes_key": "updateSequenceNumber", "value": "const:0"},
        {"eccodes_key": "dataCategory", "value": "const:0"},
        {"eccodes_key": "internationalDataSubCategory", "value": "const:2"},
        {"eccodes_key": "masterTablesVersionNumber", "value": "const:30"},
        {"eccodes_key": "numberOfSubsets", "value": "const:1"},
        {"eccodes_key": "observedData", "value": "const:1"},
        {"eccodes_key": "compressedData", "value": "const:0"},
        {"eccodes_key": "typicalYear", "value": "data:year"},
        {"eccodes_key": "typicalMonth", "value": "data:month"},
        {"eccodes_key": "typicalDay", "value": "data:day"},
        {"eccodes_key": "typicalHour", "value": "data:hour"},
        {"eccodes_key": "typicalMinute", "value": "data:minute"},
        {"eccodes_key": "unexpandedDescriptors", "value":"array:301150, 307096"} # noqa
    ],
    "data": [
        {"eccodes_key": "#1#wigosIdentifierSeries", "value":"data:wsi_series"}, # noqa
        {"eccodes_key": "#1#wigosIssuerOfIdentifier", "value":"data:wsi_issuer"}, # noqa
        {"eccodes_key": "#1#wigosIssueNumber", "value":"data:wsi_issue_number"}, # noqa
        {"eccodes_key": "#1#wigosLocalIdentifierCharacter", "value":"data:wsi_local"}, # noqa
        {"eccodes_key": "#1#latitude", "value": "data:latitude"},
        {"eccodes_key": "#1#longitude", "value": "data:longitude"},
        {"eccodes_key": "#1#heightOfStationGroundAboveMeanSeaLevel", "value":"data:station_height_above_msl"}, # noqa
        {"eccodes_key": "#1#heightOfBarometerAboveMeanSeaLevel", "value":"data:barometer_height_above_msl"}, # noqa
        {"eccodes_key": "#1#blockNumber", "value": "data:wmo_block_number"},
        {"eccodes_key": "#1#stationNumber", "value": "data:wmo_station_number"}, # noqa
        {"eccodes_key": "#1#stationType", "value": "data:station_type"},
        {"eccodes_key": "#1#year", "value": "data:year"},
        {"eccodes_key": "#1#month", "value": "data:month"},
        {"eccodes_key": "#1#day", "value": "data:day"},
        {"eccodes_key": "#1#hour", "value": "data:hour"},
        {"eccodes_key": "#1#minute", "value": "data:minute"},
        {"eccodes_key": "#1#nonCoordinatePressure", "value": "data:station_pressure"}, # noqa
        {"eccodes_key": "#1#pressureReducedToMeanSeaLevel", "value": "data:msl_pressure"}, # noqa
        {"eccodes_key": "#1#nonCoordinateGeopotentialHeight", "value": "data:geopotential_height"}, # noqa
        {"eccodes_key": "#1#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform", "value": "data:thermometer_height"}, # noqa
        {"eccodes_key": "#1#airTemperature", "value": "data:air_temperature"},  # noqa
        {"eccodes_key": "#1#dewpointTemperature", "value": "data:dewpoint_temperature"}, # noqa
        {"eccodes_key": "#1#relativeHumidity", "value": "data:relative_humidity"}, # noqa
        {"eccodes_key": "#1#methodOfStateOfGroundMeasurement", "value": "data:method_of_ground_state_measurement"}, # noqa
        {"eccodes_key": "#1#stateOfGround", "value": "data:ground_state"},
        {"eccodes_key": "#1#methodOfSnowDepthMeasurement", "value": "data:method_of_snow_depth_measurement"}, # noqa
        {"eccodes_key": "#1#totalSnowDepth", "value": "data:snow_depth"},
        {"eccodes_key": "#1#precipitationIntensityHighAccuracy", "value": "data:precipitation_intensity"},  # noqa
        {"eccodes_key": "#8#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform", "value": "data:anemometer_height"}, # noqa
        {"eccodes_key": "#3#timeSignificance", "value": "const:2"},
        {"eccodes_key": "#6#timePeriod", "value": "data:time_period_of_wind"},
        {"eccodes_key": "#1#windDirection", "value": "data:wind_direction"},
        {"eccodes_key": "#1#windSpeed", "value": "data:wind_speed"},
        {"eccodes_key": "#7#timePeriod", "value": "const:-10"},
        {"eccodes_key": "#1#maximumWindGustDirection", "value": "data:maximum_wind_gust_direction_10_minutes"}, # noqa
        {"eccodes_key": "#1#maximumWindGustSpeed", "value": "data:maximum_wind_gust_speed_10_minutes"}, # noqa
        {"eccodes_key": "#8#timePeriod", "value": "const:-60"},
        {"eccodes_key": "#2#maximumWindGustDirection", "value": "data:maximum_wind_gust_direction_1_hour"}, # noqa
        {"eccodes_key": "#2#maximumWindGustSpeed", "value": "data:maximum_wind_gust_speed_1_hour"}, # noqa
        {"eccodes_key": "#9#timePeriod", "value": "const:-180"},
        {"eccodes_key": "#3#maximumWindGustDirection", "value": "data:maximum_wind_gust_direction_3_hours"}, # noqa
        {"eccodes_key": "#3#maximumWindGustSpeed", "value": "data:maximum_wind_gust_speed_3_hours"}, # noqa
        {"eccodes_key": "#8#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform", "value": "data:rain_sensor_height"}, # noqa
        {"eccodes_key": "#17#timePeriod", "value": "const:-1"},
        {"eccodes_key": "#1#totalPrecipitationOrTotalWaterEquivalent", "value": "data:total_precipitation_1_hour"}, # noqa
        {"eccodes_key": "#18#timePeriod", "value": "const:-3"},
        {"eccodes_key": "#2#totalPrecipitationOrTotalWaterEquivalent", "value": "data:total_precipitation_3_hours"}, # noqa
        {"eccodes_key": "#19#timePeriod", "value": "const:-6"},
        {"eccodes_key": "#3#totalPrecipitationOrTotalWaterEquivalent", "value": "data:total_precipitation_6_hours"}, # noqa
        {"eccodes_key": "#20#timePeriod", "value": "const:-12"},
        {"eccodes_key": "#4#totalPrecipitationOrTotalWaterEquivalent", "value": "data:total_precipitation_12_hours"}, # noqa
        {"eccodes_key": "#21#timePeriod", "value": "const:-24"},
        {"eccodes_key": "#5#totalPrecipitationOrTotalWaterEquivalent", "value": "data:total_precipitation_24_hours"} # noqa
    ]
}
