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
FROM wmoim/dim_eccodes_baseimage:2.28.0

RUN apt-get update -y && apt-get install curl python3-pip git unzip -y

ENV PYGEOAPI_CONFIG=/data/wis2box/config/pygeoapi/local.config.yml
ENV PYGEOAPI_OPENAPI=/data/wis2box/config/pygeoapi/local.openapi.yml

COPY . /app
COPY wis2box_api/templates/admin /pygeoapi/pygeoapi/templates/admin
COPY ./docker/pygeoapi-config.yml $PYGEOAPI_CONFIG

RUN cd /app \
    && python3 setup.py install \
    && pip3 install git+https://github.com/geopython/pygeoapi.git@master \
    && pip3 install https://github.com/wmo-im/pywcmp/archive/master.zip \
    && pip3 install --no-cache-dir https://github.com/wmo-im/pymetdecoder/archive/refs/tags/v0.1.7.zip \
    && pip3 install --no-cache-dir https://github.com/wmo-im/synop2bufr/archive/refs/tags/v0.5.0.tar.gz \
    && pip3 install --no-cache-dir https://github.com/david-i-berry/wis2box-api-plugin-synop/archive/main.zip \
    && pip3 install osgeo elasticsearch \
    && chmod +x /app/docker/es-entrypoint.sh /app/docker/wait-for-elasticsearch.sh

ENTRYPOINT [ "/app/docker/es-entrypoint.sh" ]
