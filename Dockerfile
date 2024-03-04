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
FROM wmoim/dim_eccodes_baseimage:2.34.0

ENV PYGEOAPI_CONFIG=/data/wis2box/config/pygeoapi/local.config.yml
ENV PYGEOAPI_OPENAPI=/data/wis2box/config/pygeoapi/local.openapi.yml

RUN apt-get update -y && apt-get install cron curl python3-pip git unzip -y
# install gunicorn, gevent, gdal, elasticsearch
RUN apt-get install -y --no-install-recommends \
    libgdal-dev gunicorn python3-gevent python3-gdal python3-elasticsearch libudunits2-dev dos2unix wget \
    && rm -rf /var/lib/apt/lists/*

# install pygeoapi, pywcmp, pymetdecoder, synop2bufr, csv2bufr, bufr2geojson
RUN pip3 install --no-cache-dir git+https://github.com/geopython/pygeoapi.git@master \
    && pip3 install --no-cache-dir \
    https://github.com/wmo-im/pywis-topics/archive/main.zip \
    https://github.com/wmo-im/pywcmp/archive/master.zip \
    https://github.com/wmo-im/bufr2geojson/archive/main.zip \
    https://github.com/wmo-im/csv2bufr/archive/main.zip \
    https://github.com/wmo-im/pymetdecoder/archive/refs/tags/v0.1.10.zip  \
    https://github.com/wmo-cop/pyoscar/archive/refs/tags/0.6.4.zip \
    https://github.com/wmo-im/synop2bufr/archive/main.zip

# install csv2bufr templates
RUN mkdir /opt/csv2bufr &&  \
    cd /opt/csv2bufr && \
    wget https://github.com/wmo-im/csv2bufr-templates/archive/refs/tags/v0.2.tar.gz && \
    tar -zxf v0.2.tar.gz --strip-components=1 csv2bufr-templates-0.2/templates

# install wis2box-api
COPY . /app
COPY wis2box_api/templates/admin /pygeoapi/pygeoapi/templates/admin
COPY ./docker/pygeoapi-config.yml $PYGEOAPI_CONFIG
#COPY ./docker/pygeoapi-openapi.yml $PYGEOAPI_OPENAPI

RUN cd /app \
    && pip3 install -e . \
    && chmod +x /app/docker/es-entrypoint.sh /app/docker/wait-for-elasticsearch.sh

# add wis2box.cron to crontab
COPY ./docker/wis2box-api.cron /etc/cron.d/wis2box-api.cron

RUN chmod 0644 /etc/cron.d/wis2box-api.cron && crontab /etc/cron.d/wis2box-api.cron

ENTRYPOINT [ "/app/docker/es-entrypoint.sh" ]
#ENTRYPOINT [ "/bin/bash" ]
