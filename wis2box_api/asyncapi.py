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

import os
import logging
from typing import Any, Tuple, Union

from pygeoapi.api import API, APIRequest, F_HTML, pre_process
from pygeoapi import l10n
from pygeoapi.util import to_json, render_j2_template

from wis2box_api import __version__


LOGGER = logging.getLogger(__name__)


class AsyncAPI(API):
    """AsyncAPI object"""

    PYGEOAPI_CONFIG = os.environ.get("PYGEOAPI_CONFIG")
    PYGEOAPI_OPENAPI = os.environ.get("PYGEOAPI_OPENAPI")

    def __init__(self, config):
        """
        constructor

        :param config: configuration dict

        :returns: `wis2box_api.AsyncAPI` instance
        """

        super().__init__(config)

    @pre_process
    def get_asyncapi(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:  # noqa
        """
        Provide AsyncAPI document

        :param request: request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        content = to_json(generate_asyncapi(self.config, request.locale),
                          self.pretty_print)

        return headers, 200, content

        if request.format == F_HTML:
            content = render_j2_template(
                self.config, 'admin/index.html', self.config, request.locale
            )
        else:
            content = to_json(generate_asyncapi(), self.pretty_print)

        return headers, 200, content


def generate_asyncapi(config: dict, locale: str) -> dict:
    """
    Generate an AsyncAPI document

    :param config: `dict` of pygeoapi configuration
    :param language: A locale string (e.g. "en-US" or "en") or Babel Locale.

    :returns: `dict` of AsyncAPI document
    """

    LOGGER.debug('Generating AsyncAPI document')

    title = l10n.translate(config['metadata']['identification']['title'], locale)  # noqa
    description = l10n.translate(config['metadata']['identification']['description'], locale)  # noqa
    tags = l10n.translate(config['metadata']['identification']['keywords'], locale)  # noqa

    a = {
        'asyncapi': '2.4.0',
        'id': 'https://github.com/wmo-im/wis2box',
        'info': {
            'version': __version__,
            'title': title,
            'description': description,
            'license': {
                'name': config['metadata']['license']['name'],
                'url': config['metadata']['license']['url']
            },
            'contact': {
                'name': config['metadata']['contact']['name'],
                'url': config['metadata']['contact']['url'],
                'email': config['metadata']['contact']['email']
            }
        },
        'servers': {
            'production': {
                'url': os.environ.get('WIS2BOX_MQTT_URL'),
                'protocol': 'mqtt',
                'protocolVersion': '5.0',
                'description': description
            }
        },
        'channels': {
            'origin/a/wis2': {
                'subscribe': {
                    'operationId': 'ClientSubscribed',
                    'message': {
                        '$ref': 'https://raw.githubusercontent.com/wmo-im/wis2-notification-message/main/WIS2_Message_Format_Schema.yaml'  # noqa
                    }
                }
            }
        },
        'tags': [{'name': tag} for tag in tags]
    }

    return a
