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

import fcntl
import os
import logging
from typing import Any, Tuple, Union
import yaml

from pygeoapi.api import API, APIRequest, F_HTML, pre_process
# from pygeoapi.config import validate_config
from pygeoapi.util import to_json, render_j2_template

LOGGER = logging.getLogger(__name__)

if 'PYGEOAPI_CONFIG' not in os.environ:
    raise RuntimeError('PYGEOAPI_CONFIG environment variable not set')

PYGEOAPI_CONFIG = os.environ.get('PYGEOAPI_CONFIG')


class Admin(API):
    """Admin object"""

    def __init__(self, config):
        """
        constructor

        :param config: configuration dict

        :returns: `wis2box_api.Admin` instance
        """

        super().__init__(config)

    def write(self):
        """
        Write pygeoapi configuration to file
        """
        # validate_config(self.config)

        with open(PYGEOAPI_CONFIG, "w") as fh:
            fcntl.lockf(fh, fcntl.LOCK_EX)

            yaml.safe_dump(self.config, fh, sort_keys=False, indent=4,
                           default_flow_style=False)

    @pre_process
    def admin(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:
        """
        Provide admin document

        :param request: A request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        if request.format == F_HTML:
            content = render_j2_template(self.config,
                                         'admin/index.html',
                                         self.config,
                                         request.locale)
        else:
            content = to_json(self.config, self.pretty_print)

        return headers, 200, content

    @pre_process
    def resources(self, request: Union[APIRequest, Any]
                  ) -> Tuple[dict, int, str]:
        """
        Provide admin document

        :param request: A request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        if request.format == F_HTML:
            content = render_j2_template(self.config,
                                         'admin/index.html',
                                         self.config['resources'],
                                         request.locale)
        else:
            content = to_json(self.config['resources'], self.pretty_print)

        return headers, 200, content

    @pre_process
    def get_resource(self, request: Union[APIRequest, Any],
                     resource_id: str
                     ) -> Tuple[dict, int, str]:
        """
        Get resource configuration

        :param request: A request object
        :param resource_id:

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        try:
            resource = self.config['resources'][resource_id]
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg)

        if request.format == F_HTML:
            content = render_j2_template(self.config,
                                         'admin/index.html',
                                         resource,
                                         request.locale)
        else:
            content = to_json(resource, self.pretty_print)

        return headers, 200, content

    @pre_process
    def delete_resource(self, request: Union[APIRequest, Any],
                        resource_id: str
                        ) -> Tuple[dict, int, str]:
        """
        Delete resource configuration

        :param request: A request object
        :param resource_id:

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        try:
            resource = self.config['resources'].pop(resource_id)
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg)

        self.write()

        if request.format == F_HTML:
            content = render_j2_template(self.config,
                                         'admin/index.html',
                                         resource,
                                         request.locale)
        else:
            content = to_json(resource, self.pretty_print)

        return headers, 200, content
