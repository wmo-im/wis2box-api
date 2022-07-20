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

from copy import deepcopy
import fcntl
import os
import json
import json_merge_patch
from jsonschema.exceptions import ValidationError
import logging
from typing import Any, Tuple, Union
import yaml

from pygeoapi.api import API, APIRequest, F_HTML, pre_process

from pygeoapi.config import validate_config
from pygeoapi.openapi import get_oas, validate_openapi_document
from pygeoapi.util import to_json, render_j2_template


LOGGER = logging.getLogger(__name__)


class Admin(API):
    """Admin object"""

    PYGEOAPI_CONFIG = os.environ.get("PYGEOAPI_CONFIG")
    PYGEOAPI_OPENAPI = os.environ.get("PYGEOAPI_OPENAPI")

    def __init__(self, config):
        """
        constructor

        :param config: configuration dict

        :returns: `wis2box_api.Admin` instance
        """

        super().__init__(config)

    def validate(self, config):
        """
        Validate pygeoapi configuration and open api to file

        :param config: configuration dict
        """
        # validate pygeoapi configuration
        LOGGER.debug('Validating configuration')
        validate_config(config)
        # validate open api document
        LOGGER.debug('Validating openapi document')
        oas = get_oas(config)
        validate_openapi_document(oas)

    def write(self, config):
        """
        Write pygeoapi configuration and open api to file

        :param config: configuration dict
        """
        self.write_config(config)
        self.write_oas(config)

    def write_config(self, config):
        """
        Write pygeoapi configuration file

        :param config: configuration dict
        """
        # validate pygeoapi configuration
        validate_config(config)

        # write pygeoapi configuration
        LOGGER.debug('Writing configutation')
        with open(self.PYGEOAPI_CONFIG, "w") as fh:
            fcntl.lockf(fh, fcntl.LOCK_EX)

            yaml.safe_dump(
                config,
                fh,
                sort_keys=False,
                default_flow_style=False,
            )
        LOGGER.debug('Finished writing configutation')

    def write_oas(self, config):
        """
        Write pygeoapi open api document

        :param config: configuration dict
        """
        # validate open api document
        oas = get_oas(config)
        validate_openapi_document(oas)

        # write open api document
        LOGGER.debug('Writing open api document')
        with open(self.PYGEOAPI_OPENAPI, "w") as fh:
            fcntl.lockf(fh, fcntl.LOCK_EX)

            yaml.safe_dump(
                oas,
                fh,
                sort_keys=False,
                default_flow_style=False,
            )
        LOGGER.debug('Finished writing open api document')

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
            content = render_j2_template(
                self.config, 'admin/index.html', self.config, request.locale
            )
        else:
            content = to_json(self.config, self.pretty_print)

        return headers, 200, content

    @pre_process
    def resources(
        self, request: Union[APIRequest, Any]
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
            content = render_j2_template(
                self.config,
                'admin/index.html',
                self.config['resources'],
                request.locale,
            )
        else:
            content = to_json(self.config['resources'], self.pretty_print)

        return headers, 200, content

    @pre_process
    def post_resource(
        self, request: Union[APIRequest, Any]
    ) -> Tuple[dict, int, str]:
        """
        Add resource configuration

        :param request: A request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        data = request.data
        if not data:
            msg = 'missing request data'
            return self.get_exception(
                400, headers, request.format, 'MissingParameterValue', msg
            )

        try:
            # Parse data
            data = data.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        try:
            data = json.loads(data)
        except (json.decoder.JSONDecodeError, TypeError) as err:
            # Input is not valid JSON
            LOGGER.error(err)
            msg = 'invalid request data'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg
            )

        resource_id = next(iter(data.keys()))

        if config['resources'].get(resource_id) is not None:
            # Resource already exists
            msg = f'Resource exists: {resource_id}'
            LOGGER.error(msg)
            return self.get_exception(
                400, headers, request.format, 'NoApplicableCode', msg
            )

        LOGGER.debug(f'Adding resource: {resource_id}')
        config['resources'].update(data)

        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config)

        LOGGER.error(request.path_info)
        content = f'Location: /{request.path_info}/{resource_id}'

        return headers, 201, content

    @pre_process
    def get_resource(
        self, request: Union[APIRequest, Any], resource_id: str
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
                400, headers, request.format, 'ResourceNotFound', msg
            )

        if request.format == F_HTML:
            content = render_j2_template(
                self.config, 'admin/index.html', resource, request.locale
            )
        else:
            content = to_json(resource, self.pretty_print)

        return headers, 200, content

    @pre_process
    def delete_resource(
        self, request: Union[APIRequest, Any], resource_id: str
    ) -> Tuple[dict, int, str]:
        """
        Delete resource configuration

        :param request: A request object
        :param resource_id: resource identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        try:
            LOGGER.debug(f'Removing resource configuration for: {resource_id}')
            config['resources'].pop(resource_id)
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg
            )

        LOGGER.debug('Resource removed, validating and saving configuration')
        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config)

        return headers, 204, {}

    @pre_process
    def put_resource(
        self, request: Union[APIRequest, Any], resource_id: str,
    ) -> Tuple[dict, int, str]:
        """
        Update complete resource configuration

        :param request: A request object
        :param resource_id: resource identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        try:
            LOGGER.debug('Verifying resource exists')
            config['resources'][resource_id]
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg
            )

        data = request.data
        if not data:
            msg = 'missing request data'
            return self.get_exception(
                400, headers, request.format, 'MissingParameterValue', msg
            )

        try:
            # Parse data
            data = data.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        try:
            data = json.loads(data)
        except (json.decoder.JSONDecodeError, TypeError) as err:
            # Input is not valid JSON
            LOGGER.error(err)
            msg = 'invalid request data'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg
            )

        LOGGER.debug('Updating resource')
        config['resources'].update({resource_id: data})

        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config)

        return headers, 204, {}

    @pre_process
    def patch_resource(
        self, request: Union[APIRequest, Any], resource_id: str
    ) -> Tuple[dict, int, str]:
        """
        Update partial resource configuration

        :param request: A request object
        :param resource_id: resource identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        try:
            LOGGER.debug('Verifying resource exists')
            resource = config['resources'][resource_id]
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg
            )

        data = request.data
        if not data:
            msg = 'missing request data'
            return self.get_exception(
                400, headers, request.format, 'MissingParameterValue', msg
            )

        try:
            # Parse data
            data = data.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        try:
            data = json.loads(data)
        except (json.decoder.JSONDecodeError, TypeError) as err:
            # Input is not valid JSON
            LOGGER.error(err)
            msg = 'invalid request data'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg
            )

        LOGGER.debug('Merging resource block')
        data = json_merge_patch.merge(resource, data)
        LOGGER.debug('Updating resource')
        config.update({resource_id: data})

        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config)

        content = to_json(resource, self.pretty_print)

        return headers, 200, content
