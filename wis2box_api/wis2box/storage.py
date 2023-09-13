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

import logging

from typing import Any

from wis2box_api.wis2box.env import STORAGE_SOURCE
from wis2box_api.wis2box.env import STORAGE_USERNAME
from wis2box_api.wis2box.env import STORAGE_PASSWORD

LOGGER = logging.getLogger(__name__)


class Storage():
    """storage manager base class"""
    def __init__(self, name) -> None:

        self.name = name
        self.source = STORAGE_SOURCE
        self.username = STORAGE_USERNAME
        self.password = STORAGE_PASSWORD

    def get(self, identifier: str) -> Any:
        """
        Get data from storage

        :param identifier: identifier of data

        :returns: `bytes`, of data
        """

        raise NotImplementedError()

    def put(self, data: bytes, identifier: str) -> bool:
        """
        Put data to storage

        :param data: data to store
        :param identifier: identifier of data

        :returns: `bool`, of success
        """

        raise NotImplementedError()

    def delete(self, identifier: str) -> bool:
        """
        Delete data from storage

        :param identifier: identifier of data

        :returns: `bool`, of success
        """

        raise NotImplementedError()

    def __repr__(self):
        return f'<Storage ({self.source})>'
