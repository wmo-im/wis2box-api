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

import os

from typing import Any

LOGGER = logging.getLogger(__name__)

STORAGE_SOURCE = os.environ.get('WIS2BOX_STORAGE_SOURCE')
STORAGE_USERNAME = os.environ.get('WIS2BOX_STORAGE_USERNAME')
STORAGE_PASSWORD = os.environ.get('WIS2BOX_STORAGE_PASSWORD')


class Storage():
    """storage manager base class"""
    def __init__(self, name, channel) -> None:

        self.name = name
        self.channel = channel
        self.source = STORAGE_SOURCE
        self.username = STORAGE_USERNAME
        self.password = STORAGE_PASSWORD

    def get(self, identifier: str) -> Any:

        raise NotImplementedError

    def put(self, data: bytes, identifier: str) -> bool:

        raise NotImplementedError

    def delete(self, identifier: str) -> bool:

        raise NotImplementedError

    def __repr__(self):
        return f'<Storage ({self.source})>'
