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

from minio import Minio

from io import BytesIO

from typing import Any

from wis2box_api.wis2box.storage import Storage

from urllib.parse import urlparse

LOGGER = logging.getLogger(__name__)


class MinIOStorage(Storage):
    """MinIO storage manager"""
    def __init__(self, name, channel) -> None:

        super().__init__(name=name, channel=channel)

        is_secure = False

        urlparsed = urlparse(self.source)

        if self.source.startswith('https://'):
            is_secure = True

        self.client = Minio(endpoint=urlparsed.netloc,
                            access_key=self.username,
                            secret_key=self.password,
                            secure=is_secure)

    def get(self, identifier: str) -> Any:

        LOGGER.debug(f'Getting object {self.channel}/{identifier} from bucket={self.name}') # noqa
        # Get data of an object.
        try:
            response = self.client.get_object(
                self.name, object_name={self.channel}/{identifier})
            data = response.data
            response.close()
            response.release_conn()
        except Exception as err:
            LOGGER.error(err)
            raise err

        return data

    def put(self, data: bytes, identifier: str) -> bool:

        object_key = f'{self.channel}/{identifier}'
        LOGGER.info(f'Putting data as object={object_key} in bucket={self.name}') # noqa
        self.client.put_object(bucket_name=self.name,
                               object_name=object_key,
                               data=BytesIO(data), length=-1,
                               part_size=10*1024*1024)

        return True

    def delete(self, identifier: str) -> bool:

        LOGGER.debug(f'Deleting object {self.channel}/{identifier}')
        self.client.remove_object(self.name, {self.channel}/{identifier}) # noqa
        return True

    def __repr__(self):
        return f'<MinioStorage ({self.source})>'
