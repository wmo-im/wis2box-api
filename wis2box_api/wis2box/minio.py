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
import os

from io import BytesIO

from typing import Any

from urllib.parse import urlparse

LOGGER = logging.getLogger(__name__)

STORAGE_SOURCE = os.environ.get('WIS2BOX_STORAGE_SOURCE')
STORAGE_USERNAME = os.environ.get('WIS2BOX_STORAGE_USERNAME')
STORAGE_PASSWORD = os.environ.get('WIS2BOX_STORAGE_PASSWORD')

class MinIOStorage():
    """MinIO storage manager"""
    def __init__(self, bucket_name, channel) -> None:

        is_secure = False

        urlparsed = urlparse(STORAGE_SOURCE)

        if STORAGE_SOURCE.startswith('https://'):
            is_secure = True

        self.bucket_name = bucket_name
        self.channel = channel
        self.client = Minio(endpoint=urlparsed.netloc,
                            access_key=STORAGE_USERNAME,
                            secret_key=STORAGE_PASSWORD,
                            secure=is_secure)

    def get(self, identifier: str) -> Any:

        LOGGER.debug(f'Getting object {self.channel}/{identifier} from bucket={self.bucket_name}')
        # Get data of an object.
        try:
            response = self.client.get_object(
                self.bucket_name, object_name={self.channel}/{identifier})
            data = response.data
            response.close()
            response.release_conn()
        except Exception as err:
            LOGGER.error(err)
            raise err

        return data

    def put(self, data: bytes, identifier: str) -> bool:

        object_key = f'{self.channel}/{identifier}'
        LOGGER.info(f'Putting data as object={object_key} in bucket={self.bucket_name}')
        self.client.put_object(bucket_name=self.bucket_name, object_name=object_key,
                               data=BytesIO(data), length=-1,
                               part_size=10*1024*1024)

        return True

    def delete(self, identifier: str) -> bool:

        LOGGER.debug(f'Deleting object {self.channel}/{identifier}')
        self.client.remove_object(self.bucket_name, {self.channel}/{identifier})
        return True


    def __repr__(self):
        return f'<MinioStorage ({self.source})>'