# =================================================================
#
# Authors: David Berry <dberry@wmo.int>
#
# Copyright (c) 2024 WMO
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
import logging
import multiprocessing as mp
from typing import Any, Tuple

from pygeoapi.process.manager.tinydb import TinyDBManager

LOGGER = logging.getLogger(__name__)


# create a new pool of processors

_pool = mp.Pool( min(1,mp.cpu_count() - 2))  # noqa arbitrary number of processes.

class TinyDBManagerMP(TinyDBManager):
    def _execute_handler_async(self, p: BaseProcessor, job_id: str,
                               data_dict: dict) -> Tuple[str, None, JobStatus]:
        """
        Updated execution handler to execute a process in a background
        process using `multiprocessing.Process`

        https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process  # noqa

        :param p: `pygeoapi.process` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters

        :returns: tuple of None (i.e. initial response payload)
                  and JobStatus.accepted (i.e. initial job status)
        """
        _pool.apply_async(
            func=self._execute_handler_sync,
            args=(p, job_id, data_dict))

        return 'application/json', None, JobStatus.accepted