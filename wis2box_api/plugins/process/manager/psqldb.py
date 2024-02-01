# =================================================================
#
# Authors: Alexander Pilz <a.pilz@52north.org>
#
# Copyright (c) 2023 Alexander Pilz
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
import json
import logging
import traceback
from typing import Any, Dict, Tuple, Optional, OrderedDict

from sqlalchemy import (create_engine, Integer)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import URL as dbURL
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, Session)

from pygeoapi.process.base import (
    BaseProcessor,
    JobNotFoundError,
    JobResultNotFoundError,
)
from pygeoapi.process.manager.base import BaseManager

from pygeoapi.util import (
    DATETIME_FORMAT,
    JobStatus,
    ProcessExecutionMode,
    RequestedProcessExecutionMode,
)

LOGGER = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class JobManagerPygeoapi(Base):
    __tablename__ = "job_manager_pygeoapi"
    id = mapped_column(Integer, primary_key=True)
    job = mapped_column(JSONB, nullable=False)


class PsqlDBManager(BaseManager):
    def __init__(self, manager_def):
        super().__init__(manager_def)
        self.is_async = True
        # create the DB connection engine
        # First connection string
        try:
            #conn_str = dbURL.create(
            #    'postgresql+psycopg2',
            #    username=self.connection.get('user'),
            #    password=self.connection.get('password'),
            #    host=self.connection.get('host'),
            #    port=self.connection.get('port'),
            #    database=self.connection.get('name')
            #)
            conn_str = self.connection
        except:
            LOGGER.error("JOBMANAGER - connect error",
                         exc_info=(traceback))
            return False

        # now engine
        try:
            self.engine = create_engine(
                conn_str,
                connect_args={'client_encoding': 'utf8',
                              'application_name': 'pygeoapi'},
                pool_pre_ping=True
            )
            Base.metadata.create_all(self.engine)
        except:
            LOGGER.error("JOBMANAGER - connect error",
                         exc_info=(traceback))
            return False
    def _connect(self):
        try:
            self.db = self.engine.connect()
            # make sure table exists

            LOGGER.info("JOBMANAGER - psql connected")
            return True
        except Exception:
            self.destroy()
            LOGGER.error("JOBMANAGER - connect error",
                         exc_info=(traceback))
            return False

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

    def destroy(self):
        try:
            self.db.close()
            LOGGER.info("JOBMANAGER - psql disconnected")
            return True
        except Exception:
            self.destroy()
            LOGGER.error("JOBMANAGER - destroy error",
                         exc_info=(traceback))
            return False

    def get_jobs(self, status=None):
        try:
            self._connect()
            if status is not None:
                query = text("SELECT job from job_manager_pygeoapi WHERE job->>'status' = ':status'")  # noqa
                jobs = self.db.execute(query, status = status).fetchall()
            else:
                query = text("SELECT job from job_manager_pygeoapi")  # noqa
                jobs = self.db.execute(query).fetchall()

                pass
            self.destroy()
            LOGGER.info("JOBMANAGER - psql jobs queried")
            return jobs
        except Exception:
            LOGGER.error("JOBMANAGER - get_jobs error",
                         exc_info=(traceback))
            return False

    def add_job(self, job_metadata):
        try:
            self._connect()
            query = text("INSERT INTO job_manager_pygeoapi (job) VALUES (:job_metadata) RETURNING id")
            result = self.db.execute(query, job_metadata = job_metadata)
            doc_id = result.fetchone()[0]
            self.db.execute("COMMIT")
            self.destroy()
            LOGGER.info("JOBMANAGER - psql job added")
            return doc_id

        except Exception:
            LOGGER.error("JOBMANAGER - add_job error",
                         exc_info=(traceback))
            return False

    def update_job(self, job_id, update_dict):
        try:
            self._connect()
            # first get the job to update
            query = text("SELECT job from job_manager_pygeoapi WHERE id = :job_id")
            result = self.db.execute(query, job_id=job_id).fetchone()[0]
            # update the dict
            for k,v in update_dict.items():
                result[k] = v
            # now back to DB
            query = text("UPDATE job_manager_pygeoapi SET job = :update_dict where id := job_id RETURNING id")
            result = self.db.execute(query, update_dict = result, job_id = job_id)
            self.db.execute("COMMIT")
            self.destroy()
            LOGGER.info("JOBMANAGER - psql job updated")
            return True

        except Exception:
            LOGGER.error("JOBMANAGER - psql update_job error",
                         exc_info=(traceback))
            return False

    def delete_job(self, job_id):
        try:
            self._connect()
            query = text("DELETE FROM job_manager_pygeoapi where id = : job_id")
            result = self.db.execute(query, job_id = job_id)
            self.db.execute("COMMIT")
            self.destroy()
            LOGGER.info("JOBMANAGER - psql job deleted")
            return True
        except Exception:
            LOGGER.error("JOBMANAGER - psql delete_job error",
                         exc_info=(traceback))
            return False

    def get_job(self, job_id):
        try:
            self._connect()
            query = text("SELECT job from job_manager_pygeoapi WHERE id = :job_id")
            result = self.db.execute(query, job_id = job_id)
            entry = result.fetchone()[0]
            self.destroy()
            LOGGER.info("JOBMANAGER - psql job queried")
            return entry
        except Exception as err:
            LOGGER.error("JOBMANAGER - psql get_job error",
                         exc_info=(traceback))
            raise JobNotFoundError() from err

    def get_job_result(self, job_id):
        try:
            self._connect()
            query = text("SELECT job from job_manager_pygeoapi WHERE id = :job_id")
            result = self.db.execute(query, job_id = job_id)
            entry = result.fetchone()[0]
            self.destroy()
            if entry["status"] != "successful":
                LOGGER.info("JOBMANAGER - job not finished or failed")
                return (None,)
            with open(entry["location"], "r") as file:
                data = json.load(file)
            LOGGER.info("JOBMANAGER - psql job result queried")
            return entry["mimetype"], data
        except Exception as err:
            LOGGER.error("JOBMANAGER - psql get_job_result error",
                         exc_info=(traceback))
            raise JobResultNotFoundError() from err

    def __repr__(self):
        return f'<PsqlDBManager> {self.name}'