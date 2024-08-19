# =================================================================
#
# Authors: David I. Berry (dberry@wmo.int)
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

import json
import logging
import traceback
import uuid

from sqlalchemy import (create_engine, String, text, bindparam)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (DeclarativeBase, mapped_column)

from pygeoapi.process.base import (
    JobNotFoundError,
    JobResultNotFoundError,
)
from pygeoapi.process.manager.base import BaseManager


LOGGER = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class JobManagerPygeoapi(Base):
    __tablename__ = 'job_manager_pygeoapi'
    id = mapped_column(String, primary_key=True)
    job = mapped_column(JSONB, nullable=False)


class PsqlDBManager(BaseManager):
    def __init__(self, manager_def):
        super().__init__(manager_def)
        self.is_async = True
        # create the DB connection engine
        # First connection string
        try:
            conn_str = self.connection
        except Exception as err:
            LOGGER.error(f'JOBMANAGER - connect error: {err}')
            return False

        # now engine and connect
        try:
            self.engine = create_engine(
                conn_str,
                connect_args={'client_encoding': 'utf8',
                              'application_name': 'pygeoapi'},
                pool_pre_ping=True
            )
            Base.metadata.create_all(self.engine)
            self._connect()
        except Exception as err:
            LOGGER.error(f'JOBMANAGER - connect error: {err}')
            return False

    def _connect(self):
        try:
            self.db = self.engine.connect()
            # make sure table exists

            LOGGER.info('JOBMANAGER - psql connected')
            return True
        except Exception:
            self.destroy()
            LOGGER.error('JOBMANAGER - connect error',
                         exc_info=(traceback))
            return False

    def destroy(self):
        try:
            self.db.close()
            LOGGER.info('JOBMANAGER - psql disconnected')
            return True
        except Exception:
            LOGGER.error('JOBMANAGER - destroy error',
                         exc_info=(traceback))
            return False

    def get_jobs(self, status=None, limit=10, offset=0):
        query_params = {
            'limit': limit,
            'offset': offset
        }
        try:
            if status is not None:
                query_params['status'] = status
                query = text("SELECT job from job_manager_pygeoapi WHERE job->>'status' = ':status' limit :limit offset :offset")  # noqa
                result = self.db.execute(query, parameters=query_params)).fetchall()  # noqa
            else:
                query = text('SELECT job from job_manager_pygeoapi limit :limit offset :offset')  # noqa
                result = self.db.execute(query, parameters=query_params)).fetchall()  # noqa
            # now convert jobs to list of dicts
            jobs = [dict(row[0]) for row in result]
            return {
                'jobs': jobs,=
                'numberMatched': len(jobs)
            }
        except Exception:
            LOGGER.error('JOBMANAGER - get_jobs error',
                         exc_info=(traceback))
            return False

    def add_job(self, job_metadata):
        job_id = job_metadata.get('identifier')
        if job_id is None:
            job_id = str(uuid.uuid4())
        try:
            query = text('INSERT INTO job_manager_pygeoapi (id, job) VALUES (:job_id, :job_metadata) RETURNING id')  # noqa
            query = query.bindparams(bindparam('job_metadata', type_=JSONB),
                                     bindparam('job_id', type_=String))
            result = self.db.execute(query, parameters=dict(job_id = job_id, job_metadata = job_metadata))  # noqa
            LOGGER.warning(result)
            doc_id = result.fetchone()[0]
            self.db.commit()
            LOGGER.info('JOBMANAGER - psql job added')
            return doc_id

        except Exception:
            LOGGER.error('JOBMANAGER - add_job error',
                         exc_info=(traceback))
            return False

    def update_job(self, job_id, update_dict):
        try:
            # first get the job to update
            query = text('SELECT job from job_manager_pygeoapi WHERE id =:job_id')  # noqa
            query = query.bindparams(bindparam('job_id', type_=String))
            result = self.db.execute(query, parameters=dict(job_id=job_id)).fetchone()  # noqa
            LOGGER.warning(result)
            result = dict(result[0])  # convert to dict
            # update the dict
            for k, v in update_dict.items():
                result[k] = v
            LOGGER.warning('updating ...')
            # now back to DB
            query = text('UPDATE job_manager_pygeoapi SET job =:update_dict WHERE id =:job_id RETURNING id')  # noqa
            query = query.bindparams(bindparam('job_id', type_=String),
                                     bindparam('update_dict', type_=JSONB))
            self.db.execute(query.bindparams(bindparam('update_dict', type_=JSONB)), parameters=dict(update_dict = result, job_id = job_id))  # noqa
            LOGGER.warning('committing ...')
            self.db.commit()
            LOGGER.info('JOBMANAGER - psql job updated')
            return True

        except Exception:
            LOGGER.error('JOBMANAGER - psql update_job error',
                         exc_info=(traceback))
            return False

    def delete_job(self, job_id):
        try:
            query = text('DELETE FROM job_manager_pygeoapi where id =:job_id')
            query = query.bindparams(bindparam('job_id', type_=String))
            self.db.execute(query, parameters=dict(job_id=job_id))
            self.db.commit()
            LOGGER.info('JOBMANAGER - psql job deleted')
            return True
        except Exception:
            LOGGER.error('JOBMANAGER - psql delete_job error',
                         exc_info=(traceback))
            return False

    def get_job(self, job_id):
        try:
            query = text('SELECT job from job_manager_pygeoapi WHERE id =:job_id')  # noqa
            query = query.bindparams(bindparam('job_id', type_=String))
            result = self.db.execute(query, parameters=dict(job_id=job_id))
            # check if job exists
            if result.rowcount == 0:
                raise JobNotFoundError()
            entry = result.fetchone()[0]
            LOGGER.info('JOBMANAGER - psql job queried')
            return entry
        except Exception as err:
            LOGGER.error('JOBMANAGER - psql get_job error',
                         exc_info=(traceback))
            raise JobNotFoundError() from err

    def get_job_result(self, job_id):
        try:
            query = text('SELECT job from job_manager_pygeoapi WHERE id =:job_id')  # noqa
            query = query.bindparams(bindparam('job_id', type_=String))
            result = self.db.execute(query, parameters=dict(job_id=job_id))
            entry = result.fetchone()[0]
            if entry['status'] != 'successful':
                LOGGER.info('JOBMANAGER - job not finished or failed')
                return (None,)
            with open(entry['location'], 'r') as file:
                data = json.load(file)
            LOGGER.info('JOBMANAGER - psql job result queried')
            return entry['mimetype'], data
        except Exception as err:
            LOGGER.error('JOBMANAGER - psql get_job_result error',
                         exc_info=(traceback))
            raise JobResultNotFoundError() from err

    def __repr__(self):
        return f'<PsqlDBManager> {self.name}'
