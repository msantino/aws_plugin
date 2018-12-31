# -*- coding: utf-8 -*-
import hashlib
import time
import uuid
import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

logging = logging.getLogger(__name__)


class AwsAthenaHook(AwsHook):

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            **kwargs):
        self.aws_conn_id = aws_conn_id
        self.execution_date = kwargs.get('execution_date')
        super().__init__(aws_conn_id=self.aws_conn_id)

    def get_conn(self):
        return self.get_client_type('athena')

    def start_query_execution(self, sql, database, s3_output_location):
        """
        Execute a SQL query on AWS Athena

        """
        logging.info('Start running SQL query on AWS Athena')
        logging.info('SQL: {}'.format(sql))

        m = hashlib.md5()
        m.update(sql.encode('utf-8'))

        return self.get_conn().start_query_execution(
            QueryString=sql,
            ClientRequestToken=str(uuid.uuid4()),
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': s3_output_location
            }
        )

    def get_execution_status(self, query_execution_id):
        return self.get_conn().get_query_execution(QueryExecutionId=query_execution_id)

    def wait_while_executing_query(self, query_execution_id):
        break_loop = False
        while not break_loop:
            execution = self.get_execution_status(query_execution_id)
            if execution['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
                stats = execution['QueryExecution']['Statistics']
                if stats is not None and stats != {}:
                    logging.info('Waiting for query to complete [{}s] [{}Gb]. Will retry in 5s'.format(
                        stats['EngineExecutionTimeInMillis'] / 1000,
                        stats['DataScannedInBytes'] >> 30
                    ))
                time.sleep(5)
            else:
                return execution
