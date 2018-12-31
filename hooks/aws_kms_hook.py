# -*- coding: utf-8 -*-
import aws_encryption_sdk
import os
import magic
import logging
from airflow.contrib.hooks.aws_hook import AwsHook

logging = logging.getLogger(__name__)


class AwsKmsHook(AwsHook):
    """
    Interact with AWS KMS to retrieve a key and encrypt a file using boto3 library and aws_encryption_sdk.

    :param sws_kms_key_arn: AWS KMS key ARN
    :type sws_kms_key_arn: str
    :return: None
    """
    def __init__(self,
                 aws_kms_key_arn,
                 task_id,
                 aws_conn_id='aws_default',
                 *args, **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, *args, **kwargs)
        self.aws_kms_key_arn = aws_kms_key_arn
        self.aws_conn_id = aws_conn_id
        self.task_id = task_id

    def get_conn(self):
        return self.get_client_type('kms')

    def encrypt_file(self, source_file_name, target_file_name):
        """
        Encrypt a file using aws_encryption SDK and a KMS key

        :param source_file_name: File name (path) to encrypt
        :type source_file_name: str
        :param target_file_name: File name (path) to be used as target encrypted file. Use to be a NamedTemporaryFile
        :type target_file_name: str
        """

        logging.info('Open a AWS session for KMS actions using ')

        kms_key_provider = aws_encryption_sdk.KMSMasterKeyProvider(config=self.get_conn(), key_ids=[
            self.aws_kms_key_arn
        ])

        with open(source_file_name, 'rb') as pt_file, open(target_file_name, 'wb') as ct_file:
            with aws_encryption_sdk.stream(
                    mode='e',
                    source=pt_file,
                    key_provider=kms_key_provider,
                    frame_length=1024
            ) as encryptor:
                for chunk in encryptor:
                    ct_file.write(chunk)

        logging.info('Comparing source and target files:')
        logging.info('Source file size: {}kb'.format(os.stat(source_file_name).st_size >> 10))
        logging.info('Source file type: [{}]'.format(magic.from_file(source_file_name)))
        logging.info('Taget file size: {}kb'.format(os.stat(target_file_name).st_size >> 10))
        logging.info('Target file type: [{}]'.format(magic.from_file(target_file_name)))

        return target_file_name




