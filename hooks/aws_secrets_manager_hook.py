# -*- coding: utf-8 -*-
"""
Connect to a AWS Secrets Manager to retrieve a stored key
"""
# pylint: disable=import-error,missing-docstring
from airflow.contrib.hooks.aws_hook import AwsHook
from botocore.exceptions import ClientError


class AwsSecretsManagerHook(AwsHook):
    """
    Interact with AWS Secret Manager, using native AWS Hook

    :param aws_secret_name: reference to a aws secrets manager name
    :type aws_secret_name: string
    :param aws_conn_id: reference to a specific aws connection
    :type aws_conn_id: string

    :return: dict
    """
    def __init__(self,
                 aws_secret_name,
                 aws_conn_id='aws_default',
                 **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.aws_secret_name = aws_secret_name
        self.aws_conn_id = aws_conn_id

    def get_conn(self):
        return self.get_client_type('secretsmanager')

    def get_secret(self):

        try:
            get_secret_value_response = self.get_conn().get_secret_value(
                SecretId=self.aws_secret_name
            )
        except ClientError as err:
            self.log.error(str(err))
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                print("The requested secret " + self.aws_secret_name + " was not found")
            elif err.response['Error']['Code'] == 'InvalidRequestException':
                print("The request was invalid due to:", err)
            elif err.response['Error']['Code'] == 'InvalidParameterException':
                print("The request had invalid params:", err)
        else:
            # Decrypted secret using the associated KMS CMK
            # Depending on whether the secret was a string or binary,
            #   one of these fields will be populated
            return get_secret_value_response['SecretString'] \
                if 'SecretString' in get_secret_value_response \
                else get_secret_value_response['SecretBinary']
