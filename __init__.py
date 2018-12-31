# -*- coding: utf-8 -*-
"""
Plugin to store some usual PostgreSQL functions

"""
from airflow.plugins_manager import AirflowPlugin

from aws_plugin.hooks.aws_secrets_manager_hook import AwsSecretsManagerHook
from aws_plugin.hooks.aws_athena_hook import AwsAthenaHook
from aws_plugin.hooks.aws_kms_hook import AwsKmsHook

from aws_plugin.operators.emr_create_job_flow_selective_template_operator \
    import EmrCreateJobFlowSelectiveTemplateOperator

from aws_plugin.sensors.emr_cluster_sensors import EmrClusterStartSensor, EmrClusterEndSensor


class AwsPlugin(AirflowPlugin):
    name = "aws_plugin"
    operators = [EmrCreateJobFlowSelectiveTemplateOperator]
    hooks = [AwsSecretsManagerHook, AwsAthenaHook, AwsKmsHook]
    sensors = [EmrClusterStartSensor,
               EmrClusterEndSensor]
