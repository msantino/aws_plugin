from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.utils import apply_defaults


class EmrCreateJobFlowSelectiveTemplateOperator(EmrCreateJobFlowOperator):
    """
    Unfortunately, the task templater currently throws an exception if a field contains non-strings,
    so we have to separate the fields we want to template from the rest of them
    WARNING: currently, this implementation only supports separating top-level keys since the
    templated dictionary will *overwrite* any duplicate top-level keys.
    """
    template_fields = ['job_name', 'job_tags', 'job_steps']

    @apply_defaults
    def __init__(self,
                 log_uri,
                 ec2_key_name,
                 ec2_instance_type,
                 aws_conn_id='s3_default',
                 emr_conn_id='emr_default',
                 job_name='default_job_flow_name',
                 job_steps=None,
                 job_tags=None,
                 service_role='',
                 job_flow_role='EMR_EC2_DefaultRole',
                 release_label='emr-5.13.0',
                 *args, **kwargs):
        super(EmrCreateJobFlowSelectiveTemplateOperator, self).__init__(*args, **kwargs)
        self.job_steps = job_steps
        self.job_tags = job_tags
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.job_name = job_name
        self.log_uri = log_uri
        self.release_label = release_label
        self.service_role = service_role
        self.job_flow_role = job_flow_role
        self.ec2_key_name = ec2_key_name
        self.ec2_instance_type = ec2_instance_type

    def execute(self, context):
        job_flow_overrides = {
            'Name': self.job_name,
            "LogUri": self.log_uri,
            "ReleaseLabel": self.release_label,
            "ServiceRole": self.service_role,
            "JobFlowRole": self.job_flow_role,
            "ScaleDownBehavior": 'TERMINATE_AT_TASK_COMPLETION',
            "EbsRootVolumeSize": 100,
            "VisibleToAllUsers": True,
            "Instances": {
                'Ec2KeyName': self.ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-57c16b33',
                'InstanceGroups': [
                    {
                        "InstanceCount": 1,
                        "InstanceRole": 'MASTER',
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "SizeInGB": 100,
                                        "VolumeType": "gp2"
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ]
                        },
                        "InstanceType": self.ec2_instance_type,
                        "Name": "Master Instance Group"
                    }
                ],
            },
            'Steps': self.job_steps,
            'Tags': self.job_tags,
            'Applications': [{'Name': 'Hadoop'}],
        }
        self.job_flow_overrides.update(job_flow_overrides)
        return super(EmrCreateJobFlowSelectiveTemplateOperator, self).execute(context)
