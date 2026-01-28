"""CDK stack definition for provisioning the Data Pipeline infrastructure for EMR Serverless."""

import os
from aws_cdk import (
    Stack,
    Duration,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_ec2 as ec2,
    aws_sqs as sqs,
    aws_lambda_event_sources as lambda_event_sources,
)
from config.stack_config import StackConfig
from constructs import Construct
from utils.common import get_network_configuration, apply_tags, get_json_document, create_name_iam, create_name_global

class DataPipelineServerlessManager(Stack):
    """CDK Stack responsible for provisioning the Data Pipeline components."""
    def __init__(self, scope: Construct, construct_id: str, config: StackConfig, stack_tags: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.org = config.org.lower()
        self.team = config.team
        self.project = config.project.lower()
        self._environment = config._environment.lower()
        self.branch_name = config.branch_name.lower() 
        self.stack_tags = stack_tags
        application_account=config.target_account

        # Initialize sequential number
        number=1
        apply_tags(self, self.stack_tags)
        ntw_cfg = get_network_configuration(self)

        #########################
        ###   SECURITY GROUP  ###
        #########################

        # INITIALIZE LAMBDA - SECURITY GROUP
        lambda_initialize_sg_name = create_name_global(
            funcionalidad="lambda-initialize",
            tipo_recurso="sg",
            env=self._environment,
            sequence=number
        )
        lambda_initialize_sg = ec2.SecurityGroup(
            self,
            lambda_initialize_sg_name,
            security_group_name=lambda_initialize_sg_name,
            vpc=ntw_cfg['vpc'],
            description="Security Group for Initialize lambda",
            allow_all_outbound=True
        )

        _securitygroups = [lambda_initialize_sg]
        for lambda_sg in _securitygroups:
            lambda_sg.node.default_child.add_metadata(
                "cfn_nag",
                {
                    "rules_to_suppress": [
                        {
                            "id": "W28",
                            "reason": "Resource found with an explicit name, this disallows updates that require replacement of this resource."
                        },
                        {
                            "id": "W40",
                            "reason": "Security Groups egress with an IpProtocol of -1 found."
                        },
                        {
                            "id": "W5",
                            "reason": "Security Groups found with cidr open to world on egress."
                        }
                    ]
                },
            )


        ##################
        #### IAM ROLE ####
        ##################

        # Lambda initialize  - IAM Roles
        lambda_initialize_policy_name = create_name_iam(
            funcionalidad=f"{self.branch_name}-initialize",
            tipo_recurso="p",
            env=self._environment,
            sequence=number
        )
        lambda_initialize_role_name = create_name_iam(
            funcionalidad=f"{self.branch_name}-initialize",
            tipo_recurso="r",
            env=self._environment,
            sequence=number
        )

        lambda_initialize_role = iam.Role(
            self,
            lambda_initialize_role_name,
            role_name=lambda_initialize_role_name,
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal('lambda.amazonaws.com')
            ),
            inline_policies={
                lambda_initialize_policy_name: iam.PolicyDocument.from_json(
                    get_json_document(
                        folder_name='serverless',
                        policy_name='lb_initialize_role_policy',
                        substitutions={
                            'ORG': self.org,
                            'TEAM': self.team,
                            'PROJECT': self.project,
                            'REGION': self.region,
                            'ENV': self._environment,
                            'TARGET_ACCOUNT': application_account,
                            'BRANCH_NAME': self.branch_name
                        }
                    )
                )
            }
        )

        # State Machine Pipeline - IAM Roles
        sfn_policy_name = create_name_iam(
            funcionalidad=f"{self.branch_name}-pipeline-sfn",
            tipo_recurso="p",
            env=self._environment,
            sequence=number
        )
        sfn_role_name = create_name_iam(
            funcionalidad=f"{self.branch_name}-pipeline-sfn",
            tipo_recurso="r",
            env=self._environment,
            sequence=number
        )

        sfn_role = iam.Role(
            self,
            sfn_role_name,
            role_name=sfn_role_name,
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal('states.amazonaws.com')
            ),
            inline_policies={
                sfn_policy_name: iam.PolicyDocument.from_json(
                    get_json_document(
                        folder_name='serverless',
                        policy_name='sfn_pipeline_role_policy',
                        substitutions={
                            'ORG': self.org,
                            'TEAM': self.team,
                            'PROJECT': self.project,
                            'REGION': self.region,
                            'ENV': self._environment,
                            'TARGET_ACCOUNT': application_account,
                            'BRANCH_NAME': self.branch_name
                        }
                    )
                )
            }
        )

        _roles = [lambda_initialize_role, sfn_role]
        for _role in _roles:
            _role.node.default_child.add_metadata(
                "cfn_nag",
                {
                    "rules_to_suppress": [
                        {
                            "id": "F38",
                            "reason": (
                                "F38 IAM role needs iam:PassRole with '*' because Step Functions "
                                "managed-rule creation requires passing roles dynamically. "
                                "Restricting the Resource would break automatic integration."
                            ),
                        },
                        {
                            "id": "W28",
                            "reason": "W28 Resourse found with an explicit name. "
                            + "IBK prefers to have precise names following "
                            + "predefined nomenclatures to avoid having to access "
                            + "the resource to understand its functioning."
                        },
                        {
                            "id": "W11",
                            "reason": "W11 IAM role should not allow * resource on"
                            + "its permissions policy. "
                            + "Some resourses need to have * because they are cross."
                            + "Others like AWS Lake Formation does not support specifying"
                            + "a resource ARN in the Resource element of an IAM policy statement."
                            + "To allow access to AWS Lake Formation, specify Resource:"
                            + "* in your policy."
                        }
                    ]
                },
            )

        ###############
        ### LAMBDAS ###
        ###############

        global_dynamo_table_name = create_name_global(
            funcionalidad="config-global",
            tipo_recurso="ddb",
            env=self._environment,
            sequence=number
        )

        process_dynamo_table_name = create_name_global(
            funcionalidad="config-process",
            tipo_recurso="ddb",
            env=self._environment,
            sequence=number
        )

        lambda_initialize_name = create_name_global(
            funcionalidad=f"{self.branch_name}-initialize",
            tipo_recurso="lm",
            env=self._environment,
            sequence=number
        )
        lambda_initialize = _lambda.Function(
            self,
            lambda_initialize_name,
            function_name=lambda_initialize_name,
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler='lambda_function.lambda_handler',
            code=_lambda.Code.from_asset('src/lambda/initialize_serverless'),
            timeout=Duration.seconds(120),
            reserved_concurrent_executions=25,
            role=lambda_initialize_role,
            environment={
                "DB_CONFIG_GLOBAL": global_dynamo_table_name,
                "DB_CONFIG_PROCESS": process_dynamo_table_name,
                "TZ":"America/Lima"
            },
            vpc=ntw_cfg['vpc'],
            vpc_subnets=ntw_cfg['subnet_selection'],
            security_groups=[lambda_initialize_sg]
        )


        _functions = [lambda_initialize]
        for lambda_fn in _functions:
            lambda_fn.node.add_dependency(lambda_initialize_role)
            lambda_fn.node.default_child.add_metadata(
                "cfn_nag",
                {
                    "rules_to_suppress": [
                        {
                            "id": "W89",
                            "reason": "W89 is a warning raised due to Lambda functions require"
                            + " to be deployed inside VPC. Due to security guidelines at the beginning"
                            + " of the project it was determined that Lambda networking does not"
                            + " require a dedicated VPC given the serverless components of the"
                            + " solution."
                        },
                        {
                            "id": "W58",
                            "reason": "W58 Lambda functions require permission to write"
                            + "CloudWatch Logs"
                            + " Permissions for writing logs already given and tested"
                        }
                    ]
                },
            ) 

        #####################
        ### STATE MACHINE ###
        #####################

        # State machine name
        sfn_name = create_name_global(
            funcionalidad=f"{self.branch_name}",
            tipo_recurso="sfn",
            env=self._environment,
            sequence=number
        )

        sfn_athena = sfn.StateMachine(
            self,
            sfn_name,
            state_machine_name=sfn_name,
            definition_body=sfn.DefinitionBody.from_file("src/sfn/engine_serverless.json"),
            definition_substitutions={
                "ORG": self.org,
                "PROJECT": self.project,
                "TEAM": self.team,
                "TARGET_ACCOUNT": application_account,
                "DEPLOY_REGION": os.getenv('AWS_REGION'),
                "BRANCH_NAME": self.branch_name
            },
            role=sfn_role
        )
        sfn_athena.node.add_dependency(sfn_role)

