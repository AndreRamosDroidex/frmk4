"""CDK application entry point for the Data Pipeline stacks."""

import os
import aws_cdk as cdk
from utils.common import load_tags, create_name_global
from config.stack_config import StackConfig
from config.constants import (
    ORG,
    TEAM,
    PROJECT,
    REGION
)
from datapipeline.serverless import DataPipelineServerlessManager


ENV = os.getenv('ENVIRONMENT', 'false').lower()
ACCOUNT_ID = str(os.getenv('ACCOUNT_ID', 'false').lower())
BRANCH_NAME = os.getenv('BRANCH_NAME')
tags = load_tags(target=ENV)

config = StackConfig(
    org=ORG,
    team=TEAM,
    project=PROJECT,
    target_account=ACCOUNT_ID,
    environment=ENV,
    branch_name=BRANCH_NAME
)

tools_env = cdk.Environment(account=ACCOUNT_ID, region=REGION)

# Initialize sequential number
NUMBER=1

# Print the values of ORG and TEAM
print(f"ORG: {ORG}")
print(f"TEAM: {TEAM}")
print(f"PROJECT: {PROJECT}")
app = cdk.App()

# SERVERLESS DATAPIPELINE STACK
SERVERLESS_DATAPIPELINE_STACK_NAME=create_name_global(
    funcionalidad=f"{BRANCH_NAME}-datapipeline",
    tipo_recurso="cfn",
    env=ENV,
    sequence=NUMBER
)
DataPipelineServerlessManager(
    app,
    SERVERLESS_DATAPIPELINE_STACK_NAME,
    env=tools_env,
    config=config,
    stack_tags=tags
)

app.synth()
