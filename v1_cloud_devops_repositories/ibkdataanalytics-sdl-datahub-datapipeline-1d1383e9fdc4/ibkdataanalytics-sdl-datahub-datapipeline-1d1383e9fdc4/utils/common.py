"""Shared utility functions for configuration and tagging."""

import os
import json
from pathlib import Path
from typing import Dict, Any
import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
import boto3
from config import constants
from config.constants import (PROJECT)

def get_json_document(policy_name: str,folder_name:str, substitutions: Dict[str, str] = None) -> Dict[str, Any]:
    """ Load a JSON document and optionally apply string substitutions. """

    base_path = Path(__file__).resolve().parent.parent
    json_path = base_path / "policies" / f"{folder_name}" /f"{policy_name}.json"
    raw_json = json_path.read_text()

    if substitutions:
        for key, value in substitutions.items():
            if value is not None:
                raw_json = raw_json.replace(f"{{{{{key}}}}}", str(value))

    return json.loads(raw_json)

def get_network_configuration(scope):
    """Returns network configuration for resources that require a VPC."""

    ssm_client = boto3.client('ssm', region_name=constants.REGION)

    if constants.NETWORKING_CONFIG == "file":
        vpc_id = constants.NETWORKING_VPC_ID
        app_subnets_ids = constants.NETWORKING_SUBNETS_APP_ID
    elif constants.NETWORKING_CONFIG == "parameter-store":
        vpc_id = ssm_client.get_parameter(Name=constants.NETWORKING_VPC_ID)['Parameter']['Value']
        
        app_subnets_ids = ssm_client.get_parameter(Name=constants.NETWORKING_SUBNETS_APP_ID)['Parameter']['Value'].split(',')
        app_subnets_ids = [s.strip() for s in app_subnets_ids]
    else:
        raise ValueError(f"NETWORKING_CONFIG inválido: {constants.NETWORKING_CONFIG}")

    vpc = ec2.Vpc.from_lookup(scope, "ImportedVPC", vpc_id=vpc_id)

    app_subnets = [
        ec2.Subnet.from_subnet_id(scope, f"AppSubnet{i}", subnet_id)
        for i, subnet_id in enumerate(app_subnets_ids)
    ]

    subnet_selection = ec2.SubnetSelection(subnets=app_subnets)

    net = {
        "vpc": vpc,
        "app_subnets": app_subnets,
        "subnet_selection": subnet_selection
    }
    return net

def load_tags(target):
    """Load AWS tags from JSON by target."""

    path = f'config/tags/{target}_tags.json'
    try:
        if not os.path.isfile(path):
            print(f"Archivo de tags no encontrado: {path}")
            return []
        with open(path, 'r', encoding='utf-8') as _f:
            data = json.load(_f)
            if not isinstance(data, list):
                print(f"Formato de tags inválido en {path}. Se espera una lista.")
                return []
            return data
    except json.JSONDecodeError:
        print(f"Error en el formato del archivo {path}.")
        return []
    except Exception as e:
        print(f"Error cargando tags desde {path}: {e}")
        return []

def apply_tags(stack, tags):
    """Apply tags to the stack."""
    for tag in tags:
        key = tag.get('Key')
        value = tag.get('Value')
        if key and value:
            cdk.Tags.of(stack).add(key, value)

def create_name_global(funcionalidad, tipo_recurso, env, sequence, dash="-"):
    """Generates a standardized name based on provided components."""

    number_formatted = f"{sequence:02}"
    name = f"aw{dash}{env}{dash}{tipo_recurso}{dash}{PROJECT}{dash}{funcionalidad}{dash}{number_formatted}"

    return name


def create_name_iam(funcionalidad, tipo_recurso, env, sequence, dash="-"):
    """Generates a standardized IAM ROLE/ POLICY name based on provided components."""

    number_formatted = f"{sequence:02}"
    name = f"aw{dash}{env}{dash}iam{dash}{PROJECT}{dash}{tipo_recurso}{dash}{funcionalidad}{dash}{number_formatted}"

    return name


def create_name_bucket(funcionalidad, env, account_id, sequence, dash="-"):
    """Generates a standardized BUCKET name based on provided components."""

    number_formatted = f"{sequence:02}"
    name = f"aw{dash}{env}{dash}s3{dash}{PROJECT}{dash}{funcionalidad}{dash}{account_id}{dash}{number_formatted}"

    return name