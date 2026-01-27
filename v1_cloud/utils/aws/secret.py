# on_cloud/utils/aws/secrets.py
from __future__ import annotations

import os
import json
import base64
import boto3
from botocore.exceptions import ClientError

_CACHE = {}

def _aws_region() -> str:
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"

def get_secret_dict(secret_name: str, region_name: str = None) -> dict:
    """
    Devuelve el secreto como dict (parsea SecretString JSON o SecretBinary).
    Hace cache en memoria por (secret_name, region).
    """
    rn = region_name or _aws_region()
    key = (secret_name, rn)
    if key in _CACHE:
        return _CACHE[key]

    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=rn)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    if "SecretString" in resp:
        data = json.loads(resp["SecretString"])
    else:
        raw = base64.b64decode(resp["SecretBinary"]).decode("utf-8")
        data = json.loads(raw)

    _CACHE[key] = data
    return data

def get_secret_value(secret_name: str, field: str, region_name: str = None, default=None):
    """
    Devuelve un campo específico de un secreto JSON, p.ej. ('ibk/frmk4/db/teradata','password').
    """
    d = get_secret_dict(secret_name, region_name=region_name)
    return d.get(field, default)
