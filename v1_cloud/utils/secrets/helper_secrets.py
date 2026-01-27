# on_cloud/utils/secrets/helper_secrets.py
from __future__ import annotations

import json
import os
import boto3
from typing import Dict, Optional


def get_secret_map(
    secret_name: Optional[str] = None,
    region_name: Optional[str] = None
) -> Dict[str, str]:
    """
    Lee un secreto de AWS Secrets Manager y devuelve un dict con llaves comunes:
    username/user/usr -> 'username'
    password/pwd/pass -> 'password'

    Prioridad:
      1) Parámetros de función
      2) ENV: TERADATA_SECRET_NAME / TERADATA_SECRET_REGION
      3) Defaults: ibk/frmk4/db/teradata, us-east-1
    """
    name = secret_name or os.getenv("TERADATA_SECRET_NAME") or "ibk/frmk4/db/teradata"
    region = region_name or os.getenv("TERADATA_SECRET_REGION") or os.getenv("AWS_REGION") or "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=name)

    raw = resp.get("SecretString")
    if raw is None and "SecretBinary" in resp:
        raw = resp["SecretBinary"].decode("utf-8")

    out: Dict[str, str] = {}
    try:
        data = json.loads(raw)
        user = data.get("username") or data.get("user") or data.get("usr")
        pwd  = data.get("password") or data.get("pwd") or data.get("pass")
        if user:
            out["username"] = user
        if pwd:
            out["password"] = pwd
    except Exception:
        if raw:
            out["password"] = raw
    return out
