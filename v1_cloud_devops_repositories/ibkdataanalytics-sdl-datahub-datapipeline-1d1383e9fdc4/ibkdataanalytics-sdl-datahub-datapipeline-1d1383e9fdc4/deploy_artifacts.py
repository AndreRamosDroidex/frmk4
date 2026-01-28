#!/usr/bin/env python3
"""
Deploy artifacts script:
- Sube artefactos a S3
- Actualiza SSM con la subcarpeta del bucket
"""

import os
import boto3
from utils.common import create_name_bucket
from config.constants import ACCOUNT_ID
from datetime import datetime
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError

NUMBER=1
# Variables de entorno
BRANCH_NAME = os.environ['BRANCH_NAME']
ENVIRONMENT = os.environ['ENVIRONMENT']

# PROCESS - S3 BUCKET
BUCKET_SOURCE = create_name_bucket(
    funcionalidad="process",
    env=ENVIRONMENT,
    account_id=ACCOUNT_ID,
    sequence=NUMBER
)

def main():
    # Timestamp único para identificar subcarpeta
    timestamp = datetime.now(ZoneInfo("America/Lima")).strftime("%Y%m%d%H%M%S")

    prefix_artifacts = f"{BRANCH_NAME}/code/{BRANCH_NAME}"

    # Subir artefactos a S3
    s3_client = boto3.client('s3')
    local_path = './src/emr'  # Carpeta con artefactos
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"No se encontró la carpeta de artefactos: {local_path}")

    for root, _, files in os.walk(local_path):
        for file in files:
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, start=local_path)
            dest_path = f"{prefix_artifacts}/{rel_path}"
            s3_client.upload_file(src_path, BUCKET_SOURCE, dest_path)
            print(f"Subido {src_path} -> s3://{BUCKET_SOURCE}/{dest_path}")

if __name__ == '__main__':
    main()
