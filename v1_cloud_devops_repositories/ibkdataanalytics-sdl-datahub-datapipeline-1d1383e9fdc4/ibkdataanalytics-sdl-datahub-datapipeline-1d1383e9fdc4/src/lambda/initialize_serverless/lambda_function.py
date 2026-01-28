import os
import json
import logging
import boto3
from urllib.parse import urlparse
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("{levelname} - {message}", style="{")

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')


DYNAMO_TABLE = os.environ.get("DB_CONFIG_GLOBAL")

def load_emr_config(state_machine_name: str):
    """
    Lee la configuración EMR desde DynamoDB usando 'state_machine' como clave.
    """
    table = dynamodb.Table(DYNAMO_TABLE)

    try:
        response = table.get_item(Key={"state_machine": state_machine_name})
        if "Item" not in response:
            raise ValueError(f"No se encontró configuración para state_machine='{state_machine_name}' en DynamoDB")

        config_data = response["Item"]

        emr_values = (
            config_data["config_values"][0]["config_values"][0]["emr_values"]
        )

        return {
            "cluster_id": emr_values["emr_clusteridentifier"],
            "source_path": emr_values["emr_source"],
            "main_file": emr_values["emr_main"],
            "state_machine": config_data["state_machine"]
        }

    except ClientError as e:
        logger.error(f"Error al leer DynamoDB: {e}")
        raise
    except Exception as e:
        logger.error(f"Error procesando configuración DynamoDB: {e}")
        raise

def list_zip_modules_from_s3(source_path: str):
    """
    Retorna una lista de rutas completas a los .zip ubicados en el path S3 especificado.
    """
    parsed = urlparse(source_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    logger.info(f"Buscando módulos ZIP en s3://{bucket}/{prefix}")

    zip_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith('.zip'):
                zip_files.append(f"s3://{bucket}/{key}")

    if not zip_files:
        logger.warning(f"No se encontraron archivos ZIP en {source_path}")

    return zip_files

def lambda_handler(event, context):
    try:

        #Obtenemos los valores del input
        id_tracking =  event.get('id_tracking','')
        id_process =  event.get('id_process','')
        source =  event.get('source','')
        fecinformacion =  event.get('fecinformacion','')
        state_machine =  'aw-dev-sfn-frmk4-process-01-demo'

        emr_config = load_emr_config(state_machine)
        modules = list_zip_modules_from_s3(emr_config["source_path"])
        modules_str = ",".join(modules)

        emr_args = [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--py-files", modules_str,
            f"{emr_config['source_path']}{emr_config['main_file']}",
            "--id_tracking", event.get("id_tracking"),
            "--id_process", event.get("id_process"),
            "--fecinformacion", event.get("fecinformacion")
        ]

        step_name = f"{event.get('source')}-{event.get('id_tracking')}-{event.get('id_process')}-{event.get('fecinformacion')}"

        # Input for Step
        step_input = {
            "id_tracking": id_tracking,
            "id_process": id_process,
            "source": source,
            "fecinformacion": fecinformacion,
            "step_name": step_name,
            "cluster_id": emr_config["cluster_id"],
            "emr_args": emr_args
        }
        logger.info(f"EMR Step input: {json.dumps(step_input)}")

        
        return step_input


    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise