#!/bin/bash

set -e

echo "Desplegando codebuilds."

# Configurar el entorno
# export ENV="dev"
ENV="${ENVIRONMENT:-dev}"
echo "Entorno configurado..: $ENV"

# Obtener el ID de la cuenta de AWS
export ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
echo "ID de la cuenta: $ACCOUNT_ID"
echo "------------------"

# Verificar el directorio actual
echo "Directorio actual: $(pwd)"
echo "------------------"

# Instalar dependencias (si es necesario)
echo "Instalando dependencias..."
# pip install -r requirements.txt
if ! pip install -r requirements.txt; then
    echo "pip no encontrado, intentando con pip3..."
    pip3 install -r requirements.txt --break-system-packages
fi

# Paso previo a CDK
echo "Desplegando artefactos..."
python3 deploy_artifacts.py || { echo "Error en deploy_artifacts"; exit 1; }

# Sintetizar los stacks de CDK
echo "Sintetizando los stacks de CDK..."
cdk synth --require-approval never || { echo "cdk synth fallo"; exit 1; }

# Verificar si cdk.out/manifest.json existe
if [ ! -f "cdk.out/manifest.json" ]; then
  echo "Error: cdk.out/manifest.json no encontrado"
  exit 1
fi

STACK_NAME_PIPELINE="aw-${ENV}-cfn-${project}-${BRANCH_NAME}-datapipeline-01"
echo "$STACK_NAME_PIPELINE"

export ACCOUNT=${ACCOUNT_ID}
# Desplegar los stacks de CDK
echo "Desplegando los stacks de CDK..."
cdk deploy "$STACK_NAME_PIPELINE" --require-approval never -c ENVIRONMENT=$ENV
echo "------------------"