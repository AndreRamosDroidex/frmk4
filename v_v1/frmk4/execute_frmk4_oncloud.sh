#!/bin/bash

######################
## FUNCIONES UTILES ##
######################
geIDTracking() {
  local timestamp aleatorio pid fragment id
  timestamp=$(date +"%Y%m%d%H%M%S")
  aleatorio=$(printf "%05d" $((1 + RANDOM % 5000)))
  pid=$$
  fragment=$(printf "%03d" $((pid % 1000)))
  id="${timestamp}${aleatorio}${fragment}"
  echo "$id"
}

########################

ID_TRACKING=$(geIDTracking)
BATCH_ID=${1}
FECINFORMACION=${2}

PATH_PROJECT=$(dirname "$(realpath "$0")")

# Spark local del proyecto
export SPARK_HOME=${PATH_PROJECT}/spark-3.5.6-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

# Para que los workers vean los módulos Python
export PYTHONPATH=$PYTHONPATH:${PATH_PROJECT}/on_cloud

# Solo el driver JDBC de Postgres (para el extractor JDBC)
EXTRA_PACKAGES="org.postgresql:postgresql:42.7.4"

${SPARK_HOME}/bin/spark-submit \
  --master local[*] \
  --packages "$EXTRA_PACKAGES" \
  ${PATH_PROJECT}/on_cloud/main.py ${ID_TRACKING} ${BATCH_ID} ${FECINFORMACION}
