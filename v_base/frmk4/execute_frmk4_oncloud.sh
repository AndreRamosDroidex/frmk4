#!/bin/bash

######################
## FUNCIONES UTILES ##
######################

# Generar el id_tracking 
geIDTracking() {
  local timestamp aleatorio pid fragment id
  timestamp=$(date +"%Y%m%d%H%M%S")
  aleatorio=$(printf "%05d" $((1 + RANDOM % 5000)))
  pid=$$                                # PID del proceso
  fragment=$(printf "%03d" $((pid % 1000)))  # 3 dígitos del PID
  id="${timestamp}${aleatorio}${fragment}"         
  echo "$id"
}


########################

# Obtener el Id tracking y log del proceso 
ID_TRACKING=$(geIDTracking)

BATCH_ID=${1}
FECINFORMACION=${2}

PATH_PROJECT=$(dirname "$(realpath "$0")")

# Esto se agrego para que tome la nueva version del spark 
export SPARK_HOME=${PATH_PROJECT}/spark-3.5.6-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH


## On-Premise ##
# Esto se agrego para utilizar los modulos en los worker del spark
export PYTHONPATH=$PYTHONPATH:${PATH_PROJECT}/on_cloud

sh ${SPARK_HOME}/bin/spark-submit \
  --master local[*] \
  ${PATH_PROJECT}/on_cloud/main.py ${ID_TRACKING} ${BATCH_ID} ${FECINFORMACION}
