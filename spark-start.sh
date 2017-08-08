#!/bin/bash

MASTER=$1
HOST=$(hostname)

if [ "$HOST" == "$MASTER" ]; then
  $SLURM_SUBMIT_DIR/spark-2/sbin/start-master.sh
else
  sleep 6
  echo "Master: $MASTER and Worker: $HOST"
  $SLURM_SUBMIT_DIR/spark-2/sbin/start-slave.sh spark://$MASTER:7077
fi  

tail -f /dev/null
