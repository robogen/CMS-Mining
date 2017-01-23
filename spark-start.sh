#!/bin/bash

MASTER=$1
HOST=$(hostname)
echo "Master: $MASTER and Worker: $HOST"
if [ "$HOST" == "$MASTER" ]; then
  $SLURM_SUBMIT_DIR/spark-2/sbin/start-master.sh
fi  

$SLURM_SUBMIT_DIR/spark-2/sbin/start-slave.sh spark://$MASTER:7077

tail -f /dev/null
