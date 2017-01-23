#!/bin/sh
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --mem=8000
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=3
#SBATCH --network=devtype=iponly
#SBATCH --job-name=one-spark
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/onenode-run.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/onenode-run.out

MASTER=$(hostname)

srun -l $SLURM_SUBMIT_DIR/spark-start.sh $MASTER & 
sleep 60

module load anaconda/2.7
source activate cms

/bin/date +%s

$SLURM_SUBMIT_DIR/spark-2/bin/spark-submit \
    --jars $SLURM_SUBMIT_DIR/elasticsearch-hadoop/elasticsearch-hadoop-6.0.0.BUILD.SNAPSHOT.jar \
    --total-executor-cores 24 \
    --master spark://$MASTER:7077 \
    $SLURM_SUBMIT_DIR/es_sparkremote.py 1

/bin/date +%s
