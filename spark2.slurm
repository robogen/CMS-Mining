#!/bin/sh
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --mem=8000
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=3
#SBATCH --job-name=cms-confirm
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/confirm-run.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/confirm-run.out

MASTER=$(dig $(hostname) a +short)

srun -l $SLURM_SUBMIT_DIR/spark-start.sh $MASTER &
ID=$!

wait $ID

module load anaconda/2.7
source activate cms

/bin/date +%s

$SLURM_SUBMIT_DIR/spark-2/bin/spark-submit \
    --jars $SLURM_SUBMIT_DIR/elasticsearch-hadoop/elasticsearch-hadoop-6.0.0.BUILD.SNAPSHOT.jar \
    --total-executor-cores 24 \
    $SLURM_SUBMIT_DIR/es_spark.py

/bin/date +%s
