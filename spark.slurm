#!/bin/sh
#SBATCH --time=02:00:00
#SBATCH --nodes=2
#SBATCH --mem=12000
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=3
#SBATCH --job-name=SparkScatter
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/BatchErr/SparkScatter-run.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/BatchOut/SparkScatter-run.out

MASTER=$(hostname)
export PYTHONHASHSEED=4224

echo $MASTER

srun -l $SLURM_SUBMIT_DIR/spark-start.sh $MASTER & 
sleep 30

module load anaconda/2.7
module load java/1.8
source activate cms

/bin/date +%s

$SLURM_SUBMIT_DIR/spark-2/bin/spark-submit \
    --jars $SLURM_SUBMIT_DIR/elasticsearch-hadoop/elasticsearch-hadoop-6.0.0.BUILD.SNAPSHOT.jar \
    --total-executor-cores 6 \
    --executor-memory 2G \
    --master spark://$MASTER:7077 \
    $SLURM_SUBMIT_DIR/SparkScripts/es_scatter.py 1

/bin/date +%s
