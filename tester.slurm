#!/bin/sh
#SBATCH --time=00:05:00
#SBATCH --nodes=2
#SBATCH --mem=8000
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=3
#SBATCH --network=DEVTYPE=IPONLY
#SBATCH --job-name=cms-tester
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/tester-run.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/tester-run.out

MASTER=$(hostname)

srun -l $SLURM_SUBMIT_DIR/spark-start.sh $MASTER & wait

module load anaconda/2.7
source activate cms

/bin/date +%s

echo "Tester"

/bin/date +%s
