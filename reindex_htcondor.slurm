#!/bin/sh
#SBATCH --time=48:00:00          # Run time in hh:mm:ss
#SBATCH --mem-per-cpu=2500       # Maximum memory required per CPU (in megabytes)
#SBATCH --ntasks=1
#SBATCH --job-name=HTCondorReindex
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/BatchErr/HTCondor_Cluster_Reindex.%J.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/BatchOut/HTCondor_Cluster_Reindex.%J.out
 
module load anaconda/2.7
source activate cms

/bin/date +%s

python htcondor_reindex.py

/bin/date +%s
