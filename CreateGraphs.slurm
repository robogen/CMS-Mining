#!/bin/sh
#SBATCH --time=02:00:00          # Run time in hh:mm:ss
#SBATCH --mem-per-cpu=2500       # Maximum memory required per CPU (in megabytes)
#SBATCH --ntasks=1
#SBATCH --job-name=CMSCreateGraphs
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/BatchErr/CreateGraphs.%J.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/BatchOut/CreateGraphs.%J.out
 
module load anaconda/2.7
source activate cms

python es_overbyte.py
