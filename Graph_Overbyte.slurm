#!/bin/sh
#SBATCH --time=08:00:00          # Run time in hh:mm:ss
#SBATCH --mem-per-cpu=2500       # Maximum memory required per CPU (in megabytes)
#SBATCH --ntasks=1
#SBATCH --job-name=Overbyte
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/BatchErr/Graph_Overbyte.%J.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/BatchOut/Graph_Overbyte.%J.out
 
module load anaconda/2.7
source activate cms

/bin/date +%s

python RunScripts/es_overbyte.py

/bin/date +%s
