#!/bin/sh
#SBATCH --time=24:00:00          # Run time in hh:mm:ss
#SBATCH --mem-per-cpu=4500       # Maximum memory required per CPU (in megabytes)
#SBATCH --ntasks=1
#SBATCH --job-name=CMSRender
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/BatchErr/CMSRender.%J.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/BatchOut/CMSRender.%J.out
 
module load anaconda/2.7
source activate cms

/bin/date +%s

python RunScripts/index_reseter.py 
python RunScripts/es_main.py

/bin/date +%s
