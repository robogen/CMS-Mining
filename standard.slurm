#!/bin/sh
#SBATCH --time=05:00:00
#SBATCH --nodes=1
#SBATCH --mem=8000
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=3
#SBATCH --job-name=cms-python
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/python-run.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/python-run.out

/bin/date +%s

/home/swanson/jdixon/.conda/envs/cms/bin/python es_testermain.py

/bin/date +%s
