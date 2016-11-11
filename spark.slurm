#!/bin/sh
#SBATCH --time=00:10:00
#SBATCH --nodes=2
#SBATCH --mem=4000
#SBATCH --mem-per-cpu=1024
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=3
#SBATCH --job-name=cms-spark
#SBATCH --error=/work/swanson/jdixon/CMS/CMS-Mining/prime-run.err
#SBATCH --output=/work/swanson/jdixon/CMS/CMS-Mining/prime-run.out

export HADOOP_PROTOC_PATH=/work/swanson/jdixon/CMS/CMS-Mining/local/bin/protoc
export LD_LIBRARY_PATH=/work/swanson/jdixon/CMS/CMS-Mining/local/hadoop/lib/native:$LD_LIBRARY_PATH
export HADOOP_COMMON_LIB_NATIVE_DIR=/work/swanson/jdixon/CMS/CMS-Mining/local/hadoop/lib/native/
export HADOOP_OPTS="-Djava.library.path=/work/swanson/jdixon/CMS/CMS-Mining/local/hadoop/lib/"

module load anaconda/2.7
source activate cms

for i in `/usr/bin/scontrol show hostname $SLURM_JOB_NODELIST | /usr/bin/paste -d, -s | /bin/sed "s/,/ /g"`;
do
    echo $i
done

# /bin/sh $SLURM_SUBMIT_DIR/spark/sbin/start-master.sh
# echo $MASTER
