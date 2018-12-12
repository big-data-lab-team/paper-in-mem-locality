#!/usr/bin/env bash
#SBATCH --partition requestq       # Queue (Partition in slurm)
#SBATCH --reservation=spark
#SBATCH --time=99:00:00
#SBATCH --nodes=1
#SBATCH --mem=183G
#SBATCH --cpus-per-task=40
#SBATCH --ntasks-per-node=1

source /mnt/lustrefs/spark/env/exp0/bin/activate
export PATH=/mnt/lustrefs/spark/SOEN691-project/pipelines/:$PATH
export PYTHONPATH=/mnt/lustrefs/spark/SOEN691-project/pipelines:$PYTHONPATH


