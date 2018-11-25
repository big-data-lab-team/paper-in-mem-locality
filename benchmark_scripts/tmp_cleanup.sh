#!/usr/bin/env bash
#SBATCH --partition requestq       # Queue (Partition in slurm)
#SBATCH --reservation=spark
#SBATCH --time=10:00:00
#SBATCH --nodes=15
#SBATCH --mem=20G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=1

#srun ls /local/
srun sync
srun echo 1 > /proc/sys/vm/drop_caches
srun rm -rf /dev/shm/inc_exp/work
srun rm -rf /local/inc_exp/work
srun rm -rf /mnt/lustrefs/spark/inc_exp/work

