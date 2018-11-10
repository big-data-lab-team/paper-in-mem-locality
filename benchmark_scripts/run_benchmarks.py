#!/usr/bin/env python
from os import path as op
from slurmpy import Slurm

# spark in mem
#SBATCH --partition requestq       # Queue (Partition in slurm)
#SBATCH --reservation=spark
#SBATCH --time=00:05:00
#SBATCH --nodes=5
#SBATCH --mem=80G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=1


slurm_spark = {
		"partition": "requestq",
		"reservation": "spark",
		"time": "00:10:00",
		"nodes": "16",
		"mem": "183G",
		"cpus-per-task": "5",
		"ntasks-per-node": "8"
	      }
#45G mem
slurm_nipype = {
		"partition": "requestq",
		"reservation": "spark",
		"time": "00:05:00",
		"nodes": "1",
		"mem": "20G",
		"cpus-per-task": "1",
		"ntasks-per-node": "1"
	       }

spark_template = '/mnt/lustrefs/spark/spark_sbatch.sh'
nipype_template = '/mnt/lustrefs/spark/nipype_sbatch.sh'
data_dir = '/mnt/lustrefs/spark/data'

bb_125dir =  op.join(data_dir, '125_splits')
bb_30dir = op.join(data_dir, '30_splits')
bb_750dir = op.join(data_dir, '750_splits')
hbb_125dir = op.join(data_dir, 'half_bb/125_splits')
mri_125_dir = op.join(data_dir, 'mri/125_splits')

exp_cmds = [
	    '\"SOEN691-project/spark_inc.py {} sp_mem_10it125 10 --benchmark\"'.format(bb_125dir),
	    '\"SOEN691-project/spark_inc.py {} sp_local_10it10-2 10 --benchmark --cli --work_dir /local/sp10local\"'.format(bb_125dir),
	    '\"SOEN691-project/spark_inc.py {} sp_tmpfs_10it125 10 --benchmark --cli --work_dir /dev/shm/spwork/sp10tmpfs\"'.format(bb_125dir),
	    '\"SOEN691-project/spark_inc.py {} sp_local_10it750 10 --benchmark --cli --work_dir /local/sp10local\"'.format(bb_750dir),
	    '\"SOEN691-project/spark_inc.py {} sp_mem_100it2 100 --benchmark\"'.format(bb_125dir),
	    '\"python SOEN691-project/nipype_inc.py {} np_lustre_1it 1 --benchmark\"'.format(bb_125dir),
	    '\"SOEN691-project/spark_inc.py /mnt/lustrefs/spark/data/9_chunks sp_local_1it9c 1 --benchmark --cli --work_dir /local/sp1local9chunk\"'
	   ]

#'''
s = Slurm("spark_test", slurm_spark)
s.run("bash " + spark_template, cmd_kwargs={"spscript": exp_cmds[0]}, _cmd="sbatch")

'''
s = Slurm("nipype_test", slurm_nipype)

with open('/mnt/lustrefs/spark/data/125_splits_legend.txt') as legend:
    images = legend.read().split()
    num_images = len(images)
    num_nodes = 15
    pn_images = num_images/num_nodes
    pn_remain = num_images % num_nodes

    count_rem = 0
    idx = 0
    for i in range(0, num_images - pn_remain, pn_images):
        files = None
        if count_rem < pn_remain:
            files = images[idx: idx+pn_images+1]
            count_rem += 1
            idx += 1
        else:
            files = images[idx: idx+pn_images]
        idx += pn_images
        #ncmd = '\"python SOEN691-project/nipype_inc.py {0} np_local_10it750 10 --benchmark --cli --work_dir /local/npwork/np_l10it750_work_{1}\"'.format(",".join(files), i)
        ncmd = '\"python SOEN691-project/nipype_inc.py {0} np_tmpfs_10it125 10 --benchmark --cli --work_dir /dev/shm/npwork/np_l10it750_work_{1}\"'.format(",".join(files), i)
        s.run("bash " + nipype_template, cmd_kwargs={"npscript": ncmd}, _cmd="sbatch")
'''
