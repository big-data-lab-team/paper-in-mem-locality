#!/usr/bin/env python
from os import path as op, remove
from slurmpy import Slurm
from random import shuffle
from time import sleep
import sys
import subprocess

slurm_spark = {
               "partition": "requestq",
               "reservation": "spark",
               "time": "35:00:00",
               "nodes": "16",
               "mem": "183G",
               "cpus-per-task": "40",
               "ntasks-per-node": "1"
	          }

slurm_nipype = {
                "partition": "requestq",
                "reservation": "spark",
                "time": "35:00:00",
                "nodes": "1",
                "mem": "183G",
                "cpus-per-task": "1",
                "ntasks-per-node": "1"
	           }

bscripts_dir = '/mnt/lustrefs/spark/SOEN691-project/benchmark_scripts'

spark_template = op.join(bscripts_dir, 'spark_sbatch.sh')
nipype_template = op.join(bscripts_dir, 'nipype_sbatch.sh')
cleanup_script = op.join(bscripts_dir, 'tmp_cleanup.sh')
data_dir = '/mnt/lustrefs/spark/data'

tmpfs = '/dev/shm/inc_exp'
local = '/local/inc_exp'
lustre = '/mnt/lustrefs/spark/inc_exp'

done_file = op.join(lustre, "COMPLETED")

filesystems = { 
                'tmpfs': tmpfs,
                'local': local,
                'lustre': lustre
              }

spark_script='/mnt/lustrefs/spark/SOEN691-project/spark_inc.py'
nipype_script='/mnt/lustrefs/spark/SOEN691-project/nipype_inc.py'

bb_125dir =  op.join(data_dir, '125_splits')
bb_30dir = op.join(data_dir, '30_splits')
bb_750dir = op.join(data_dir, '750_splits')
hbb_125dir = op.join(data_dir, 'half_bb/125_splits')
mri_125dir = op.join(data_dir, 'mri/125_splits')

num_nodes = 15

legends = {
            bb_125dir: op.join(data_dir, '125_splits_legend.txt'),
            bb_30dir: op.join(data_dir, '30_splits_legend.txt'),
            bb_750dir: op.join(data_dir, '750_splits_legend.txt'),
            hbb_125dir: op.join(data_dir, 'half_bb/125_splits_legend.txt'),
            mri_125dir: op.join(data_dir, 'mri/125_splits_legend.txt')
          }

conditions = [
              { 
                  "framework": "spark",
                  "filesystem": "mem",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 1
              },
              {
                  "framework": "spark",
                  "filesystem": "mem",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "mem",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 100
              },
              {
                  "framework": "spark",
                  "filesystem": "mem",
                  "delay": 10,
                  "dataset": bb_750dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "mem",
                  "delay": 55,
                  "dataset": hbb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "mem",
                  "delay": 55,
                  "dataset": mri_125dir,
                  "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "mem",
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "mem",
                 "delay": 900,
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              { 
                  "framework": "spark",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 1
              },
              {
                  "framework": "spark",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "local",
                  "delay": 229,
                  "dataset": bb_30dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "local",
                  "delay": 10,
                  "dataset": bb_750dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": hbb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": mri_125dir,
                  "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "local",
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "local",
                 "delay": 55,
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              { 
                  "framework": "spark",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 1
              },
              {
                  "framework": "spark",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "tmpfs",
                  "delay": 229,
                  "dataset": bb_30dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "tmpfs",
                  "delay": 10,
                  "dataset": bb_750dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": hbb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": mri_125dir,
                  "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "tmpfs",
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "tmpfs",
                 "delay": 900,
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              { 
                  "framework": "spark",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 1
              },
              {
                  "framework": "spark",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 100
              },
              {
                  "framework": "spark",
                  "filesystem": "lustre",
                  "delay": 229,
                  "dataset": bb_30dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "lustre",
                  "delay": 10,
                  "dataset": bb_750dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": hbb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "spark",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": mri_125dir,
                  "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "lustre",
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              {
                 "framework": "spark",
                 "filesystem": "lustre",
                 "delay": 900,
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              { 
                  "framework": "nipype",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 1
              },
              {
                  "framework": "nipype",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "local",
                  "delay": 229,
                  "dataset": bb_30dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "local",
                  "delay": 10,
                  "dataset": bb_750dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": hbb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "local",
                  "delay": 55,
                  "dataset": mri_125dir,
                  "iterations": 10
              },
              {
                 "framework": "nipype",
                 "filesystem": "local",
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              {
                 "framework": "nipype",
                 "filesystem": "local",
                 "delay": 900,
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              { 
                  "framework": "nipype",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 1
              },
              {
                  "framework": "nipype",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "tmpfs",
                  "delay": 229,
                  "dataset": bb_30dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "tmpfs",
                  "delay": 10,
                  "dataset": bb_750dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": hbb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "tmpfs",
                  "delay": 55,
                  "dataset": mri_125dir,
                  "iterations": 10
              },
              {
                 "framework": "nipype",
                 "filesystem": "tmpfs",
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              {
                 "framework": "nipype",
                 "filesystem": "tmpfs",
                 "delay": 900,
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              { 
                  "framework": "nipype",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 1
              },
              {
                  "framework": "nipype",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": bb_125dir,
                  "iterations": 100
              },
              {
                  "framework": "nipype",
                  "filesystem": "lustre",
                  "delay": 229,
                  "dataset": bb_30dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "lustre",
                  "delay": 10,
                  "dataset": bb_750dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": hbb_125dir,
                  "iterations": 10
              },
              {
                  "framework": "nipype",
                  "filesystem": "lustre",
                  "delay": 55,
                  "dataset": mri_125dir,
                  "iterations": 10
              },
              {
                 "framework": "nipype",
                 "filesystem": "lustre",
                 "dataset": bb_125dir,
                 "iterations": 10
              },
              {
                 "framework": "nipype",
                 "filesystem": "lustre",
                 "delay": 900,
                 "dataset": bb_125dir,
                 "iterations": 10
              },
             ]

shuffle(conditions)

for cdn in conditions:
    cmd = []
    out_dir = ""
    work_dir = ""
    cdn_ident = "_{0}_{1}it".format(cdn["filesystem"],
                                    cdn["iterations"])

    slurm_conf = ""

    if cdn["framework"] == "spark":
        cmd.append(spark_script)
        out_dir = "sp"
        work_dir = "spwork"
        slurm_conf = slurm_spark
    else:
        cmd.append(nipype_script)
        out_dir = "np"
        work_dir = "npwork"
        slurm_conf = slurm_nipype

    cmd.append(cdn["dataset"])

    if cdn["dataset"] == bb_125dir:
        cdn_ident += "125BB"
        slurm_conf["cpus-per-task"] = 9
    elif cdn["dataset"] == bb_30dir:
        cdn_ident += "30BB"
        slurm_conf["cpus-per-task"] = 2
    elif cdn["dataset"] == bb_750dir:
        cdn_ident += "750BB"
        slurm_conf["cpus-per-task"] = 25
    elif cdn["dataset"] == mri_125dir:
        cdn_ident += "125MRI"
        slurm_conf["cpus-per-task"] = 9
    else:
        cdn_ident += "125HBB"
        slurm_conf["cpus-per-task"] = 9

    if "delay" in cdn:
        out_dir = op.join(lustre, 'results',
                          out_dir + cdn_ident + "_{}delay".format(
                                                         cdn["delay"]))

        cmd += [out_dir, str(cdn["iterations"]), "--benchmark",
                "--delay", str(cdn["delay"])]
    else:
        out_dir = op.join(lustre, 'results', out_dir + cdn_ident)
        cmd += [out_dir, str(cdn["iterations"]), "--benchmark"]


    if cdn["filesystem"] != "mem":
        work_dir = op.join(filesystems[cdn["filesystem"]],
                           'work',
                           work_dir + cdn_ident)

        cmd += ["--cli", "--work_dir", work_dir]


    s = Slurm("incrementation", slurm_conf)

    if cdn["framework"] == "spark":
        cmd = " ".join(cmd)
        cmd = "\"{}\"".format(cmd)

        print("Submitting command: ", cmd)

        s.run("bash " + spark_template, cmd_kwargs={"spscript": cmd},
              _cmd=sys.argv[1])
    else:
        with open(legends[cdn["dataset"]]) as legend:
            images = legend.read().split()
            num_images = len(images)
            pn_images = num_images/num_nodes
            pn_remain = num_images % num_nodes

            count_rem = 0
            idx = 0

            for i in range(0, num_images - pn_remain, pn_images):
                files = None

                if count_rem < pn_remain:
                    files = images[idx: idx+pn_images+1]
                    count_rem += 1
                    idx += pn_images + 1
                else:
                    files = images[idx: idx+pn_images]
                    idx += pn_images
                
                cmd[1] = ",".join(files)
                ncmd = " ".join(cmd)
                ncmd += "_{}".format(i)
                ncmd = "\"{}\"".format(ncmd)

                print("Submitting command: ", ncmd)
                s.run("bash " + nipype_template, cmd_kwargs={"npscript": ncmd},
                      _cmd=sys.argv[1])

    p = subprocess.call(['sbatch', cleanup_script])
    
    if p:
        print('ERROR: was not able to clean tmp files')
        sys.exit(p)

    p = subprocess.Popen(['squeue -u tristan.concordia | wc -l'],
                         shell=True, stdout=subprocess.PIPE)
    (out, _) = p.communicate()

    while int(out) > 1:
        sleep(300)
        p = subprocess.Popen(['squeue -u tristan.concordia | wc -l'],
                         shell=True, stdout=subprocess.PIPE)
        (out, _) = p.communicate()
