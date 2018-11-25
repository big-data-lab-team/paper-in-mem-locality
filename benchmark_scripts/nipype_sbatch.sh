#!/bin/bash

source /mnt/lustrefs/spark/env/exp0/bin/activate
export PATH=/mnt/lustrefs/spark/SOEN691-project/pipelines/:$PATH

echo "{\"n_procs\": ${SLURM_CPUS_PER_TASK}}" > /mnt/lustrefs/spark/multiproc_conf.json

$npscript --plugin MultiProc --plugin_args /mnt/lustrefs/spark/multiproc_conf.json

