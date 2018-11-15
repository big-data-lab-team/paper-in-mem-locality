#!/bin/bash

source /mnt/lustrefs/spark/env/exp0/bin/activate
export PATH=/mnt/lustrefs/spark/SOEN691-project:$PATH

echo "{\"n_procs:\" ${SLURM_NTASKS_PER_NODE}}" > /mnt/lustrefs/spark/multiproc_conf.json

$npscript --plugin MultiProc --plugin_args /mnt/lustrefs/spark/multiproc_conf.json

