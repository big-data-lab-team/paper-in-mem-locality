#!/bin/bash

export SPARK_HOME='/mnt/lustrefs/spark/spark-2.3.2-bin-hadoop2.7/'
# export SPARK_HOME='/tmp/software/spark-2.3.2-bin-hadoop2.7/'
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH 
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH

export SPARK_IDENT_STRING=$SLURM_JOBID
#export SPARK_WORKER_DIR=/tmp/spark_worker
export SPARK_WORKER_DIR=/mnt/lustrefs/spark/todelete-sparklogs
export SPARK_LOG_DIR=/mnt/lustrefs/spark/todelete-sparklogs
# export SPARK_LOG_DIR=/tmp/spark_logs
export PYTHONIOENCODING=utf8

source /mnt/lustrefs/spark/env/exp0/bin/activate
export PATH=/mnt/lustrefs/spark/SOEN691-project:$PATH

$SPARK_HOME/sbin/start-master.sh
while [ -z "$MASTER_URL" ]
do
	MASTER_URL=$(curl -s http://localhost:8082/json/ | python -c "import sys, json; print json.load(sys.stdin)['url']")
	echo "master not found"
	sleep 5
done
echo $MASTER_URL

NNODES=$((SLURM_NNODES - 1))
NWORKERS=$((SLURM_NTASKS - SLURM_NTASKS_PER_NODE))
NEXECUTORS=$((SLURM_NTASKS_PER_NODE * NNODES))
NEXECUTORS_PER_NODE=$((NEXECUTORS / NNODES))
MEM_PER_EXEC=$((SLURM_MEM_PER_NODE / NEXECUTORS_PER_NODE))
HEAP_OVERHEAD=$(echo $MEM_PER_EXEC*0.07 | bc)
MEM_PER_EXEC=$((MEM_PER_EXEC - HEAP_OVERHEAD))
#MEM_PER_EXEC=$((SLURM_MEM_PER_NODE / NEXECUTORS))
#NEXECUTORS= ${SLURM_NTASKS_PER_NODE}
echo ${NEXECUTORS_PER_NODE}
echo ${MEM_PER_EXEC}
echo ${HEAP_OVERHEAD}

SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS} -N ${NNODES} --label --output=$SPARK_LOG_DIR/spark-%j-workers.out $SPARK_HOME/sbin/start-slave.sh -m ${MEM_PER_EXEC}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &
slaves_pid=$!

echo 'Starting execution'
srun -n 1 -N 1 $SPARK_HOME/bin/spark-submit --master=${MASTER_URL} --num-executors=${NEXECUTORS} --executor-memory=${MEM_PER_EXEC}M --driver-memory=10G --conf spark.locality.wait=600s $spscript

kill $slaves_pid
$SPARK_HOME/sbin/stop-master.sh

