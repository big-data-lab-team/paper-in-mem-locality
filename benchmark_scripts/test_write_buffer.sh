#!/bin/bash

#rm /local/todelete/*

cdir=/mnt/lustrefs/spark/data/9_chunks/
chunks=(${cdir}*)
odir=/local/todelete
write_data() {
    for i in `seq 1 10`;
        do
            /mnt/lustrefs/spark/SOEN691-project/increment.py ${c} ${odir}/${i} >> bench_wb/${x}_${i}.out
	    if [ "$i" -le 1 ]; then
                c=${odir}/${i}/inc-$(basename ${c})
            else
                c=${odir}/${i}/$(basename ${c})
            fi
        done
}

for x in `seq 0 8`;
    do
        c=${chunks[${x}]} write_data&
    done
