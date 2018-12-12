#!/usr/bin/env bash

python get_makespan.py ../benchmarks incrementation_makespan.out

python d_c_graph.py ../benchmarks incrementation_makespan.out ../figures/incrementation.pdf
python gen_figures.py 1 incrementation_makespan.out ../figures/iterations.pdf
python gen_figures.py 2 incrementation_makespan.out ../figures/numchunks.pdf
python gen_figures.py 3 incrementation_makespan.out ../figures/datasize.pdf
python gen_figures.py 4 incrementation_makespan.out ../figures/cputime.pdf
