#!/usr/bin/env bash

python results/code/get_makespan.py results/benchmarks results/code/incrementation_makespan.out

python results/code/d_c_graph.py results/benchmarks results/code/incrementation_makespan.out results/figures/incrementation.pdf
python results/code/gen_figures.py 1 results/code/incrementation_makespan.out results/figures/iterations.pdf
python results/code/gen_figures.py 2 results/code/incrementation_makespan.out results/figures/numchunks.pdf
python results/code/gen_figures.py 3 results/code/incrementation_makespan.out results/figures/datasize.pdf
python results/code/gen_figures.py 4 results/code/incrementation_makespan.out results/figures/cputime.pdf
