#!/usr/bin/env bash

python results/code/get_makespan.py results/benchmarks results/code/incrementation_makespan.out

python results/code/d_c_graph.py results/benchmarks results/code/incrementation_makespan.out results/figures/incrementation.pdf
python results/code/gen_figures.py 1 results/code/incrementation_makespan.out -f "spark" results/figures/s-iterations.pdf
python results/code/gen_figures.py 2 results/code/incrementation_makespan.out -f "spark" results/figures/s-numchunks.pdf
python results/code/gen_figures.py 3 results/code/incrementation_makespan.out -f "spark" results/figures/s-datasize.pdf
python results/code/gen_figures.py 4 results/code/incrementation_makespan.out -f "spark" results/figures/s-cputime.pdf
python results/code/gen_figures.py 1 results/code/incrementation_makespan.out -f "nipype" results/figures/n-iterations.pdf
python results/code/gen_figures.py 2 results/code/incrementation_makespan.out -f "nipype" results/figures/n-numchunks.pdf
python results/code/gen_figures.py 3 results/code/incrementation_makespan.out -f "nipype" results/figures/n-datasize.pdf
python results/code/gen_figures.py 4 results/code/incrementation_makespan.out -f "nipype" results/figures/n-cputime.pdf
