# Paper Big Data Strategies

In this paper we quantify and discuss the performance brought by 
in-memory computing, data locality and lazy evaluation on 
neuroinformatics pipelines, using the (Apache 
Spark)[https://spark.apache.org] and 
(Nipype)[http://nipype.readthedocs.io/en/latest] workflow engines.

## Experiment: Incrementation in Nipype and Spark
[![Build Status](https://travis-ci.org/ValHayot/paper-in-mem-locality.svg?branch=master)](https://travis-ci.org/ValHayot/paper-in-mem-locality.svg?branch=master)

## Paper 
A pdf is uploaded for every release of the paper:

[Paper v0.1](https://github.com/big-data-lab-team/paper-in-mem-locality/releases/download/0.1/paper.pdf)

To contribute, fork the repository, edit ```paper.tex``` and 
```biblio.bib```, and make a pull-request. 

To generate the pdf:
1. Generate the figures: `./results/code/gen_all_figures.sh` (requires matplotlib)
2. Generate the pdf: `pdflatex paper ; bibtex paper ; pdflatex paper ; pdflatex paper`

## Data and code

* `pipelines` contains the application pipelines benchmarked in the paper.
* `sample_data` and `tests` are only used for testing.
* `benchmark_scripts` are used for additional infrastructure benchmarks
