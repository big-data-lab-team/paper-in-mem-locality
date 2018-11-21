# Paper Big Data strategies

In this paper we quantify and discuss the performance brought by 
in-memory computing, data locality and lazy evaluation on 
neuroinformatics pipelines, using the (Apache 
Spark)[https://spark.apache.org] and 
(Nipype)[http://nipype.readthedocs.io/en/latest] workflow engines.

## Experiments

### Experiment 0: Incrementation in Nipype and Spark
[![Build Status](https://travis-ci.org/ValHayot/SOEN691-project.svg?branch=experiment0)](https://travis-ci.org/ValHayot/SOEN691-project)

### Experiment 1: Simple binarization in Nipype and Spark
### Experiment 2: K-means workflow in Nipype and Spark
### Experiment 3: Reimplementation of an existing (fMRIPrep)[https://fmriprep.readthedocs.io/en/latest/index.html] workflow in Apache Spark
- The fMRIPrep workflow selected is anatomical preprocessing without reconall (command-line call to be added here)
- Spark implementation can be found under `sparkprep.py`

Project report: https://docs.google.com/document/d/1gVQ5X9mEMPXmoTBowtW_Bbj371v056FeQJ407lKZCOo/edit?usp=sharing

## Paper 

A pdf is uploaded for every release of the paper:
* There is no release yet!

To contribute, fork the repository, edit ```paper.tex``` and 
```biblio.bib```, and make a pull-request. 

## Data and code

* `pipelines` contains the application pipelines benchmarked in the paper.
* `sample_data` and `tests` are only used for testing.
* `benchmark_scripts` are used for additional infrastructure benchmarks
