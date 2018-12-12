import pytest
import subprocess
import hashlib
import os
import shutil
from os import path as op
import nipype_inc as ca
import nibabel as nib
import numpy as np


chunk  = op.abspath('sample_data/dummy_1.nii')
chunks = [chunk, op.abspath('sample_data/dummy_3.nii')]

def test_increment():
    delay = 0
    benchmark = False
    cli = True
    im = ca.increment_chunk(chunk, delay, benchmark, cli=True)

    assert op.isfile(im)

    original = nib.load(chunk).get_data()
    incremented = nib.load(im).get_data()

    assert np.array_equal(incremented, original + 1)

def test_increment_wf():
    delay = 0
    benchmark = False
    benchmark_dir = None
    cli = True
    wf_name = 'test_incwf'
    avg = None
    work_dir = 'test_incwf_work'
    inc_chunks = ca.increment_wf([chunks], delay, benchmark, benchmark_dir,
                                  cli, wf_name, avg, work_dir)
    for im in inc_chunks:
        assert op.isfile(im)

def test_compute_avg():
    benchmark = False
    benchmark_dir = None
    avg = ca.compute_avg([chunks], benchmark, benchmark_dir)

    images = [i for c in [chunks] for i in c]

    data = None
    print(images)

    for im in images:
        if data is None:
            data = nib.load(im).get_data()
        else:
            data += nib.load(im).get_data().astype(data.dtype, copy=False)

    data = data / len(images)

    assert op.isfile(avg)
    assert np.array_equal(nib.load(avg).get_data(), data)

def test_compute_avg_wf():
    from nipype import Workflow

    nnodes = 1
    work_dir = 'test_ca_wf_work'
    chunk = chunks
    delay = 0
    benchmark_dir = None
    benchmark = False
    cli = True

    wf = Workflow('test_ca_wf')
    wf.base_dir = work_dir

    inc_1, ca_1 = ca.computed_avg_node('ca_bb',
                                       nnodes, work_dir,
                                       chunk=chunk,
                                       delay=delay,
                                       benchmark_dir=benchmark_dir,
                                       benchmark=benchmark,
                                       cli=cli)
        
    wf.add_nodes([inc_1])

    wf.connect([(inc_1, ca_1, [('inc_chunk', 'chunks')])])
    nodename = 'inc_2_test'
    inc_2, ca_2 = ca.computed_avg_node(nodename, nnodes, work_dir, delay=delay,
                                 benchmark_dir=benchmark_dir,
                                 benchmark=benchmark, cli=cli)

    wf.connect([(ca_1, inc_2, [('avg_chunk', 'avg')])])
    wf.connect([(inc_1, inc_2, [('inc_chunk', 'chunk')])])
    wf_out = wf.run('SLURM',
                    plugin_args={
                      'template': 'benchmark_scripts/nipype_kmeans_template.sh'
                    })

    node_names = [i.name for i in wf_out.nodes()]
    result_dict = dict(zip(node_names, wf_out.nodes()))
    saved_chunks = (result_dict['ca1_{0}'.format(nodename)]
                                 .result
                                 .outputs
                                 .inc_chunk)

    results = [i for c in saved_chunks for i in c]
    
    im_1 = nib.load(chunks[0]).get_data()
    im_3 = nib.load(chunks[1]).get_data()

    avg = ((im_1 + 1) + (im_3 + 1)) / 2

    im_1 = im_1 + 1 + avg + 1
    im_3 = im_3 + 1 + avg + 1

    for i in results:
        assert op.isfile(i)
        if '1' in i:
            assert np.array_equal(nib.load(i).get_data(), im_1)
        else:
            assert np.array_equal(nib.load(i).get_data(), im_3)


'''
def test_benchmark_nipype():

    shutil.rmtree('npinc_out')
    p = subprocess.Popen(['python', 'pipelines/nipype_inc.py', 'sample_data',
                          'npinc_out', '1', '--benchmark'],
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    (out, err) = p.communicate()

    out = [fn for fn in os.listdir('npinc_out') if fn.startswith('benchmark')]
    assert len(out) == 1'''
