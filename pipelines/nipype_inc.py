#!/usr/bin/env python

from nipype import Workflow, MapNode, Node, Function
from time import time
from math import ceil
import argparse
import os
import shutil
import glob
import uuid
import socket
import json
try:
    from threading import get_ident
except Exception as e:
    from thread import get_ident


def increment_chunk(chunk, delay, benchmark, benchmark_dir=None, cli=False,
                    avg=None):
    import nibabel as nib
    import os
    import socket
    import subprocess
    import uuid
    from time import time
    try:
        from threading import get_ident
    except Exception as e:
        from thread import get_ident

    def write_bench(name, start_time, end_time, node, benchmark_dir,
                    filename, executor):

        benchmark_file = os.path.join(benchmark_dir,
                                      "bench-{}.txt".format(str(uuid.uuid1())))

        with open(benchmark_file, 'a+') as f:
            f.write('{0} {1} {2} {3} {4} {5}\n'.format(name, start_time,
                                                       end_time, node,
                                                       filename, executor))

    start_time = time()

    inc_chunk = os.path.basename(chunk)

    if not cli:
        im = nib.load(chunk)
        data = im.get_data()

        data += 1

        if avg is not None:
            avg_chunk = nib.load(avg).get_data().astype(data.dtype, copy=False)

            data += avg_chunk

        inc_im = nib.Nifti1Image(data, im.affine)
        nib.save(inc_im, inc_chunk)

        inc_file = os.path.abspath(inc_chunk)

    else:
        program = 'increment.py'

        cmd = [program, chunk, os.getcwd(), '--delay', str(delay)]
        if benchmark:
            benchmark_file = os.path.join(benchmark_dir,
                                          "bench-{}.txt".format(
                                              str(uuid.uuid1())))
            cmd.extend(['--benchmark', benchmark_file])

        if avg is not None:
            cmd.extend(['--avg', avg])

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        (out, err) = p.communicate()
        print(out)
        print(err)
        if 'inc-' not in inc_chunk:
            inc_chunk = 'inc-{}'.format(inc_chunk)
        inc_file = os.path.abspath(inc_chunk)

        if not os.path.isfile(inc_file):
            inc_file = err

    end_time = time()

    if benchmark and benchmark_dir is not None:
        write_bench('inc_chunk', start_time, end_time, socket.gethostname(),
                    benchmark_dir, os.path.basename(chunk), get_ident())

    return inc_file


def save_results(input_img, output_dir, it=0, benchmark=False,
                 benchmark_dir=None):
    import shutil
    from os import path as op
    from time import time
    import uuid
    import socket

    try:
        from threading import get_ident
    except Exception as e:
        from thread import get_ident

    def write_bench(name, start_time, end_time, node, benchmark_dir, filename,
                    executor):

        benchmark_file = op.join(benchmark_dir,
                                 "bench-{}.txt".format(str(uuid.uuid1())))

        with open(benchmark_file, 'a+') as f:
            f.write('{0} {1} {2} {3} {4} {5}\n'.format(name, start_time,
                                                       end_time,
                                                       node, filename,
                                                       executor))

    start = time()

    in_fn = op.basename(input_img).replace('inc-', '')

    outimg_name = "inc{}-{}".format(it, in_fn)
    output_file = op.join(output_dir, outimg_name)

    shutil.copy(input_img, output_file)

    end = time()

    if benchmark and benchmark_dir is not None:
        write_bench('save_results', start, end, socket.gethostname(),
                    benchmark_dir, outimg_name, get_ident())

    return output_file


def save_node(nname, input_img, output_dir, it, benchmark=False,
              benchmark_dir=None):
    from nipype import Node

    s_node = Node(Function(input_names=['input_img', 'output_dir', 'it',
                                       'benchmark', 'benchmark_dir'],
                           output_names=['output_filename'],
                           function=save_results),
                    name=nname)

    s_node.inputs.input_img = input_img
    s_node.inputs.output_dir = output_dir
    s_node.inputs.it = it
    s_node.inputs.benchmark = benchmark
    s_node.inputs.benchmark_dir = benchmark_dir

    return s_node

def increment_node(node_name, chunk=None, delay=0, benchmark_dir=None, 
                   benchmark=False, cli=False, avg=None):
    inc_node = Node(Function(input_names=['chunk', 'delay',
                                          'benchmark',
                                          'benchmark_dir', 'cli', 'avg'],
                             output_names=['inc_chunk'],
                             function=increment_chunk),
                    name=node_name)

    inc_node.inputs.chunk = chunk
    inc_node.inputs.delay = delay
    inc_node.inputs.benchmark_dir = benchmark_dir
    inc_node.inputs.benchmark = benchmark
    inc_node.inputs.cli = cli
    inc_node.inputs.avg = avg

    return inc_node

def computed_avg_node(node_name, nnodes, work_dir, 
                      chunk=None, delay=0,
                      benchmark_dir=None, benchmark=False, cli=False,
                      avg=None):
    files = get_partitions(chunk, nnodes)

    if delay is None:
        delay = 0
    
    ca_name = 'ca1_{0}'.format(node_name)
    ca2_name = 'ca2_{0}'.format(node_name)

    ca_1 = MapNode(Function(input_names=['chunk', 'delay', 'benchmark',
                                         'benchmark_dir', 'cli', 'wf_name',
                                         'avg', 'work_dir'], 
                            output_names=['inc_chunk'],
                            function=increment_wf),
                   name=ca_name,
                   iterfield='chunk')
    ca_1.inputs.chunk = files
    ca_1.inputs.delay = delay
    ca_1.inputs.benchmark = benchmark
    ca_1.inputs.benchmark_dir = benchmark_dir
    ca_1.inputs.cli = cli
    ca_1.inputs.wf_name = 'incwf_{}'.format(ca_name)
    ca_1.inputs.avg = avg
    ca_1.inputs.work_dir = work_dir

    ca_2 = Node(Function(input_names=['chunks', 'benchmark',
                                      'benchmark_dir'],
                         output_names=['avg_chunk'],
                         function=compute_avg),
                name=ca2_name)

    ca_2.inputs.benchmark = benchmark
    ca_2.inputs.benchmark_dir = benchmark_dir

    
    return ca_1, ca_2


def compute_avg(chunks, benchmark, benchmark_dir):
    from time import time
    import nibabel as nib
    from numpy import eye
    from os.path import abspath
   
    chunks = [fn for c in chunks for fn in c]
    print(chunks)
    avg = None
    num_chunks = len(chunks)

    for c in chunks:
        im_data = nib.load(c).get_data()
        
        if avg is None:
            avg = im_data
        else:
            avg += im_data.astype(avg.dtype, copy=False)

    avg /= num_chunks
    fn = 'avg.nii'
    avg_im = nib.Nifti1Image(avg, eye(4))
    nib.save(avg_im, fn)

    return abspath(fn)


def increment_wf(chunk, delay, benchmark, benchmark_dir,
                 cli, wf_name, avg, work_dir):
    from nipype import Workflow
    from nipype_inc import increment_node

    wf = Workflow(wf_name)
    wf.base_dir = work_dir
    idx = 0
    node_names = []

    if any(isinstance(i, list) for i in chunk):
        chunk = [i for c in chunk for i in c]

    print('chunks', chunk)
    for fn in chunk:
        inc1_nname = 'inc_wf_{}'.format(idx)
        inc_1 = increment_node(inc1_nname, fn, delay, benchmark_dir,
                               benchmark, cli, avg)
        wf.add_nodes([inc_1])

        node_names.append(inc1_nname)
        idx += 1

    wf_out = wf.run('MultiProc')
    node_names = [i.name for i in wf_out.nodes()]
    result_dict = dict(zip(node_names, wf_out.nodes()))
    inc_chunks = ([result_dict['inc_wf_{}'.format(i)]
                                 .result
                                 .outputs
                                 .inc_chunk for i in range(0, len(chunk))])
    return inc_chunks


def cluster_save(nname, chunks, output_dir, it, benchmark, benchmark_dir,
                 nnodes, work_dir):
    files = get_partitions(chunks, nnodes)
    sp_name = 'sp_{}'.format(nname)

    sp = MapNode(Function(input_names=['input_img', 'output_dir', 'it',
                              'benchmark', 'benchmark_dir', 'work_dir'],
                          output_names=['output_filename'],
                          function=save_wf),
                 name=sp_name,
                 iterfield='input_img')
    sp.inputs.input_img = files
    sp.inputs.output_dir = output_dir
    sp.inputs.it = it
    sp.inputs.benchmark = benchmark
    sp.inputs.benchmark_dir = benchmark_dir
    sp.inputs.work_dir = work_dir 

    return sp

def save_wf(input_img, output_dir, it, benchmark, benchmark_dir, work_dir):
    from nipype import Workflow
    from nipype_inc import save_node

    wf = Workflow('save_wf')
    wf.base_dir = work_dir
    idx = 0
    for im in input_img:
        sn_name = 'sn_{}'.format(idx)
        sn = save_node(sn_name, im, output_dir, it, benchmark, benchmark_dir)
        wf.add_nodes([sn])
        idx += 1

    wf_out = wf.run('MultiProc')

    node_names = [i.name for i in wf_out.nodes()]
    result_dict = dict(zip(node_names, wf_out.nodes()))
    saved_chunks = ([result_dict['sn_{}'.format(i)]
                                 .result
                                 .outputs
                                 .output_filename 
                     for i in range(0, len(input_img))])

    return saved_chunks


def get_partitions(chunks, nnodes):

    num_images = None
    pn_images = None
    pn_remain = None
    files = []

    print(chunks)
    if chunks is not None:
        num_images = len(chunks)
        pn_images = num_images / nnodes
        pn_remain = num_images % nnodes

        count_rem = 0
        idx = 0


        if pn_images == 0:
            pn_images = 1
            pn_remain = 0
        for i in range(0, num_images - pn_remain, pn_images):

            if count_rem < pn_remain:
                files.append(chunks[idx: idx+pn_images+1])
                count_rem += 1
                idx += pn_images + 1
            else:
                files.append(chunks[idx: idx+pn_images])
                idx += pn_images
    return files
    

def main():
    parser = argparse.ArgumentParser(description="BigBrain "
                                     "nipype incrementation")
    parser.add_argument('bb_dir', type=str,
                        help='The folder containing BigBrain NIfTI images '
                             '(local fs only) or image file')
    parser.add_argument('output_dir', type=str,
                        help='the folder to save incremented images to '
                             '(local fs only)')
    parser.add_argument('iterations', type=int, help='number of iterations')
    parser.add_argument('--cli', action='store_true',
                        help='use CLI application')
    parser.add_argument('--work_dir', type=str, help='working directory')
    parser.add_argument('--delay', type=float, default=0,
                        help='task duration time (in s)')
    parser.add_argument('--benchmark', action='store_true',
                        help='benchmark pipeline')
    parser.add_argument('--plugin', type=str, choices=['SLURM', 'MultiProc'],
                        default='MultiProc', help='Plugin to use')
    parser.add_argument('--plugin_args', type=str,
                        help='Plugin arguments file in dictionary format')
    parser.add_argument('--nnodes', type=int, help='Number of nodes available')

    args = parser.parse_args()
    start = time()
    wf = Workflow('npinc_bb')

    if args.work_dir is not None:
        wf.base_dir = os.path.abspath(args.work_dir)

    output_dir = os.path.abspath(args.output_dir)

    try:
        os.makedirs(output_dir)
    except Exception as e:
        pass

    benchmark_dir = None
    app_uuid = str(uuid.uuid1())

    if args.benchmark:
        benchmark_dir = os.path.abspath(os.path.join(args.output_dir,
                                                     'benchmarks-{}'.format(
                                                                    app_uuid)))
        try:
            os.makedirs(benchmark_dir)
        except Exception as e:
            pass

    #benchmark_dir = None
    #args.benchmark = False
    bb_dir = os.path.abspath(args.bb_dir)

    if os.path.isdir(bb_dir):
        # get all files in directory
        bb_files = glob.glob(os.path.join(os.path.abspath(args.bb_dir), '*'))
    elif os.path.isfile(bb_dir):
        bb_files = [bb_dir]
    else:
        bb_files = bb_dir.split(',')

    if args.plugin == 'SLURM':
        bb_files = [bb_files]

    assert args.iterations > 0

    count = 0
    for chunk in bb_files:
        ca = None
        if args.plugin == 'MultiProc':
            inc_1 = increment_node('inc_bb{}'.format(count), chunk, args.delay,
                                   benchmark_dir, args.benchmark, args.cli)
        else:
            inc_1, ca_1 = computed_avg_node('ca_bb{}'.format(count),
                                            args.nnodes, args.work_dir,
                                            chunk=chunk,
                                            delay=args.delay,
                                            benchmark_dir=benchmark_dir,
                                            benchmark=args.benchmark,
                                            cli=args.cli)
        
        wf.add_nodes([inc_1])

        if args.plugin == 'SLURM':
            wf.connect([(inc_1, ca_1, [('inc_chunk', 'chunks')])])


        inc_2 = None

        for i in range(0, args.iterations - 1):
            node_name = 'inc_bb{0}_{1}'.format(count, i+1)
            ca_2 = None
     
            if args.plugin == 'MultiProc':
                inc_2 = increment_node(node_name, delay=args.delay,
                                       benchmark_dir=benchmark_dir,
                                       benchmark=args.benchmark, cli=args.cli)

            else:
                inc_2, ca_2 = computed_avg_node(
                                        'ca_bb{0}_{1}'.format(count, i+1),
                                        args.nnodes,
                                        args.work_dir,
                                        delay=args.delay,
                                        benchmark_dir=benchmark_dir,
                                        benchmark=args.benchmark, cli=args.cli)

            wf.connect([(inc_1, inc_2, [('inc_chunk', 'chunk')])])
            
            if args.plugin == 'SLURM':
                wf.connect([(ca_1, inc_2, [('avg_chunk', 'avg')])])
                wf.connect([(inc_2, ca_2, [('inc_chunk', 'chunks')])])


            inc_1 = inc_2
            ca_1 = ca_2

        s_nname = 'save_res{}'.format(count)
        save_res = None
        if args.plugin == 'MultiProc':
            save_res = save_node(s_nname, None, output_dir, args.iterations,
                                 args.benchmark, benchmark_dir)
        else:
            save_res = cluster_save(s_nname, None, output_dir, args.iterations,
                                    args.benchmark, benchmark_dir,
                                    args.nnodes, args.work_dir)

        if inc_2 is None:
            wf.connect([(inc_1, save_res, [('inc_chunk', 'input_img')])])
        else:
            wf.connect([(inc_2, save_res, [('inc_chunk', 'input_img')])])

        count += 1

    if args.plugin_args is not None:
        wf.run(plugin=args.plugin, plugin_args={'template': args.plugin_args})
    else:
        wf.run(plugin=args.plugin)

    wf.write_graph(graph2use='colored')

    end = time()

    if args.benchmark:
        fname = 'benchmark-{}.txt'.format(app_uuid)
        benchmark_file = os.path.abspath(os.path.join(args.output_dir, fname))
        print(benchmark_file)

        with open(benchmark_file, 'a+') as bench:
            bench.write('{0} {1} {2} {3} {4} {5}\n'.format('driver_program',
                                                           start,
                                                           end,
                                                           socket
                                                           .gethostname(),
                                                           'allfiles',
                                                           get_ident()))

            for b in os.listdir(benchmark_dir):
                with open(os.path.join(benchmark_dir, b), 'r') as f:
                    bench.write(f.read())

        shutil.rmtree(benchmark_dir)


if __name__ == '__main__':
    main()
