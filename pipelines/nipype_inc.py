#!/usr/bin/env python

from nipype import Workflow, MapNode, Node, Function
from time import time
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


def write_bench(name, start_time, end_time, node, benchmark_dir, filename,
                executor):
    """Write benchmarks to a file.

    Keyword arguments:
    name -- benchmarked task's name
    start_time -- task execution start time (seconds)
    end_time -- task execution end time (seconds)
    node -- the hostname id of the host that executed the task
    benchmark_dir -- the directory to save the benchmark file to
    filename -- the file that was processed by the task
    executor -- the process id that executed the task
    """
    benchmark_file = op.join(benchmark_dir,
                             "bench-{}.txt".format(str(uuid.uuid1())))

    with open(benchmark_file, 'a+') as f:
        f.write('{0} {1} {2} {3} {4} {5}\n'.format(name, start_time,
                                                   end_time,
                                                   node, filename,
                                                   executor))


def increment_chunk(chunk, delay, benchmark_dir=None, cli=False):
    """Increment neuroimaging data by 1.

    Keyword arguments:
    chunk -- image to increment
    delay -- task duration (sleep time) in seconds
    benchmark_dir -- directory to save benchmarks (default None)
    cli -- increment data using a command-line tool (default False)

    Returns: incremented image filename
    """
    import nibabel as nib
    import os
    import socket
    import subprocess
    import uuid
    from time import time
    from nipype_inc import write_bench
    try:
        from threading import get_ident
    except Exception as e:
        from thread import get_ident

    start_time = time()

    inc_chunk = os.path.basename(chunk)

    # process data directly in code
    if not cli:
        im = nib.load(chunk)
        data = im.get_data()

        data += 1

        inc_im = nib.Nifti1Image(data, im.affine)
        nib.save(inc_im, inc_chunk)

        inc_file = os.path.abspath(inc_chunk)

    # use cli to process data
    else:
        program = 'increment.py'

        if benchmark_dir is not None:
            benchmark_file = os.path.join(benchmark_dir,
                                          "bench-{}.txt".format(
                                              str(uuid.uuid1())))
            p = subprocess.Popen([program, chunk, os.getcwd(),
                                  '--benchmark_file', benchmark_file,
                                  '--delay', str(delay)],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen([program, chunk, os.getcwd(),
                                  '--delay', str(delay)],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)

        (out, err) = p.communicate()
        print(out)
        print(err)
        if 'inc-' not in inc_chunk:
            inc_chunk = 'inc-{}'.format(inc_chunk)
        inc_file = os.path.join(os.getcwd(), inc_chunk)

    end_time = time()

    if benchmark_dir is not None:
        write_bench('inc_chunk', start_time, end_time, socket.gethostname(),
                    benchmark_dir, os.path.basename(chunk), get_ident())

    return inc_file


def save_results(input_img, output_dir, it=0, benchmark_dir=None):
    """Copy results from Nipype node's working directory to user-specified
    output directory.

    Keyword arguments:
    input_img -- the filepath of the image to be copied
    output_dir -- output directory path
    it -- the total number of incrementation iterations performed (default 0)
    benchmark_dir -- the benchmark directory to save benchmarks to

    Returns: output filepath
    """
    import shutil
    from os import path as op
    from time import time
    import uuid
    import socket
    from nipype_inc import write_bench
    try:
        from threading import get_ident
    except Exception as e:
        from thread import get_ident

    start = time()

    in_fn = op.basename(input_img).replace('inc-', '')

    # Append number of iterations to output filename
    outimg_name = "inc{}-{}".format(it, in_fn)
    output_file = op.join(output_dir, outimg_name)

    shutil.copy(input_img, output_file)

    end = time()

    if benchmark_dir is not None:
        write_bench('save_results', start, end, socket.gethostname(),
                    benchmark_dir, outimg_name, get_ident())

    return output_file


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

    bb_dir = os.path.abspath(args.bb_dir)

    if os.path.isdir(bb_dir):
        # get all files in directory
        bb_files = glob.glob(os.path.join(os.path.abspath(args.bb_dir), '*'))
    elif os.path.isfile(bb_dir):
        bb_files = [bb_dir]
    else:
        bb_files = bb_dir.split(',')

    assert args.iterations > 0, 'Number of iterations too small. Terminating'

    count = 0
    for chunk in bb_files:
        inc_1 = Node(Function(input_names=['chunk', 'delay',
                                           'benchmark_dir', 'cli'],
                              output_names=['inc_chunk'],
                              function=increment_chunk),
                     name='inc_bb{}'.format(count))

        inc_1.inputs.chunk = chunk
        inc_1.inputs.delay = args.delay
        inc_1.inputs.benchmark_dir = benchmark_dir
        inc_1.inputs.cli = args.cli

        # Add first incrementation node to workflow
        wf.add_nodes([inc_1])

        inc_2 = None

        for i in range(0, args.iterations - 1):
            node_name = 'inc_bb{0}_{1}'.format(count, i+1)
            inc_2 = Node(Function(input_names=['chunk', 'delay',
                                               'benchmark_dir', 'cli'],
                                  output_names=['inc_chunk'],
                                  function=increment_chunk),
                         name=node_name)

            inc_2.inputs.delay = args.delay
            inc_2.inputs.benchmark_dir = benchmark_dir
            inc_2.inputs.cli = args.cli

            # Connect previous iteration's output images to current iteration's
            # input images
            wf.connect([(inc_1, inc_2, [('inc_chunk', 'chunk')])])

            inc_1 = inc_2

        # One iterations are complete, copy final outputs to output folder
        save_res = Node(Function(input_names=['input_img', 'output_dir', 'it',
                                              'benchmark_dir'],
                                 output_names=['output_filename'],
                                 function=save_results),
                        name='save_res{}'.format(count))

        save_res.inputs.output_dir = output_dir
        save_res.inputs.it = args.iterations
        save_res.inputs.benchmark_dir = benchmark_dir

        if inc_2 is None:
            wf.connect([(inc_1, save_res, [('inc_chunk', 'input_img')])])
        else:
            wf.connect([(inc_2, save_res, [('inc_chunk', 'input_img')])])

        count += 1

    if args.plugin_args is not None:
        with open(args.plugin_args, 'r') as f:
            pargs = json.load(f)
            wf.run(plugin=args.plugin, plugin_args=pargs)
    else:
        wf.run(plugin=args.plugin)

    wf.write_graph(graph2use='colored')

    end = time()

    # Clean up benchmarking folder by merging all benchmarks into a single
    # file
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
