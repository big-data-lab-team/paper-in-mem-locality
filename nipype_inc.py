#!usr/bin/env python3

from nipype import Workflow, MapNode, Node, Function
from time import time
import argparse
import os
import shutil
import glob
import uuid
import socket


def increment_chunk(chunk, delay, benchmark, output_dir=None,
                    benchmark_dir=None, final=True, it=0, cli=False):
    import nibabel as nib
    import numpy as np
    import os
    import socket
    import uuid
    import subprocess
    import shutil
    from time import time

    start_time = time()

    def write_bench(name, start_time, end_time, node, benchmark_dir, filename):

        benchmark_file = os.path.join(benchmark_dir,
                                      "bench-{}.txt".format(str(uuid.uuid1())))

        with open(benchmark_file, 'a+') as f:
            f.write('{0} {1} {2} {3} {4}\n'.format(name, start_time, end_time,
                                                   node, filename))

    inc_chunk = os.path.basename(chunk)

    if not cli:
        im = nib.load(chunk)
        data = im.get_data()

        data += 1

        inc_im = nib.Nifti1Image(data, im.affine)
        nib.save(inc_im, inc_chunk)

        inc_file = os.path.abspath(inc_chunk)

        if final:
            print("Saving final results to output folder: "
                  "{}".format(output_dir))
            inc_out = os.path.join(output_dir,
                                   'inc{}-{}'.format(it, inc_chunk))
            shutil.copy(inc_file, inc_out)

    else:
        program = 'increment.py'

        p = subprocess.Popen([program, chunk, os.getcwd(),
                              '--delay', str(delay)],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        if 'inc' not in inc_chunk:
            inc_chunk = 'inc-{}'.format(inc_chunk)
        inc_file = os.path.join(os.getcwd(), inc_chunk)
        print(out, err)
        print(final)
        if final:
            print("Saving final results to output folder***: "
                  "{}".format(output_dir))
            inc_out = os.path.join(output_dir, inc_chunk)
            shutil.copy(inc_file, inc_out)
            print(out, err)
            inc_file = inc_out

    end_time = time()

    if benchmark:
        write_bench('inc_chunk', start_time, end_time, socket.gethostname(),
                    benchmark_dir, os.path.basename(chunk))

    return inc_file


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
    parser.add_argument('--delay', type=int, default=0,
                        help='task duration time (in s)')
    parser.add_argument('--benchmark', action='store_true',
                        help='benchmark pipeline')

    args = parser.parse_args()

    start = time()
    wf = Workflow('nipinc_bb')

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

    assert args.iterations > 0

    inc_1 = MapNode(Function(input_names=['chunk', 'delay',
                                          'benchmark',
                                          'output_dir', 'benchmark_dir',
                                          'final', 'it', 'cli'],
                             output_names=['inc_chunk'],
                             function=increment_chunk),
                    iterfield=['chunk'],
                    name='inc_bb')

    inc_1.inputs.chunk = bb_files
    inc_1.inputs.delay = args.delay
    inc_1.inputs.output_dir = output_dir
    inc_1.inputs.benchmark_dir = benchmark_dir
    inc_1.inputs.benchmark = args.benchmark
    inc_1.inputs.cli = args.cli

    if args.iterations == 1:
        inc_1.inputs.final = True
    else:
        inc_1.inputs.final = False

    inc_1.inputs.it = 1
    wf.add_nodes([inc_1])

    for i in range(0, args.iterations - 1):
        node_name = 'inc_bb{}'.format(i+1)
        inc_2 = MapNode(Function(input_names=['chunk', 'delay',
                                              'benchmark',
                                              'output_dir', 'benchmark_dir',
                                              'final', 'it', 'cli'],
                                 output_names=['inc_chunk'],
                                 function=increment_chunk),
                        iterfield=['chunk'],
                        name=node_name)

        inc_2.inputs.delay = args.delay
        inc_2.inputs.output_dir = output_dir
        inc_2.inputs.benchmark_dir = benchmark_dir
        inc_2.inputs.benchmark = args.benchmark
        inc_2.inputs.it = i + 2
        inc_2.inputs.cli = args.cli

        if i + 1 == args.iterations - 1:
            inc_2.inputs.final = True
        else:
            inc_2.inputs.final = False

        wf.connect([(inc_1, inc_2, [('inc_chunk', 'chunk')])])

        inc_1 = inc_2

    wf.run(plugin='MultiProc')

    end = time()

    if args.benchmark:
        fname = 'benchmark-{}.txt'.format(app_uuid)
        benchmark_file = os.path.abspath(os.path.join(args.output_dir, fname))
        print(benchmark_file)

        with open(benchmark_file, 'a+') as bench:
            bench.write('{0} {1} {2} {3} {4}\n'.format('driver_program', start,
                                                       end,
                                                       socket.gethostname(),
                                                       'allfiles'))

            for b in os.listdir(benchmark_dir):
                with open(os.path.join(benchmark_dir, b), 'r') as f:
                    bench.write(f.read())

        shutil.rmtree(benchmark_dir)


if __name__ == '__main__':
    main()
