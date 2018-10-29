from nipype import Workflow, MapNode, Node, Function
from time import time
import argparse
import os
import shutil
import glob
import uuid


def increment_chunk(chunk, delay, benchmark, start, output_dir=None,
                    benchmark_dir=None):
    import nibabel as nib
    import numpy as np
    import os
    import socket
    import uuid
    from time import time

    start_time = time() - start

    def write_bench(name, start_time, end_time, node, benchmark_dir, filename):

        benchmark_file = os.path.join(benchmark_dir,
                                      "bench-{}.txt".format(str(uuid.uuid1())))

        with open(benchmark_file, 'a+') as f:
            f.write('{0} {1} {2} {3} {4}\n'.format(name, start_time, end_time,
                                                   node, filename))

    im = nib.load(chunk)
    data = im.get_data()

    data += 1

    inc_im = nib.Nifti1Image(data, im.affine)

    inc_file = os.path.join(output_dir, os.path.basename(chunk))
    nib.save(inc_im, inc_file)

    end_time = time() - start

    if benchmark:
        write_bench('inc_chunk', start_time, end_time, socket.gethostname(),
                    benchmark_dir, os.path.basename(chunk))

    return os.path.abspath(inc_file)


def main():
    parser = argparse.ArgumentParser(description="BigBrain "
                                     "nipype incrementation")
    parser.add_argument('bb_dir', type=str,
                        help='The folder containing BigBrain NIfTI images '
                             '(local fs only)')
    parser.add_argument('output_dir', type=str,
                        help='the folder to save incremented images to '
                             '(local fs only)')
    parser.add_argument('iterations', type=int, help='number of iterations')
    parser.add_argument('--delay', type=int, default=0,
                        help='task duration time (in s)')
    parser.add_argument('--benchmark', action='store_true',
                        help='benchmark pipeline')

    args = parser.parse_args()

    start = time()
    wf = Workflow('nipinc_bb')
    wf.base_dir = os.path.dirname(args.output_dir)

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    benchmark_dir = None
    app_uuid = str(uuid.uuid1())

    if args.benchmark:
        benchmark_dir = os.path.join(args.output_dir,
                                     'benchmarks-{}'.format(app_uuid))
        os.makedir(benchmark_dir, exist_ok=True)

    # get all files in directory
    bb_files = glob.glob(os.path.join(os.path.abspath(args.bb_dir), '*'))

    assert args.iterations > 0

    inc_1 = MapNode(Function(input_names=['chunk', 'delay',
                                          'benchmark', 'start',
                                          'output_dir', 'benchmark_dir'],
                             output_names=['inc_chunk'],
                             function=increment_chunk),
                    iterfield=['chunk'],
                    name='inc_bb')

    inc_1.inputs.chunk = bb_files
    inc_1.inputs.delay = args.delay
    inc_1.inputs.output_dir = output_dir
    inc_1.inputs.benchmark_dir = benchmark_dir
    inc_1.inputs.benchmark = args.benchmark
    inc_1.inputs.start = start
    wf.add_nodes([inc_1])

    for i in range(args.iterations - 1):
        node_name = 'inc_bb{}'.format(i+1)
        inc_2 = MapNode(Function(input_names=['chunk', 'delay',
                                              'benchmark', 'start',
                                              'output_dir'],
                                 output_names=['inc_chunk'],
                                 function=increment_chunk),
                        iterfield=['chunk'],
                        name=node_name)

        inc_2.inputs.delay = args.delay
        inc_2.inputs.output_dir = output_dir
        inc_2.inputs.benchmark_dir = benchmark_dir
        inc_2.inputs.benchmark = args.benchmark
        inc_2.inputs.start = start

        wf.connect([(inc_1, inc_2, [('inc_chunk', 'chunk')])])

        inc_1 = inc_2

    wf.run(plugin='SLURM')

    end = time() - start

    if args.benchmark:
        fname = 'benchmark-{}.txt'.format(app_uuid)
        benchmark_file = os.path.join(args.output_dir, fname)

        with open(benchmark_file, 'a+') as bench:
            bench.write('{0} {1} {2} {3} {4}\n'.format('driver program', start,
                                                       end,
                                                       socket.gethostname(),
                                                       'allfiles'))

            for b in os.listdir(benchmark_file):
                with open(os.path.join(benchmark_dir, b), 'r') as f:
                    bench.write(f.read())

        shutil.rmtree(benchmark_dir)


if __name__ == '__main__':
    main()
