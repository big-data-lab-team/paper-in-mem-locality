from nipype import Workflow, MapNode, Node, Function
from time import time
import argparse, os
import glob


def binarize_chunk(chunk, threshold, benchmark, start, output_dir=None):
    import nibabel as nib
    import numpy as np
    import os, socket, uuid
    from time import time

    start_time = time() - start

    def write_bench(name, start_time, end_time, node, output_dir, filename):
        
        benchmark_dir = os.path.join(output_dir, 'benchmarks')
        os.makedirs(benchmark_dir, exist_ok=True)

        benchmark_file = os.path.join(benchmark_dir, "bench-{}.txt".format(str(uuid.uuid1())))
        
        with open(benchmark_file, 'a+') as f:
            f.write('{0} {1} {2} {3} {4}\n'.format(name, start_time, end_time, node, filename))

    im = nib.load(chunk)
    data = im.get_data()

    # to avoid returning a blank image
    if data.max() == 1:
        threshold = 0

    data = np.where(data > threshold, 1, 0)
    
    bin_im = nib.Nifti1Image(data, im.affine)

    bin_file = os.path.join(output_dir, os.path.basename(chunk)) 
    nib.save(bin_im, bin_file)

    end_time = time() - start

    if benchmark:
        write_bench('binarize_chunk', start_time, end_time, socket.gethostname(), 
                output_dir, os.path.basename(chunk))

    return os.path.abspath(bin_file)

def main():
    parser = argparse.ArgumentParser(description="BigBrain binarization")

    parser.add_argument('bb_dir',type=str, help='The folder containing BigBrain NIfTI images (local fs only)')
    parser.add_argument('output_dir', type=str, help='the folder to save binarized images to (local fs only)')
    parser.add_argument('threshold', type=int, help='binarization threshold')
    parser.add_argument('iterations', type=int, help='number of iterations')
    parser.add_argument('--benchmark', action='store_true', help='benchmark pipeline')

    args = parser.parse_args()

    start = time()
    wf = Workflow('bin_bb')
    wf.base_dir = os.path.dirname(args.output_dir)

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # get all files in directory
    bb_files = glob.glob(os.path.join(os.path.abspath(args.bb_dir), '*'))

    #loaded_data = MapNode(Function(input_names=))
    binarized_1 = MapNode(Function(input_names=['chunk', 'threshold', 
                                                'benchmark', 'start', 'output_dir'],
                                 output_names=['bin_chunk'],
                                 function=binarize_chunk),
                                 iterfield=['chunk'],
                                 name='binarize_bb')


    binarized_1.inputs.chunk = bb_files
    binarized_1.inputs.threshold = args.threshold
    binarized_1.inputs.output_dir = output_dir
    binarized_1.inputs.benchmark = args.benchmark
    binarized_1.inputs.start = start
    wf.add_nodes([binarized_1])

    for i in range(args.iterations - 1):
        node_name = 'binarize_bb{}'.format(i+1)
        binarized_2 = MapNode(Function(input_names=['chunk', 'threshold', 
                                                    'benchmark', 'start', 'output_dir'],
                                     output_names=['bin_chunk'],
                                     function=binarize_chunk),
                                     iterfield=['chunk'],
                                     name=node_name)

        binarized_2.inputs.threshold = args.threshold
        binarized_2.inputs.output_dir = output_dir
        binarized_2.inputs.benchmark = args.benchmark
        binarized_2.inputs.start = start

        wf.connect([(binarized_1, binarized_2, [('bin_chunk', 'chunk')])])

        binarized_1 = binarized_2

    wf.run(plugin='SLURM')

if __name__ == '__main__':
    main()
