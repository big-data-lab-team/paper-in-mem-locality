from nipype import Workflow, MapNode, Node, Function
from io import BytesIO
import nibabel as nib
import numpy as np
import argparse, os
import glob

def main():
    parser = argparse.ArgumentParser(description="BigBrain binarization")

    parser.add_argument('bb_dir',type=str, help='The folder containing BigBrain NIfTI images (local fs only)')
    parser.add_argument('output_dir', type=str, help='the folder to save binarized images to (local fs only)')
    parser.add_argument('threshold', type=int, help='binarization threshold')

    args = parser.parse_args()

    wf = Workflow('bin_bb')
    wf.base_dir = os.getcwd()

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # get all files in directory
    bb_files = glob.glob(args.bb_dir)

    #loaded_data = MapNode(Function(input_names=))
    binarized_1 = MapNode(Function(input_names=['chunk', 'threshold', 'output_dir'],
                                 output_names=['bin_chunk'],
                                 function=binarize_chunk),
                                 iterfield=['chunk'],
                                 name='binarize_bb')


    binarized_1.inputs.chunk = bb_files
    binarized_1.inputs.threshold = args.threshold
    binarized_1.inputs.output_dir = output_dir
    wf.add_nodes([binarized_1])

    for i in range(9):
        binarized_2 = MapNode(Function(input_names=['chunk', 'threshold', 'output_dir'],
                                     output_names=['bin_chunk'],
                                     function=binarize_chunk),
                                     iterfield=['chunk'],
                                     name='binarize_bb')

        binarized_2.inputs.threshold = args.threshold
        binarized_2.inputs.output_dir = output_dir

        wf.connect(binarized_1, binarized_2, [(bin_chunk, chunk)])

        binarized_1 = binarized_2

    wf.run(plugin='MultiProc')

def binarize_data(chunk, threshold, output_dir):

    im = nib.load(chunk)
    data = im.get_data()

    data = np.where(data > threshold, 1, 0)
    
    bin_im = nib.Nifti1Image(data, im.affine)

    bin_file = os.path.join(output_dir, 'bin-' + os.path.basename(chunk))
    nib.save(bin_im, bin_file)

    return bin_file

if __name__ == '__main__':
    main()
