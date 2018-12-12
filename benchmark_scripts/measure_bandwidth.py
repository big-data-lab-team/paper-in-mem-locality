#!/usr/bin/env python
import nibabel as nib
from random import randint, shuffle
import sys
from os import listdir, makedirs
from os.path import abspath, join, basename
from json import dump
from time import time
from numpy import eye
from shutil import rmtree


def main():

    assert len(sys.argv) == 7, ('data_dir, output_dir, min_num_splits, '
                                 'max_num_splits, step, bench')

    data_dir = sys.argv[1]
    output_dir = sys.argv[2]
    min_num_splits = int(sys.argv[3])
    max_num_splits = int(sys.argv[4])
    step = int(sys.argv[5])
    bench = sys.argv[6]

    
    data_dict = {}
    bench_dict = {}

    bb_files = [abspath(join('data/125_splits', i)) for i in listdir(data_dir)]
    num_chunks = len(bb_files)

    order = [i for i in range(min_num_splits, max_num_splits, step)]
    shuffle(order)

    for chunk in bb_files:
        data_dict[chunk] = nib.load(chunk).get_data()

    for i in order:

        try:
            makedirs(output_dir)
        except Exception as e:
            pass

        write = 0
        print('writing data...', i)

        for j in range(0, i):
            print(j)
            pos = j % num_chunks
            data_dict[bb_files[pos]] = data_dict[bb_files[pos]] + randint(1, 50)
            im = nib.Nifti1Image(data_dict[bb_files[pos]], eye(4))
            start = time()

            nib.save(im, join(output_dir, 'bb-{}.nii'.format(j % 54)))

            write += (time() - start)

        print('Num chunks: ', i, ' Write: ', write)
        bench_dict[i] = write
        rmtree(output_dir)
    
    with open(bench, 'w') as f:
        dump(bench_dict, f)


if __name__=='__main__':
    main()
