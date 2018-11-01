#!/usr/bin/env python

import argparse
import nibabel as nib
from os import path as op, makedirs as md
import time


def increment(fn, outdir, delay):
    print('Incrementing image: ', fn)
    im = nib.load(fn)

    inc_data = im.get_data() + 1

    im = nib.Nifti1Image(inc_data, affine=im.affine, header=im.header)
    
    out_fn = ('inc-{}'.format(op.basename(fn))
              if 'inc' not in op.basename(fn)
              else op.basename(fn))

    out_fn = op.join(outdir, out_fn)

    nib.save(im, out_fn)

    time.sleep(delay)
    print('Saved image to: ', out_fn)

def main():

    print('Incrementation CLI started')
    parser = argparse.ArgumentParser(description="BigBrain incrementation")
    parser.add_argument('filename', type=str,
                        help=('the file to be incremented'))
    parser.add_argument('output_dir', type=str,
                        help='the output directory')
    parser.add_argument('--delay', type=int, default=0,
                        help='task duration time (in s)')

    args = parser.parse_args()

    md(args.output_dir, exist_ok=True)
    increment(args.filename, args.output_dir, args.delay)


if __name__ == '__main__':
    main()
