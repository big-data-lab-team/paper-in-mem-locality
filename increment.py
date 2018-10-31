import argparse
import nibabel as nib
from os import path as op
import time


def increment(fn, outdir, delay):
    im = nib.load(fn)

    inc_data = im.get_data() + 1

    im = nib.Nifti1Image(inc_data, affine=im.affine, header=im.header)

    out_fn = op.join(outdir, 'inc-{}'.format(op.basename(fn)))

    nib.save(im, out_fn)

    time.sleep(delay)


def main():

    start = time()

    parser = argparse.ArgumentParser(description="BigBrain incrementation")
    parser.add_argument('filename', type=str,
                        help=('the file to be incremented'))
    parser.add_argument('output_dir', type=str,
                        help='the output directory')
    parser.add_argument('--delay', type=int, default=0,
                        help='task duration time (in s)')

    args = parser.parse_args()

    increment(args.filename, args.outdir, args.delay)


if __name__ == '__main__':
    main()
