#!/usr/bin/env python

import argparse
import nibabel as nib
from os import path as op, makedirs as md
import time
import subprocess


def increment(fn, outdir, delay):
    print('Incrementing image: ', fn)
    '''
    p = subprocess.Popen("iostat -y 1 1", shell=True, stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))
    '''
    start = time.time()
    im = nib.load(fn)
    print("read time", time.time() - start)
    '''p = subprocess.Popen("iostat -y 1 1", shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))'''
    inc_data = im.get_data() + 1

    im = nib.Nifti1Image(inc_data, affine=im.affine, header=im.header)

    '''p = subprocess.Popen("top -b -n 1 | head -n 10 | tail -n 2", shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))

    p = subprocess.Popen("lsof /local", shell=True, stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))

    p = subprocess.Popen("ps -ef", shell=True, stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))


    p = subprocess.Popen("free", stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))'''

    out_fn = ('inc-{}'.format(op.basename(fn))
              if 'inc' not in op.basename(fn)
              else op.basename(fn))

    out_fn = op.join(outdir, out_fn)

    '''p = subprocess.Popen("iostat -y 1 1", shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))'''

    start = time.time()
    nib.save(im, out_fn)
    print("write time", time.time() - start)

    '''p = subprocess.Popen("iostat -y 1 1", shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = p.communicate()

    print(out.encode("utf-8"))
    print(err.encode("utf-8"))'''
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

    try:
        md(args.output_dir)
    except Exception as e:
        pass

    increment(args.filename, args.output_dir, args.delay)


if __name__ == '__main__':
    main()
