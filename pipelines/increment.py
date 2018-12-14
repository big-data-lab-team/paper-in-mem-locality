#!/usr/bin/env python

import argparse
import nibabel as nib
from os import path as op, makedirs as md
import time
import subprocess
import socket
try:
    from threading import get_ident
except Exception as e:
    from thread import get_ident


def increment(fn, outdir, delay, benchmark_file, start_time):
    """Increment image data by 1

    Keyword arguments:
    fn -- filename containing image to increment
    outdir -- output directory to save data to
    delay -- task duration (sleep time) in seconds
    benchmark_file -- file to save benchmarks to
    start_time -- application driver start time
    """
    print('Incrementing image: ', fn)

    start = time.time() - start_time
    im = nib.load(fn)
    end = time.time() - start_time
    print("read time", end)

    if benchmark_file is not None:
        write_bench(benchmark_file, "read_file", start, end,
                    socket.gethostname(), op.basename(fn), get_ident())

    inc_data = im.get_data() + 1

    im = nib.Nifti1Image(inc_data, affine=im.affine, header=im.header)

    out_fn = ('inc-{}'.format(op.basename(fn))
              if 'inc' not in op.basename(fn)
              else op.basename(fn))

    out_fn = op.join(outdir, out_fn)

    start = time.time() - start_time
    nib.save(im, out_fn)
    end = time.time() - start_time
    print("write time", time.time() - start)

    if benchmark_file is not None:
        write_bench(benchmark_file, "write_file", start, end,
                    socket.gethostname(), op.basename(out_fn), get_ident())

    time.sleep(delay)
    print('Saved image to: ', out_fn)


def write_bench(benchmark_file, name, start_time, end_time, node, filename,
                executor):

    with open(benchmark_file, 'a+') as f:
        f.write('{0} {1} {2} {3} {4} {5}\n'.format(name, start_time, end_time,
                                                   node, filename, executor))


def main():

    print('Incrementation CLI started')
    parser = argparse.ArgumentParser(description="BigBrain incrementation")
    parser.add_argument('filename', type=str,
                        help=('the file to be incremented'))
    parser.add_argument('output_dir', type=str,
                        help='the output directory')
    parser.add_argument('--benchmark_file', type=str, default=None,
                        help='the file to write benchmarks to')
    parser.add_argument('--start', type=float, default=0,
                        help='start time of the application')
    parser.add_argument('--delay', type=float, default=0,
                        help='task duration time (in s)')

    args = parser.parse_args()

    try:
        md(args.output_dir)
    except Exception as e:
        pass

    increment(args.filename, args.output_dir, args.delay,
              args.benchmark_file, args.delay)


if __name__ == '__main__':
    main()
