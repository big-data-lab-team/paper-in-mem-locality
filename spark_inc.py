from pyspark import SparkContext, SparkConf
from io import BytesIO
from time import sleep, time
import os
import socket
import uuid
import numpy as np
import nibabel as nib
import argparse


def write_bench(name, start_time, end_time, node, output_dir, filename):

    benchmark_dir = os.path.join(output_dir, 'benchmarks')
    os.makedirs(benchmark_dir, exist_ok=True)

    benchmark_file = os.path.join(
            benchmark_dir,
            "bench-{}.txt".format(str(uuid.uuid1()))
            )

    with open(benchmark_file, 'a+') as f:
        f.write('{0} {1} {2} {3} {4}\n'.format(name, start_time, end_time,
                                               node, filename))


def read_img(filename, data, benchmark, start, output_dir):

    start_time = time() - start

    # load binary data into Nibabel
    fh = nib.FileHolder(fileobj=BytesIO(data))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    data = im.get_data()

    end_time = time() - start

    bn = os.path.basename(filename)

    if benchmark:
        write_bench('read_img', start_time, end_time,
                    socket.gethostname(), output_dir, bn)

    return (filename, data, (im.affine, im.header))


def increment_data(filename, data, metadata, delay,
                   benchmark, start, output_dir):

    start_time = time() - start

    data += 1
    sleep(delay)

    end_time = time() - start

    if benchmark:
        bn = os.path.basename(filename)
        write_bench('increment_data', start_time, end_time,
                    socket.gethostname(), output_dir, tn)

    return (filename, data, metadata)


def save_incremented(filename, data, metadata, benchmark, start, output_dir):

    start_time = time() - start

    bn = os.path.basename(filename)
    im = nib.Nifti1Image(data, metadata[0], header=metadata[1])
    out_fn = os.path.join(output_dir, 'inc-' + bn)
    nib.save(im, out_fn)

    end_time = time() - start

    if benchmark:
        write_bench('save_incremented', start_time, end_time,
                    socket.gethostname(), output_dir, bn)

    return (out_fn, 'SUCCESS')


def main():

    start = time()

    parser = argparse.ArgumentParser(description="BigBrain incrementation")
    parser.add_argument('bb_dir', type=str,
                        help=('The folder containing BigBrain NIfTI images'
                              '(local fs only)'))
    parser.add_argument('output_dir', type=str,
                        help=('the folder to save incremented images to'
                              '(local fs only)'))
    parser.add_argument('iterations', type=int, help='number of iterations')
    parser.add_argument('--delay', type=int, default=0,
                        help='task duration time (in s)')
    parser.add_argument('--benchmark', action='store_true',
                        help='benchmark results')

    args = parser.parse_args()

    conf = SparkConf().setAppName("Spark BigBrain incrementation")
    sc = SparkContext.getOrCreate(conf=conf)

    delay = args.delay
    os.makedirs(args.output_dir, exist_ok=True)

    # read binary data stored in folder and create an RDD from it
    imRDD = sc.binaryFiles('file://' + os.path.abspath(args.bb_dir)) \
              .map(lambda x: read_img(x[0], x[1],
                                      args.benchmark,
                                      start, args.output_dir))

    for i in range(args.iterations):
        imRDD = imRDD.map(lambda x: increment_data(x[0], x[1], x[2], delay,
                                                   args.benchmark, start,
                                                   args.output_dir))

    imRDD.map(lambda x: save_incremented(x[0], x[1], x[2],
                                        args.benchmark, start,
                                        args.output_dir)) \
         .collect()

    end = time() - start

    if args.benchmark:
        write_bench('driver program', start, end, socket.gethostname(),
                    args.output_dir, 'allfiles')


if __name__ == '__main__':
    main()
