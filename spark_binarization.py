from pyspark import SparkContext, SparkConf
from io import BytesIO
from time import time
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


def binarize_data(filename, data, metadata, threshold,
                  benchmark, start, output_dir):

    start_time = time() - start

    # in order to keep original binarization instead
    # of returning a blank image
    if data.max() == 1:
        threshold = 0

    data = np.where(data > threshold, 1, 0)

    end_time = time() - start

    bn = os.path.basename(filename)
    if benchmark:
        write_bench('binarize_data', start_time, end_time,
                    socket.gethostname(), output_dir, bn)

    return (filename, data, metadata)


def save_binarized(filename, data, metadata, benchmark, start, output_dir):

    start_time = time() - start

    bn = os.path.basename(filename)
    im = nib.Nifti1Image(data, metadata[0], header=metadata[1])
    out_fn = os.path.join(output_dir, 'bin-' + bn)
    nib.save(im, out_fn)

    end_time = time() - start

    if benchmark:
        write_bench('save_binarized', start_time, end_time,
                    socket.gethostname(), output_dir, bn)

    return (out_fn, 'SUCCESS')


def main():

    start = time()

    parser = argparse.ArgumentParser(description="BigBrain binarization")
    parser.add_argument('bb_dir', type=str,
                        help=('The folder containing BigBrain NIfTI images'
                        '(local fs only)'))
    parser.add_argument('output_dir', type=str,
                        help=('the folder to save binarized images to '
                        '(local fs only)'))
    parser.add_argument('threshold', type=int, help='binarization threshold')
    parser.add_argument('iterations', type=int, help='number of iterations')
    parser.add_argument('--benchmark', action='store_true',
                        help='benchmark results')

    args = parser.parse_args()

    conf = SparkConf().setAppName("Spark binarization")
    sc = SparkContext.getOrCreate(conf=conf)

    threshold = args.threshold
    os.makedirs(args.output_dir, exist_ok=True)

    # read binary data stored in folder and create an RDD from it
    imRDD = sc.binaryFiles('file://' + os.path.abspath(args.bb_dir)) \
              .map(lambda x: read_img(x[0], x[1],
                                      args.benchmark,
                                      start, args.output_dir))

    for i in range(args.iterations):
        imRDD = imRDD.map(lambda x: binarize_data(x[0], x[1], x[2], threshold,
                                                  args.benchmark, start,
                                                  args.output_dir))

    imRDD.map(lambda x: save_binarized(x[0], x[1], x[2],
                                       args.benchmark, start,
                                       args.output_dir)) \
         .collect()


if __name__ == '__main__':
    main()
