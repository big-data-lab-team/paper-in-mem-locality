from pyspark import SparkContext, SparkConf
from io import BytesIO
from time import sleep, time
import os
import socket
import uuid
import shutil
import numpy as np
import nibabel as nib
import argparse


def write_bench(name, start_time, end_time, node, output_dir,
                filename, benchmark_dir=None, benchmark_file=None):

    if not benchmark_file:
        try:
            os.makedirs(benchmark_dir, exist_ok=True)
            benchmark_file = os.path.join(
                    benchmark_dir,
                    "bench-{}.txt".format(str(uuid.uuid1()))
                    )
        except:
            print('ERROR: benchmark_dir parameter has not been defined.')

    with open(benchmark_file, 'a+') as f:
        f.write('{0} {1} {2} {3} {4}\n'.format(name, start_time, end_time,
                                               node, filename))

    return benchmark_file

def read_img(filename, data, benchmark, start, output_dir, bench_dir=None):

    start_time = time() - start

    # load binary data into Nibabel
    fh = nib.FileHolder(fileobj=BytesIO(data))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    data = im.get_data()

    end_time = time() - start

    bn = os.path.basename(filename)

    bench_file = None
    if benchmark:
        bench_file = write_bench('read_img', start_time, end_time,
                                 socket.gethostname(), output_dir, bn,
                                 benchmark_dir=bench_dir)

    return (filename, data, (im.affine, im.header), bench_file)


def increment_data(filename, data, metadata, delay,
                   benchmark, start, output_dir, bench_file=None):

    start_time = time() - start

    data += 1
    sleep(delay)

    end_time = time() - start

    if benchmark:
        bn = os.path.basename(filename)
        write_bench('increment_data', start_time, end_time,
                    socket.gethostname(), output_dir, bn,
                    benchmark_file=bench_file)

    return (filename, data, metadata, bench_file)


def save_incremented(filename, data, metadata, benchmark, start,
                     output_dir, iterations, bench_file=None):

    start_time = time() - start

    bn = os.path.basename(filename)
    im = nib.Nifti1Image(data, metadata[0], header=metadata[1])
    out_fn = os.path.join(output_dir, 'inc{0}-{1}'.format(iterations, bn))
    nib.save(im, out_fn)

    end_time = time() - start

    if benchmark:
        write_bench('save_incremented', start_time, end_time,
                    socket.gethostname(), output_dir, bn,
                    benchmark_file=bench_file)

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
    app_uuid = str(uuid.uuid1())
    benchmark_dir = os.path.join(args.output_dir, 
                                 'benchmarks-{}'.format(app_uuid))

    # read binary data stored in folder and create an RDD from it
    imRDD = sc.binaryFiles('file://' + os.path.abspath(args.bb_dir)) \
              .map(lambda x: read_img(x[0], x[1],
                                      args.benchmark,
                                      start, args.output_dir,
                                      bench_dir=benchmark_dir))

    for i in range(args.iterations):
        imRDD = imRDD.map(lambda x: increment_data(x[0], x[1], x[2], delay,
                                                   args.benchmark, start,
                                                   args.output_dir, x[3]))

    imRDD.map(lambda x: save_incremented(x[0], x[1], x[2],
                                         args.benchmark, start,
                                         args.output_dir,
                                         args.iterations, x[3])) \
         .collect()

    end = time() - start

    if args.benchmark:
        fname = 'benchmark-{}.txt'.format(app_uuid)
        benchmark_file = os.path.join(args.output_dir, fname)
        write_bench('driver program', start, end, socket.gethostname(),
                    args.output_dir, 'allfiles', benchmark_file=benchmark_file)

        with open(benchmark_file, 'a+') as bench:
            for b in os.listdir(benchmark_dir):
                with open(os.path.join(benchmark_dir, b), 'r') as f:
                    bench.write(f.read())

        shutil.rmtree(benchmark_dir)



if __name__ == '__main__':
    main()
