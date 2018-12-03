#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from io import BytesIO
from os import path as op, makedirs
from time import time
import argparse
import sys
import nibabel as nib
import numpy as np
from socket import gethostname
import benchmark as bench
try:
    from threading import get_ident
except Exception as e:
    from thread import get_ident

def get_nearest_centroid(d, c):

    distance = None
    nearest_c = None

    for centroid in c:
        c_dist = abs(d[0]-centroid)

        if (distance is None or c_dist < distance
                or (c_dist == distance
                    and ((d[0] % 2 == 1 and nearest_c < centroid)
                         or (d[0] % 2 == 0 and nearest_c > centroid)))):
            distance = c_dist
            nearest_c = centroid

    return (nearest_c, (d[0], d[1]))


def update_centroids(d):
    # format = list of tuples
    # e.g. [(0, 1), (2, 4), (3, 2)]
    sum_els = float(sum([i[0]*i[1] for i in d]))
    num_els = sum(i[1] for i in d)
    updated = sum_els/num_els

    return updated


def get_voxels(d):

    # read data into nibabel
    fh = nib.FileHolder(fileobj=BytesIO(d[1]))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    data = im.get_data()

    return data.flatten('F')


def save_segmented(d, assignments, out):

    # read data into nibabel
    fh = nib.FileHolder(fileobj=BytesIO(d[1]))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    data = im.get_data()

    assigned_class = [c[0] for c in assignments]
    
    for i in range(0, len(assigned_class)):
        assigned_voxels = [l[0] for c in assignments if c[0] == assigned_class[i] for l in c[1]]#list(set(assignments[i][1]))
        data[np.where(np.isin(data, assigned_voxels))] = assigned_class[i]
    im_seg = nib.Nifti1Image(data, im.affine)

    # save segmented image
    output_file = op.join(out, 'classified-' + op.basename(d[0]))
    nib.save(im_seg, output_file)

    return (output_file, "SAVED")


def main():
    # mri centroids: 0.0, 125.8, 251.6, 377.4
    start = time()

    conf = SparkConf().setAppName("Spark kmeans")
    sc = SparkContext.getOrCreate(conf=conf)

    parser = argparse.ArgumentParser(description="BigBrain k-means"
                                                 " segmentation")
    parser.add_argument('bb_dir', type=str, help="The folder containing "
                        "BigBrain NIfTI images (local"
                        " fs only)")
    parser.add_argument('iters', type=int, help="maximum number of kmean "
                                                "iterations")
    parser.add_argument('centroids', type=float, nargs='+',
                        help="cluster centroids")
    parser.add_argument('output_dir', type=str, help="the folder to save "
                                                     "segmented images to "
                                                     "(local fs only)")
    parser.add_argument('--benchmark', action='store_true',
                        help='Benchmark pipeline')
    args = parser.parse_args()

    centroids = args.centroids

    output_dir = op.abspath(args.output_dir)
    benchmark_dir = None

    try:
        makedirs(output_dir)
    except Exception as e:
        pass

    if args.benchmark:
        benchmark_dir = op.join(output_dir, 'benchmarks')
        try:
            makedirs(benchmark_dir)
        except Exception as e:
            pass

    # read binary data stored in folder and create an RDD from it
    # will return an RDD with format RDD[(filename, binary_data)]
    imRDD = sc.binaryFiles('file://' + op.abspath(args.bb_dir)).cache()

    voxelRDD = imRDD.flatMap(get_voxels).cache()

    c_changed = True
    count = 0
    assignments = None

    while c_changed and count < args.iters:
        start_1 = time() - start
        assignments = voxelRDD.map(lambda x: (x, 1)) \
                              .reduceByKey(lambda x,y: x+y) \
                              .map(lambda x: get_nearest_centroid(x,
                                                                  centroids)) \
                              .groupByKey().sortByKey()

        updated_centroids = sc.parallelize(centroids) \
                              .zipWithIndex() \
                              .join(assignments) \
                              .map(lambda x: update_centroids(x[1][1])) \
                              .collect()

        c_changed = len(set(centroids)
                            .intersection(updated_centroids)) < len(centroids) 

        centroids = sorted(updated_centroids)

        if c_changed:
            print("it", count, centroids)
        count += 1

        end_1 = time() - start

        if args.benchmark:
            bench.write_bench('updated_centroids', start_1, end_1,
                              gethostname(), 'allfiles', get_ident(),
                              benchmark_dir)

    
    start_2 = time() - start
    assignments = assignments.zipWithIndex().map(lambda x: (x[1], x[0][1])) \
                                            .collect()
    results = imRDD.map(lambda x: save_segmented(x, assignments, output_dir)
                        ).collect()
    end_2 = time() - start

    print("***FINAL CENTROIDS***:", count, centroids)
    print(results)

    end = time()


    if args.benchmark:
        bench.write_bench('classified_voxels', start_2, end_2,
                          gethostname(), 'allfiles', get_ident(),
                          benchmark_dir)
        bench.write_bench('driver_program', 0, end, gethostname(),          
                          'allfiles', get_ident(), benchmark_dir)

if __name__ == '__main__':
    main()
