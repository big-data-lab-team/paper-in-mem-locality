#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from io import BytesIO
from os import path as op
from os import makedirs
import argparse
import sys
import nibabel as nib
import numpy as np


def get_nearest_centroid(d, c):

    distance = None
    nearest_c = None

    for centroid in c:
        c_dist = abs(d-centroid)

        if distance is None or c_dist < distance:
            distance = c_dist
            nearest_c = centroid

    return (nearest_c, d)


def update_centroids(d):

    updated = sum(d)/len(d)

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

    for i in range(0, len(assignments)):
        assigned_voxels = list(set(assignments[i][1]))
        data[np.where(np.isin(data, assigned_voxels))] = assigned_class[i]

    im_seg = nib.Nifti1Image(data, im.affine)

    # save segmented image
    output_file = op.join(out, 'seg-' + op.basename(d[0]))
    nib.save(im_seg, output_file)

    return (output_file, "SAVED")


def main():
    conf = SparkConf().setAppName("Spark kmeans")
    sc = SparkContext.getOrCreate(conf=conf)

    parser = argparse.ArgumentParser(description="BigBrain k-means"
                                                 " segmentation")
    parser.add_argument('bb_dir', type=str, help="The folder containing "
                        "BigBrain NIfTI images (local"
                        " fs only)")
    parser.add_argument('iters', type=int, help="maximum number of kmean "
                                                "iterations")
    parser.add_argument('output_dir', type=str, help="the folder to save "
                                                     "segmented images to "
                                                     "(local fs only)")
    args = parser.parse_args()

    # fixed centroids to be able to compare with nipype implementation
    centroids = [50314.747730447438, 5.4095965052460562, 29218.970083958127,
                 60767.571375735897]
    # [60741.945981249322, 28998.276891703455, 5.3324138454658172,
    #   49808.209711495881]
    # [0, 26214, 45874.5, 65535]

    output_dir = op.abspath(args.output_dir)

    try:
        makedirs(output_dir)
    except Exception as e:
        pass

    # read binary data stored in folder and create an RDD from it
    # will return an RDD with format RDD[(filename, binary_data)]
    imRDD = sc.binaryFiles('file://' + op.abspath(args.bb_dir)).cache()

    voxelRDD = imRDD.flatMap(get_voxels).cache()

    c_changed = True
    count = 1
    assignments = None

    while c_changed or count > args.iters:
        assignments = voxelRDD.map(lambda x: get_nearest_centroid(x,
                                                                  centroids)) \
                              .groupByKey()

        updated_centroids = sc.parallelize(centroids) \
                              .zipWithIndex() \
                              .join(assignments) \
                              .map(lambda x: update_centroids(x[1][1])) \
                              .collect()

        c_changed = not bool(set(centroids).intersection(updated_centroids))

        centroids = updated_centroids

        if c_changed:
            print("it", count, centroids)
        count += 1

    assignments = assignments.zipWithIndex().map(lambda x: (x[1],
                                                            x[0][1])).collect()
    results = imRDD.map(lambda x: save_segmented(x, assignments,
                                                 output_dir)
                        ).collect()

    print("***FINAL CENTROIDS***:", centroids)
    print(results)


if __name__ == '__main__':
    main()
