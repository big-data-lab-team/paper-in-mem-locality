import nibabel as nib
import numpy as np
import os
import argparse
from pyspark import SparkContext, SparkConf
from io import BytesIO

def read_img(filename, data):
    # load binary data into Nibabel
    fh = nib.FileHolder(fileobj=BytesIO(data))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    data = im.get_data()

    return (filename, data, im.affine)

def binarize_data(filename, data, affine, threshold):

    # in order to keep original binarization instead
    # of returning a blank image
    if data.max() == 1:
        threshold = 0

    data = np.where(data > threshold, 1, 0)

    return (filename, data, affine)

def save_binarized(filename, data, affine, output_dir):
    
    im = nib.Nifti1Image(data, affine)
    out_fn = os.path.join(output_dir, 'bin-' + os.path.basename(filename))
    nib.save(im, out_fn)
    return (out_fn, 'SUCCESS')

def main():
    parser = argparse.ArgumentParser(description="BigBrain binarization")
    parser.add_argument('bb_dir',type=str, 
        help='The folder containing BigBrain NIfTI images (local fs only)')
    parser.add_argument('output_dir', type=str, 
        help='the folder to save binarized images to (local fs only)')
    parser.add_argument('threshold', type=int, help='binarization threshold')
    
    args = parser.parse_args()

    conf = SparkConf().setAppName("Spark binarization")
    sc = SparkContext.getOrCreate(conf=conf)

    threshold = args.threshold
    os.makedirs(args.output_dir, exist_ok=True)

    # read binary data stored in folder and create an RDD from it
    imRDD = sc.binaryFiles('file://' + os.path.abspath(args.bb_dir)) \
              .map(lambda x: read_img(x[0], x[1]))

    for i in range(10):
        imRDD = imRDD.map(lambda x: binarize_data(x[0], x[1], x[2], threshold))


    imRDD.map(lambda x: save_binarized(x[0], x[1], x[2], args.output_dir)) \
         .collect()

    
if __name__ == '__main__':
    main()


