import nibabel as nib
import os
from pyspark import SparkContext, SparkConf
from io import BytesIO

def read_img(filename, data):
    # load binary data into Nibabel
    fh = nib.FileHolder(fileobj=BytesIO(data))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    data = im.get_data()

    return (filename, data, im.affine)

def binarize_data(filename, data, affine, threshold):

    for i in range(0,len(data)):
        for j in range(0, len(data[0])):
            for k in range(0, len(data[0][0])):
                if data[i][j][k] > threshold:
                    data[i][j][k] = 1
                else:
                    data[i][j][k] = 0

    return (filename, data, affine)

def save_binarized(filename, data, affine):
    
    im = nib.Nifti1Image(data, affine)
    nib.save(im, 'bin-' + os.path.basename(filename))
    return ('SUCCESS')

conf = SparkConf().setAppName("Spark binarization")
sc = SparkContext.getOrCreate(conf=conf)
threshold = 500

# read binary data stored in folder and create an RDD from it
imRDD = sc.binaryFiles('file://' + os.path.abspath('./bb_data'))
bin1 = imRDD.map(lambda x: read_img(x[0], x[1])) \
            .map(lambda x: binarize_data(x[0], x[1], x[2], threshold)) \
            .map(lambda x: binarize_data(x[0], x[1], x[2], threshold)) \
            .map(lambda x: save_binarized(x[0], x[1], x[2])) \
            .collect()

print(bin1)
    




