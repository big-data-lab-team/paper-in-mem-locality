from pyspark import SparkContext, SparkConf
import argparse, os
import nibabel as nib

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

    updated = sum(d[1])/len(d)

    return updated

def get_voxels(d):

    #read data into nibabel
    fh = nib.FileHolder(fileobj=BytesIO(d[1]))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    data = im.get_data()

    return data.flatten('F')

def main():
    conf = SparkConf().setAppName("Spark kmeans")
    sc = SparkContext.getOrCreate(conf=conf)

    parser = argparse.ArgumentParser(description="BigBrain k-means segmentation")
    parser.add_argument('bb_dir',type=str, help='The folder containing BigBrain NIfTI images (local fs only)')
    #parser.add_argument('output_dir', type=str, help='the folder to save segmented images to (local fs only)')
    args = parser.parse_args()

    #fixed centroids to be able to compare with nipype implementation
    centroids = [0, 26214, 45874.5, 65535]

    # read binary data stored in folder and create an RDD from it
    # will return an RDD with format RDD[(filename, binary_data)]
    imRDD = sc.binaryFiles('file://' + os.path.abspath(args.bb_dir)).cache()

    c_changed = True
    count = 1
    while c_changed:
        assignments = imRDD.flatMap(lambda x: get_voxels) \
                           .map(lambda x: get_nearest_centroid(x, centroids)) \
                           .groupByKey()

        updated_centroids = sc.parallelize(centroids) \
                              .join(assignments) \
                              .map(update_centroids) \
                              .collect()

        c_changed = bool(set(centroids).intersection(update_centroid))

        centroids = updated_centroids

        if c_changed:
            print("it", count)
        
    print(centroids)



if __name__ == '__main__':
    main()
