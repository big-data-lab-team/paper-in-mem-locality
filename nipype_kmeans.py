from nipype import Workflow, MapNode, Node, Function
from io import BytesIO
import argparse, os
import glob

def get_nearest_centroid(chunk, cent_0, cent_1, cent_2, cent_3):
    import nibabel as nib
    import numpy as np
    import os

    im = nib.load(chunk)
    data = im.get_data()

    centroids = [cent_0, cent_1, cent_2, cent_3]

    asgmt_0 = []
    asgmt_1 = []
    asgmt_2 = []
    asgmt_3 = []

    file_0 = 'asgmt_0.txt'
    file_1 = 'asgmt_1.txt'
    file_2 = 'asgmt_2.txt'
    file_3 = 'asgmt_3.txt'

    for vox in data.flatten():
        distance = [abs(c - vox) for c in centroids]
        c_idx = np.argmin(distance)
        
        if c_idx == 0:
            asgmt_0.append(vox)
        elif c_idx == 1:
            asgmt_1.append(vox)
        elif c_idx == 2:
            asgmt_2.append(vox)
        else:
            asgmt_3.append(vox)
    #print(asgmt_0, asgmt_1, asgmt_2, asgmt_3)
    np.savetxt(file_0, asgmt_0, header=str(cent_0))
    np.savetxt(file_1, asgmt_1, header=str(cent_1))
    np.savetxt(file_2, asgmt_2, header=str(cent_2))
    np.savetxt(file_3, asgmt_3, header=str(cent_3))


    return (os.path.abspath(file_0), os.path.abspath(file_1),
            os.path.abspath(file_2), os.path.abspath(file_3))

def update_centroids(assignment_files):
    import numpy as np
    import os

    v_sum = 0
    length = 0
    cent = None

    with open(assignment_files[0], 'r') as f:
        cent = float(f.readline().strip('#').strip())

    for f in assignment_files:
        v = np.loadtxt(f)
        v_sum += v.sum()
        length += v.size

    fn = 'ucent.txt'
    # keep centroid the same if no elements are assigned
    if sum == 0:
        with open(fn, 'w+') as f:
            f.write(str(cent))
        return os.path.abspath(fn)

    with open(fn, 'w+') as f:
        f.write(str(v_sum/length))

    return os.path.abspath(fn)

def main():
    parser = argparse.ArgumentParser(description="BigBrain K-means")

    parser.add_argument('bb_dir',type=str, help='The folder containing BigBrain NIfTI images (local fs only)')
    #parser.add_argument('output_dir', type=str, help='the folder to save binarized images to (local fs only)')
    parser.add_argument('iters', type=int, help='The number of kmeans iterations')

    args = parser.parse_args()

    wf = Workflow('km_bb')
    wf.base_dir = os.getcwd()

    #output_dir = os.path.abspath(args.output_dir)
    #os.makedirs(output_dir, exist_ok=True)

    # get all files in directory
    bb_files = glob.glob(os.path.join(os.path.abspath(args.bb_dir), '*'))
    centroids = [50314.747730447438, 5.4095965052460562, 29218.970083958127,
                                                         60767.571375735897]

    #loaded_data = MapNode(Function(input_names=))

    func_asgmt = Function(input_names=['chunk', 'cent_0', 'cent_1', 
                                     'cent_2', 'cent_3'],
                                     output_names=['asgmt_0', 'asgmt_1', 'asgmt_2',
                                     'asgmt_3'],
                                     function=get_nearest_centroid)

    func_updatec = Function(input_names=['assignment_files'],
                                     output_names=['up_cent'],
                                     function=update_centroids)

    up_centroid0 = None
    up_centroid1 = None
    up_centroid2 = None
    up_centroid3 = None

    for i in range(args.iters):

        asgmt_name = 'assignments{}'.format(i)
        updated0_name = 'upcentroids0_{}'.format(i)
        updated1_name = 'upcentroids1_{}'.format(i)
        updated2_name = 'upcentroids2_{}'.format(i)
        updated3_name = 'upcentroids3_{}'.format(i)

        assignments = MapNode(func_asgmt, iterfield=['chunk'],
                                name='assignments{}'.format(i))

        assignments.inputs.chunk = bb_files

        if i == 0:
            assignments.inputs.cent_0 = centroids[0]
            assignments.inputs.cent_1 = centroids[1]
            assignments.inputs.cent_2 = centroids[2]
            assignments.inputs.cent_3 = centroids[3]
         
            wf.add_nodes([assignments])

        else:
            wf.connect([(up_centroid0, assignments, [('up_cent','cent_0')]),
                        (up_centroid1, assignments, [('up_cent','cent_1')]),
                        (up_centroid2, assignments, [('up_cent','cent_2')]),
                        (up_centroid3, assignments, [('up_cent','cent_3')])
                      ]) 

        up_centroid0 = Node(func_updatec, name=updated0_name)
        up_centroid1 = Node(func_updatec, name=updated1_name)
        up_centroid2 = Node(func_updatec, name=updated2_name)
        up_centroid3 = Node(func_updatec, name=updated3_name)

        wf.connect([(assignments, up_centroid0, [('asgmt_0','assignment_files')]),
                   (assignments, up_centroid1, [('asgmt_1','assignment_files')]),
                   (assignments, up_centroid2, [('asgmt_2','assignment_files')]),
                   (assignments, up_centroid3, [('asgmt_3','assignment_files')])
                  ]) 


    wf.run(plugin='MultiProc')

if __name__ == '__main__':
    main()
