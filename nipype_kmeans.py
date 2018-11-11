from nipype import Workflow, MapNode, Node, Function
from numpy import iinfo
from random import seed, sample
import argparse
import os
import glob


def get_nearest_centroid(img, centroids):
    import nibabel as nib
    import pickle
    from os import path as op

    data = nib.load(img).get_fdata().flatten()

    assignments = {}

    for vox in data:
        distance = None
        nearest_c = None

        for c in centroids:
            c_dist = abs(vox - c[1])

            if distance is None or c_dist < distance:
                distance = c_dist
                nearest_c = c[0]

        if nearest_c not in assignments:
            assignments[nearest_c] = [vox]
        else:
            assignments[nearest_c].append(vox)

    outfiles = []

    try:
        for k, v in assignments.items():
            out_name = 'centroid-{}.out'.format(k)
            with open(out_name, 'ab+') as f:
                pickle.dump(v, f)

            outfiles.append((k, op.abspath(out_name)))

    except Exception as e:
        for k, v in assignments.iteritems():
            out_name = 'centroid-{}.out'.format(k)

            with open('centroid-{}.out'.format(k), 'ab+') as f:
                pickle.dump(v, f)

            outfiles.append((k, op.abspath(out_name)))

    return outfiles


def update_centroids(centroid, assignments):
    import pickle

    a_files = [t[1] for l in assignments for t in l if t[0] == centroid[0]]

    sum_elements = 0
    num_elements = 0

    for fn in a_files:
        with open(fn, 'rb') as f:
            elements = pickle.load(f)
            sum_elements += sum([float(i) for i in elements])
            num_elements += len(elements)

    if sum_elements == num_elements == 0:
        return centroid
    else:
        return (centroid[0], sum_elements/num_elements)


def main():
    parser = argparse.ArgumentParser(description='BigBrain K-means')
    parser.add_argument('bb_dir', type=str, help='The folder containing '
                        'BigBrain NIfTI images (local fs only)')
    parser.add_argument('output_dir', type=str, help='the folder to save '
                        'the final centroids to (local fs only)')
    parser.add_argument('iters', type=int, help='The number of iterations')

    args = parser.parse_args()

    output_dir = os.path.abspath(args.output_dir)

    try:
        os.makedirs(output_dir)
    except Exception as e:
        pass

    # get all files in directory
    bb_files = glob.glob(os.path.join(os.path.abspath(args.bb_dir), '*'))
    seed(2)
    dtype = iinfo('uint16')

    centroids = sample(range(dtype.min, dtype.max), 5)

    centroids = list(zip(range(0, len(centroids)), centroids))

    c_changed = True

    idx = 0
    while c_changed and idx < args.iters:
        wf = Workflow('km_bb{}'.format(idx))

        gc = MapNode(Function(input_names=['img', 'centroids'],
                              output_names=['assignment_files'],
                              function=get_nearest_centroid),
                     name='gc_{}'.format(idx),
                     iterfield=['img'])

        gc.inputs.img = bb_files
        gc.inputs.centroids = centroids

        wf.add_nodes([gc])

        uc = MapNode(Function(input_names=['centroid', 'assignments'],
                              output_names=['updated_centroids'],
                              function=update_centroids),
                     name='uc_{}'.format(idx),
                     iterfield=['centroid'])

        uc.inputs.centroid = centroids

        wf.connect([(gc, uc, [('assignment_files', 'assignments')])])

        wf_out = wf.run(plugin='MultiProc')

        # Convert to dictionary to more easily extract results
        node_names = [i.name for i in wf_out.nodes()]
        result_dict = dict(zip(node_names, wf_out.nodes()))

        new_centroids = (result_dict['uc_{}'.format(idx)].result
                                                         .outputs
                                                         .updated_centroids)

        old_centroids = set(centroids)
        diff = [x for x in new_centroids if x not in old_centroids]
        c_changed = bool(diff)
        centroids = new_centroids

        c_vals = [i[1] for i in centroids]
        idx += 1

        if c_changed and idx < args.iters:
            print("it", idx, c_vals)
        else:
            print("***FINAL CENTROIDS***:", c_vals)


if __name__ == '__main__':
    main()
