from nipype import Workflow, MapNode, Node, Function
from numpy import iinfo
from random import seed, sample
from time import time
from shutil import rmtree
import argparse
import os
import glob
import uuid



def get_nearest_centroid(img, centroids):
    import nibabel as nib
    import pickle
    from os import path as op

    data = nib.load(img).get_fdata().flatten()

    assignments = {}

    for vox in data:
        distance = None
        nearest_c = None
        nearest_cv = None

        for c in centroids:
            c_dist = abs(vox - c[1])

            if distance is None or c_dist < distance:
                distance = c_dist
                nearest_c = c[0]
                nearest_cv = c[1]
            '''elif c_dist == distance:
                if vox % 2 == 1:
                    if nearest_cv < c[1]:
                        nearest_c = c[0]
                        nearest_cv = c[1]
                else:
                    if nearest_cv > c[1]:
                        nearest_c = c[0]
                        nearest_cv = c[1]'''

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


def classify_chunks(img, assignments):
    import nibabel as nib
    import pickle
    from os import path as op
    from numpy import where, isin

    # assume all assignment files fit in memory
    a_files = [t for l in assignments for t in l]
    i = nib.load(img)
    data = i.get_fdata()
    shape = i.shape

    assignments = {}

    for t in a_files:
        if t[0] not in assignments:
            with open(t[1], 'rb') as f:
                assignments[t[0]] = set(pickle.load(f))
        else:
            with open(t[1], 'rb') as f:
                assignments[t[0]].update(pickle.load(f))


    for k in assignments:
        assigned_voxels = list(assignments[k])
        data[where(isin(data, assigned_voxels))] = k

    i_out = nib.Nifti1Image(data, i.affine, i.header)
    i_name = op.basename(img)
    nib.save(i_out, i_name)

    return op.abspath(i_name)


def save_classified(img, output_dir):
    import shutil
    from os import path as op

    out_name = 'classified_{}'.format(op.basename(img))

    out_file = op.join(output_dir, out_name)

    return shutil.copy(img, out_file)


def main():
    parser = argparse.ArgumentParser(description='BigBrain K-means')
    parser.add_argument('bb_dir', type=str, help='The folder containing '
                        'BigBrain NIfTI images (local fs only)')
    parser.add_argument('iters', type=int, help='The number of iterations')
    parser.add_argument('centroids', type=float, nargs='+',
                        help="cluster centroids")
    parser.add_argument('output_dir', type=str, help='the folder to save '
                        'the final centroids to (local fs only)')
    parser.add_argument('--benchmark', action='store_true',
                        help='benchmark pipeline')

    args = parser.parse_args()

    start = time()
    output_dir = os.path.abspath(args.output_dir)

    try:
        os.makedirs(output_dir)
    except Exception as e:
        pass

    benchmark_dir = None
    app_uuid = str(uuid.uuid1())
    
    if args.benchmark:
        benchmark_dir = os.path.abspath(os.path.join(args.output_dir,
                                                        'benchmarks-{}'.format(
                                                                    app_uuid)))
        try:
            os.makedirs(benchmark_dir)
        except Exception as e:
            pass

    # get all files in directory
    bb_files = glob.glob(os.path.join(os.path.abspath(args.bb_dir), '*'))
    dtype = iinfo('uint16')

    centroids = list(zip(range(0, len(args.centroids)), args.centroids))

    c_changed = True

    idx = 0
    while c_changed and idx < args.iters:
        wf = Workflow('km_bb{}'.format(idx))

        gc_nname = 'gc_{}'.format(idx)
        gc = MapNode(Function(input_names=['img', 'centroids'],
                              output_names=['assignment_files'],
                              function=get_nearest_centroid),
                     name=gc_nname,
                     iterfield=['img'])

        gc.inputs.img = bb_files
        gc.inputs.centroids = centroids

        wf.add_nodes([gc])

        uc_nname = 'uc_{}'.format(idx)
        uc = MapNode(Function(input_names=['centroid', 'assignments'],
                              output_names=['updated_centroids'],
                              function=update_centroids),
                     name=uc_nname,
                     iterfield=['centroid'])

        uc.inputs.centroid = centroids

        wf.connect([(gc, uc, [('assignment_files', 'assignments')])])

        wf_out = wf.run(plugin='MultiProc')

        # Convert to dictionary to more easily extract results
        node_names = [i.name for i in wf_out.nodes()]
        result_dict = dict(zip(node_names, wf_out.nodes()))

        new_centroids = (result_dict[uc_nname].result
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
            print("***FINAL CENTROIDS***:", idx ,c_vals)

            res_wf = Workflow('km_classify')

            c_idx = 0
            for chunk in bb_files:
                cc = Node(Function(input_names=['img', 'assignments'],
                                   output_names=['out_file'],
                                   function=classify_chunks),
                          name='cc_{}'.format(c_idx))

                cc.inputs.img = chunk
                cc.inputs.assignments = (result_dict[gc_nname].result
                                         .outputs
                                         .assignment_files)
                res_wf.add_nodes([cc])

                sc = Node(Function(input_names=['img', 'output_dir'],
                                   output_names=['out_file'],
                                   function=save_classified),
                          name='sc_{}'.format(c_idx))

                sc.inputs.output_dir = output_dir

                res_wf.connect([(cc, sc, [('out_file', 'img')])])

                c_idx += 1

            res_wf.run(plugin='MultiProc')
        end = time()

        if args.benchmark:
            fname = 'benchmark-{}.txt'.format(app_uuid)
            benchmark_file = os.path.abspath(os.path.join(args.output_dir,
                                                          fname))
            print(benchmark_file)

            with open(benchmark_file, 'a+') as bench:
                bench.write('{0} {1} {2} {3} '
                            '{4} {5}\n'.format('driver_program',
                                               start,
                                               end,
                                               socket.gethostname(),
                                               'allfiles',
                                               get_ident()))

                for b in os.listdir(benchmark_dir):
                    with open(os.path.join(benchmark_dir, b), 'r') as f:
                        bench.write(f.read())
                        
            rmtree(benchmark_dir)

if __name__ == '__main__':
    main()
