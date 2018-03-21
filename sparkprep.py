from pyspark import SparkContext, SparkConf
from niworkflows.nipype.interfaces.ants import N4BiasFieldCorrection
from niworkflows.nipype.interfaces import (
    utility as niu,
    freesurfer as fs,
    base
)
from fmriprep.interfaces import(
        DerivativesDataSink, MakeMidthickness, FSInjectBrainExtracted,
        FSDetectInputs, NormalizeSurf, GiftiNameSource, TemplateDimensions, Conform, Reorient,
        ConcatAffines, RefineBrainMask, BIDSDataGrabber, BIDSFreeSurferDir, BIDSInfo
)
from fmriprep.utils.misc import add_suffix, fix_multi_T1w_source_name
from fmriprep.utils.bids import collect_participants, collect_data
from pkg_resources import resource_filename as pkgr
from collections import namedtuple
import os, bunch, socket, argparse


# Global variables to eventually be adapted for command-line tool
#workflow_name = 'test_anat_template'
#input_t1w = [os.path.abspath('data/ds052/sub-01/anat/sub-01_run-01_T1w.nii.gz'),
#        os.path.abspath('data/ds052/sub-01/anat/sub-01_run-02_T1w.nii.gz')]
#longitudinal=False
#num_t1w=2
#workdir=os.getcwd() + '/' +  workflow_name
#omp_nthreads=1


# helper functions

def get_runtime(interface_dir):

    if not os.path.isdir(interface_dir):
        os.makedirs(interface_dir)

    runtime = bunch.Bunch(
                        cwd=interface_dir,
                        returncode=0,
                        environ=dict(os.environ),
                        hostname=socket.gethostname()
                        )
    return runtime

# pipeline activites
def templateDimensionsTask(part):
    print('executing template dimensions')
    in_img = list(part)

    if len(in_img) > 0:
        td = TemplateDimensions()
        td.inputs.t1w_list = in_img

        interface_dir = workdir + '/TemplateDimensions'

        td._run_interface(get_runtime(interface_dir))
   
    target_zooms = [td._results['target_zooms']] * len(td._results['t1w_valid_list'])
    target_shapes = [td._results['target_shape']] * len(td._results['t1w_valid_list'])
    out_report = [td._results['out_report']] * len(td._results['t1w_valid_list'])

    output = [(a, (b, c, d)) for a, b, c, d in zip(td._results['t1w_valid_list'],
                                                target_zooms, target_shapes, out_report)]
    return output

def t1ConformTask(img):
    print('executing t1 conform')
    c = Conform()

    c.inputs.in_file = img[0]
    c.inputs.target_zooms = img[1][0]
    c.inputs.target_shape = img[1][1]
   
    interface_dir = workdir + '/Conform'

    c._run_interface(get_runtime(interface_dir))

    return (c._results['out_file'], c._results['transform'])

def n4BiasFieldCorrectionTask(img):
    print('exectuing n4 bias field correction')
    n4 = N4BiasFieldCorrection(dimension=3, copy_header=True)

    n4.inputs.input_image = img[0]

    interface_dir = workdir + '/N4BiasFieldCorrection'

    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    n4._run_interface(get_runtime(interface_dir))

    out = n4._list_outputs()
    
    os.chdir(curr_dir)

    # returning input image so it can be joined to other RDDs later on 
    return (n4.inputs.input_image, out['output_image'])

def robustTemplateTask(part):
    part = list(part)
    print('executing robust template task')
    t1_merge = fs.RobustTemplate(auto_detect_sensitivity=True,
                      initial_timepoint=1,      # For deterministic behavior
                      intensity_scaling=True,   # 7-DOF (rigid + intensity)
                      subsample_threshold=200,
                      fixed_timepoint=not longitudinal,
                      no_iteration=not longitudinal,
                      transform_outputs=True,
                      )


    def _set_threads(in_list, maximum):
        return min(len(in_list), maximum)

    out_file = [i[0] for i in part]
    output_image = [i[1][1] for i in part]
    

    t1_merge.inputs.num_threads = _set_threads(out_file, omp_nthreads)
    t1_merge.inputs.out_file = add_suffix(out_file, '_template')
    t1_merge.inputs.in_files = output_image

    interface_dir = workdir + '/RobustTemplate'

    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    t1_merge._run_interface(get_runtime(interface_dir))
    out = t1_merge._list_outputs()

    os.chdir(curr_dir)

    rt_out_file = [out['out_file']] * len(out_file)
    transform_out = [out['transform_outputs']] * len(out_file)

    output = [(a, (b, c)) for a, b, c in zip(out_file, rt_out_file, transform_out)]

    return output

   
def outputIdentityTaskSingleT1(part):
    print('Collecting outputs for single T1 image')
    part = list(part)

    out_id = createOutputInterfaceObj()

    out_id.inputs.template_transforms = [pkgr('fmriprep', 'data/itkIdentityTransform.txt')]
    out_id.inputs.t1_template = part[0][0]
    out_id.inputs.t1w_valid_list = [i[0] for i in part]
    out_id.inputs.out_report = part[0][1][1][2]

    out = out_id.run()

    return (out.outputs.t1w_valid_list, (out.outputs.t1_template, 
            out.outputs.template_transforms, out.outputs.out_report))

def createOutputInterfaceObj():
    return niu.IdentityInterface(fields=['t1_template', 't1w_valid_list', 'template_transforms', 'out_report'])

def create_spark_context(workflow_name):
    conf = SparkConf().setAppName(workflow_name)
    return SparkContext.getOrCreate(conf=conf)

def execute_anat_template_wf(workflow_name, workdir):

    if not os.path.isdir(workdir):
        os.makedirs(workdir)

    sc = create_spark_context(workflow_name)

    temp_dim_rdd = sc.parallelize(input_t1w) \
                    .coalesce(1) \
                    .mapPartitions(templateDimensionsTask) \
                    .cache()

    t1_conform_rdd = temp_dim_rdd.repartition(8) \
                    .map(t1ConformTask) \
                    .cache()

    out_rdd = t1_conform_rdd.coalesce(1) \
                            .join(temp_dim_rdd) \
                            .cache()
    if num_t1w == 1:
        out_rdd = out_rdd.coalesce(1) \
                         .mapPartitions(
                                 outputIdentityTaskSingleT1
                                 ) \
                         .collect()
        return out_rdd

    # fmriprep code running on single proc for reproducibility
    n4_correct_rdd = t1_conform_rdd.coalesce(1) \
                                   .map(n4BiasFieldCorrectionTask)

    # joined rdd takes format (input_image, (template, corrected))
    t1_merge_rdd = t1_conform_rdd.join(n4_correct_rdd) \
                                 .coalesce(1) \
                                 .mapPartitions(robustTemplateTask) \
                                 .collect()

    print(t1_merge_rdd)

    

def fsdir(output_dir, output_spaces, work_dir):
    print('Executing BIDSFreeSurferDir interface')

    bidsfs = BIDSFreeSurferDir(
                derivatives=output_dir,
                freesurfer_home=os.getenv('FREESURFER_HOME'),
                spaces=output_spaces)


    bidsfs._run_interface(get_runtime(work_dir))
    out = bidsfs._list_outputs()

    return out['subjects_dir']

def get_subject_data(subject_id, task_id, bids_dir):
    subject_data, layout = collect_data(bids_dir, subject_id, task_id)

    # Subject = namedtuple('Subject', ['id', 'data'])
    # sub = Subject(id=subject_id, data=subject_data)

    return (subject_id, subject_data)

def bidssrc(s, anat_only, work_dir):
    print('Executing BIDSDataGrabber interface')

    bsrc = BIDSDataGrabber(subject_id=s[0], subject_data=s[1], anat_only=anat_only)

    bsrc._run_interface(get_runtime(work_dir))
    out = bsrc._list_outputs()

    BSRC = namedtuple('BSRC', ['t1w', 't2w', 'bold'])
    b = BSRC(t1w=out['t1w'], t2w=out['t2w'], bold=out['bold'])

    return (s[0], b)

def bids_info(s, work_dir):
    print('Executing BIDSInfo interface')

    binfo = BIDSInfo()
    binfo.inputs.in_file = fix_multi_T1w_source_name(s[1].t1w)
    binfo._run_interface(get_runtime(work_dir))
    out = binfo._list_outputs()
    
    BInfo = namedtuple('BInfo', ['subject_id'])
    b = BInfo(subject_id=out['subject_id'])
    
    return (s[0], b)

def init_main_wf(subject_list, task_id, ignore, anat_only, longitudinal, 
                 t2s_coreg, skull_strip_template, work_dir, output_dir, bids_dir,
                 freesurfer, output_spaces, template, medial_surface_nan,
                 hires, use_bbr, bold2t1w_dof, fmap_bspline, fmap_demean,
                 use_syn, force_syn, use_aroma, output_grid_ref, wf_name='sprep_wf'):
    
    subjects_dir = fsdir(output_dir, output_spaces, work_dir)

    sc = create_spark_context(wf_name)

    subject_rdd = sc.parallelize(subject_list) \
            .map(lambda x: get_subject_data(x, task_id, bids_dir)) \
            .cache()

    bidssrc_rdd = subject_rdd.map(lambda x: bidssrc(x, anat_only, work_dir))

    bidsinfo_rdd = bidssrc_rdd.map(lambda x: bids_info(x, work_dir))
    print(bidsinfo_rdd.collect())

def main():
    parser = argparse.ArgumentParser(description="Spark partial implementation of fMRIprep")
    parser.add_argument('bids_dir', action='store', help='root BIDS directory')
    parser.add_argument('output_dir', action='store', help='output directory')
    parser.add_argument('analysis_level', choices=['participant'],help='BIDS analysis level (participant only)')
    parser.add_argument('--anat-only', action='store_true', help='run anatomical workflow only')
    parser.add_argument('-w', '--work-dir', action='store', help='working directory')

    args = parser.parse_args()

    # currently forced parameters (some of which are only pertinent to fmriprep and will be removed)

    ignore = []
    anat_only = args.anat_only
    longitudinal = False
    t2s_coreg = False
    skull_strip_template = 'OASIS'
    run_reconall = True
    output_space = ['template', 'fsaverage5']
    template = 'MNI152NLin2009cAsym'
    medial_surface_nan = False
    output_grid_ref = None
    hires = True
    use_bbr = None
    bold2t1w_dof = 9
    fmap_bspline = False
    fmap_no_demean = True
    use_syn_sdc = False
    force_syn = False
    use_aroma = False

    output_dir = os.path.abspath(args.output_dir)
    work_dir = os.path.abspath(args.work_dir)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(work_dir, exist_ok=True)

    bids_dir = os.path.abspath(args.bids_dir)
    subject_list = collect_participants(bids_dir, 
            participant_label=None)


    init_main_wf(
            subject_list=subject_list,
            task_id=None,
            ignore=ignore,
            anat_only=anat_only,
            longitudinal=longitudinal,
            t2s_coreg=t2s_coreg,
            skull_strip_template=skull_strip_template,
            work_dir=work_dir,
            output_dir=output_dir,
            bids_dir=bids_dir,
            freesurfer=run_reconall,
            output_spaces=output_space,
            template=template,
            medial_surface_nan=medial_surface_nan,
            output_grid_ref=output_grid_ref,
            hires=hires,
            use_bbr=use_bbr,
            bold2t1w_dof=bold2t1w_dof,
            fmap_bspline=fmap_bspline,
            fmap_demean=fmap_no_demean,
            use_syn=use_syn_sdc,
            force_syn=force_syn,
            use_aroma=use_aroma
     )        



if __name__ == '__main__':
    main()
