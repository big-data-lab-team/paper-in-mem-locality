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
        ConcatAffines, RefineBrainMask,
)
from fmriprep.utils.misc import add_suffix
from pkg_resources import resource_filename as pkgr
import os, bunch, socket


# Global variables to eventually be adapted for command-line tool
workflow_name = 'test_anat_template'
input_t1w = [os.path.abspath('data/ds052/sub-01/anat/sub-01_run-01_T1w.nii.gz'),
        os.path.abspath('data/ds052/sub-01/anat/sub-01_run-02_T1w.nii.gz')]
longitudinal=False
num_t1w=2
workdir=os.getcwd() + '/' +  workflow_name
omp_nthreads=1
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

    return (out['out_file'], out['transform_outputs'])

   
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

def execute_wf(workflow_name, workdir):

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

    
execute_wf(workflow_name, workdir)
