from pyspark import SparkContext, SparkConf
from niworkflows.nipype.interfaces.ants import N4BiasFieldCorrection
from niworkflows.nipype.interfaces import (
    utility as niu,
    base
)
from fmriprep.interfaces import(
        DerivativesDataSink, MakeMidthickness, FSInjectBrainExtracted,
        FSDetectInputs, NormalizeSurf, GiftiNameSource, TemplateDimensions, Conform, Reorient,
        ConcatAffines, RefineBrainMask,
)
from pkg_resources import resource_filename as pkgr
import os, bunch, socket


# Global variables to eventually be adapted to command-line tool
workflow_name = 'test_anat_template'
input_t1w = [os.path.abspath('data/ds052/sub-01/anat/sub-01_run-01_T1w.nii.gz'),
        os.path.abspath('data/ds052/sub-01/anat/sub-01_run-02_T1w.nii.gz')]
longitudinal=False
num_t1w=2
workdir = os.getcwd() + '/' +  workflow_name

def runTemplateDimensions(part):
    in_img = list(part)


    if len(in_img) > 0:
        td = TemplateDimensions()
        td.inputs.t1w_list = in_img

        interface_dir = workdir + '/TemplateDimensions'

        if not os.path.isdir(interface_dir):
            os.makedirs(interface_dir)

        runtime = bunch.Bunch(
                            cwd=interface_dir,
                            returncode=0,
                            environ=dict(os.environ),
                            hostname=socket.gethostname()
                            )

        td._run_interface(runtime)
   
    target_zooms = [td._results['target_zooms']] * len(td._results['t1w_valid_list'])
    target_shapes = [td._results['target_shape']] * len(td._results['t1w_valid_list'])

    output = [(a, b, c) for a, b, c in zip(td._results['t1w_valid_list'], target_zooms, target_shapes)]

    return output

def t1ConformTask(img):
    c = Conform()

    c.inputs.in_file = img[0]
    c.inputs.target_zooms = img[1]
    c.inputs.target_shape = img[2]
   
    interface_dir = workdir + '/Conform'

    if not os.path.isdir(interface_dir):
        os.makedirs(interface_dir)

    runtime = bunch.Bunch(
                        cwd=interface_dir,
                        returncode=0,
                        environ=dict(os.environ),
                        hostname=socket.gethostname()
                        )

    c._run_interface(runtime)

    return (c._results['out_file'], c._results['transform'])

def create_spark_context(workflow_name):
    conf = SparkConf().setAppName(workflow_name)
    return SparkContext.getOrCreate(conf=conf)

def execute_wf(workflow_name, workdir):

    if not os.path.isdir(workdir):
        os.makedirs(workdir)

    sc = create_spark_context(workflow_name)

    in_t1_rdd = sc.parallelize(input_t1w) \
                    .coalesce(1) \
                    .mapPartitions(runTemplateDimensions) \
                    .repartition(8) \
                    .map(t1ConformTask) \
                    .cache()

    out_identity = niu.IdentityInterface(fields=['t1_template', 't1w_valid_list', 'template_transforms', 'out_report'])


    if num_t1w == 1:
        def _get_first(in_list):
            if isinstance(in_list, (list, tuple)):
                return in_list[0]
            return in_list

        



    print(in_t1_rdd.collect())

    # t1_template_dimensions = pe.Node(TemplateDimensions(), name='t1_template_dimensions')
    # t1_conform = pe.MapNode(Conform(), iterfield='in_file', name='t1_conform')
    
execute_wf(workflow_name, workdir)
