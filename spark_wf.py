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
from fmriprep.utils.misc import add_suffix
from pkg_resources import resource_filename as pkgr
import os, bunch, socket


# Global variables to eventually be adapted for command-line tool
workflow_name = 'test_anat_template'
input_t1w = [os.path.abspath('data/ds052/sub-01/anat/sub-01_run-01_T1w.nii.gz'),
        os.path.abspath('data/ds052/sub-01/anat/sub-01_run-02_T1w.nii.gz')]
longitudinal=False
num_t1w=1
workdir = os.getcwd() + '/' +  workflow_name

def templateDimensionsTask(part):
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
    out_report = [td._results['out_report']] * len(td._results['t1w_valid_list'])

    output = [(a, (b, c, d)) for a, b, c, d in zip(td._results['t1w_valid_list'],
                                                target_zooms, target_shapes, out_report)]
    return output

def t1ConformTask(img):
    c = Conform()

    c.inputs.in_file = img[0]
    c.inputs.target_zooms = img[1][0]
    c.inputs.target_shape = img[1][1]
   
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

#def n4BiasFieldCorrectionTask(img):


def outputIdentityTaskSingleT1(part):
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




    #print(t1_conform_rdd.collect())

    
execute_wf(workflow_name, workdir)
