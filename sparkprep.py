from pyspark import SparkContext, SparkConf
from niworkflows.nipype.interfaces.ants import BrainExtraction, N4BiasFieldCorrection
from niworkflows.nipype.interfaces import (
    utility as niu,
    freesurfer as fs,
    c3,
    base
)
from niworkflows.data import get_ants_oasis_template_ras
from fmriprep.interfaces import(
        DerivativesDataSink, MakeMidthickness, FSInjectBrainExtracted,
        FSDetectInputs, NormalizeSurf, GiftiNameSource, TemplateDimensions, Conform, Reorient,
        ConcatAffines, RefineBrainMask, BIDSDataGrabber, BIDSFreeSurferDir, BIDSInfo,
        SubjectSummary
)
from fmriprep.utils.misc import add_suffix, fix_multi_T1w_source_name
from fmriprep.utils.bids import collect_participants, collect_data
from pkg_resources import resource_filename as pkgr
from collections import namedtuple
from multiprocessing import cpu_count
import os, bunch, socket, argparse



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


   
#def outputIdentityTaskSingleT1(part):

def createOutputInterfaceObj():
    return niu.IdentityInterface(fields=['t1_template', 't1w_valid_list', 'template_transforms', 'out_report'])

def create_spark_context(workflow_name):
    conf = SparkConf().setAppName(workflow_name)
    return SparkContext.getOrCreate(conf=conf)

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

def summary(s, subjects_dir, output_spaces, template, work_dir):
    print('Executing SubjectSummary interface')

    ssum = SubjectSummary(output_spaces=output_spaces, template=template)
    ssum.inputs.subjects_dir = subjects_dir
    ssum.inputs.t1w = s[1][0].t1w
    ssum.inputs.t2w = s[1][0].t2w
    ssum.inputs.bold = s[1][0].bold
    ssum.inputs.subject_id = s[1][1].subject_id

    ssum._run_interface(get_runtime(work_dir))

    out = ssum._list_outputs()
    
    SS = namedtuple('SS', ['subject_id', 'out_report'])
    subs= SS(subject_id=out['subject_id'], out_report=out['out_report'])
    
    return (s[0], subs)

def format_anat_preproc_rdd(s, subjects_dir):

    AP = namedtuple('AP', ['subjects_dir', 't1w', 't2w', 'subject_id'])
    a = AP(subjects_dir=subjects_dir, t1w=s[1][0].t1w, t2w=s[1][0].t2w, subject_id=s[1][1].subject_id)

    return (s[0], a)

def format_anat_template_rdd(s):

    AT = namedtuple('AT', ['t1w'])
    a = AT(t1w=s[1].t1w)

    return (s[0], a)


def t1_template_dimensions(s, work_dir):
    print('executing template dimensions')

    td = TemplateDimensions()
    td.inputs.t1w_list = s[1].t1w

    interface_dir = os.path.join(work_dir, s[0], 't1_template_dimensions')
    os.makedirs(interface_dir, exist_ok=True)

    td._run_interface(get_runtime(interface_dir))
  
    TemplateDim = namedtuple('TemplateDim', ['t1w_valid_list', 'target_zooms', 
                                                'target_shape', 'out_report'])
    tempDim = TemplateDim(
                          t1w_valid_list=td._results['t1w_valid_list'],
                          target_zooms=td._results['target_zooms'],
                          target_shape=td._results['target_shape'],
                          out_report=td._results['out_report']
                          )

    return (s[0], tempDim)

def t1_conform(s, work_dir):
    print('executing t1 conform')
    c = Conform()

    c.inputs.in_file = s[1][0]
    c.inputs.target_zooms = s[1][1][0]
    c.inputs.target_shape = s[1][1][1]
   
    interface_dir = os.path.join(work_dir, s[0], 't1_conform')
    os.makedirs(interface_dir, exist_ok=True)

    c._run_interface(get_runtime(interface_dir))

    T1Conform = namedtuple('T1Conform', ['out_file', 'transform'])
    tconf = T1Conform(out_file=c._results['out_file'], transform=c._results['transform'])

    return (s[0], tconf)

def t1_template_output_single(s):
    print('Collecting outputs for single-subject w/ single T1 image')

    out_id = createOutputInterfaceObj()

    out_id.inputs.template_transforms = [pkgr('fmriprep', 'data/itkIdentityTransform.txt')]

    def _get_first(in_list):
        if isinstance(in_list, (list, tuple)):
            return in_list[0]
        return in_list

    
    out_id.inputs.t1_template = _get_first(s[1][0].out_file)
    out_id.inputs.t1w_valid_list = s[1][1].t1w_valid_list 
    out_id.inputs.out_report = s[1][1].out_report

    out = out_id.run()

    OutputSingle = namedtuple('OutputSingle', ['t1w_valid_list', 't1_template', 
                                               'template_transforms', 'out_report'])
    os = OutputSingle(t1w_valid_list=out.outputs.t1w_valid_list, t1_template=out.outputs.t1_template,
                      template_transforms=out.outputs.template_transforms, out_report=out.outputs.out_report)

    return (s[0], os)

def t1_template_output_multiple(s):
    print('Collecting outputs for single-subject w/ multiple T1 images')

    out_id = createOutputInterfaceObj()

    out_id.inputs.t1_template = s[1][1].out_file
    out_id.inputs.t1w_valid_list = s[1][0][0].t1w_valid_list 
    out_id.inputs.out_report = s[1][0][0].out_report
    out_id.inputs.template_transforms = [fi.itk_transform for fi in s[1][0][1]]

    out = out_id.run()

    OutputSingle = namedtuple('OutputSingle', ['t1w_valid_list', 't1_template', 
                                               'template_transforms', 'out_report'])
    os = OutputSingle(t1w_valid_list=out.outputs.t1w_valid_list, t1_template=out.outputs.t1_template,
                      template_transforms=out.outputs.template_transforms, out_report=out.outputs.out_report)

    return (s[0], os)

def n4_correct(s, work_dir):
    print('exectuing n4 bias field correction')
    n4 = N4BiasFieldCorrection(dimension=3, copy_header=False)

    interface_dir = os.path.join(work_dir, s[0], 'n4_correct')
    os.makedirs(interface_dir, exist_ok=True)
    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    output_image = []

    for t1_conform in s[1]:
        n4.inputs.input_image = t1_conform.out_file
        n4._run_interface(get_runtime(interface_dir))
        out = n4._list_outputs()
        output_image.append(out['output_image'])
    
    os.chdir(curr_dir)

    # returning input image so it can be joined to other RDDs later on 
    return (s[0], output_image)

def t1_merge(s, longitudinal, omp_nthreads, work_dir):
    out_file = [t1conf.out_file for t1conf in s[1][0]]
    print('executing robust template task')
    t1m = fs.RobustTemplate(auto_detect_sensitivity=True,
                            initial_timepoint=1,      # For deterministic behavior
                            intensity_scaling=True,   # 7-DOF (rigid + intensity)
                            subsample_threshold=200,
                            fixed_timepoint=not longitudinal,
                            no_iteration=not longitudinal,
                            transform_outputs=True,
                            )


    def _set_threads(in_list, maximum):
        return min(len(in_list), maximum)


    t1m.inputs.num_threads = _set_threads(out_file, omp_nthreads)
    t1m.inputs.out_file = add_suffix(out_file, '_template')
    t1m.inputs.in_files = s[1][1]

    interface_dir = os.path.join(work_dir, s[0], 't1_merge')
    os.makedirs(interface_dir, exist_ok=True)
    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    t1m._run_interface(get_runtime(interface_dir))
    out = t1m._list_outputs()

    os.chdir(curr_dir)

    T1Merge = namedtuple('T1Merge', ['out_file', 'transform_outputs', 'in_files'])
    m_out = T1Merge(out['out_file'], out['transform_outputs'], s[1][1])

    return (s[0], m_out) 

def t1_reorient(s, work_dir):
    print('executing reorient')
    r = Reorient()

    interface_dir = os.path.join(work_dir, s[0], 't1_reorient')
    os.makedirs(interface_dir, exist_ok=True)
    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    r.inputs.in_file = s[1].out_file
    r._run_interface(get_runtime(interface_dir))
    out = r._list_outputs()
    
    T1Reorient = namedtuple('T1Reorient', ['out_file', 'transform'])
    ro = T1Reorient(out_file=out['out_file'], transform=out['transform'])
    os.chdir(curr_dir)

    # returning input image so it can be joined to other RDDs later on 
    return (s[0], ro)

def lta_to_fsl(s, work_dir):
    print('executing LTA Convert')
    lta = fs.utils.LTAConvert(out_fsl=True)
    sub_dir_name = "_".join(os.path.basename(s[2]).split('.')[0].split('_')[:2])
    
    interface_dir = os.path.join(
                            work_dir, 
                            s[0],
                            'lta_to_fsl',
                            sub_dir_name
                            )

    os.makedirs(interface_dir, exist_ok=True)
    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    lta.inputs.in_lta = s[1]
    lta._run_interface(get_runtime(interface_dir))
    out = lta._list_outputs()
    
    LTAConvert = namedtuple('LTAConvert', ['out_fsl', 'run'])
    l = LTAConvert(out_fsl=out['out_fsl'], run=sub_dir_name)
    os.chdir(curr_dir)

    # returning input image so it can be joined to other RDDs later on 
    return (s[0], l)

def concat_affines(s, work_dir):
    print('executing Concat Affines')

    ca = ConcatAffines(3, invert=True)
    interface_dir = os.path.join(work_dir, s[0], 'concat_affines', s[1][0][1].run)

    os.makedirs(interface_dir, exist_ok=True)
    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    ca.inputs.mat_AtoB = s[1][0][0].transform
    ca.inputs.mat_BtoC = s[1][0][1].out_fsl
    ca.inputs.mat_CtoD = s[1][1].transform

    ca._run_interface(get_runtime(interface_dir))
    out = ca._list_outputs()

    ConcatAff = namedtuple('ConcatAff', ['out_mat','run'])
    c = ConcatAff(out_mat=out['out_mat'], run=s[1][0][1].run)
    os.chdir(curr_dir)

    return (s[0], c)

def fsl_to_itk(s,work_dir):
    print('executing C3d Affine Tool')

    fi = c3.C3dAffineTool(fsl2ras=True, itk_transform=True)
    interface_dir = os.path.join(work_dir, s[0], 'fsl_to_itk', s[1][0][1].run)

    os.makedirs(interface_dir, exist_ok=True)

    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)

    fi.inputs.source_file = s[1][0][0]
    fi.inputs.reference_file = s[1][1].out_file
    fi.inputs.transform_file = s[1][0][1].out_mat

    fi._run_interface(get_runtime(interface_dir))
    out = fi._list_outputs()

    Fsl2Itk = namedtuple('Fsl2Itk', ['itk_transform'])
    f = Fsl2Itk(itk_transform=out['itk_transform'])

    os.chdir(curr_dir)

    return (s[0], f)

def t1_skull_strip(s, brain_template, brain_probability_mask, extraction_registration_mask, work_dir):
    print("executing Brain Extraction")

    ss = BrainExtraction(dimension=3, use_floatingpoint_precision=1, debug=False, keep_temporary_files=1)
    interface_dir = os.path.join(work_dir, s[0], 't1_skull_strip')

    os.makedirs(interface_dir, exist_ok=True)

    # a terrible workaround to ensure that nipype looks 
    # for the output dir in the correct directory
    curr_dir = os.getcwd()

    os.chdir(interface_dir)
    
    ss.inputs.brain_template = brain_template
    ss.inputs.brain_probability_mask = brain_probability_mask
    ss.inputs.extraction_registration_mask = extraction_registration_mask
    ss.inputs.anatomical_image = s[1].t1_template 

    ss._run_interface(get_runtime(interface_dir))
    out = ss._list_outputs()

    SkullStrip = namedtuple('SkullStrip', ['BrainExtractionMask', 'BrainExtractionBrain',
                                           'BrainExtractionSegmentation', 'N4Corrected0'])
    sstrip = SkullStrip(out['BrainExtractionMask'], out['BrainExtractionBrain'],
                        out['BrainExtractionSegmentation'], out['N4Corrected0'])

    return (s[0], sstrip)

def init_spark_anat_template(sc, rdd, longitudinal, omp_nthreads, work_dir):
    
    t1_tempdim_rdd = rdd.map(lambda x: t1_template_dimensions(x, work_dir)) \
                        .cache()
    
    # create an tuple for each existing t1w image in RDD
    t1w_list_rdd = t1_tempdim_rdd.flatMap(lambda x: 
                        [(a,b) for a,b in zip([x[0]]*len(x[1].t1w_valid_list), x[1].t1w_valid_list)])

    t1_targets_rdd = t1_tempdim_rdd.map(lambda x: (x[0], (x[1].target_zooms, x[1].target_shape)))

    t1_conform_rdd = t1w_list_rdd.join(t1_targets_rdd) \
                                 .map(lambda x: t1_conform(x, work_dir))

    multi_t1w = t1_tempdim_rdd.map(lambda x: (x[0], len(x[1].t1w_valid_list))) \
                                 .filter(lambda x: x[1] > 1) \
                                 .map(lambda x: x[0]) \
                                 .collect()
   
    # filter out the subjects that have more than one t1w
    t1_conform_1_rdd = t1_conform_rdd.filter(lambda x: x[0] not in multi_t1w)

    t1_output_rdd = t1_conform_1_rdd.join(t1_tempdim_rdd) \
                                    .map(t1_template_output_single)

    # filter out the subjects that only have one t1w. 'p1' stands for +1 images
    t1_conform_p1_rdd = t1_conform_rdd.filter(lambda x: x[0] in multi_t1w) \
                                      .cache()
   
    t1p1_conform_grouped_rdd = t1_conform_p1_rdd.groupByKey() \
                                                .map(lambda x: (x[0], list(x[1])))

    n4_correct_rdd = t1p1_conform_grouped_rdd.map(lambda x: n4_correct(x, work_dir))
   
    t1_merge_rdd = t1p1_conform_grouped_rdd.join(n4_correct_rdd) \
                                           .map(lambda x: t1_merge(x, longitudinal, omp_nthreads, work_dir)) \
                                           .cache()

    t1_reorient_rdd = t1_merge_rdd.map(lambda x: t1_reorient(x, work_dir)) \
                                  .cache()

    lta_to_fsl_rdd = t1_merge_rdd.flatMap(lambda x: 
                           [(a,b,c) for a,b,c in zip([x[0]]*len(x[1].transform_outputs), x[1].transform_outputs,x[1].in_files)]) \
                           .map(lambda x: lta_to_fsl(x, work_dir))

    # run without submitting
    concat_affines_prep = t1_conform_p1_rdd.join(lta_to_fsl_rdd) \
                                          .join(t1_reorient_rdd) \
                                          .filter(lambda x: x[1][0][1].run in x[1][0][0].out_file) \
                                          .collect()

    concat_affines_seq = [concat_affines(el, work_dir) for el in concat_affines_prep]

    concat_affines_rdd = sc.parallelize(concat_affines_seq)

    fsl_to_itk_rdd = t1w_list_rdd.join(concat_affines_rdd) \
                                 .join(t1_reorient_rdd) \
                                 .filter(lambda x: x[1][0][1].run in x[1][0][0]) \
                                 .map(lambda x: fsl_to_itk(x, work_dir)) \
                                 .groupByKey()

    output_rdd = t1_tempdim_rdd.join(fsl_to_itk_rdd) \
                               .join(t1_reorient_rdd) \
                               .map(t1_template_output_multiple) \
                               .union(t1_output_rdd)

    return output_rdd

def init_skull_strip_ants(sc, rdd, skull_strip_template, omp_nthreads, work_dir):

    #if skull_strip_template = OASIS. Taken directly from fmriprep
    template_dir = get_ants_oasis_template_ras()
    brain_template = os.path.join(template_dir, 'T_template0.nii.gz')
    brain_probability_mask = os.path.join(
                                template_dir, 'T_template0_BrainCerebellumProbabilityMask.nii.gz')
    extraction_registration_mask = os.path.join(
                                template_dir, 'T_template0_BrainCerebellumRegistrationMask.nii.gz')

    t1_skull_strip_rdd = rdd.map(lambda x: t1_skull_strip(x, brain_template,
                                            brain_probability_mask, extraction_registration_mask, work_dir))

    print(t1_skull_strip_rdd.collect())

def init_spark_anat_preproc(sc, rdd, skull_strip_template, output_spaces, template, omp_nthreads,
                            longitudinal, freesurfer, reportlets_dir, output_dir, work_dir):

    anat_template_rdd = rdd.map(lambda x: format_anat_template_rdd(x))

    anat_template_rdd = init_spark_anat_template(sc, anat_template_rdd, longitudinal, omp_nthreads, work_dir) \
                        .cache()

    skull_strip_ants_rdd = init_skull_strip_ants(sc, anat_template_rdd, skull_strip_template, omp_nthreads, work_dir)



def init_main_wf(subject_list, task_id, ignore, anat_only, longitudinal, 
                 t2s_coreg, skull_strip_template, work_dir, output_dir, bids_dir,
                 freesurfer, output_spaces, template, medial_surface_nan,
                 hires, use_bbr, bold2t1w_dof, fmap_bspline, fmap_demean,
                 use_syn, force_syn, use_aroma, output_grid_ref, omp_nthreads, wf_name='sprep_wf'):
    
    sc = create_spark_context(wf_name)

    subjects_dir = fsdir(output_dir, output_spaces, work_dir)
    reportlets_dir = os.path.join(work_dir, 'reportlets')

    subject_rdd = sc.parallelize(subject_list) \
            .map(lambda x: get_subject_data(x, task_id, bids_dir))

    bidssrc_rdd = subject_rdd.map(lambda x: bidssrc(x, anat_only, work_dir)) \
                             .cache()

    bidsinfo_rdd = bidssrc_rdd.map(lambda x: bids_info(x, work_dir))

    summary_rdd = bidssrc_rdd.join(bidsinfo_rdd) \
                             .map(lambda x: summary(x, subjects_dir, output_spaces, 
                                                    template, work_dir))

    anat_preproc_rdd = bidssrc_rdd.join(summary_rdd) \
                                  .map(lambda x: format_anat_preproc_rdd(x, subjects_dir))

    
    init_spark_anat_preproc(
                            sc=sc,
                            rdd=anat_preproc_rdd,
                            skull_strip_template=skull_strip_template,
                            output_spaces=output_spaces,
                            template=template,
                            longitudinal=longitudinal,
                            freesurfer=freesurfer,
                            reportlets_dir=reportlets_dir,
                            output_dir=output_dir,
                            work_dir=work_dir,
                            omp_nthreads=omp_nthreads
                            )
    
    #print(anat_preproc_rdd.collect())

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
    nthreads = 0
    omp_nthreads = 0

    output_dir = os.path.abspath(args.output_dir)
    work_dir = os.path.abspath(args.work_dir)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(work_dir, exist_ok=True)

    bids_dir = os.path.abspath(args.bids_dir)
    subject_list = collect_participants(bids_dir, 
            participant_label=None)

    if nthreads < 1:
        nthreads = cpu_count()

    if omp_nthreads == 0:
        omp_nthreads = min(nthreads - 1 if nthreads > 1 else cpu_count(), 8)

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
            use_aroma=use_aroma,
            omp_nthreads=omp_nthreads
     )        



if __name__ == '__main__':
    main()
