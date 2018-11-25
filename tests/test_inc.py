import pytest
import subprocess
import hashlib
import os
import shutil
from os import path as op


def test_increment_spark():

    p = subprocess.Popen(['python', 'pipelines/spark_inc.py',
                          'sample_data', 'inc_out',
                          '1'])
    p.communicate()

    h_prog_1 = hashlib.md5(open('inc_out/inc1-dummy_1.nii', 'rb').read()) \
                      .hexdigest()
    h_exp_1 = hashlib.md5(open('tests/test_outputs/testinc_1_1.nii', 'rb')
                          .read()) \
                     .hexdigest()

    assert h_prog_1 == h_exp_1

    p = subprocess.Popen(['python', 'pipelines/spark_inc.py',
                          'sample_data', 'inc_out',
                          '10'])
    p.communicate()

    h_prog_10 = hashlib.md5(open('inc_out/inc10-dummy_1.nii', 'rb').read()) \
                       .hexdigest()
    h_exp_10 = hashlib.md5(open('tests/test_outputs/testinc_1_10.nii', 'rb')
                           .read()) \
                      .hexdigest()

    assert h_prog_10 == h_exp_10


def test_increment_spark_cli():

    shutil.rmtree('inc_out', ignore_errors=True)
    shutil.rmtree('inc_work', ignore_errors=True)

    p = subprocess.Popen(['python', 'pipelines/spark_inc.py',
                          'sample_data', 'inc_out',
                          '1', '--cli', '--work_dir', 'inc_work'])
    p.communicate()

    h_prog_1 = hashlib.md5(open('inc_out/inc1-dummy_1.nii', 'rb').read()) \
                      .hexdigest()
    h_exp_1 = hashlib.md5(open('tests/test_outputs/testinc_1_1.nii', 'rb')
                          .read()) \
                     .hexdigest()

    assert h_prog_1 == h_exp_1

    p = subprocess.Popen(['python', 'pipelines/spark_inc.py',
                          'sample_data', 'inc_out',
                          '10', '--cli', '--work_dir', 'inc_work'])
    p.communicate()

    h_prog_10 = hashlib.md5(open('inc_out/inc10-dummy_1.nii',
                                 'rb').read()) \
                       .hexdigest()
    h_exp_10 = hashlib.md5(open('tests/test_outputs/testinc_1_10.nii', 'rb')
                           .read()) \
                      .hexdigest()

    assert h_prog_10 == h_exp_10


def test_increment_nipype():

    shutil.rmtree('npinc_out', ignore_errors=True)
    p = subprocess.Popen(['python', 'pipelines/nipype_inc.py', 'sample_data',
                          'npinc_out', '1'])
    (out, err) = p.communicate()

    h_prog_1 = hashlib.md5(open('npinc_out/inc1-dummy_1.nii', 'rb').read()) \
                      .hexdigest()
    h_exp_1 = hashlib.md5(open('tests/test_outputs/testinc_1_1.nii', 'rb')
                          .read()) \
                     .hexdigest()

    assert h_prog_1 == h_exp_1

    shutil.rmtree('npinc_out')
    p = subprocess.Popen(['python', 'pipelines/nipype_inc.py', 'sample_data',
                          'npinc_out', '10'])
    p.communicate()

    h_prog_10 = hashlib.md5(open('npinc_out/inc10-dummy_1.nii', 'rb')
                            .read()) \
                       .hexdigest()
    h_exp_10 = hashlib.md5(open('tests/test_outputs/testinc_1_10.nii', 'rb')
                           .read()) \
                      .hexdigest()

    assert h_prog_10 == h_exp_10


def test_increment_nipype_cli():

    shutil.rmtree('npinc_out', ignore_errors=True)
    p = subprocess.Popen(['python', 'pipelines/nipype_inc.py', 'sample_data',
                          'npinc_out', '1', '--cli', '--work_dir',
                          'npinc_tmp'])
    (out, err) = p.communicate()

    h_prog_1 = hashlib.md5(open('npinc_out/inc1-dummy_1.nii', 'rb').read()) \
                      .hexdigest()
    h_exp_1 = hashlib.md5(open('tests/test_outputs/testinc_1_1.nii', 'rb')
                          .read()) \
                     .hexdigest()

    assert h_prog_1 == h_exp_1

    shutil.rmtree('npinc_out')
    p = subprocess.Popen(['python', 'pipelines/nipype_inc.py', 'sample_data',
                          'npinc_out', '10', '--cli', '--work_dir',
                          'npinc_tmp'])
    p.communicate()

    h_prog_10 = hashlib.md5(open('npinc_out/inc10-dummy_1.nii', 'rb')
                            .read()) \
                       .hexdigest()
    h_exp_10 = hashlib.md5(open('tests/test_outputs/testinc_1_10.nii', 'rb')
                           .read()) \
                      .hexdigest()

    assert h_prog_10 == h_exp_10


def test_benchmark_spark():

    shutil.rmtree('inc_out', ignore_errors=True)
    p = subprocess.Popen(['python', 'pipelines/spark_inc.py',
                          'sample_data', 'inc_out',
                          '1', '--benchmark'], stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    (out, err) = p.communicate()

    out = [fn for fn in os.listdir('inc_out') if fn.startswith('benchmark')]
    assert len(out) == 1


def test_benchmark_nipype():

    shutil.rmtree('npinc_out')
    p = subprocess.Popen(['python', 'pipelines/nipype_inc.py', 'sample_data',
                          'npinc_out', '1', '--benchmark'],
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    (out, err) = p.communicate()

    out = [fn for fn in os.listdir('npinc_out') if fn.startswith('benchmark')]
    assert len(out) == 1
