import pytest
import subprocess
import hashlib


def test_increment():

    p = subprocess.Popen(['python', 'spark_inc.py', 'sample_data', 'inc_out',
                          '1'])
    p.communicate()

    h_prog_1 = hashlib.md5(open('inc_out/inc1-dummy_1.nii', 'rb').read()) \
                      .hexdigest()
    h_exp_1 = hashlib.md5(open('tests/test_outputs/testinc_1_1.nii', 'rb')
                          .read()) \
                     .hexdigest()

    assert h_prog_1 == h_exp_1

    p = subprocess.Popen(['python', 'spark_inc.py', 'sample_data', 'inc_out',
                          '10'])
    p.communicate()

    h_prog_10 = hashlib.md5(open('inc_out/inc10-dummy_1.nii', 'rb').read()) \
                       .hexdigest()
    h_exp_10 = hashlib.md5(open('tests/test_outputs/testinc_1_10.nii', 'rb')
                           .read()) \
                      .hexdigest()

    assert h_prog_10 == h_exp_10
