#!/usr/bin/env python
from os import listdir
from os.path import join
import sys
import matplotlib.pyplot as plt


read_bandwidths = write_bandwidths = {
                                      'tmpfs': [],
                                      'local': [],
                                      'lustre': [],
                                     }

for bench_f in listdir(sys.argv[1]):
    filename = bench_f[:-4]
    conditions = filename.split('_')
    # delay = float(conditions[3][:-5])
    # engine = conditions[0]
    fs = conditions[1]
    # iterations, chunks = conditions[2].split('it')
    chunks = conditions[2].split('it')[1]

    im_size = 13
    im = 'MRI'

    if fs == 'mem': continue
    if '125' not in chunks: continue

    if 'HB' in chunks:
        continue
        im_size = 38364
        im = 'HBB'
    elif 'BB' in chunks:
        im_size = 76727
        im = 'BB'
    else:
        continue
           
    # iterations = int(iterations)

    fullpath = join(sys.argv[1], bench_f)

    with open(fullpath, 'r') as f:
        for line in f:
            data = line.split(' ')
           
            io_time = float(data[2]) - float(data[1])
            if data[0] == 'read_file':
                if io_time == 0:
                    read_bandwidths[fs].append(im_size/0.001)
                else:
                    read_bandwidths[fs].append(im_size/io_time)

            if data[1] == 'write_file':
                if io_time == 0:
                    write_bandwidths[fs].append(im_size/0.001)
                else:
                    write_bandwidths[fs].append(im_size/io_time)

plt.boxplot([read_bandwidths['tmpfs'], read_bandwidths['local'],
             read_bandwidths['lustre']], 0, '')
plt.ylim(7.66*10**7, 7.68*10**7)
plt.xlabel(['tmpfs', 'local', 'lustre'])
plt.savefig('read_bandwidths')

plt.figure()
plt.boxplot([write_bandwidths['tmpfs'], write_bandwidths['local'],
             write_bandwidths['lustre']], 0, '')
plt.ylim(7.66*10**7, 7.68*10**7)
plt.xlabel(['tmpfs', 'local', 'lustre'])
plt.savefig('write_bandwidths')
