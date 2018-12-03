#!/usr/bin/env python
from os import listdir
from os.path import join
import sys
import matplotlib.pyplot as plt


read_bandwidths = {
                   'tmpfs': [],
                   'local': [],
                   'lustre': [],
                  }
write_bandwidths = {
                    'tmpfs': [],
                    'local': [],
                    'lustre': [],
                   }
spr_bandwidths = {
                  'tmpfs': [],
                  'local': [],
                  'lustre' : []
                 }
npr_bandwidths = {
                  'tmpfs': [],
                  'local': [],
                  'lustre' : []
                 }
spw_bandwidths = {
                  'tmpfs': [],
                  'local': [],
                  'lustre' : []
                 }
npw_bandwidths = {
                  'tmpfs': [],
                  'local': [],
                  'lustre' : []
                 }


rb_outf = 'read_bandwidth.pdf'
wb_outf = 'write_bandwidth.pdf'
snrb_out = 'nsp-' + rb_outf
snwb_out = 'nsp-' + wb_outf

for bench_f in listdir(sys.argv[1]):
    filename = bench_f[:-4]
    fullpath = join(sys.argv[1], bench_f)

    conditions = filename.split('_')
    engine = conditions[0]

    fs = conditions[1]
    chunks = conditions[2].split('it')[1]

    chunk_size = 102752/(1024**2)
    im = 'MRI'

    if fs == 'mem': continue

    if 'HB' in chunks:
        chunk_size = 321813010/(1024**2)
        im = 'HBB'
    elif 'BB' in chunks:
        im_size = 76727
        num_chunks = int(chunks[:-2])
        chunk_size = im_size/num_chunks
        im = 'BB'

    with open(fullpath, 'r') as f:
        for line in f:
            data = line.split(' ')
           
            io_time = float(data[2]) - float(data[1])
            if data[0] == 'read_file':
                if io_time == 0:
                    rb = chunk_size/0.004
                else:
                    rb = chunk_size/io_time

                read_bandwidths[fs].append(rb)

                if engine == 'sp':
                    spr_bandwidths[fs].append(rb)
                else:
                    npr_bandwidths[fs].append(rb)

            elif data[0] == 'write_file':
                if io_time == 0:
                    wb = chunk_size/0.004
                else:
                    wb = chunk_size/io_time

                write_bandwidths[fs].append(wb)

                if engine == 'sp':
                    spw_bandwidths[fs].append(wb)
                else:
                    npw_bandwidths[fs].append(wb)

#plt.boxplot([read_bandwidths['tmpfs'], read_bandwidths['local'],
#             read_bandwidths['lustre']], 0, 'b,')

fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(13, 5), sharey=True)
axes[0].violinplot([npr_bandwidths['tmpfs'], npr_bandwidths['local'],
                 npr_bandwidths['lustre']], showmeans=False, 
                 showmedians=True)
axes[0].set_title('Nipype')

axes[1].violinplot([spr_bandwidths['tmpfs'], spr_bandwidths['local'],
                 spr_bandwidths['lustre']], showmeans=False, 
                 showmedians=True)
axes[1].set_title('Spark')

for ax in axes:
    ax.set_xlabel('Filesystem')
    ax.set_ylabel('Bandwidth (MB/s)')

plt.setp(axes, xticks=[1, 2, 3],
         xticklabels=['tmpfs', 'local', 'lustre'])
plt.subplots_adjust()
plt.savefig(snrb_out)


fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(13, 5), sharey=True)
axes[0].violinplot([npw_bandwidths['tmpfs'], npw_bandwidths['local'],
                 npw_bandwidths['lustre']], showmeans=False, 
                 showmedians=True)
axes[0].set_title('Nipype')

axes[1].violinplot([spw_bandwidths['tmpfs'], spw_bandwidths['local'],
                 spw_bandwidths['lustre']], showmeans=False, 
                 showmedians=True)
axes[1].set_title('Spark')

for ax in axes:
    ax.set_xlabel('Filesystem')
    ax.set_ylabel('Bandwidth (MB/s)')

plt.setp(axes, xticks=[1, 2, 3],
         xticklabels=['tmpfs', 'local', 'lustre'])
plt.subplots_adjust()
plt.savefig(snwb_out)
'''
plt.figure()
plt.boxplot([write_bandwidths['tmpfs'],
             write_bandwidths['local'], 
             write_bandwidths['lustre']], 0, 'b,')
plt.xticks([1, 2, 3], ['tmpfs', 'local', 'lustre'])

plt.savefig(wb_outf)
'''
