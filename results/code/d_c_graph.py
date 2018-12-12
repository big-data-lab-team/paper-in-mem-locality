#!/usr/bin/env python

import sys  
import os
import argparse

def parse_bench_file_name(file_name):
    # Assumes name looks like sp_lustre_100it125BB_2.4delay.out
    strings = file_name.split('_')
    wf = strings[0]
    fs = strings[1]
    delay = float(strings[3].replace('delay.out', ''))
    stringsit = strings[2].split('it')
    iterations = int(stringsit[0])
    blocks = 30
    if '125' in stringsit[1]:
        blocks = 125
    elif '750' in stringsit[1]:
        blocks = 750
    data = 'MRI'
    if 'HBB' in stringsit[1]:
        data = 'HBB'
    elif 'BB' in stringsit[1]:
        data = 'BB'
    if delay == 320:
        delay = int(delay)
    
    return iterations, data, fs, wf, delay, blocks

def d_c_fig(bench_dir, makespan_file, out_file):
    sizes = {  # in MiB
        'MRI': 13016/1024,
        'HBB': 39284504/1024,
        'BB': 78568516/1024
    }
    
    bandwidths = { # MiB/s
        'lustre': 504.03,
        'local': 193.64, 
        'tmpfs': 1377.18
    }
    
    gammas = { # by number of    blocks
        30: 2,
        125: 9,
        750: 25
    }
    
    Gammas = { # by number of blocks
        30: 30,
        125: 125,
        750: 350
    }
    
    benches = {}
    for file_name in os.listdir(bench_dir):
        it, data, fs, wf, delay, blocks = parse_bench_file_name(file_name)
        benches[file_name] = {
            'iterations': it,
            'data_file': data,
            'file_system': fs,
            'blocks': blocks,
            'wf': wf,
            'task_duration': delay,
            'C': delay*it*blocks,
            'D': sizes[data]*it,
            'g': gammas[blocks],
            'G': Gammas[blocks]
        }
        benches[file_name]
    
    # Set makespans
    with open(makespan_file, 'r') as f:
        for line in f:
            splits = line.split(' ')
            file_name = splits[0].split(':')[0]
            makespan = float(splits[2])
            benches[file_name]['makespan'] = makespan
    
    
    # Set memory speed-ups
    for file_name in os.listdir(bench_dir):
        b = benches[file_name]
        in_mem_file = "sp_mem_{}it{}{}_{}delay.out".format(b['iterations'],
                                                           b['blocks'],
                                                           b['data_file'],
                                                           b['task_duration'])
        if benches.get(in_mem_file) == None:
            continue # file isn't in benchmark
        b['memory-speed-up'] = (
            b['makespan'] / benches[in_mem_file]['makespan']
        )
    
    x_mem = []
    y_mem = []
    x_tmpfs = []
    y_tmpfs = []
    x_disk = []
    y_disk = []
    x_sfs = []
    y_sfs = []
    for file_name in os.listdir(sys.argv[1]):
        if benches[file_name].get('memory-speed-up') == None:
            continue # file isn't in benchmark
        fs = benches[file_name]['file_system']
        if fs == 'mem':
            continue
        elif fs == 'tmpfs':
            x = x_tmpfs
            y = y_tmpfs
        elif fs == 'lustre':
            x = x_sfs
            y = y_sfs
        elif fs == 'local':
            x = x_disk
            y = y_disk
        gamma = gammas[benches[file_name]['blocks']]
        if fs == 'lustre':
            gamma = Gammas[benches[file_name]['blocks']]
        if fs == 'tmpfs':
            x.append(benches[file_name]['D'])
        else:
            x.append((benches[file_name]['D']/benches[file_name]['C']) / (bandwidths[fs]/gamma))
        y.append(benches[file_name]['memory-speed-up'])


    from matplotlib import pyplot as plt
    from numpy import polyfit, poly1d
    fit = polyfit(x_tmpfs,y_tmpfs,1)
    fit_fn = poly1d(fit)
    #plt.plot(x_mem, y_mem, 'o', label="In memory")
    plt.plot(x_tmpfs, y_tmpfs, 'g+', label="tmpfs")
    plt.plot(x_tmpfs, fit_fn(x_tmpfs), '--k', label=fit_fn)
    plt.xlabel("D")
    plt.ylabel("Speed-up of Spark in-mem")
    plt.legend()
    plt.ylim(0)
    plt.xlim(0)
    #plt.show()
    plt.savefig(os.path.join(os.path.dirname(out_file), 
                'tmpfs-{}'.format(os.path.basename(out_file))))
    
    plt.figure()
    fit = polyfit(x_disk, y_disk,1)
    fit_fn = poly1d(fit)
    plt.plot(x_disk, y_disk, 'b+', label="Local Disk")
    plt.plot(x_disk, fit_fn(x_disk), '--k', label=fit_fn)
    rect = plt.Rectangle([1, 0], 150, 1, color='gray', edgecolor=None)
    plt.gca().add_patch(rect)
    rect = plt.Rectangle([0, 1], 1, 5, color='gray', edgecolor=None)
    plt.gca().add_patch(rect)
    plt.xlabel("(D/C) / (d(D)elta/g(G)amma)")
    plt.ylabel("Speed-up of Spark in-mem")
    plt.legend()
    plt.ylim(0)
    plt.xlim(0)
    #plt.show()
    plt.savefig(os.path.join(os.path.dirname(out_file), 
                'local-{}'.format(os.path.basename(out_file))))
    plt.figure()
    fit = polyfit(x_sfs, y_sfs,1)
    fit_fn = poly1d(fit)
    plt.plot(x_sfs, y_sfs, 'r+', label='Lustre')
    plt.plot(x_sfs, fit_fn(x_sfs), '--k', label=fit_fn)
    rect = plt.Rectangle([1, 0], 250, 1, color='gray', edgecolor=None)
    plt.gca().add_patch(rect)
    rect = plt.Rectangle([0, 1], 1, 5, color='gray', edgecolor=None)
    plt.gca().add_patch(rect)
    plt.xlabel("(D/C) / (d(D)elta/g(G)amma)")
    plt.ylabel("Speed-up of Spark in-mem")
    plt.legend()
    plt.ylim(0)
    plt.xlim(0)
    #plt.show()
    plt.savefig(os.path.join(os.path.dirname(out_file), 
                'lustre-{}'.format(os.path.basename(out_file))))


def main():
    parser = argparse.ArgumentParser(prog='Generate D/C graph')
    parser.add_argument('bench_directory', type=str, 
                        help='benchmark directory')
    parser.add_argument('makespan_file', type=str, help='makespan file')
    parser.add_argument('out_file', type=str, help='output_file')

    args = parser.parse_args()

    d_c_fig(args.bench_directory, args.makespan_file, args.out_file)

if __name__=="__main__":
    main()
