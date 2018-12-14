#!/usr/bin/env python
import matplotlib.pyplot as plt
import sys
import argparse
from numpy import asarray, arange

exp14_delays = [2.4, 3.44, 7.68, 320]
exp2_delays = [0.59, 3.52, 14.67]
exp3_delays = 1.76

def genfig(exp, makef, outf, cond):
    bench_res = {('sp', 'mem'): [],
                 ('sp', 'tmpfs'): [],
                 ('sp', 'local'): [],
                 ('sp', 'lustre'): [],
                 ('np', 'tmpfs'): [],
                 ('np', 'local'): [],
                 ('np', 'lustre'): []
                }

    if exp == 1 and cond is None: cond = 3.44
    elif exp == 4 and cond is None: cond = 10

    with open(makef, 'r') as f:
        for line in f:
            filename, bench = line.split(':')
            filename = filename[:-4]
            conditions = filename.split('_')
            delay = float(conditions[3][:-5])
            engine = conditions[0]
            fs = conditions[1]
            iterations, chunks = conditions[2].split('it')

            if 'HBB' in chunks or 'MRI' in chunks:
                im = chunks[3:]
                chunks = 125
            else:
                im = 'BB'
                chunks = int(chunks[:-2])
       
            iterations = int(iterations)

            if exp == 3 and fs == 'mem' and im == 'MRI':
                continue

            if exp == 1:
                if delay != cond: continue
                bench_res[(engine, fs)].append((iterations,
                                                float(bench.split(' ')[2])))
            elif exp == 2:
                if delay not in exp2_delays: continue
                bench_res[(engine, fs)].append((chunks,
                                                float(bench.split(' ')[2])))
            elif exp == 3:
                if delay != exp3_delays: continue
                bench_res[(engine, fs)].append((im,
                                                float(bench.split(' ')[2])))
            else:
                if delay not in exp14_delays or iterations != cond: continue
                bench_res[(engine, fs)].append((delay,
                                                float(bench.split(' ')[2])))

        for key in bench_res:
            if exp == 3:
                bench_res[key] = sorted(bench_res[key],
                                        key=lambda tup: tup[0],
                                        reverse=True)
            else:
                bench_res[key] = sorted(bench_res[key], key=lambda tup: tup[0])
            x_labels = [x[0] for x in bench_res[key]]

            if exp == 1:
                labels = [1, 10, 100]
            elif exp == 2:
                labels = [30, 125, 750]
            elif exp == 3:
                labels = ['MRI', 'HBB', 'BB']
            else:
                labels = [2.4, 3.44, 7.68, 320]

            index = [i for i in range(0, len(labels)) if labels[i] in x_labels]


            y_labels = [y[1] for y in bench_res[key]]
            bench_res[key] = (asarray(index), y_labels)

        bar_width = 0.1
        fig, ax = plt.subplots()

        spim = ax.bar(bench_res[('sp', 'mem')][0] - bar_width * 3,
                      bench_res[('sp', 'mem')][1],
                      bar_width,color='#F4CC70', label='Spark in-memory')
        
        sptmpfs = ax.bar(bench_res[('sp', 'tmpfs')][0] - bar_width * 2,
                         bench_res[('sp', 'tmpfs')][1],
                         bar_width, color='#DE7A22', label='Spark tmpfs')
        nptmpfs = ax.bar(bench_res[('np', 'tmpfs')][0] - bar_width,
                         bench_res[('np', 'tmpfs')][1], bar_width, alpha=0.6,
                         color='#DE7A22', label='Nipype tmpfs', hatch='/')
        splocal = ax.bar(bench_res[('sp', 'local')][0],
                         bench_res[('sp', 'local')][1],
                         bar_width, color='#20948B', label='Spark local')
        nplocal = ax.bar(bench_res[('np', 'local')][0] + bar_width,
                         bench_res[('np', 'local')][1], bar_width, alpha=0.6,
                         color='#20948B', label='Nipype local', hatch='/')
        splustre = ax.bar(bench_res[('sp', 'lustre')][0] + bar_width * 2,
                          bench_res[('sp', 'lustre')][1],
                          bar_width, color='#1a1aff', label='Spark lustre')
        nplustre = ax.bar(bench_res[('np', 'lustre')][0] + bar_width * 3,
                          bench_res[('np', 'lustre')][1], bar_width, alpha=0.6,
                          color='#1a1aff', label='Nipype lustre', hatch='/')
        
        ax.set_ylabel('Makespan (s)')


        if exp == 4:
            index = arange(4)
        else:
            index = arange(3)

        ax.set_xticks(index + bar_width / 2)
        if exp == 1:
            ax.set_xticklabels((1, 10, 100))
            ax.set_xlabel('Iterations')
        elif exp == 2:
            ax.set_xticklabels(('30', '125', '750'))
            ax.set_xlabel('Number of chunks (Big Brain)')
        elif exp == 3:
            ax.set_xticklabels(('MRI', 'Half BigBrain', 'BigBrain'))
            ax.set_xlabel('Image')
        else:
            ax.set_xticklabels((2.4, 3.44, 7.68, 320))
            ax.set_xlabel('Task duration (s)')

        fig.tight_layout()
        #plt.box(False)
        plt.legend()
        plt.savefig(outf)


def main():
    parser = argparse.ArgumentParser(prog='Generate experiment '
                                     'makespan line graphs')
    parser.add_argument('exp', type=int, choices=[1, 2, 3, 4], 
                        help='Experiment number')
    parser.add_argument('makespan_file', type=str, help='makespan file')
    parser.add_argument('out_file', type=str, help='output_file')
    parser.add_argument('-c', '--condition', type=float,
                        help='If experiment 1 or 4 is selected, it is '
                        'possible to vary processing time or number of '
                        'iterations respectively. Option is ignored for other '
                        'experiments')

    args = parser.parse_args()

    genfig(args.exp, args.makespan_file, args.out_file, args.condition)


if __name__=="__main__":
    main()
