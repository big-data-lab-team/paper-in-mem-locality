#!/usr/bin/env python
import matplotlib.pyplot as plt
import sys
import argparse

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
            y_labels = [y[1] for y in bench_res[key]]
            bench_res[key] = (x_labels, y_labels)

        plt.plot(bench_res[('sp', 'mem')][0],
                 bench_res[('sp', 'mem')][1], 
                 'y', label='Spark in-memory')
        plt.plot(bench_res[('sp', 'tmpfs')][0],
                 bench_res[('sp', 'tmpfs')][1],
                 'g', label='Spark tmpfs')
        plt.plot(bench_res[('sp', 'local')][0],
                 bench_res[('sp', 'local')][1],
                 'b', label='Spark local')
        plt.plot(bench_res[('sp', 'lustre')][0],
                 bench_res[('sp', 'lustre')][1],
                 'r', label='Spark lustre')
        plt.plot(bench_res[('np', 'tmpfs')][0],
                 bench_res[('np', 'tmpfs')][1],
                 'g--', label='Nipype tmpfs')
        plt.plot(bench_res[('np', 'local')][0],
                 bench_res[('np', 'local')][1],
                 'b--', label='Nipype local')
        plt.plot(bench_res[('np', 'lustre')][0],
                 bench_res[('np', 'lustre')][1],
                 'r--', label='Nipype lustre')

        #plt.xlim(0, 20)
        #plt.ylim(0, 200)
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
