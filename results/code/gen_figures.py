#!/usr/bin/env python
import matplotlib.pyplot as plt
import sys
import argparse


def genfig(exp, makef, outf, cond):
    bench_res = {('sp', 'mem'): ([], []),
                 ('sp', 'tmpfs'): ([], []),
                 ('sp', 'local'): ([], []),
                 ('sp', 'lustre'): ([], []),
                 ('np', 'tmpfs'): ([], []),
                 ('np', 'local'): ([], []),
                 ('np', 'lustre'): ([], [])
                }

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
                if cond is not None and delay == cond:
                    bench_res[(engine, fs)][0].append(iterations)
                    bench_res[(engine, fs)][1].append(float(
                        bench.split(' ')[2]))
                elif cond is None and delay == 3.44:
                    bench_res[(engine, fs)][0].append(iterations)
                    bench_res[(engine, fs)][1].append(float(
                        bench.split(' ')[2]))
            elif exp == 2:
                bench_res[(engine, fs)][0].append(int(chunks))
                bench_res[(engine, fs)][1].append(float(bench.split(' ')[2]))
            elif exp == 3:
                bench_res[(engine, fs)][0].append(im)
                bench_res[(engine, fs)][1].append(float(bench.split(' ')[2]))
            else:
                if cond is not None and iterations == cond:
                    bench_res[(engine, fs)][0].append(delay)
                    bench_res[(engine, fs)][1].append(float(
                        bench.split(' ')[2]))
                elif cond is None and iterations == 10:
                    bench_res[(engine, fs)][0].append(delay)
                    bench_res[(engine, fs)][1].append(float(
                        bench.split(' ')[2]))

        plt.plot(bench_res[('sp', 'mem')][0],
                 bench_res[('sp', 'mem')][1], 
                 'yo', label='Spark in-memory')
        plt.plot(bench_res[('sp', 'tmpfs')][0],
                 bench_res[('sp', 'tmpfs')][1],
                 'go', label='Spark tmpfs')
        plt.plot(bench_res[('sp', 'local')][0],
                 bench_res[('sp', 'local')][1],
                 'bo', label='Spark local')
        plt.plot(bench_res[('sp', 'lustre')][0],
                 bench_res[('sp', 'lustre')][1],
                 'ro', label='Spark lustre')
        plt.plot(bench_res[('np', 'tmpfs')][0],
                 bench_res[('np', 'tmpfs')][1],
                 'g+', label='Nipype tmpfs')
        plt.plot(bench_res[('np', 'local')][0],
                 bench_res[('np', 'local')][1],
                 'b+', label='Nipype local')
        plt.plot(bench_res[('np', 'lustre')][0],
                 bench_res[('np', 'lustre')][1],
                 'r+', label='Nipype lustre')

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
