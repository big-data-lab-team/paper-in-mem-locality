import json
import sys


with open(sys.argv[1], 'r') as f:
    bench = {}
    bench['tasks'] = []
    count = 0

    driver_start = None
    for line in f:

        experiment = line.split(':')[0]
        l = line.split(' ')
        l[0] = l[0].replace(experiment + ':', '')
        print(l)
        node = {}
        node['name'] = l[0]
        node['id'] = count
        node['start-time'] = float(l[1])
        node['end-time'] = float(l[2]) - node['start-time']
        node['node'] = experiment

        bench['tasks'].append(node)
        count += 1

    with open(sys.argv[2], 'w') as j:
        json.dump(bench, j)

