import json
import sys

start_time = None

if len(sys.argv) > 3 and sys.argv[3] == '--nipype':
    with open(sys.argv[1], 'r') as f:
        for line in f:
            l = line.split(' ')

            if l[0] == 'driver_program' and (start_time is None 
                                             or float(l[1]) < start_time):
                start_time = float(l[1])

if start_time is None:
    start_time = 0

with open(sys.argv[1], 'r') as f:
    bench = {}
    bench['tasks'] = []
    count = 0

    driver_start = None
    for line in f:
        l = line.split(' ')
        node = {}
        node['name'] = l[0]
        node['id'] = count
        node['start-time'] = float(l[1]) - start_time
        node['end-time'] = float(l[2]) - start_time
        node['node'] = l[3]

        bench['tasks'].append(node)
        count += 1

    with open(sys.argv[2], 'w') as j:
        json.dump(bench, j)

