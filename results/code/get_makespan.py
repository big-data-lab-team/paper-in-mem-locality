#!/usr/bin/env python
import sys
import os

# clear contents of file
with open(sys.argv[2], 'w+') as f:
    pass

for bench in os.listdir(sys.argv[1]):
    start = None
    end = None
    driver_line = None
    if 'np' in bench:
        with open(os.path.join(sys.argv[1], bench), 'r') as f:
            for line in f:
                if 'driver_program' in line:
                    row = line.split(' ')
                    start_time = float(row[1])
                    end_time = float(row[2])
                    if start is None or start_time < start:
                        driver_line = row
                        start = start_time
                    if end is None or end_time > end:
                        end = end_time
                        driver_line[2] = str(end - start)
                        driver_line[1] = "0"

            driver_line[0] = os.path.basename(bench) + ":" + driver_line[0]

    else:
        with open(os.path.join(sys.argv[1], bench), 'r') as f:
            for line in f:
                if 'driver_program' in line:
                    driver_line = line.split(" ")
                    break
            driver_line[0] = (os.path.basename(bench) + ":" + driver_line[0])
            

    with open(sys.argv[2], 'a') as f:
        f.write(" ".join(driver_line))

