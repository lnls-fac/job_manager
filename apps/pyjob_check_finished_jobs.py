#!/usr/bin/env python3

import os

def find_rms_dirs(dirpath):
    dirs = [x for x in os.walk(dirpath)];
    for i in range(len(dirs)):
        if any('rms' in x for x  in dirs[i][1]):
            par_dir = dirs[i][0]
            rms_dirs = [os.path.join(par_dir, x) for x in dirs[i][1] if 'rms' in x]
            return par_dir, rms_dirs


par_dir, rms_dirs = find_rms_dirs(os.getcwd())
ma = []
ex = []
xy = []
for d in rms_dirs:
    files = os.listdir(d)
    if 'dynap_ma_out.txt' in files:
        ma.append(d.split(os.sep)[-1])
    if 'dynap_ex_out.txt' in files:
        ex.append(d.split(os.sep)[-1])
    if 'dynap_xy_out.txt' in files:
        xy.append(d.split(os.sep)[-1])

print("\nDynamic aperture results found in :", par_dir)
print("xy: ", sorted(xy))
print("ex: ", sorted(ex))
print("ma: ", sorted(ma), "\n")
