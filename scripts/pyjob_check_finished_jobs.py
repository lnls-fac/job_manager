#!/usr/bin/env python-sirius

import os
import subprocess
import argparse


def find_rms_dirs(dirpath):
    dirs = [x for x in os.walk(dirpath)];
    for i in range(len(dirs)):
        if any('rms' in x for x  in dirs[i][1]):
            par_dir = dirs[i][0]
            rms_dirs = [os.path.join(par_dir, x) for x in dirs[i][1] if 'rms' in x]
            return par_dir, rms_dirs
    return None, None


def get_running_jobs_dir():
    p = subprocess.Popen(
        ['pyjob_qstat.py', '-c', 'working_dir'], stdout=subprocess.PIPE)
    lines = p.stdout.readlines()
    p.kill()
    running_jobs_dir = {}
    for i in range(1, len(lines)):
        job = [x for x in lines[i].decode("utf-8").split(" ") if len(x) != 0]
        job_id = job[0]
        job_dir = job[1].replace('\n', '')
        if job_dir in running_jobs_dir:
            running_jobs_dir[job_dir].append(job_id)
        else:
            running_jobs_dir[job_dir] = [job_id]
    return running_jobs_dir


def get_running_jobs_descr():
    p = subprocess.Popen(
        ['pyjob_qstat.py', '-c',  'description'], stdout=subprocess.PIPE)
    lines = p.stdout.readlines()
    p.kill()
    running_jobs_descr = {}
    for i in range(1, len(lines)):
        job = [x for x in lines[i].decode("utf-8").split(" ") if len(x) != 0]
        running_jobs_descr[job[0]] = " ".join(job[1:])
    return running_jobs_descr


def main():
    parser = argparse.ArgumentParser(
        description="Check finished jobs in rms directories")
    parser.add_argument("-d", "--directory", type=str, help="search directory")
    args = parser.parse_args()

    curdir = os.getcwd()
    if args.directory:
        os.chdir(args.directory)

    ma_finished, ex_finished, xy_finished = [], [], []
    ma_running, ex_running, xy_running = [], [], []
    ma_not_found, ex_not_found, xy_not_found = [], [], []

    par_dir, rms_dirs = find_rms_dirs(os.getcwd())

    if par_dir is not None and rms_dirs is not None:
        running_jobs_dir = get_running_jobs_dir()
        running_jobs_descr = get_running_jobs_descr()

        for d in rms_dirs:
            rms = d.split(os.sep)[-1]
            files = os.listdir(d)
            job_ids = []
            if any(d in job_dir for job_dir in running_jobs_dir.keys()):
                job_ids = running_jobs_dir[d]

            if 'dynap_ma_out.txt' in files:
                ma_finished.append(rms)
            elif len(job_ids)!= 0 and any('MA:' in running_jobs_descr[job_id].upper() for job_id in job_ids):
                ma_running.append(rms)
            else:
                ma_not_found.append(rms)

            if 'dynap_ex_out.txt' in files:
                ex_finished.append(rms)
            elif len(job_ids)!= 0 and any('EX:' in running_jobs_descr[job_id].upper() for job_id in job_ids):
                ex_running.append(rms)
            else:
                ex_not_found.append(rms)

            if 'dynap_xy_out.txt' in files:
                xy_finished.append(rms)
            elif len(job_ids)!= 0 and any('XY:' in running_jobs_descr[job_id].upper() for job_id in job_ids):
                xy_running.append(rms)
            else:
                xy_not_found.append(rms)

        print("\nDynamic aperture results found in :", par_dir)

        print("\nFinished jobs:")
        print("xy: ", sorted(xy_finished))
        print("ex: ", sorted(ex_finished))
        print("ma: ", sorted(ma_finished))

        print("\nRunning jobs:")
        print("xy: ", sorted(xy_running))
        print("ex: ", sorted(ex_running))
        print("ma: ", sorted(ma_running))

        print("\nNot found:")
        print("xy: ", sorted(xy_not_found))
        print("ex: ", sorted(ex_not_found))
        print("ma: ", sorted(ma_not_found), "\n")

    else:
        print("\nDynamic aperture results not found in :", os.getcwd(), "\n")

    os.chdir(curdir)

if __name__ == '__main__':
    main()
