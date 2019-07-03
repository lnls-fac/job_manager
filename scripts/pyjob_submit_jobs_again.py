#!/usr/bin/env python-sirius

import os
import subprocess
import argparse


def run_jobs_again(job_type, rms):
    flatfile = 'flatfile.txt'
    input_file = 'input_' + job_type.lower() + '.py'
    exec_file = 'runjob_' + job_type.lower() + '.sh'

    trackcpp_dir = os.getcwd()
    dirs = trackcpp_dir.split(os.sep)
    if len(dirs) > 5:
        label = '-'.join(dirs[-5:]) + '-submitting_again.'
    else:
        label = '-'.join(dirs) + '-submitting_again.'

    for m in rms:
        mlabel = 'rms{0:02d}'.format(m)
        os.chdir(os.path.join(trackcpp_dir, mlabel))
        files = os.listdir(os.getcwd())
        kicktable_files = ','.join(
            [f for f in files if f.endswith('_kicktable.txt')])
        if len(kicktable_files) != 0:
            inputs = ','.join([kicktable_files, flatfile, input_file])
        else:
            inputs = ','.join([flatfile, input_file])
        description = ': '.join([mlabel, job_type.upper(), label])
        p = subprocess.Popen([
            'pyjob_qsub.py', '--inputFiles', inputs, '--exec', exec_file,
            '--description', description])
        p.wait()
        os.chdir(trackcpp_dir)


def main():
    parser = argparse.ArgumentParser(description="Submit jobs again")
    parser.add_argument(
        "type", type=str, help="job type", choices=['ma', 'xy', 'ex'])
    parser.add_argument("rms", type=int, nargs='+', help="rms numbers")
    parser.add_argument(
        "-d", "--directory", type=str, help="trackcpp directory path")
    args = parser.parse_args()

    rms = [int(x) for x in args.rms]
    job_type = args.type

    curdir = os.getcwd()
    if args.directory and any([args.directory.endswith('trackcpp'),
                               args.directory.endswith('trackcpp/')]):
        os.chdir(args.directory)
        run_jobs_again(job_type, rms)
        os.chdir(curdir)
    elif curdir.endswith('trackcpp'):
        run_jobs_again(job_type, rms)
    else:
        print(
            'Change the current working directory to trackcpp directory or '
            'pass its path as argument.')


if __name__ == '__main__':
    main()
