#!/usr/bin/env python3

import os
import sys
import subprocess

right_inputs = False
if len(sys.argv) > 2 :
    tp = sys.argv[1]
    rms = [int(x) for x in sys.argv[2:]]
    if tp in ['ma', 'ex', 'xy']: right_inputs = True

curdir = os.getcwd()
if right_inputs:
    if curdir.endswith('trackcpp'):
        flatfile = 'flatfile.txt'
        input_file = 'input_' + tp.lower() + '.py'
        exec_file = 'runjob_' + tp.lower() + '.sh'
        dirs = curdir.split(os.sep)
        label = '-'.join(dirs[-5:]) + '-submitting_again.'
        for m in rms:
            mlabel = 'rms%02i'%m
            os.chdir(os.path.join(curdir, mlabel))
            files = os.listdir(os.getcwd())
            kicktable_files = ','.join([f for f in files if f.endswith('_kicktable.txt')])
            if len(kicktable_files) != 0:
                inputs = ','.join([kicktable_files, flatfile,input_file])
            else:
                inputs = ','.join([flatfile,input_file])
            description = ': '.join([mlabel, tp.upper(), label])
            p = subprocess.Popen(['pyjob_qsub.py', '--inputFiles', inputs, '--exec', exec_file, '--description', description])
            p.wait()
            os.chdir(curdir)
    else:
        print('Change the current working directory to trackcpp directory.')
else:
    print('Invalid inputs')
