#!/usr/bin/env python-sirius

import optparse
import os
import Global
from builtins import isinstance

def main():
    # Begin the creation of the job
    job = Global.Jobs()

    # configuration of the parser for the arguments
    parser = optparse.OptionParser()
    parser.add_option('-d','--description',dest='description',type='str',
                      help="description of the job [default: 'generic job']")
    parser.add_option('-e','--exec',dest='exec_script_name',type='str',
                      help=("name of the script to run in the cluster "
                            "[no Default, mandatory]"))
    parser.add_option('-i','--inputFiles',dest='input_file_names',type='str',
                      help=("name of the input files needed to run the job."
                            " For more than one file, separate the names with"
                            " commas and no space. [default: '']"))
    parser.add_option('-w','--workingDirectory',dest='work_dir',type='str',
                      help=("Working directory of the job: where the execution"
                            " file and input files are and the results will "
                            "be put [default: pwd]"))
    parser.add_option('-p','--priority',dest='prior',type='int',
                      help=("Integer which specify the priority of the job. "
                            "Higher numbers have higher priority (negative "
                            "allowed) [default: 0]"))
    parser.add_option('-H', '--possibleHosts', dest='hosts', type='str',
                     help="Set the list of possible hosts to run the"
                     "jobs [format: append=host1,host2,... or set=host1,host2"
                     ",... default = 'all'. " + Global.MATCH_RULE)

    parser.set_description(description='This command submit one job to the '
                           ' queue.')
    (opts, _) = parser.parse_args()

    # Load execution script
    if opts.exec_script_name is None:
        print('Job not submitted: must specify -e or --exec option')
        return
    else:
        data = Global.load_file(opts.exec_script_name)
        if data is not None:
            script_name = os.path.split(opts.exec_script_name)[-1]
            job.execution_script.update({script_name:data})
        else:
            print('Error loading Files')
            return
    #Load name
    if opts.description is not None:
        job.description = opts.description

    #Load working_dir
    if opts.work_dir is not None:
        job.working_dir = os.path.abspath(opts.work_dir)

    #Load priority
    if opts.prior is not None:
        if isinstance(opts.prior, int):
            job.priority = int(float(opts.prior))
        else:
            print('Could not set the priority. Using default')


    hosts = 'all'
    if opts.hosts is not None:
        if opts.hosts != 'all':
            keys2Match = set(opts.hosts.split(','))
            try:
                hosts = set(Global.match_clients(keys2Match).keys())
            except Global.MatchClientsErr as err:
                print(err)
                return

    job.possiblehosts = hosts

    if opts.input_file_names is not None:
        input_file_names = opts.input_file_names.split(',')
        for name in input_file_names:
            data = Global.load_file(name)
            if data is not None:
                name = os.path.split(name)[-1]
                job.input_files.update({name: data})
            else:
                print('Error loading Files')
                return

    ok, data = Global.handle_request('NEW_JOB',job)

    if ok:
        print('Success. Job id is :', data)



main()
