#!/usr/bin/python3

import sys
import subprocess
import os
import datetime
import calendar
import time
import signal
import psutil
import shutil
import Global

TEMPFOLDER    = os.path.join(os.path.split(
                             os.path.split(Global.__file__)[0])[0],
                             '.TempFolders')
RESULTSFOLDER = os.path.join(os.path.split(
                             os.path.split(Global.__file__)[0])[0],
                             '.Results')
RECSCRPT = (
'''#!/bin/bash

cp -a {0} {1} ''')
RECSCRPTNAME  = "recover.sh"
FOLDERFORMAT  = 'jobid-{0:08d}'
JOBFILE       = 'pid-{0:d}'
JOBDONE       = 'done'
SUBMITSCR= (
'''#!/bin/bash

echo running {0} on $(hostname) > {1:08d}.out
./{0} >> {1:08d}.out 2> {1:08d}.err
touch {2}
echo job {1:08d} done >> {1:08d}.out ''')
SUBMITSCRNAME = 'run_{0:08d}'
WAIT_TIME = Global.WAIT_TIME


def handle_request(*items):
    return Global.handle_request(*items, exit_on_err = False)

def load_jobs_from_last_run():
    ''' Check if there are jobs running from last call of the script.
    Change the status to ended if the job terminated in between calls
    Change the status to terminated if it was interrupted
    Set status to paused if the job is Stopped in the os'''

    if not os.path.isdir(TEMPFOLDER):
        os.mkdir(path=TEMPFOLDER)
    for folder in os.listdir(path=TEMPFOLDER):
        if os.path.isdir(os.path.join(TEMPFOLDER,folder)):
            jobid = int(folder.split('-')[1])
            for file in os.listdir(path='/'.join([TEMPFOLDER,folder])):
                if file.startswith( JOBFILE[:4]):
                    pid = int(file.split('-')[1])
                    break
            with open('/'.join([TEMPFOLDER,folder,JOBFILE.format(pid)])) as fh:
                file_data = fh.read()

            proc = Global.MimicsPsutilPopen(pid = pid)
            job = eval(file_data)
            state = proc.poll()
            folder = FOLDERFORMAT.format(jobid)
            if (state is not None or
                proc.name not in '/'.join([folder,
                                           SUBMITSCRNAME.format(jobid)])):
                if os.path.isfile('/'.join([TEMPFOLDER,folder,JOBDONE])):
                    job.status_key = 'e'
                else:
                    job.status_key = 't'
            else:
                if proc.status == 'stopped':
                    job.status_key = 'p'
            jobid2proc.update({jobid : proc})
            MyQueue.update({jobid : job})

def signal_handler(sig, frame):
    if sig == signal.SIGTERM:
        shutdown()
    elif sig == signal.SIGUSR1:
        pause()

def shutdown():
    ok = handle_request('GOODBYE', True)
    if ok:
        sys.exit()
    sys.exit(1)

def pause():
    handle_request('GOODBYE', False)
    time.sleep(2*WAIT_TIME)

def check_running_jobs():

        # get the time consumed by the job so far
    def get_time_process(proc):
        a = proc.get_cpu_times()
        time = a.system+a.user
        proc_list = proc.get_children()
        for proc in proc_list:
            time += get_time_process(proc)
        return time

    count = 0
    for jobid, proc in jobid2proc.items():
        state = proc.poll()
        folder = FOLDERFORMAT.format(jobid)
        if state is not None:
            if os.path.isfile('/'.join([TEMPFOLDER,folder,JOBDONE])):
                MyQueue[jobid].status_key = 'e'
            else:
                if MyQueue[jobid].status_key != 'q':
                    MyQueue[jobid].status_key = 't'
        else:
            if proc.status in {'running','sleeping'}:
                count +=1
            a = get_time_process(proc)
            a = str(datetime.timedelta(seconds=int(a)))
            MyQueue[jobid].running_time = a
    MyConfigs.running = count
    return count


def deal_with_finished_jobs():
    for k, v in MyQueue.items():
        if v.status_key in {'e', 't', 'q'}:
            folder = '/'.join([TEMPFOLDER, FOLDERFORMAT.format(k)])
            files = os.listdir(path=folder)
            for file in set(files) - (v.input_files.keys() |
                                       set([JOBDONE,
                                            SUBMITSCRNAME.format(k)])):
                data = Global.load_file(os.path.join(folder,file))
                v.output_files.update({file: data})
            for file in v.input_files.keys(): # Reload the input_files
                data = Global.load_file(os.path.join(folder,file))
                v.input_files.update({file: data})
            MyQueue.update({k:v})

def deal_with_configs():

    # set nice of the job and its children
    def set_nice_process(proc):
        if proc.get_nice() < MyConfigs.niceness:
            proc.set_nice(MyConfigs.niceness)
        proc_list = proc.get_children()
        for proc in proc_list:
            set_nice_process(proc)

    agora = datetime.datetime.now()
    allowed = MyConfigs.Calendar.get((calendar.day_name[agora.weekday()],
                                      agora.hour, agora.minute),
                                     MyConfigs.defNumJobs)
    for proc in jobid2proc.values():
        state = proc.poll()
        if state is None:
            set_nice_process(proc)

    return allowed

def submit_jobs(NewQueue):
    for k, v in NewQueue.items():
        #create temporary directory
        tempdir = '/'.join([TEMPFOLDER,FOLDERFORMAT.format(k)])
        os.mkdir(tempdir)
        #create files
        Global.createfile(name ='/'.join([tempdir,SUBMITSCRNAME.format(k)]),
                          data =SUBMITSCR.format(v.execution_script_name, k,
                                                 JOBDONE),
                          stats= Global.MyStats(st_mode=0o774))
        for name, info in v.execution_script.items():
            Global.createfile(name = '/'.join([tempdir,name]),
                              data = info[1], stats = info[0])
        for name, info in v.input_files.items():
            Global.createfile(name='/'.join([tempdir, name]),
                              data = info[1], stats = info[0])
        for name, info in v.output_files.items():
            Global.createfile(name='/'.join([tempdir, name]),
                              data = info[1], stats = info[0])
        #submit job
        proc = psutil.Popen('/'.join([tempdir, SUBMITSCRNAME.format(k)]),
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            start_new_session=True,
                            cwd = tempdir)
        #update queues
        v.status_key = 'r'
        proc.set_nice(MyConfigs.niceness)
        MyQueue.update({k:v})
        jobid2proc.update({k:proc})
        #create job file for, if necessary, later loading
        Global.createfile(name = '/'.join([tempdir,JOBFILE.format(proc.pid)]),
                          data = repr(v))

def stop_jobs(jobs2Stop = 0):
    RunningQueue = MyQueue.SelAttrVal(attr='status_key', value={'r'})
    for _ in range(jobs2Stop):
        k, v = RunningQueue.poplast()
        os.killpg(jobid2proc[k].pid,signal.SIGSTOP)
        v.status_key = 'p'
        MyQueue.update({k:v})

def continue_stopped_jobs(jobs2Continue):
    StoppedQueue = MyQueue.SelAttrVal(attr='status_key', value={'p'})
    if not len(StoppedQueue): return 0
    RunningQueue = MyQueue.SelAttrVal(attr='status_key', value={'r'})
    numJobsRunning = len(RunningQueue)
    RunningQueue.update(StoppedQueue)
    total = numJobsRunning + jobs2Continue
    i = 0
    for k, v in RunningQueue.items():
        if i < total:
            os.killpg(jobid2proc[k].pid,signal.SIGCONT)
            v.status_key = 'r'
            i +=1
        else:
            os.killpg(jobid2proc[k].pid,signal.SIGSTOP)
            v.status_key = 'p'
        MyQueue.update({k:v})
    return i - numJobsRunning

def deal_with_results(ResQueue):

    for k, v in ResQueue.items():
        working_dir = v.working_dir
        try:
            with open(os.path.join(working_dir,'test'),mode='w') as fh:
                fh.write('teste')
            os.remove(os.path.join(working_dir,'test'))
        except PermissionError:
            if not os.path.isdir(RESULTSFOLDER):
                os.mkdir(path=RESULTSFOLDER)
            working_dir = '/'.join([RESULTSFOLDER,FOLDERFORMAT.format(k)])
            os.mkdir(working_dir)

        files = []
        for name, content in v.output_files.items():
            if not name.startswith(JOBFILE[0:4]):
                files.append(name)
                Global.createfile(name = os.path.join(working_dir,name),
                                  data = content[1], stats = content[0])
        for name, content in v.input_files.items():
            files.append(name)
            Global.createfile(name = os.path.join(working_dir,name),
                              data = content[1], stats = content[0])

        # create script to copy the files to the right folder
        if working_dir != v.working_dir:
            rec_file = RECSCRPT.format(working_dir + '/{'+','.join(files)+'}',
                                      v.working_dir)
            Global.createfile(name = '/'.join([working_dir,RECSCRPTNAME]),
                              data = rec_file,
                              stats = Global.MyStats(st_mode=0o774))

def deal_with_signals(Jobs2Sign):
    # Verify if the server thinks we have a job that we don't and return
    # them to the queue:
    NotMine = Global.JobQueue()
    isbigger = set(Jobs2Sign.keys()) - set(MyQueue.keys())
    for k in isbigger:
        v = Jobs2Sign.pop(k)
        v.status_key  = 'q'
        v.runninghost = None
        NotMine.update({k:v})

    #Deal with jobs which are really ours:
    for k, v in Jobs2Sign.items():
        if MyQueue[k].status_key in {'e','t','q'}:
            continue
        try:
            if v.status_key in {'pu','tu','ru','qu'}:
                if v.status_key == 'pu':
                    os.killpg(jobid2proc[k].pid, signal.SIGSTOP)
                elif v.status_key == 'tu':
                    os.killpg(jobid2proc[k].pid, signal.SIGTERM)
                    os.killpg(jobid2proc[k].pid, signal.SIGCONT)
                    v.status_key = 't'
                elif v.status_key == 'ru':
                    if MyQueue[k].status_key == 'pu':
                        v.status_key = 'p'
                    else:
                        v.status_key = MyQueue[k].status_key
                elif v.status_key == 'qu':
                    os.killpg(jobid2proc[k].pid, signal.SIGTERM)
                    os.killpg(jobid2proc[k].pid, signal.SIGCONT)
                    v.runninghost = None
                    v.status_key = 'q'
                MyQueue[k].update(v)
            elif v != MyQueue[k] and v.status_key == MyQueue[k].status_key:
                MyQueue[k].update(v)
        except ProcessLookupError:
            continue
    return NotMine

def wait_for_server():
    time.sleep(WAIT_TIME)



MyQueue = Global.JobQueue()
jobid2proc = dict()
MyConfigs = Global.Configs()

def main():

    load_jobs_from_last_run()
    deal_with_finished_jobs()

    global MyConfigs
    try:
        ok, data = handle_request('GIME_CONFIGS', MyConfigs)
        if ok:
            MyConfigs = data
    except Global.ServerDown:
        wait_for_server()


    while True:
        try:
            MyConfigs.totalJobs = len(MyQueue)
            num_running = check_running_jobs()
            deal_with_finished_jobs()
            num_allowed = deal_with_configs()
            jobs2Continue = num_allowed - num_running
            if jobs2Continue >= 0:
                continued = continue_stopped_jobs(jobs2Continue)
                jobs2Submit = jobs2Continue - continued
                if jobs2Submit > 0 and MyConfigs.MoreJobs:
                    ok, NewQueue = handle_request('GIME_JOBS',jobs2Submit)
                    if ok:
                        submit_jobs(NewQueue)
            elif jobs2Continue < 0 :
                jobs2Stop = -jobs2Continue
                stop_jobs(jobs2Stop)

            time.sleep(WAIT_TIME)


            ok, Queue2Deal = handle_request('STATUS_QUEUE', True)
            NotMine = Global.JobQueue()
            if ok:
                NotMine = deal_with_signals(Queue2Deal)

            #These are jobviews of the jobs we don't have:
            Queue2Send = Global.JobQueue()
            for k,v in NotMine.items():
                Queue2Send.update({k:v})
            # Just send the complete jobs if needed, otherwise send jobviews
            for k, v in MyQueue.items():
                if v.status_key in {'e','t','q'}:
                    Queue2Send.update({k:v})
                else:
                    if Queue2Deal.get(k) is not None:
                        Queue2Send.update({k:Global.JobView(v)})
                    else:
                        Queue2Send.update({k:v})

            deal_with_finished_jobs()

            ok, keys2remove = handle_request('UPDATE_JOBS', Queue2Send)
            if ok:
                for key in keys2remove:
                    jobid2proc.pop(key)
                    MyQueue.pop(key)
                    shutil.rmtree('/'.join([TEMPFOLDER,
                                            FOLDERFORMAT.format(key)]))


            ok, ResQueue = handle_request('GIME_RESULTS')
            if ok:
                deal_with_results(ResQueue)

            ok, data = handle_request('GIME_CONFIGS',MyConfigs)
            if ok:
                MyConfigs = data

            if MyConfigs.shutdown:
                shutdown()
        except psutil._error.NoSuchProcess:
            continue
        except Global.ServerDown:
            wait_for_server()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGUSR1, signal_handler)
    proclist = psutil.get_process_list()
    mod_name = os.path.split(__file__)[1]
    mypid = os.getpid()
    for proc in proclist:
        try:
            for cmdline in proc.cmdline:
                if mod_name in cmdline and proc.pid != mypid:
                    print('There is already one instance of {0}'
                          ' running on this computer: exiting'.format(mod_name))
                    sys.exit(1)
        except psutil._error.NoSuchProcess:
            continue
    main()
