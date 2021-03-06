#!/usr/bin/env python-sirius

import sys
import subprocess
import os
import datetime
import calendar
import time
import signal
import psutil
import shutil

import pyjob
from pyjob import WAIT_TIME, handle_request as _handle_request, ServerDown,\
    JobQueue, Configs, MimicsPsutilPopen, createfile, load_file, MyStats, \
    JobQueue, JobView

TMPFLDR = os.path.join(os.path.expanduser('~'), '.pyjob', 'TempFolders')
RESFLDR = os.path.join(os.path.expanduser('~'), '.pyjob', 'Results')
RECSCRPT = (
    '''#!/bin/bash

    cp -a {0} {1} ''')
RECSCRPTNAME = "recover.sh"
FOLDERFORMAT = 'jobid-{0:08d}'
JOBFILE = 'pid-{0:d}'
JOBDONE = 'done'
SUBMITSCR = (
    '''#!/bin/bash

    echo running {0} on $(hostname) > {1:08d}.out
    ./{0} >> {1:08d}.out 2> {1:08d}.err
    touch {2}
    echo job {1:08d} done >> {1:08d}.out ''')
SUBMITSCRNAME = 'run_{0:08d}'


def handle_request(*items):
    server_down = True
    while server_down:
        try:
            result = _handle_request(*items, exit_on_err=False)
            server_down = False
        except ServerDown:
            time.sleep(WAIT_TIME)
            locally_manage_jobs()
    return result


def signal_handler(sig, frame):
    if sig == signal.SIGTERM:
        shutdown()
    elif sig == signal.SIGUSR1:
        pause()


def shutdown():
    ok = handle_request('GOODBYE', True)
    sys.exit(0 if ok else 1)


def pause():
    handle_request('GOODBYE', False)
    time.sleep(2*WAIT_TIME)


MyQueue = JobQueue()
jobid2proc = dict()
MyConfigs = Configs()


def load_jobs_from_last_run():
    ''' Check if there are jobs running from last call of the script.
    Change the status to ended if the job terminated in between calls
    Change the status to terminated if it was interrupted
    Set status to paused if the job is Stopped in the os'''

    if not os.path.isdir(TMPFLDR):
        os.mkdir(path=TMPFLDR)
    for folder in os.listdir(path=TMPFLDR):
        if os.path.isdir(os.path.join(TMPFLDR, folder)):
            jobid = int(folder.split('-')[1])
            for file in os.listdir(path='/'.join([TMPFLDR, folder])):
                if file.startswith(JOBFILE[:4]):
                    pid = int(file.split('-')[1])
                    break
            with open('/'.join([TMPFLDR, folder, JOBFILE.format(pid)])) as fh:
                file_data = fh.read()

            proc = MimicsPsutilPopen(pid=pid)
            job = eval(file_data)
            state = proc.poll()
            folder = FOLDERFORMAT.format(jobid)
            if (state is not None or proc.name() not in '/'.join([
                                    folder, SUBMITSCRNAME.format(jobid)])):
                if os.path.isfile('/'.join([TMPFLDR, folder, JOBDONE])):
                    job.status_key = 'e'
                else:
                    job.status_key = 't'
            else:
                if proc.status() == psutil.STATUS_STOPPED:
                    job.status_key = 'p'
            jobid2proc.update({jobid: proc})
            MyQueue.update({jobid: job})


def get_and_deal_with_configs():

    # set nice of the job and its children
    def set_nice_process(proc):
        try:
            if proc.nice() < MyConfigs.niceness:
                proc.nice(MyConfigs.niceness)
            proc_list = proc.children(recursive=True)
            for pr in proc_list:
                pr.nice(MyConfigs.niceness)
        except psutil.NoSuchProcess:
            return

    # get configs from server
    global MyConfigs
    agora = datetime.datetime.now()
    ok, data = handle_request('GIME_CONFIGS', MyConfigs)
    if ok:
        MyConfigs = data
        agora = MyConfigs.last_contact  # It is preferable to use server clock.
    # shutdown if requested:
    if MyConfigs.shutdown:
        shutdown()
    # set niceness of processess
    for proc in jobid2proc.values():
        state = proc.poll()
        if state is None:
            set_nice_process(proc)

    # returns the number of jobs that can run in this client now:
    allowed = MyConfigs.Calendar.get(
        (calendar.day_name[agora.weekday()], agora.hour, agora.minute),
        MyConfigs.defNumJobs)
    return allowed


def get_new_jobs_and_submit(njobstoget):

    # If it still can run more jobs, I ask the server for new ones:
    if (njobstoget > 0) and MyConfigs.MoreJobs:
        ok, NewQueue = handle_request('GIME_JOBS', njobstoget)
        if not ok:
            return
        for k, v in NewQueue.items():
            # create temporary directory
            tempdir = '/'.join([TMPFLDR, FOLDERFORMAT.format(k)])
            os.mkdir(tempdir)
            # create files
            createfile(
                name='/'.join([tempdir, SUBMITSCRNAME.format(k)]),
                data=SUBMITSCR.format(v.execution_script_name, k, JOBDONE),
                stats=MyStats(st_mode=0o774))
            for name, info in v.execution_script.items():
                createfile(
                    name='/'.join([tempdir, name]),
                    data=info[1], stats=info[0])
            for name, info in v.input_files.items():
                createfile(
                    name='/'.join([tempdir, name]),
                    data=info[1], stats=info[0])
            for name, info in v.output_files.items():
                createfile(
                    name='/'.join([tempdir, name]),
                    data=info[1], stats=info[0])
            # submit job
            proc = psutil.Popen(
                '/'.join([tempdir, SUBMITSCRNAME.format(k)]),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True, cwd=tempdir)
            # update queues
            v.status_key = 'r'
            proc.nice(MyConfigs.niceness)
            MyQueue.update({k: v})
            jobid2proc.update({k: proc})
            # create job file to be loaded later, if necessary:
            createfile(
                name='/'.join([tempdir, JOBFILE.format(proc.pid)]),
                data=repr(v))


def locally_manage_jobs(allowed=None):  # returns njobstoget

    # get the time consumed by the job so far
    def get_time_process(proc):
        try:
            a = proc.cpu_times()
            time = a.system+a.user
            proc_list = proc.children()
            for proc in proc_list:
                time += get_time_process(proc)
            return time
        except psutil.NoSuchProcess:
            return 0

    # find out which jobs are finished
    count = 0
    for jobid, proc in jobid2proc.items():
        folder = FOLDERFORMAT.format(jobid)
        if proc.poll() is not None:
            if os.path.isfile('/'.join([TMPFLDR, folder, JOBDONE])):
                MyQueue[jobid].status_key = 'e'
            else:
                if MyQueue[jobid].status_key != 'q':
                    MyQueue[jobid].status_key = 't'
        else:
            statuses = {psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING}
            if proc.status() in statuses:
                count += 1
            a = get_time_process(proc)
            a = str(datetime.timedelta(seconds=int(a)))
            MyQueue[jobid].running_time = a
    MyConfigs.running = count

    # load data from finished and stopped jobs
    for k, v in MyQueue.items():
        if v.status_key in {'e', 't', 'q'}:
            folder = '/'.join([TMPFLDR, FOLDERFORMAT.format(k)])
            files = os.listdir(path=folder)
            st = v.input_files.keys() | set([JOBDONE, SUBMITSCRNAME.format(k)])
            for file in set(files) - st:
                data = load_file(os.path.join(folder, file))
                v.output_files.update({file: data})
            for file in v.input_files.keys():  # Reload the input_files
                data = load_file(os.path.join(folder, file))
                v.input_files.update({file: data})
            MyQueue.update({k: v})

    # Get the number of jobs that can run in this client now:
    agora = MyConfigs.last_contact or datetime.datetime.now()
    if allowed is None:
        agora = datetime.datetime.now()
        allowed = MyConfigs.Calendar.get(
            (calendar.day_name[agora.weekday()], agora.hour, agora.minute),
            MyConfigs.defNumJobs)

    # Stop or continue the jobs in this client
    i = 0
    CanChangeQueue = MyQueue.SelAttrVal(attr='status_key', value={'p', 'r'})
    for k, v in CanChangeQueue.items():
        try:
            if i < allowed:
                os.killpg(jobid2proc[k].pid, signal.SIGCONT)
                v.status_key = 'r'
                i += 1
            else:
                os.killpg(jobid2proc[k].pid, signal.SIGSTOP)
                v.status_key = 'p'
            MyQueue.update({k: v})
        except ProcessLookupError:
            continue
    njobstoget = allowed - i

    # updated number of jobs in this client
    MyConfigs.totalJobs = len(MyQueue)

    NumJobs = MyConfigs.Calendar.get(
        (calendar.day_name[agora.weekday()], agora.hour, agora.minute),
        MyConfigs.defNumJobs)
    print(
        '{0:19s}: NJPermtd={1:03d}, '.format(
            agora.strftime('%Y/%m/%d %H:%M:%S'), MyConfigs.totalJobs) +
        'NJRecvd={0:03d},  NJRunning={1:03d}'.format(
            NumJobs, MyConfigs.running))
    return njobstoget if njobstoget > 0 else 0


def get_and_deal_with_job_signals():  # returns NotMine

    # Get jobviews from server:
    ok, Queue2Deal = handle_request('STATUS_QUEUE', True)
    if not ok:
        return

    # Verify if the server thinks this client has jobs which it doesn't and
    # return them to the queue in case they are not finished yet:
    NotMine = JobQueue()
    isbigger = set(Queue2Deal.keys()) - set(MyQueue.keys())
    for k in isbigger:
        v = Queue2Deal.pop(k)
        if v.status_key not in {'e', 't'}:
            v.status_key = 'q'
            v.runninghost = None
            NotMine.update({k: v})

    # Deal with jobs which are really ours:
    for k, v in Queue2Deal.items():
        if MyQueue[k].status_key in {'e', 't', 'q'}:
            continue
        try:
            if v.status_key == 'pu':
                os.killpg(jobid2proc[k].pid, signal.SIGSTOP)
                MyQueue[k].update(v)
            elif v.status_key == 'tu':
                os.killpg(jobid2proc[k].pid, signal.SIGTERM)
                os.killpg(jobid2proc[k].pid, signal.SIGCONT)
                v.status_key = 't'
                MyQueue[k].update(v)
            elif v.status_key == 'ru':
                if MyQueue[k].status_key == 'pu':
                    # set to paused and the manager decides later
                    v.status_key = 'p'
                else:
                    v.status_key = MyQueue[k].status_key
                MyQueue[k].update(v)
            elif v.status_key == 'qu':
                os.killpg(jobid2proc[k].pid, signal.SIGTERM)
                os.killpg(jobid2proc[k].pid, signal.SIGCONT)
                v.runninghost = None
                v.status_key = 'q'
                MyQueue[k].update(v)
            elif v != MyQueue[k] and v.status_key == MyQueue[k].status_key:
                MyQueue[k].update(v)  # in case other parameter was changed
        except ProcessLookupError:
            continue
    # Send jobs which are not in this client
    return NotMine


def update_jobs_on_server_and_remove_finished_jobs(Queue2Send):
    for k, v in MyQueue.items():
        if v.status_key in {'e', 't', 'q'}:
            Queue2Send.update({k: v})
        else:
            Queue2Send.update({k: JobView(v)})

    ok, keys2remove = handle_request('UPDATE_JOBS', Queue2Send)
    if ok:
        for key in keys2remove:
            jobid2proc.pop(key)
            MyQueue.pop(key)
            shutil.rmtree('/'.join([TMPFLDR, FOLDERFORMAT.format(key)]))


def get_results_from_server_and_save():
    ok, ResQueue = handle_request('GIME_RESULTS')
    if not ok:
        return

    for k, v in ResQueue.items():
        working_dir = v.working_dir
        try:
            with open(os.path.join(working_dir, 'test'), mode='w') as fh:
                fh.write('teste')
            os.remove(os.path.join(working_dir, 'test'))
        except PermissionError:
            if not os.path.isdir(RESFLDR):
                os.mkdir(path=RESFLDR)
            working_dir = '/'.join([RESFLDR, FOLDERFORMAT.format(k)])
            os.mkdir(working_dir)

        files = []
        for name, content in v.output_files.items():
            if not name.startswith(JOBFILE[0:4]):
                files.append(name)
                createfile(
                    name=os.path.join(working_dir, name),
                    data=content[1], stats=content[0])
        for name, content in v.input_files.items():
            files.append(name)
            createfile(
                name=os.path.join(working_dir, name),
                data=content[1], stats=content[0])

        # create script to copy the files to the right folder
        if working_dir != v.working_dir:
            rec_file = RECSCRPT.format(working_dir + '/{'+','.join(files)+'}',
                                       v.working_dir)
            createfile(
                name='/'.join([working_dir, RECSCRPTNAME]),
                data=rec_file, stats=MyStats(st_mode=0o774))


def main():

    load_jobs_from_last_run()
    locally_manage_jobs()

    while True:
        # Get configuration from server and deal with it
        njobsallowed = get_and_deal_with_configs()

        njobstoget = locally_manage_jobs(allowed=njobsallowed)
        get_new_jobs_and_submit(njobstoget)

        time.sleep(WAIT_TIME)

        # Returns jobviews of the jobs this client doesn't have:
        Queue2Send = get_and_deal_with_job_signals()
        locally_manage_jobs(allowed=njobsallowed)

        # Only send the complete jobs if needed, otherwise send jobviews
        update_jobs_on_server_and_remove_finished_jobs(Queue2Send)

        get_results_from_server_and_save()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGUSR1, signal_handler)
    mod_name = os.path.split(__file__)[1]
    mypid = os.getpid()
    for pr in psutil.process_iter():
        try:
            cmdline = pr.cmdline()
            pid = pr.pid
        except psutil.NoSuchProcess:
            pass
        else:
            for cmdl in cmdline:
                if mod_name in cmdl and pid != mypid:
                    print('There is already one instance of ' + mod_name +
                          ' running on this computer: exiting')
                    sys.exit(1)
    main()
