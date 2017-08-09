#!/usr/bin/env python3

import socket
import struct
import pickle
import sys
import os
import signal
import getpass
import pwd
import psutil
import datetime

# Address = ('fernando-linux', 8804)
Address = ('lnls350-linux', 8804)
VERSION = '0.0.0'.encode('utf-8')
MAX_BLOCK_LEN = 1024*4
WAIT_TIME = 10  # in seconds
PICKLE_PROTOCOL = 3
SET_STRUCT_PARAM = "!I 5s"
STATUS = dict(q=1,  # queued
              qu=1.5,  # queued by user
              r=4,  # running
              ru=4.5,  # sched to continue by user
              p=2,  # paused
              pu=2.5,  # paused by the user
              w=3,  # waiting
              t=5,  # terminated
              tu=5.5,  # terminated by the user
              e=6,  # ended
              s=7)  # sent
PROPERTIES = dict(
    description=('{:^20s}', 'Description', lambda x: str(x)),
    user=('{:^10s}', 'User', lambda x: str(x)),
    working_dir=('{:^20s}', 'Working Directory', lambda x: str(x)),
    creation_date=('{:^13s}', 'Creation', lambda x: x.strftime('%m/%d %H:%M')),
    status_key=('{:^8s}', 'Status', lambda x: str(x)),
    hostname=('{:^10s}', 'Hostname', lambda x: x.split('-')[0]),
    priority=('{:^7s}', 'Prior', lambda x: str(x)),
    runninghost=('{:^10s}', 'Run Host', lambda x: (x or "_").split('-')[0]),
    possiblehosts=('{:^20s}', 'Can Run On', lambda x: x if x == 'all' else
                   ','.join([y.split('-')[0] for y in sorted(x)])),
    running_time=('{:^12s}', 'Time Run', lambda x: str(x)))


class _SocketManager:
    def __init__(self, address):
        self.address = address

    def __enter__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.address)
        return self.sock

    def __exit__(self, *ignore):
        self.sock.close()


class ServerDown(Exception):
    pass


def handle_request(*items, wait_for_reply=True, exit_on_err=True):
    InfoStruct = struct.Struct(SET_STRUCT_PARAM)
    data = pickle.dumps(items, PICKLE_PROTOCOL)
    try:
        with _SocketManager(Address) as sock:
            sock.sendall(InfoStruct.pack(len(data), VERSION))
            sock.sendall(data)
            if not wait_for_reply:
                return
            size_data = sock.recv(InfoStruct.size)
            size = InfoStruct.unpack(size_data)[0]
            result = bytearray()
            while True:
                data = sock.recv(MAX_BLOCK_LEN)
                if not data:
                    break
                result.extend(data)
                if len(result) >= size:
                    break
        return pickle.loads(result)
    except socket.error as err:
        print("{0}: is the pyjob_server running?".format(err))
        if exit_on_err:
            sys.exit(1)
        else:
            raise ServerDown()


class JobErr(Exception):
    pass


class Jobs:

    def __init__(self,
                 user=getpass.getuser(),
                 description='generic job',
                 working_dir=None,
                 creation_date=None,
                 status_key=None,
                 hostname=None,
                 possiblehosts=set(),
                 runninghost=None,
                 priority=0,
                 running_time=None,
                 input_files=dict(),  # keys are file names and values the data
                 execution_script=dict(),  # idem to input_files
                 output_files=dict()  # keys -> names values are tuples of data
                 ):                   # and creation/modification times

        self.description = description

        try:
            pwd.getpwnam(user)
        except KeyError:
            raise JobErr('Specified user does not exist')
        else:
            self.user = user

        self.working_dir = os.path.abspath(working_dir or os.getcwd())

        self.creation_date = creation_date or datetime.datetime.now()
        self.status_key = status_key or 'q'
        self.hostname = hostname or socket.gethostname()
        self.priority = priority
        self.runninghost = runninghost
        self.possiblehosts = possiblehosts or 'all'
        self.running_time = running_time or '0:00:00'
        # Load input files
        self.input_files = input_files
        # Load Execution Script
        self.execution_script = execution_script
        # Load output files
        self.output_files = output_files

    @property
    def input_file_names(self):
        return list(self.input_files.keys())

    @property
    def output_file_names(self):
        return list(self.output_files.keys())

    @property
    def execution_script_name(self):
        return list(self.execution_script.keys())[0]

    def __lt__(self, other):
        if not isinstance(other, Jobs):
            return NotImplemented
        if self.priority > other.priority:
            return True
        elif self.priority < other.priority:
            return False
        else:
            if self.creation_date < other.creation_date:
                return True
            return False

    def __eq__(self, other):
        if not isinstance(other, Jobs):
            return NotImplemented
        elif ((not (self < other or other < self)) and
              self.possiblehosts == other.possiblehosts and
              self.status_key == other.status_key):
            return True
        else:
            return False

    def __le__(self, other):
        return True if self == other or self < other else False

    def __repr__(self):
        return representational_form(self)

    def __str__(self):
        return ("description = {0.description},\n"
                "user = {0.user},\n"
                "working_dir = {0.working_dir},\n"
                "creation_date = {0.creation_date},\n"
                "status_key = {0.status_key},\n"
                "hostname = {0.hostname},\n"
                "runninghost = {0.runninghost}\n"
                "possiblehosts = {0.possiblehosts}\n"
                "priority = {0.priority},\n"
                "input_file_names = {0.input_file_names},\n"
                "execution_script_name = {0.execution_script_name},\n"
                "output_file_names = {0.output_file_names}"
                .format(self))

    def __hash__(self):
        return hash(id(self))

    def update(self, other):
        self.description = other.description
        self.user = other.user
        self.working_dir = other.working_dir
        self.creation_date = other.creation_date
        self.status_key = other.status_key
        self.hostname = other.hostname
        self.priority = other.priority
        self.runninghost = other.runninghost
        self.possiblehosts = other.possiblehosts
        self.running_time = other.running_time


class JobView(Jobs):

    def __init__(self, other):
        self.update(other)

    def __repr__(self):
        return representational_form(self)

    def update(self, other):
        super().update(other)


def keyQueue(x):
    return x[1]


class JobQueue(dict):

    def __repr__(self):
        return ("{0}.{1}(".format(self.__class__.__module__,
                                  self.__class__.__name__)
                + super().__repr__() + ")")

    def poplast(self):
        try:
            key = list(self.items())[-1][0]
        except IndexError:
            raise KeyError("poplast(): dictionary is empty")
        return key, self.pop(key)

    def popfirst(self):
        try:
            key = list(self.items())[0][0]
        except IndexError:
            raise KeyError("popfirst(): dictionary is empty")
        return key, self.pop(key)

    def values(self):
        for _, value in sorted(super().items(), key=keyQueue):
            yield value
        if len(self) == 0:
            super().values()

    def items(self):
        for key, value in sorted(super().items(), key=keyQueue):
            yield key, value
        if len(self) == 0:
            super().items()

    def __iter__(self):
        for key, _ in sorted(super().items(), key=keyQueue):
            yield key
        if len(self) == 0:
            super().keys()

    keys = __iter__

    def copy(self):
        return JobQueue(self)

    __copy__ = copy

    def SelAttrVal(self, attr='status_key', value={'r'}):
        newqueue = JobQueue()
        if isinstance(value, set):
            for k, v in self.items():
                if v.__getattribute__(attr) in value:
                    newqueue.update({k: v})
            return newqueue
        for k, v in self.items():
            if value in v.__getattribute__(attr):
                    newqueue.update({k: v})
        return newqueue


class Configs:
    def __init__(self, shutdown=False, MoreJobs=True, niceness=0,
                 defNumJobs=0, Calendar=dict(), active='on',
                 numcpus=None, last_contact=None, running=0, totalJobs=0):
        self.shutdown = shutdown
        self.MoreJobs = MoreJobs
        self.niceness = niceness
        self.defNumJobs = defNumJobs
        self.Calendar = Calendar
        self.active = active
        self.numcpus = numcpus or psutil.cpu_count()
        self.last_contact = last_contact
        self.running = running
        self.totalJobs = totalJobs

    def __repr__(self):
        return representational_form(self)


class MimicsPsutilPopen(psutil.Process):
    def __init__(self, pid=None):
        try:
            super().__init__(pid)
            self.__returncode = None
        except psutil.NoSuchProcess:
            self.__returncode = signal.SIGTERM

    @property
    def returncode(self):
        return self.__returncode

    def poll(self):
        if self.is_running() and self.status() != psutil.STATUS_ZOMBIE:
            self.__returncode = None
        else:
            self.__returncode = signal.SIGTERM
        return self.__returncode

    def send_signal(self, sign):
        super().send_signal(sign)
        self.__returncode = sign

    def kill(self):
        super().kill(signal.SIGKILL)
        self.__returncode = signal.SIGKILL

    def terminate(self):
        super().terminate()
        self.__returncode = signal.SIGTERM


class MyStats:
    def __init__(self, name=None, st_mode=0o664,
                 st_atime=None, st_mtime=None):
        if name is not None:
            file_stats = os.stat(name)
            self.st_mode = file_stats.st_mode
            self.st_atime = file_stats.st_atime
            self.st_mtime = file_stats.st_mtime
        else:
            self.st_mode = st_mode
            self.st_atime = st_atime
            self.st_mtime = st_mtime

    def __repr__(self):
        return representational_form(self)


def createfile(name=None, data=None, stats=MyStats()):
    if not name:
        raise ValueError('Name not specified')
    try:
        os.remove(name)
    except Exception:
        pass

    if isinstance(data, str):
        data = data.encode('utf-8')

    try:
        with open(name, mode='wb') as fh:
            fh.write(data)
    except (IOError, OSError) as err:
        print('Problem with output files:\n', err)
        return None
    mode = stats.st_mode
    os.chmod(name, mode)
    times = None
    if stats.st_atime or stats.st_mtime:
        atime = stats.st_atime
        mtime = stats.st_mtime
        times = (atime, mtime)
    os.utime(name, times=times)


def load_file(name, ignore=False):
    try:
        with open(name, mode='rb') as fh:
            file_data = fh.read()
    except (TypeError, IOError, OSError) as err:
        if not ignore:
            print('Problem with file {0}:\n'.format(name), err)
        return None
    file_stats = MyStats(name)
    return file_stats, file_data


def representational_form(ob):
    form = "{0}.{1}(" + ",  ".join(["{0} = {{2.{0}!r}}".format(x)
                                   for x in sorted(ob.__dict__.keys())
                                   if not x.startswith("_")]) + ")"
    return form.format(ob.__class__.__module__, ob.__class__.__name__, ob)


class JobSelParseErr(Exception):
    pass


def job_selection_parse_options(parser):
    parser.add_option('-j', '--jobs', dest='jobs', type='str',
                      help="list of jobs to interact with [format:"
                      "job1,job2,...")
    parser.add_option('-s', '--status', dest='status', type='str',
                      help="Select the jobs by their status. "
                      "[format: status1,status2,...  default: 'all']")
    parser.add_option('-u', '--user', dest='user', type='str',
                      help="Select the jobs by their user. "
                      "[format: user1,user2,...  default: 'all']")
    parser.add_option('-r', '--runninghost', dest='rhost', type='str',
                      help="Select the jobs by their Running Host. "
                      "[format: host1,host2,...  default: 'all']")
    parser.add_option('-d', '--description', dest='descr', type='str',
                      help="Select the jobs by a part of their description.")
    return parser


def job_selection_parse(opts):

    ok, Queue = handle_request('STATUS_QUEUE')
    if not ok:
        print("I don't know what happened, but the server did not respond"
              "as expected. maybe its a bug")
        return

    if opts.jobs and any((opts.status, opts.user, opts.descr)):
        print('When the option -j is given the other Job Selection Options'
              'can not be used.')
        return

    if opts.jobs is not None:
        try:
            jobs = set([int(x) for x in opts.jobs.split(',')])
        except ValueError as err:
            raise JobSelParseErr(err)
        nonexistent_jobs = list(jobs - set(Queue.keys()))
        if nonexistent_jobs:
            raise JobSelParseErr('These jobs do not exist:' +
                                 ' '.join([str(x) for x in nonexistent_jobs]))
        Queue = JobQueue({k: v for k, v in Queue.items() if k in jobs})

    if opts.status is not None:
        status = set(opts.status.split(','))
        if len(status - STATUS.keys()):
            raise JobSelParseErr(
                'Wrong status specification. Possible values are:' +
                ' '.join(list(k for k, v in sorted(STATUS.items(),
                                                   key=lambda x: x[1]))))
        Queue = Queue.SelAttrVal(attr='status_key', value=status)

    if opts.user is not None:
        user = set(opts.user.split(','))
        Queue = Queue.SelAttrVal(attr='user', value=user)

    if opts.rhost is not None:
        rhost = set(opts.rhost.split(','))
        rhost = set(match_clients(rhost).keys())
        Queue = Queue.SelAttrVal(attr='runninghost', value=rhost)

    if opts.descr is not None:
        Queue = Queue.SelAttrVal(attr='description', value=opts.descr)

    return Queue


class MatchClientsErr(Exception):
    pass


MATCH_RULE = ("It is not necessary to give the full name of the "
              "clients, only a small set of letters can be given, "
              "for example, to specify the client fernando-linux, "
              "only the word lin, or nan could be passed. However, "
              "if other clients match the key they will be selected"
              "too.")


def match_clients(keys2Match, possibleClients=None):
    if not possibleClients:
        ok, data = handle_request('GET_CONFIGS', 'all')
        if ok:
            possibleClients = data
        else:
            raise MatchClientsErr('Could not get configs of server.')

    ConfigsMatched = dict()
    for client in keys2Match:
        keys = set(possibleClients.keys())
        for k in sorted(keys):
            if client.lower() in k.lower():
                ConfigsMatched.update({k: possibleClients.pop(k)})
    if len(ConfigsMatched) < len(keys2Match):
        raise MatchClientsErr("Some keys did not match any client "
                              "in the server's list.")
    return ConfigsMatched


if __name__ == '__main__':
    job = Jobs(priority=1, possiblehosts={'asdf'},
               input_files={'teste': ('las', 2)})
    job2 = Jobs(possiblehosts={'alskdf', 'laksdi'})
    job2view = JobView(job2)
    jobview = JobView(job)
    job.update(job2)
    job.update(jobview)
    jobview.update(job2)
    print(jobview.running_time)
