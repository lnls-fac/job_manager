#!/usr/bin/env python-sirius

# import remote_machine
import datetime
import socketserver
import struct
import threading
import signal
import pickle
import sys
import os
import socket
from pyjob import Address, VERSION, MAX_BLOCK_LEN, PICKLE_PROTOCOL, WAIT_TIME,\
    SET_STRUCT_PARAM, STATUS, PROPERTIES, JobQueue, JobView, Jobs, load_file, \
    createfile

CONFIGFOLDER = os.path.join(os.path.expanduser('~'), '.pyjob', 'Configs')
IDGEN_FILENAME = 'last_id'
QUEUE_FILENAME = 'Queue'
CONFIGS_FILENAME = 'clients_configs'
SUBMITTED_FILENAME = 'submitted_jobs.txt'


class ManageJobsServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


class Finish(Exception):
    pass


class RequestHandler(socketserver.StreamRequestHandler):

    ConfigsLock = threading.Lock()
    QueueLock = threading.Lock()
    CallLock = threading.Lock()
    IdGenLock = threading.Lock()

    Configs = dict()
    IdGen = 1
    Queue = None
    Call = dict(
        GIME_JOBS=(
            lambda self, *args: self.send_job_from_queue(*args)),
        GIME_RESULTS=(
            lambda self, *args: self.send_results_to_host(*args)),
        NEW_JOB=(
            lambda self, *args: self.add_new_job_to_queue(*args)),
        UPDATE_JOBS=(
            lambda self, *args: self.update_jobs_in_queue(*args)),
        CHANGE_JOBS_REQUEST=(
            lambda self, *args: self.change_jobs_request(*args)),
        SIGNAL_JOBS=(
            lambda self, *args: self.send_signal_jobs(*args)),
        STATUS_QUEUE=(
            lambda self, *args: self.print_queue_status(*args)),
        GIME_CONFIGS=(
            lambda self, *args: self.send_configs_to_client(*args)),
        SET_CONFIGS=(
            lambda self, *args: self.set_configs_of_clients(*args)),
        GET_CONFIGS=(
            lambda self, *args: self.get_configs_of_clients(*args)),
        SHUTDOWN=(
            lambda self, *args: self.shutdown(*args)),
        GOODBYE=(
            lambda self, *args: self.client_shutdown(*args))
        )

    def handle(self):
        InfoStruct = struct.Struct(SET_STRUCT_PARAM)
        info = self.rfile.read(InfoStruct.size)
        size, version = InfoStruct.unpack(info)
        data = pickle.loads(self.rfile.read(size))
        if version != VERSION:
            reply = (False, 'client is incompatible')
        else:
            try:
                with self.CallLock:
                    function = self.Call[data[0]]
                reply = function(self, *data[1:])
            except Finish:
                return
        data = pickle.dumps(reply, PICKLE_PROTOCOL)
        self.wfile.write(InfoStruct.pack(len(data), VERSION))
        self.wfile.write(data)

    def add_new_job_to_queue(self, job):
        clientName = get_client_name(self)
        cls = self.__class__
        with cls.IdGenLock, cls.QueueLock:
            jobid = cls.IdGen
            cls.IdGen += 1
            job.creation_date = datetime.datetime.now()
            job.hostname = clientName
            cls.Queue.update({jobid: job})
        save_id()
        return (True, jobid)

    def send_results_to_host(self):
        clientName = get_client_name(self)
        with self.QueueLock:
            ResQueue = self.Queue.SelAttrVal(
                attr='status_key', value={'e', 't'})
        EnvQueue = ResQueue.SelAttrVal(attr='hostname', value={clientName})
        if EnvQueue:
            with self.QueueLock:
                for k in EnvQueue.keys():
                    self.Queue.pop(k)
            return (True, EnvQueue)
        return (False, None)

    def send_job_from_queue(self, jobs2send):
        QueuedJobs = JobQueue()
        Jobs2Send = JobQueue()
        clientName = get_client_name(self)
        with self.QueueLock:
            QueuedJobs.update(
                self.Queue.SelAttrVal(attr='status_key', value={'q'}))
            if not len(QueuedJobs):
                return (False, None)
            for k, v in QueuedJobs.items():
                if v.possiblehosts == 'all' or clientName in v.possiblehosts:
                    v.status_key = 's'
                    v.runninghost = get_client_name(self)
                    Jobs2Send.update({k:v})
                    self.Queue.update({k:v})
                    if len(Jobs2Send) >= jobs2send:
                        break

        #Write file with job information:
        ordem = ['creation_date', 'runninghost', 'description']
        data = ''
        for k, v in Jobs2Send.items():
            data += '{:^7d}'.format(k)
            for at in ordem:
                data += PROPERTIES[at][0].format(
                    PROPERTIES[at][2](getattr(v, at)))
            data += '\n'
        try:
            name = os.path.join(CONFIGFOLDER, SUBMITTED_FILENAME)
            with open(name,mode='a') as fh:
                fh.write(data)
        except (TypeError, IOError, OSError) as err:
            print('Problem with file {0}:\n'.format(name), err)

        #Return the Queue
        return (True, Jobs2Send)

    def update_jobs_in_queue(self, ItsQueue):
        keys2remove = []

        with self.QueueLock:
            sts = {'e', 't', 'q'}
            for k, v in ItsQueue.items():
                if not isinstance(v, JobView) and v.status_key in sts:
                    keys2remove.append(k)
                    self.Queue.update({k:v}) # not a jobview
                elif isinstance(v, JobView):
                    if k in self.Queue:
                        self.Queue[k].update(v)# jobview
                    else:
                        jo = Jobs() # turn JobView in a Jobs object
                        jo.update(v)
                        self.Queue.update({k: jo})
                else:
                    self.Queue.update({k: v})# not a jobview

        return (True, keys2remove)

    def change_jobs_request(self, ChanQueue):
        keyschanged = set()
        with self.QueueLock:
            ChanQueueNoRun = ChanQueue.SelAttrVal(
                attr='runninghost', value={None})
            for k,v in ChanQueueNoRun.items():
                vl = self.Queue.get(k)
                if not vl or vl.runninghost:
                    continue
                keyschanged.add(k)
                self.Queue[k].update(v) # v is a jobview!
                if v.status_key == 'tu':
                    self.Queue.pop(k)

            keys2change = set(ChanQueue.keys()) - set(ChanQueueNoRun.keys())
            keyssched2change = set()
            for k in keys2change:
                if self.Queue[k].status_key in {'t', 'e', 'tu', 'qu', 'q'}:
                    continue
                self.Queue[k].update(ChanQueue[k]) # jobview!!!
                keyssched2change.add(k)
            return (True, keyschanged, keyssched2change)

    def print_queue_status(self, onlymine=False):
        if onlymine:
            Queue2Send = self.Queue.SelAttrVal(
                attr='runninghost', value={get_client_name(self)})
        else:
            Queue2Send = JobQueue(self.Queue)

        for k, v in Queue2Send.items():
            Queue2Send.update({k: JobView(v)})

        return (True, Queue2Send)

    def send_configs_to_client(self, ItsConfigs):
        clientName = get_client_name(self)
        with self.ConfigsLock:
            if clientName in self.Configs.keys():
                self.Configs[clientName].numcpus = ItsConfigs.numcpus
                self.Configs[clientName].totalJobs = ItsConfigs.totalJobs
                self.Configs[clientName].running = ItsConfigs.running
                self.Configs[clientName].active = 'on'
                self.Configs[clientName].last_contact = datetime.datetime.now()
                return (True, self.Configs[clientName])

            self.Configs.update({clientName:ItsConfigs})
            self.Configs[clientName].active = 'on'
            self.Configs[clientName].last_contact = datetime.datetime.now()
            return (False, True)

    def get_configs_of_clients(self, clients):
        if clients == 'all':
            clients = tuple(self.Configs.keys())
        elif clients == 'this':
            clients = (get_client_name(self),)

        Configs2Send = {}
        with self.ConfigsLock:
            for clientName in clients:
                if clientName in self.Configs.keys():
                    if (self.Configs[clientName].active == 'on' and
                        3*WAIT_TIME < datetime.datetime.now().timestamp() -
                        self.Configs[clientName].last_contact.timestamp()):
                        self.Configs[clientName]. active = 'dead'
                    ClientConfigs = self.Configs[clientName]
                    Configs2Send.update({clientName: ClientConfigs})
            return (True, Configs2Send)

    def set_configs_of_clients(self, NewConfigs, RmClie):
        clients = tuple(NewConfigs.keys() - self.Configs.keys())
        if clients:
            return (False, clients)
        with self.ConfigsLock:
            self.Configs.update(NewConfigs)
            for client in RmClie:
                self.Configs.pop(client)
        return (True, None)

    def shutdown(self):
        with self.ConfigsLock:
            for k in self.Configs:
                self.Configs[k].active = 'off'
            self.server.shutdown()
        raise Finish()

    def client_shutdown(self, down):
        clientName = get_client_name(self)
        with self.ConfigsLock:
            if down:
                self.Configs[clientName].active = 'off'
                self.Configs[clientName].shutdown = False
                self.Configs[clientName].MoreJobs = True
            else:
                self.Configs[clientName].active = 'paused'
        return True


def get_client_name(client):
    clientName = socket.gethostbyaddr(client.client_address[0])[0]
    if clientName == 'localhost':
        clientName = socket.gethostname()
    return clientName

def load_existing_Queue():
    name   = os.path.join(CONFIGFOLDER, QUEUE_FILENAME)
    data = load_file(name=name, ignore=True)
    if data and data[1]:
        return eval(data[1])
    return

def load_last_id():
    name   = os.path.join(CONFIGFOLDER, IDGEN_FILENAME)
    data = load_file(name=name, ignore=True)
    if data and data[1]:
        return eval(data[1])

def load_existing_Configs():
    name = os.path.join(CONFIGFOLDER, CONFIGS_FILENAME)
    data = load_file(name=name, ignore=True)
    if data and data[1]:
        return eval(data[1])

def save_Configs():
    if not os.path.isdir(CONFIGFOLDER):
        os.mkdir(path=CONFIGFOLDER)
    configname = os.path.join(CONFIGFOLDER, CONFIGS_FILENAME)
    createfile(name=configname, data=repr(RequestHandler.Configs))

def save_Queue():
    if not os.path.isdir(CONFIGFOLDER):
        os.mkdir(path=CONFIGFOLDER)
    queuename = os.path.join(CONFIGFOLDER, QUEUE_FILENAME)
    createfile(name=queuename, data=repr(RequestHandler.Queue))

def save_id():
    if not os.path.isdir(CONFIGFOLDER):
        os.mkdir(path=CONFIGFOLDER)
    idgenname = os.path.join(CONFIGFOLDER, IDGEN_FILENAME)
    createfile(name=idgenname, data=repr(RequestHandler.IdGen))

def save_all():
    if not os.path.isdir(CONFIGFOLDER):
        os.mkdir(path=CONFIGFOLDER)
    idgenname = os.path.join(CONFIGFOLDER, IDGEN_FILENAME)
    queuename = os.path.join(CONFIGFOLDER, QUEUE_FILENAME)
    configname = os.path.join(CONFIGFOLDER, CONFIGS_FILENAME)
    createfile(name=idgenname, data=repr(RequestHandler.IdGen))
    createfile(name=queuename, data=repr(RequestHandler.Queue))
    createfile(name=configname, data=repr(RequestHandler.Configs))

def signal_handler(signal, frame):
    save_all()
    sys.exit()

def main():
    RequestHandler.Queue = load_existing_Queue() or JobQueue()
    RequestHandler.IdGen = load_last_id() or int(1)
    RequestHandler.Configs = load_existing_Configs() or dict()
    server = None
    try:
        server = ManageJobsServer(("", Address[1]), RequestHandler)
        server.serve_forever()
    except Exception as err:
        print("ERROR", err)
    finally:
        if server is not None:
            server.shutdown()
        save_all()

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    main()
