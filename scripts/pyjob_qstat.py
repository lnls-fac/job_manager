#!/usr/bin/env python-sirius

import argparse
from pyjob import STATUS, PROPERTIES, job_selection_parse, JobSelParseErr,\
    job_selection_parse_options

EXPLICATION = dict(
    q='In the queue',
    qu='The user scheduled the job back to the queue but the'
       'signal was not sent to the host which is running the job yet.',
    r='Running',
    ru='Scheduled to continue by user, but the signal has not '
       'reached the host of the job.',
    p='Paused',
    pu='Job paused by the user. The only way to continue this job is'
       "through a 'continue' command with pyjob_qsig.py.",
    w='The job was sent to the host for running, but it has not '
      'initiated yet. This status will only appear when the host is a'
      'cluster, such as sgi or sunhpc.',
    t='The job was terminated anomalously. Possible causes are: user'
      'command, host reboot, power outage.',
    tu="The user terminated the job, but the signal hasn't been "
       'sent to the host.',
    e='Job ended successfully.',
    s='Job has been sent to the host, but its current status was not'
      ' confirmed yet.')


def main():
    parser = argparse.ArgumentParser()
    parser = job_selection_parse_options(parser)
    parser.description = 'This command lists the jobs in the  queue.'
    parser.add_argument(
        '--explicate', dest='explicate', action='store_true',
        help="This option explains the meaning of the several "
             "status flag of the jobs.",
        default=False)
    parser.add_argument(
        '-c', '--choose', dest='choose', type='str',
        help="If this option is given, the user can specify which "
             "job properties will be shown. [format: "
             "prop1,prop2,...  default: 'prior,status,user,runninghost,"
             "running_time,description']. It is not needed to give "
             "the whole property name, only a fraction of it is enough."
             "Possible Values:\n" + ', '.join(list(PROPERTIES.keys())))
    parser.add_argument(
        '--summary', dest='summary', action='store_true',
        help='Show a summary of the queue.', default=False)

    opts = parser.parse_args()

    if opts.explicate:
        print('{:^6s}{:^74s}'.format('STATUS', 'EXPLICATION'))
        for k, v in sorted(EXPLICATION.items(), key=lambda x: x[0]):
            v = v.split()
            print('{:^6s}{}'.format(k, v[0]), end=' ')
            leng = len(v[0])
            for ii in range(1, len(v)):
                leng += len(v[ii])
                if leng <= 74:
                    print(v[ii], end=' ')
                else:
                    leng = len(v[ii])
                    print('\n', ' '*4, v[ii], end=' ')
            print('\n')
        return

    try:
        Queue = job_selection_parse(opts)
    except JobSelParseErr as err:
        print(err)
        return

    if opts.summary and Queue:
        print('STATUS  NJOBS')
        for status in sorted(list(STATUS.keys())):
            SelQueue = Queue.SelAttrVal(attr='status_key', value={status})
            if SelQueue:
                print('{0:^6s} {1:^5d}'.format(status, len(SelQueue)))
        print('TOTAL ={0:^5d}'.format(len(Queue)))
        return

    choose = 'prior,status,user,runninghost,running_time,description'
    if opts.choose is not None:
        choose = opts.choose

    ordem = []
    for cho in choose.split(','):
        for k in sorted(PROPERTIES):
            if cho.lower() in k:
                ordem += [k]

    if Queue:
        myprint('{:^7s}'.format('JobID'))
        for at in ordem:
            myprint(PROPERTIES[at][0].format(PROPERTIES[at][1]))
        print()
    for k, v in Queue.items():
        myprint('{:^7d}'.format(k))
        for at in ordem:
            myprint(PROPERTIES[at][0].format(
                PROPERTIES[at][2](getattr(v, at))))
        print()


def myprint(*items):
    return print(*items, end='')

if __name__ == '__main__':
    main()
