#!/usr/bin/env python-sirius

import argparse
from pyjob import job_selection_parse, job_selection_parse_options, \
    MATCH_RULE, JobSelParseErr, handle_request, match_clients, MatchClientsErr


def main():
    # configuration of the parser for the arguments
    parser = argparse.ArgumentParser()
    group = parser.add_argument_group('Job Selection Options')
    group = job_selection_parse_options(group)
    group = parser.add_argument_group("Signals Options")
    group.add_argument(
        '-S', '--signal', dest='signal', type=str,
        help="Send signal to jobs. Options are: kill, pause, "
             "continue and queue. The last signal brings back the jobs "
             "to the queue. If they have begun to run"
             " the outputs generated so far will be loaded.")
    group.add_argument(
        '-P', '--priority', dest='priority', type=int,
        help="Change priority of jobs. Must be an integer")
    group.add_argument(
        '-H', '--possibleHosts', dest='hosts', type=str,
        help="Change the list of possible hosts to run the"
             "jobs [format: append=host1,host2,... or set=host1,host2"
             ",..." + MATCH_RULE)

    parser.description = \
        'This command send signals to specific jobs and allows you ' +\
        'to change the possibleHosts and priority.'

    opts = parser.parse_args()

    if not any((opts.jobs, opts.status, opts.user, opts.descr)):
        print('At least one Job Selection Option must be given')
        return

    try:
        Queue = job_selection_parse(opts)
    except JobSelParseErr as err:
        print(err)
        return

    if Queue.SelAttrVal(attr='status_key', value={'t', 'e', 'tu'}):
        print("You are trying to change a job which is finished: Operation"
              " not allowed.")
        return

    signals = dict({
        'kill': 'tu', 'pause': 'pu', 'continue': 'ru', 'queue': 'qu'})
    if opts.signal is not None:
        opts.signal = opts.signal.lower()
        if opts.signal not in signals.keys():
            print('Signal not supported. Options: ',
                  ' '.join(list(signals.keys())))
            return
        for k, v in Queue.items():
            if signals[opts.signal] in {'ru', 'qu'} and v.status_key == 'q':
                continue
            Queue[k].status_key = signals[opts.signal.lower()]

    if opts.hosts is not None:
        action = opts.hosts.split('=')
        if len(action) != 2:
            print('Wrong -H assignment.')
            return
        if action[1] != 'all':
            keys2Match = set(action[1].split(','))
            try:
                hosts = set(match_clients(keys2Match).keys())
            except MatchClientsErr as err:
                print(err)
                return
        else:
            hosts = 'all'
        for k, v in Queue.items():
            if hosts == 'all':
                v.possiblehosts = 'all'
                continue
            if 'append'.startswith(action[0].lower()):
                if v.possiblehosts == 'all':
                    continue
                v.possiblehosts += hosts
            elif 'set'.startswith(action[0].lower()):
                v.possiblehosts = hosts
            else:
                print('Wrong -H assignment.')
                return
            Queue.update({k: v})

    if opts.priority is not None:
        for k, v in Queue.items():
            v.priority = opts.priority
            Queue.update({k: v})

    ok, data1, data2 = handle_request('CHANGE_JOBS_REQUEST', Queue)
    if ok:
        pr1 = [str(x) for x in data1]
        pr2 = [str(x) for x in data2]
        print('These jobs were successfully changed:', ' '.join(pr1))
        print('These jobs were scheduled to change: ', ' '.join(pr2))
        left = [str(x) for x in set(Queue.keys()) - (data1 | data2)]
        if left:
            print('These jobs could not be changed :', ' '.join(left))


if __name__ == '__main__':
    main()
