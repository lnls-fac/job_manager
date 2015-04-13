#!/usr/bin/env python3

import optparse
import Global

STATUS = Global.STATUS

EXPLICATION = dict(q='In the queue',
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
iden= lambda x:str(x)
PROPERTIES = dict(description=('{:^20s}','Description',iden),
                  user=('{:^10s}','User',iden),
                  working_dir=('{:^20s}','Working Directory',iden),
                  creation_date=('{:^13s}','Creation',
                                 lambda x:x.strftime('%m/%d %H:%M')),
                  status_key=('{:^8s}','Status',iden),
                  hostname=('{:^10s}','Hostname', lambda x:x.split('-')[0]),
                  priority=('{:^7s}','Prior', iden),
                  runninghost=('{:^10s}','Run Host',lambda x:(x or "_"
                                                              ).split('-')[0]),
                  possiblehosts=('{:^20s}','Can Run On',
                                lambda x:x if x=='all' else 
                                ','.join([y.split('-')[0]
                                for y in sorted(x)])),
                  running_time=('{:^12s}','Time Run',iden))
def main():
    parser = optparse.OptionParser()
    parser = Global.job_selection_parse_options(parser)
    parser.set_description(description='This command lists the jobs in the '
                           ' queue.')
    parser.add_option('--explicate',dest='explicate', action='store_true',
                      help="This option explains the meaning of the several "
                      "status flag of the jobs.", default=False)
    parser.add_option('-c','--choose',dest='choose',type='str',
                      help="If this option is given, the user can specify which "
                      "job properties will be shown. [format: "
                      "prop1,prop2,...  default: 'prior,status,user,creation_"
                      "date,runninghost,description']. It is not needed to give "
                      "the whole property name, only a fraction of it is enough."
                      "Possible Values:\n" + ', '.join(list(PROPERTIES.keys())))
    
    (opts, _) = parser.parse_args()
    
    if opts.explicate:
        print('{:^6s}{:^74s}'.format('STATUS', 'EXPLICATION'))
        for k,v in sorted(EXPLICATION.items(),key=lambda x:x[0]):
            v = v.split()
            print('{:^6s}{}'.format(k,v[0]),end=' ')
            leng = len(v[0])
            for ii in range(1,len(v)):
                leng += len(v[ii])
                if leng <= 74:
                    print(v[ii],end=' ')
                else:
                    leng = len(v[ii])
                    print('\n',' '*4,v[ii], end=' ')
            print('\n')
        return
    

    try:
        Queue = Global.job_selection_parse(opts)
    except Global.JobSelParseErr as err:
        print(err)
        return

    choose = 'prior,status,user,creation_date,runninghost,description'
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
    for k,v in Queue.items():
        myprint('{:^7d}'.format(k))
        for at in ordem:
            myprint(PROPERTIES[at][0].format(PROPERTIES[at][2](getattr(v,at))))
        print()

    
def myprint(*items):
    return print(*items,end='')
    
if __name__ == '__main__':
    main()