#!/usr/bin/env python3

import optparse
import calendar
import datetime
import Global

def main():
    # configuration of the parser for the arguments
    parser = optparse.OptionParser()
    parser.add_option('-c','--clients',dest='clients',type='str',
                      help="list of hosts to get the configs. [format: "
                      "client1,client2,...  default: 'all']" + Global.MATCH_RULE)
    parser.add_option('--showCalendar',dest='sCal',action='store_true',
                      help="Show the calendar of each client", default=False)
    (opts, _) = parser.parse_args()
    
    try:
        if opts.clients == 'all' or opts.clients is None:
            clients = opts.clients
            ok, ConfigsReceived = Global.handle_request('GET_CONFIGS','all')
        else:
            clients = set(opts.clients.split(","))
            ConfigsReceived = Global.match_clients(clients)
            ok = True
        if not ok:
            raise MatchClientsErr('Could not get configs of server.')
    except Global.MatchClientsErr as err:
        print(err)
        return
  
    if opts.sCal:
        sortCal = lambda x:(getattr(calendar,x[0][0].upper()),x[0][1],x[0][2])
        sorTab = lambda x:getattr(calendar,x[0].upper())
        days = tuple(x for x in calendar.day_name)
        hours = tuple(range(24))
        minutes = tuple(range(60))
        print(' '*14, calendar.weekheader(9))
        for k, v in sorted(ConfigsReceived.items(),key=lambda x: x[0]):
            np = ConfigsReceived[k].defNumJobs
            table = {x:dict() for x in calendar.day_name}
            conj = set()
            Cal = {(x,y,z): np for x in days for y in hours for z in minutes}
            Cal.update(ConfigsReceived[k].Calendar)
            previous = None
            lastday = calendar.day_name[0]
            for kl, vl in sorted(Cal.items(), key=sortCal):
                if vl != previous or kl[0] != lastday:
                    table[kl[0]].update({kl[1:]:vl})
                    conj.add(kl[1:])
                    previous = vl
                    lastday = kl[0]
            print(k)
            lasttime = dict() 
            for kl in sorted(conj, key=lambda x:x):
                nums = ''
                for day, dados in sorted(table.items(), key=sorTab):
                    vl = dados.get(kl)
                    if vl is None:
                        vl = dados.get(lasttime[day])
                    else:
                        lasttime[day] = kl
                    
                    nums += '{:^10d}'.format(vl)
                print('{:^17s}{:s}'.format('{0:02d}:{1:02d}'
                                           .format(kl[0], kl[1]),nums))  
        return
    
    print('='*78)
    print('{:17s}{:^7s}{:^7s}{:^10s}{:^10s}{:^10s}{:^11s}{:^6s}'
          .format('hostname','State', 'NCPUs','NJPermtd','NJRecvd',
                  'NJRunning', 'Accepting', 'Nice'))
    print('='*78)
    agora = datetime.datetime.now()
    TNCPUs, TNAllow, TJThere, TJRun = 0, 0, 0, 0
    for k,v in sorted(ConfigsReceived.items(),key=lambda x: x[0]):
        NumJobs = v.Calendar.get((calendar.day_name[agora.weekday()],
                                  agora.hour, agora.minute), v.defNumJobs)
        print('{key:17s}{val.active!s:^7s}{val.numcpus:^7d}{N:^10d}'
              '{val.totalJobs:^10d}{val.running:^10d}{acc:^11}'
              '{val.niceness:^6d}'.format(key=k,val=v,N=NumJobs, 
                                          acc='yes' if v.MoreJobs else 'no'))
        TNCPUs += v.numcpus
        TNAllow += NumJobs
        TJThere += v.totalJobs
        TJRun += v.running
    print('='*78)
    print('{0:17s}{1:^7s}{2:^7d}{3:^10d}{4:^10d}{5:^10d}{1:^10s}{1:^6s}'
          .format('Total',' ',TNCPUs, TNAllow, TJThere, TJRun))
        
        
if __name__ == '__main__':
    main()

