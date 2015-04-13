#!/usr/bin/env python3

import optparse
import Global

def main():
 
    # configuration of the parser for the arguments
    parser = optparse.OptionParser()
    parser.add_option('--sure', dest='sure',action='store_true',
                      help="Option to bypass the 'Are You Sure' question",
                      default=False)
    parser.set_description(description='When this command is called it shuts'
                           ' down the server')
    
    (opts, _) = parser.parse_args()
    
    if not opts.sure:
        Noyes = input('ARE YOU SURE you really want to'
                      ' shutdown the server [NO/yes]: ')
        if not Noyes or not 'yes'.startswith(Noyes.lower()):
            print('Wise decision! you did not shutdown the server.')
            return
        print('Ok, then...')
    
    # Load execution script
    Global.handle_request('SHUTDOWN',  wait_for_reply=False)
    print('The server was shutdown!')
    
    
    
main()
