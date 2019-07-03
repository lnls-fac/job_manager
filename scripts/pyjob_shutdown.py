#!/usr/bin/env python-sirius

import argparse
from pyjob import handle_request


def main():
    # configuration of the parser for the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--sure', dest='sure', action='store_true', default=False,
        help="Option to bypass the 'Are You Sure' question")
    parser.description = 'When this command is called it shuts down the server'

    opts = parser.parse_args()

    if not opts.sure:
        Noyes = input(
            'ARE YOU SURE you really want to shutdown the server [No/Yes]: ')
        if not Noyes or not 'yes'.startswith(Noyes.lower()):
            print('Wise decision! you did not shutdown the server.')
            return
        print('Ok, then...')

    # Load execution script
    handle_request('SHUTDOWN', wait_for_reply=False)
    print('The server was shutdown!')


if __name__ == '__main__':
    main()
