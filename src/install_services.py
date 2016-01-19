#!/usr/bin/env python3

import os
import socket


def install_pyjob_run():
    print('installing pyjob_run')
    os.system('stop pyjob_run 2>&1 > /dev/null')
    os.system('cp -f ./pyjob_run.conf /etc/init/')
    os.system('start pyjob_run 2>&1 > /dev/null &')


def install_pyjob_server():
    print('installing pyjob_server')
    os.system('stop pyjob_server 2>&1 > /dev/null')
    os.system('cp -f ./pyjob_server.conf /etc/init/')
    os.system('start pyjob_server 2>&1 > /dev/null &')


install_pyjob_run()
if socket.gethostname() == "lnls82-linux":
    install_pyjob_server()
