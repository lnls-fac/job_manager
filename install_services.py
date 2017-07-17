#!/usr/bin/env python3

import os
import socket


def install_pyjob_run():
    print('installing pyjob_run')
    #ubuntu 14.04
    os.system('stop pyjob_run 2>&1 > /dev/null')
    os.system('cp -f ./src/pyjob_run.conf /etc/init/')
    os.system('start pyjob_run 2>&1 > /dev/null &')
    #ubuntu 16.04
    os.system('systemctl stop pyjob_run.service 2>&1 > /dev/null')
    os.system('cp -f ./src/pyjob_run.service /etc/systemd/system/')
    os.system('systemctl enable pyjob_run.service 2>&1 > /dev/null &')
    os.system('systemctl start pyjob_run.service 2>&1 > /dev/null &')


def install_pyjob_server():
    print('installing pyjob_server')
    #ubuntu 14.04
    os.system('stop pyjob_server 2>&1 > /dev/null')
    os.system('cp -f ./src/pyjob_server.conf /etc/init/')
    os.system('start pyjob_server 2>&1 > /dev/null &')
    #ubuntu 16.04
    os.system('systemctl stop pyjob_server.service 2>&1 > /dev/null')
    os.system('cp -f ./src/pyjob_server.service /etc/systemd/system/')
    os.system('systemctl enable pyjob_server.service 2>&1 > /dev/null &')
    os.system('systemctl start pyjob_server.service 2>&1 > /dev/null &')


install_pyjob_run()
if socket.gethostname() == "lnls82-linux":
    install_pyjob_server()
