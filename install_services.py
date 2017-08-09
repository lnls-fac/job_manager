#!/usr/bin/env python-sirius

import os
import socket


def install_pyjob_run(version):
    print('installing pyjob_run')
    # ubuntu 14.04
    if version == '14.04':
        os.system('stop pyjob_run 2>&1 > /dev/null')
        os.system('cp -f ./src/pyjob_run.conf /etc/init/')
        os.system('start pyjob_run 2>&1 > /dev/null &')
        return
    # ubuntu 16.04 or later
    os.system('systemctl stop pyjob_run.service 2>&1 > /dev/null')
    os.system('cp -f ./src/pyjob_run.service /etc/systemd/system/')
    os.system('systemctl enable pyjob_run.service 2>&1 > /dev/null &')
    os.system('systemctl start pyjob_run.service 2>&1 > /dev/null &')


def install_pyjob_server(version):
    print('installing pyjob_server')
    # ubuntu 14.04
    if version == '14.04':
        os.system('stop pyjob_server 2>&1 > /dev/null')
        os.system('cp -f ./src/pyjob_server.conf /etc/init/')
        os.system('start pyjob_server 2>&1 > /dev/null &')
        return
    # ubuntu 16.04
    os.system('systemctl stop pyjob_server.service 2>&1 > /dev/null')
    os.system('cp -f ./src/pyjob_server.service /etc/systemd/system/')
    os.system('systemctl enable pyjob_server.service 2>&1 > /dev/null &')
    os.system('systemctl start pyjob_server.service 2>&1 > /dev/null &')


with open('/etc/lsb-release', 'r') as f:
    data = f.readlines()

version = data[1].split('=')[1][:-1]
install_pyjob_run(version)
if socket.gethostname().startswith("lnls350-linux"):
    install_pyjob_server(version)
