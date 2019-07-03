#!/usr/bin/env python-sirius

from setuptools import setup, find_packages

with open('VERSION', 'r') as _f:
    __version__ = _f.read().strip()

with open('requirements.txt', 'r') as _f:
    _requirements = _f.read().strip().split('\n')

setup(
    name='pyjob',
    version=__version__,
    author='lnls-fac',
    description='Software to run beam dynamics simulations.',
    url='https://github.com/lnls-fac/job_manager',
    download_url='https://github.com/lnls-fac/job_manager',
    license='GNU GPLv3',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering'
        ],
    packages=find_packages(),
    install_requirements=_requirements,
    package_data={'pyjob': ['VERSION']},
    include_package_data=True,
    scripts=[
        'scripts/pyjob_check_finished_jobs.py',
        'scripts/pyjob_configs_get.py',
        'scripts/pyjob_configs_set.py',
        'scripts/pyjob_qsig.py',
        'scripts/pyjob_qstat.py',
        'scripts/pyjob_qsub.py',
        'scripts/pyjob_run.py',
        'scripts/pyjob_server.py',
        'scripts/pyjob_shutdown.py',
        'scripts/pyjob_submit_jobs_again.py',
        ],
    zip_safe=False
)
