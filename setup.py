#!/usr/bin/env python

from setuptools import setup, find_packages
from version import get_git_version

setup(
    name='mtrpc',
    version=get_git_version(),
    packages=find_packages(exclude=['mtrpc.test']),
    install_requires=['amqplib', 'decorator', 'flask', 'gunicorn'],
    author='MegiTeam',
    author_email='admin@megiteam.pl',
    description='Easy JSONRPC over AMQP',
    license='MIT',
    keywords='mtrpc rpc json jsonrpc amqp',
    entry_points={
        'console_scripts': [
            'mtrpc-server = mtrpc.server.__main__:main',
            'mtrpc-request = mtrpc.mtrpc_request:main'
        ],
    }
)
