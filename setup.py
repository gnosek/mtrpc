#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='mtrpc',
    version='0.1',
    packages=find_packages(exclude=['mtrpc.test']),
)
