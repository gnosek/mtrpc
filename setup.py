#!/usr/bin/env python

from setuptools import setup, find_packages
from version import get_git_version

setup(
    name='mtrpc',
    version=get_git_version(),
    packages=find_packages(exclude=['mtrpc.test']),
)
