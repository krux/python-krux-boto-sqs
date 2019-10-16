# -*- coding: utf-8 -*-
#
# © 2016-2019 Salesforce.com, inc.
#

"""
Package setup for krux-sqs
"""

#
# Standard libraries
#
from __future__ import absolute_import
from setuptools import setup, find_packages

#
# Internal libraries
#

from krux_sqs import __version__


# URL to the repository on Github.
REPO_URL = 'https://github.com/krux/python-krux-boto-sqs'
# Github will generate a tarball as long as you tag your releases, so don't
# forget to tag!
DOWNLOAD_URL = ''.join((REPO_URL, '/tarball/release/', __version__))


setup(
    name='krux-sqs',
    version=__version__,
    author='Peter Han',
    author_email='phan@krux.com',
    maintainer='Peter Han',
    maintainer_email='phan@krux.com',
    description='Library for interacting with AWS SQS built on krux-boto',
    url=REPO_URL,
    download_url=DOWNLOAD_URL,
    license='All Rights Reserved.',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'krux-boto',
    ],
    entry_points={
        'console_scripts': [
            'krux-sqs-test = krux_sqs.cli:main',
        ],
    },
    test_suite='test',
)
