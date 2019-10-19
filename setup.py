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
# We use the version to construct the DOWNLOAD_URL.
DOWNLOAD_URL = ''.join((REPO_URL, '/tarball/release/', __version__))

REQUIREMENTS = ['krux-boto', 'simplejson', 'six']
TEST_REQUIREMENTS = ['coverage', 'mock', 'pytest', 'pytest-runner', 'pytest-cov', 'pytest-flake8']
LINT_REQUIREMENTS = ['flake8']


setup(
    name='krux-sqs',
    version=__version__,
    author='Salesforce AS Platform Engineering',
    author_email='krux-pe@salesforce.com',
    description='Library for interacting with AWS SQS built on krux-boto',
    url=REPO_URL,
    download_url=DOWNLOAD_URL,
    license='All Rights Reserved.',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=find_packages(exclude=['tests']),
    entry_points={
        'console_scripts': [
            'krux-sqs-test = krux_sqs.cli:main',
        ],
    },
    install_requires=REQUIREMENTS,
    tests_require=TEST_REQUIREMENTS,
    extras_require={
        'dev': TEST_REQUIREMENTS + LINT_REQUIREMENTS,
    },
)
