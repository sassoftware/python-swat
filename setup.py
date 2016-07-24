#!/usr/bin/env python

''' Install the SWAT module '''

from setuptools import setup, find_packages
from setuptools.command.install import install
import os
import struct
import sys

try:
    input = raw_input
except:
    pass


def accept_license():
    ''' Display TK license and check for acceptance '''
    import glob
    import pydoc
    if len(glob.glob('swat/lib/*/*')) > 10:
        os.environ['LESS'] = os.environ.get('LESS', '') + ' -e'
        pydoc.pager(open('LICENSE.txt', 'r').read())
        out = input('Do you accept the terms of the license? [y/N] ')
        if out.lower().strip().startswith('y'):
            return True
        return False
    return True


class SWATInstaller(install):
    ''' Make sure that the Python executable is 64-bit '''
    def run(self):
        size = struct.calcsize('P')  # integer size
        if size != 8:
            print('Sorry, you must have 64bit Python installed.')
            print('Exiting.')
            print('This version of Python is %dbit:' % (size*8))
            print(sys.version)
            raise Exception('This packages requires 64bit Python')
        if accept_license():
            install.run(self)


setup(
    cmdclass = {'install': SWATInstaller},
    zip_safe = False,
    name = 'swat',
    version = '0.9.0',
    description = 'SAS Wrapper for Analytics Transfer (SWAT)',
    long_description = open('README.rst').read(),
    author = 'Kevin D Smith',
    author_email = 'Kevin.Smith@sas.com',
    url = 'http://github.com/sassoftware/python-swat/',
    license = 'LICENSE.txt',
    packages = find_packages(),
    package_data = {
        'swat': ['lib/*/*'],
    },
    install_requires = [
        "pandas >= 0.16.0",
        "six >= 0.9.0",
    ],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
