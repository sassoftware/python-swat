#!/usr/bin/env python

''' Install the SWAT module '''

from setuptools import setup, find_packages
from setuptools.command.install import install
import glob
import os
import struct
import sys

try:
    input = raw_input
except:
    pass


def has_tk():
    ''' See if TK components exist '''
    return len(glob.glob('swat/lib/*/*')) > 10


def accept_license():
    ''' Display TK license and check for acceptance '''
    import pydoc
    os.environ['LESS'] = os.environ.get('LESS', '') + ' -e'
    pydoc.pager(open(os.path.join('LICENSES', 'SAS-TK.txt'), 'r').read())
    out = input('Do you accept the terms of the license? [y/N] ')
    if out.lower().strip().startswith('y'):
        return True
    return False


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
        if os.environ.get('CONDA_BUILD', None):
            install.run(self)
        elif os.environ.get('ACCEPT_SAS_TK_LICENSE', '').lower().startswith('y'):
            install.run(self)
        elif not has_tk():
            print('NOTE: Only the REST interface is supported with the pure Python installation.')
            print('      Use the pip or conda installers for binary protocol support.')
            install.run(self)
        elif has_tk() and accept_license():
            install.run(self)

setup(
    cmdclass = {'install': SWATInstaller},
    zip_safe = False,
    name = 'swat',
    version = '0.9.0',
    description = 'SAS Wrapper for Analytics Transfer (SWAT)',
    long_description = open('README.rst', 'r').read(),
    author = 'Kevin D Smith',
    author_email = 'Kevin.Smith@sas.com',
    url = 'http://github.com/sassoftware/python-swat/',
    license = 'LICENSE/SWAT.txt',
    packages = find_packages(),
    package_data = {
        'swat': ['lib/*/*'],
    },
    install_requires = [
        'pandas >= 0.16.0',
        'six >= 0.9.0',
        'requests',
    ],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
