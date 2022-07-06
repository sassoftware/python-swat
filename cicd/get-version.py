#!/usr/bin/env python

'''
Return the version of the package

This command looks for the `__version__` attribute in the `swat/__init__.py`
file to determine the package version.

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import argparse
import glob
import os
import re
import sys


def print_err(*args, **kwargs):
    ''' Print a message to stderr '''
    sys.stderr.write(*args, **kwargs)
    sys.stderr.write('\n')


def main(args):
    ''' Main routine '''

    version = None

    try:
        init = glob.glob(os.path.join(args.root, 'swat', '__init__.py'))[0]
    except IndexError:
        sys.stderr.write('ERROR: Could not locate swat/__init__.py file\n')
        return 1

    with open(init, 'r') as init_in:
        for line in init_in:
            m = re.search(r'''^__version__\s*=\s*['"]([^'"]+)['"]''', line)
            if m:
                version = m.group(1)
                if version.endswith('-dev'):
                    version = version.replace('-dev', '.dev0')

    if version:
        if args.as_expr:
            print('=={}'.format(version))
        else:
            print(version)
        return

    print_err('ERROR: Could not find __init__.py file.')

    return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip())

    parser.add_argument('root', type=str, metavar='<directory>', nargs='?',
                        default='.', help='root directory of Python package')

    parser.add_argument('-e', '--as-expr', action='store_true',
                        help='format the version as a dependency expression')

    args = parser.parse_args()

    sys.exit(main(args) or 0)
