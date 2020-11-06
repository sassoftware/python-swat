#!/usr/bin/env python

'''
Return the version of the package

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import argparse
import glob
import os
import re
import sys


def main(args):
    ''' Main routine '''

    version = None

    init = glob.glob(os.path.join(args.root, 'swat', '__init__.py'))[0]
    with open(init, 'r') as init_in:
        for line in init_in:
            m = re.search(r'''^__version__\s*=\s*['"]([^'"]+)['"]''', line)
            if m:
                version = m.group(1)
                if version.endswith('-dev'):
                    version = version.replace('-dev', '.dev0')

    if version:
        if args.as_expr:
            print('={}'.format(version))
        else:
            print(version)
        return

    print('ERROR: Could not find __init__.py file.', file=sys.stderr)
    return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip())

    parser.add_argument('root', type=str, metavar='<directory>', nargs='?',
                        default='.', help='root directory of Python package')

    parser.add_argument('-e', '--as-expr', action='store_true',
                        help='format the version as a dependency expression')

    args = parser.parse_args()

    sys.exit(main(args) or 0)
