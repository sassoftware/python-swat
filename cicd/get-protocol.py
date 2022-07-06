#!/usr/bin/env python

'''
Return the preferred CAS protocol ('cas' if TK is available; 'http' otherwise)

This utility checks the package `__init__.py` file for a `__tk_version__`
parameter that indicates the packaged version of the TK libraries.
If it finds it, the `cas` protocol will be returned. If the value is
set to `None`, the `http` protocol is returned.

'''

import argparse
import glob
import os
import re
import sys


def main(args):
    ''' Main routine '''
    version = None

    try:
        init = glob.glob(os.path.join(args.root, 'swat', '__init__.py'))[0]
    except IndexError:
        sys.stderr.write('ERROR: Could not locate swat/__init__.py file\n')
        sys.exit(1)

    with open(init, 'r') as init_in:
        for line in init_in:
            m = re.match(r'''__tk_version__\s*=\s*['"]([^'"]+)['"]''', line)
            if m:
                version = m.group(1)
                if version == 'none':
                    version = None
                break

    print(version and 'cas' or 'http')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip(),
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('root', type=str, metavar='<directory>',
                        default='.', nargs='?',
                        help='root directory of Python package')

    args = parser.parse_args()

    sys.exit(main(args) or 0)
