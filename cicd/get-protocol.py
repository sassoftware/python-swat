#!/usr/bin/env python

'''
Return the preferred CAS protocol ('cas' if TK is available; 'http' otherwise)

'''

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
            m = re.match(r'''__tk_version__\s*=\s*['"]([^'"]+)['"]''', line)
            if m:
                version = m.group(1)
                if version == 'none':
                    version = None
                break

    print(version and 'cas' or 'http')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip())

    parser.add_argument('root', type=str, metavar='<directory>',
                        default='.', nargs='?',
                        help='root directory of Python package')

    args = parser.parse_args()

    sys.exit(main(args) or 0)
