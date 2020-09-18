#!/usr/bin/env python

''' Retrieve the package version number from source files '''

import argparse
import glob
import os
import re
import sys


def get_version():
    ''' Get the version number from the source files '''
    files = ['setup.py'] + glob.glob('*/__init__.py')

    for f in files:
        if not os.path.isfile(f):
            continue
        with open(f, 'r') as in_file:
            txt = in_file.read()
            m = re.search(r'^(\s*(?:__version__|version)\s*=\s*[\'"])([^\'"]+)([\'"])',
                          txt, flags=re.M)
            if m:
                return m.group(2)

    raise RuntimeError('No version found in source files')


def main(args):
    ''' Main routine '''
    os.chdir(args.root)
    print(get_version())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='get-version')

    parser.add_argument('--root', '-r', type=str, metavar='dir', default='.',
                        help='root directory of package')

    args = parser.parse_args()

    try:
        sys.exit(main(args))
    except RuntimeError as exc:
        sys.stderr.write('ERROR: {}\n'.format(exc))
    except KeyboardInterrupt:
        sys.exit(1)
