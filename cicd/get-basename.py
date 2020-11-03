#!/usr/bin/env python

'''
Return the basename for the package

If no release is specified, only the short version of the basename is returned.

    python-swat-{version}

If a release is specified, the complete basename is returned. If no platform is
specified, the platform the program is running on is used.

    python-swat-{version}+{release}-{platform}

'''

import argparse
import glob
import os
import platform
import re
import sys


def get_platform():
    ''' Return the Anaconda platform name for the current platform '''
    plat = platform.system().lower()
    if 'darwin' in plat:
        return 'osx-64'
    if plat.startswith('win'):
        return 'win-64'
    if 'linux' in plat:
        machine = platform.machine().lower()
        if 'x86' in machine:
            return 'linux-64'
        if 'ppc' in machine:
            return 'linux-ppc64le'
    return 'unknown'


def main(args):
    ''' Main routine '''
    files = glob.glob(os.path.join(args.directory, 'setup.py'))

    for f in files:
        if os.path.isfile(f):
            with open(f, 'r') as f_in:
                for line in f_in:
                    m = re.search(r'''^\s*version\s*=\s*['"]([^'"]+)['"]''', line)
                    if m:
                        if args.release:
                            print('python-swat-{}+{}-{}'
                                  .format(m.group(1), args.release, args.platform))
                        else:
                            print('python-swat-{}'.format(m.group(1)))
                        return 0

    sys.stderr.write('ERROR: Could not find setup.py file.\n')
    return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip())

    parser.add_argument('directory', type=str, metavar='dir', nargs='?', default='.',
                        help='directory to retrieve version information from')

    parser.add_argument('--release', '-r', type=str, metavar='tk-release',
                        help='TK release to use in output name')
    parser.add_argument('--platform', '-p', type=str, metavar='platform',
                        default=get_platform(),
                        help='platform name to use in output name')

    args = parser.parse_args()

    sys.exit(main(args))
