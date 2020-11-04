#!/usr/bin/env python

'''
Return the preferred CAS protocol ('cas' if TK is available; 'http' otherwise)

'''

import argparse
import os
import platform
import re
import requests
import sys

# Possible locations for CAS client TK packages
TK_URL = '{tk_root}/sashpa/day/mva-{release}/cda/zippkg/{platform}' + \
         '/release/caspythnclnt/caspythnclnt_en.zip'

# Map of conda platform names to SAS platform names
PLATFORM_MAP = {
    'linux-64': 'lax',
    'linux-ppc64le': 'plx',
    'osx-64': 'm64',
    'win-64': 'wx6',
}


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
    if args.platform == 'win-64':
        args.tk_root = args.tk_root.replace('unix', 'win')

    url = TK_URL.format(tk_root=args.tk_root, release=args.release,
                        platform=PLATFORM_MAP[args.platform])

    resp = requests.head(url)
    if resp.status_code > 200:
        print('http')
        return

    print('cas')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip())

    parser.add_argument('--tk-root', type=str, metavar='url', required=True,
                        help='root path / URL of the CAS client packages')
    parser.add_argument('-p', '--platform', type=str, metavar='name',
                        default=get_platform(),
                        choices=['linux-64', 'linux-ppc64le', 'osx-64', 'win-64'],
                        help='platform to query files for')
    parser.add_argument('-r', '--release', type=str, metavar='vbXXXX',
                        default='vbviya',
                        help='TK library release')

    args = parser.parse_args()

    sys.exit(main(args) or 0)
