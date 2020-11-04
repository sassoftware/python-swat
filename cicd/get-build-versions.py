#!/usr/bin/env python

'''
Get the available Python extension versions for the given release

'''

from __future__ import print_function

import argparse
import io
import json
import os
import platform
import re
import requests
import subprocess
import sys
import zipfile


# Possible locations for CAS client TK packages
TK_URLS = [x + '/release/caspythnclnt/caspythnclnt_en.zip' for x in [
    '{tk_root}/sashpa/dev/mva-{release}f/cda/zippkg/{platform}',
    '{tk_root}/sashpa/day/mva-{release}/cda/zippkg/{platform}',
    '{tk_root}/sashpa/dev/mva-{release}f/cda/zippkg/lax',
    '{tk_root}/sashpa/day/mva-{release}/cda/zippkg/lax',
]]

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


def get_python_versions(platform):
    '''
    Retrieve all possible Python versions for the given platform

    This function actually uses information about the pandas library
    instead of Python itself. Pandas is the primary dependency of
    SWAT, so it is the limiting factor on what Python versions can
    be used.

    '''
    cmd = ['conda', 'search', '-q', '--json', '--subdir', platform, 'anaconda::pandas']
    out = subprocess.check_output(cmd)

    versions = set()
    for item in json.loads(out)['pandas']:
        pyver = [x for x in item['depends'] if x.startswith('python')][0]
        pyver = re.findall(r'(\d+\.\d+)(?:\.\d+)?', pyver)[0]
        versions.add(pyver)

    return versions


def main(args):
    ''' Main routine '''
    c_ext_versions = set()

    if args.platform == 'win-64':
        args.tk_root = args.tk_root.replace('unix', 'win')

    for url in TK_URLS:
        url = url.format(tk_root=args.tk_root, release=args.release,
                         platform=PLATFORM_MAP[args.platform])

        resp = requests.head(url)
        if resp.status_code > 200:
            continue

        resp = requests.get(url)

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                name = name.replace('pyswat', 'py27swat')
                m = re.search(r'py(\d)(\d)swat', name)
                if m:
                    c_ext_versions.add('{}.{}'.format(m.group(1), m.group(2)))

    if not c_ext_versions:
        print('ERROR: Could not locate C extensions.', file=sys.stderr)
        return 1

    py_versions = get_python_versions(args.platform)

    if not py_versions:
        print('ERROR: Could not retrieve Python versions.', file=sys.stderr)
        return 1

    print(' '.join(list(sorted(c_ext_versions.intersection(py_versions)))))


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
