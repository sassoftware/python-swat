#!/usr/bin/env python

'''
Generate a Tox config file for all test configurations

To create a test matrix for all supported versions of Python,
this utility takes a template `tox.ini` file and adds additional
Tox environments to the end of it. The version of Python and
pandas used in the environments are determined by those available
on Anaconda.

The versions of Python on Anaconda are intesected with the versions
of Python supported in the SWAT C extensions in the package
(e.g., _pyswat.so (2.7), _pyswa35.so (3.5), etc.). Pandas versions
are distributed across the version of Python to increase overall
coverage.

'''

import argparse
import glob
import json
import os
import platform
import re
import subprocess
import sys


def print_err(*args, **kwargs):
    ''' Print a message to stderr '''
    sys.stderr.write(*args, **kwargs)
    sys.stderr.write('\n')


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


def get_supported_versions(root):
    ''' Get the Python versions supported in the current version of SWAT '''
    out = set()
    for ver in glob.glob(os.path.join(root, 'swat', 'lib', '*', '_py*swat*.*')):
        ver = re.match(r'_py(\d*)swat', os.path.basename(ver)).group(1) or '27'
        if (ver[0] == '2'):
            # print_err("get_supported_versions: skipping {}".format(ver))
            continue
        if ((int(ver[1:]) < 7) and (ver[0] == '3')):
            # print_err("get_supported_versions: skipping {}".format(ver))
            continue
        out.add('{}.{}'.format(ver[0], ver[1:]))
    return list(sorted(out))


def main(args):
    ''' Main routine '''

    info = set()

    cmd = ['conda', 'search', '-q', '--json',
           '--subdir', args.platform, 'defaults::pandas']
    out = subprocess.check_output(cmd)

    for item in json.loads(out)['pandas']:
        pyver = [x for x in item['depends'] if x.startswith('python')][0]
        pyver = re.findall(r'(\d+\.\d+)(?:\.\d+)?', pyver)[0]
        pdver = re.findall(r'(\d+\.\d+)(?:\.\d+)?', item['version'])[0]
        info.add((pyver, pdver))

    supported = get_supported_versions(args.root)
    if not supported:
        print_err('ERROR: Could not determine supported versions of Python.')
        return 1

    final = {}
    for pyver, pdver in sorted(info):
        if pyver not in supported:
            continue
        pdvers = final.setdefault(pyver, set())
        pdvers.add(pdver)
        final[pyver] = pdvers

    # Pick a subset of the matrix to test. Try taking unique combinations.
    subset = []
    pyvers = list(sorted(final.keys()))

    # Map oldest Python version to oldest pandas version
    pdvers = list(sorted(final[pyvers[0]]))
    subset.append((pyvers[0], pdvers[0]))

    # Remaining Python versions get mapped to same interval of pandas version
    pyvers = pyvers[1:]
    for i in range(-len(pyvers), 0):
        pdvers = list(sorted(final[pyvers[i]]))
        try:
            subset.append((pyvers[i], pdvers[i]))
        except IndexError:
            subset.append((pyvers[i], pdvers[0]))

    if not pyvers:
        print_err('ERROR: No Python versions were found.')
        return 1

    if not pdvers:
        print_err('ERROR: No pandas versions were found.')
        return 1

    # Generate Tox configurations for testenvs
    for pkg in ['conda', 'pip']:
        out = ['', '#', '# BEGIN GENERATED ENVIRONMENTS', '#', '']
        envlist = []
        prev_pyver = ''
        for pyver, pdver in subset:
            if prev_pyver != pyver:
                out.append('#')
                out.append('# Python {}'.format(pyver))
                out.append('#')
            prev_pyver = pyver

            name = 'py{}-{}-cicd'.format(pyver.replace('.', ''), pkg)
            envlist.append(name)
            out.append('[testenv:{}]'.format(name))
            out.append('commands = {{[testenv:{}]commands}}'.format(pkg))
            out.append('conda_deps =')
            out.append('    {[testenv]conda_deps}')
            out.append('    pandas=={}*'.format(pdver))
            out.append('')

        # Write new Tox configuration
        with open(args.tox_ini, 'r') as tox_in:
            lines = iter(tox_in.readlines())

        out_file = '{}-{}.ini'.format(os.path.splitext(args.tox_ini)[0], pkg)
        with open(out_file, 'w') as tox_out:
            for line in lines:
                # Override envlist
                if line.startswith('envlist'):
                    tox_out.write('envlist =\n')
                    for item in envlist:
                        tox_out.write('    {}\n'.format(item))
                    for line in lines:
                        if not line.startswith(' '):
                            break
                tox_out.write(line)

            # Write new environments
            for item in out:
                tox_out.write(item)
                tox_out.write('\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip(),
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('tox_ini', type=str, metavar='ini-file',
                        help='path to tox.ini file')

    parser.add_argument('--root', type=str, metavar='<directory>', default='.',
                        help='root directory of Python package')
    parser.add_argument('--platform', '-p', type=str, metavar='<platform>',
                        choices=['linux-64', 'osx-64', 'win-64', 'linux-ppc64le'],
                        default=get_platform(),
                        help='platform of the resulting package')

    args = parser.parse_args()

    sys.exit(main(args) or 0)
