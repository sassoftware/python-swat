#!/usr/bin/env python

'''
Install TK libraries and C extensions

In order to support the binary CAS protocol, the SWAT package needs both
a C extension which surfaces the API for that protocol as well as the underlying
support libraries (known as TK). This utility will download the TK libraries
and the C extension from a repository and insert the extensions into the
proper directory in the SWAT package directory structure. This location is:

    swat/libs/{platform}/

Where `{platform}` is `win` or `linux`.

Since there may be multiple TK versions available at each release (for example,
shipped and hot-fix versions), the libraries are searched for in the proper
order.

'''

import argparse
import io
import json
import os
import platform
import re
try:
    import requests
except ImportError:
    pass
import subprocess
import sys
import zipfile

# Name of the package that contains C extensions
TK_PACKAGE_NAME = 'caspythnclnt'

# Possible locations for CAS client TK packages
TK_URLS = [x + '/release/{package}/{package}_en.zip' for x in [
    '{tk_base}/sashpa/dev/mva-{release}f/cda/zippkg/{platform}',
    '{tk_base}/sashpa/day/mva-{release}/cda/zippkg/{platform}',
    '{tk_base}/sashpa/wky/mva-{release}f/cda/zippkg/{platform}',
    '{tk_base}/sashpa/wky/mva-{release}/cda/zippkg/{platform}',
]]

# TK packages to install
TK_PKGS = [
    TK_PACKAGE_NAME,
    'tk',
    'tkcore',
    'tkcas3rdclnt',
    'tknls',
    'tkformats',
    'tkl4sas',
]

# File patterns that should be excluded from the installation
TK_EXCLUDES = [re.compile('^' + x + '$', flags=re.I) for x in [
    r'mkl_.+\.(so|dll)',
    r'libmkl_custom\.(so|dll)',
    r'tkclang\.(so|dll)',
    r'xorgpkg\.(so|dll)',
    r't[0-9][a-z][0-9](de|es|ja|zh|zt|ko|it|pl|fr)\.(so|dll)',
    r'(htclient|httplogin|.*arrow.*|libpaquet|libtkcpp).(so|dll)',
    r'(tkcasl|tkconsul|tkcudajit|tkhttpc?|tkek8s|tkmgpu|tkscript).(so|dll)',
]]

# File patterns that should be included in the installation
TK_INCLUDES = [re.compile('^' + x + '$', flags=re.I) for x in [
    r'.+\.so',
    r'.+\.pyd',
    r'.+\.dll',
    r'.+\.dylib',
]]

# Map of conda platform names to SAS platform names
PLATFORM_MAP = {
    'linux-64': 'lax',
    'linux-ppc64le': 'plx',
    'osx-64': 'm64',
    'win-64': 'wx6',
}


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


def extract_zip(root, data, py_versions=None):
    ''' Write files from zip file to directory '''
    with zipfile.ZipFile(io.BytesIO(data)) as zip_in:
        for name in zip_in.namelist():

            basename = os.path.basename(name)

            # Filter unneeded files
            exclude = True
            for item in TK_INCLUDES:
                if item.match(basename):
                    exclude = False
                    break
            if exclude:
                continue

            exclude = False
            for item in TK_EXCLUDES:
                if item.match(basename):
                    exclude = True
                    break
            if exclude:
                continue

            # Filter out extensions not supported by Anaconda
            if py_versions and re.match(r'_py\d*swat', basename):
                ver = re.match(r'_py(\d*)swat', basename).group(1) or '27'
                if ver not in py_versions:
                    print_err("***** extract_zip : version {} not in py_versions"
                              .format(ver))
                    continue

            # Write file
            out_path = os.path.join(root, basename)
            with open(out_path, 'wb') as out_file:
                out_file.write(zip_in.read(name))


def create_placeholders(root, data, py_versions=None):
    ''' Write placeholder files for each Python extension file '''
    with zipfile.ZipFile(io.BytesIO(data)) as zip_in:
        for name in zip_in.namelist():

            basename = os.path.basename(name)

            if not re.match(r'_py\d*swat', basename):
                continue

            # Filter out extensions not supported by Anaconda
            if py_versions:
                ver = re.match(r'_py(\d*)swat', basename).group(1) or '27'
                if ver not in py_versions:
                    print_err("***** create_placeholders : version {} not in py_versions"
                              .format(ver))
                    continue

            # Write placeholder file
            out_path = os.path.join(root, basename.split('.')[0] + '.na')
            with open(out_path, 'wb') as out_file:
                out_file.write(b'')


def create_restonly_placeholders(root):
    ''' Write placeholder files for each Python extension file '''
    py_versions = [x.replace('.', '')
                   for x in get_python_versions()]

    for ver in py_versions:
        if ver == '27':
            out_path = os.path.join(root, "_pyswat.na")
            print_err("***** create_restonly_placeholders : write placeholder file {}"
                      .format(out_path))
            with open(out_path, 'wb') as out_file:
                out_file.write(b'')
            out_path = os.path.join(root, "_pyswatw.na")
            print_err("***** create_restonly_placeholders : write placeholder file {}"
                      .format(out_path))
            with open(out_path, 'wb') as out_file:
                out_file.write(b'')
        else:
            out_path = os.path.join(root, "_py{}swat.na".format(ver))
            print_err("***** create_restonly_placeholders : write placeholder file {}"
                      .format(out_path))
            with open(out_path, 'wb') as out_file:
                out_file.write(b'')


def update_tk_version(root, version):
    ''' Add the TK version to the __init__.py file of the package '''
    tk_file = os.path.join(root, 'swat', '__init__.py')
    with open(tk_file, 'r') as tk_file_in:
        txt = tk_file_in.read()
        txt = re.sub(r'(__tk_version__\s*=\s*)(\S+)', r"\1'{}'"
                     .format(re.sub(r'f$', r'', version)), txt)
    with open(tk_file, 'w') as tk_file_out:
        tk_file_out.write(txt)


def get_packages(lib_root, tk_base, release, platform, pkgs, versions_only=False):
    '''
    Retrieve all TK packages

    Parameters
    ----------
    lib_root : string
        Directory where SWAT package exists
    tk_base : string
        Base URL of TK package repository
    release : string
        TK release
    platform : string
        Platform to search for TK components
    pkgs : list
        List of TK package names to retrieve
    versions_only : bool, optional
        Should the packages only be used to create placeholders for
        C extensions? This is used on platforms where TK components
        do not exist, but we need to know which versions of Python
        to build packages for.

    '''
    py_versions = []
    resp = None
    is_installed = True

    for pkg in pkgs:
        for url in TK_URLS:
            url = url.format(tk_base=tk_base,
                             release=release,
                             platform=PLATFORM_MAP[platform],
                             package=pkg)

            resp = requests.head(url, allow_redirects=True)

            if resp.status_code == 404:
                continue

            if resp.status_code != 200:
                raise RuntimeError('{} code occurred during download of {}'
                                   .format(resp.status_code, url))

            print_err(url)

            resp = requests.get(url, allow_redirects=True)

            if not py_versions:
                py_versions = [x.replace('.', '')
                               for x in get_python_versions()]

            if versions_only:
                create_placeholders(lib_root, resp.content, py_versions=py_versions)
            else:
                extract_zip(lib_root, resp.content, py_versions=py_versions)

            break

        # Package was not found, bail out
        if resp.status_code == 404:
            print_err(("***** get_packages : resp.status_code was 404, "
                       "bail out and set is_installed to False"))
            is_installed = False
            break

    return is_installed


def get_python_versions():
    '''
    Retrieve all possible Python versions.
    Currently, every platform supports the same python versions.
    '''

    versions = set()
    versions.add('2.7')
    versions.add('3.4')
    versions.add('3.6')
    versions.add('3.7')
    versions.add('3.8')
    versions.add('3.9')
    versions.add('3.10')
    versions.add('3.11')
    versions.add('3.12')
    return versions


def main(args):
    ''' Main routine '''
    if args.platform == 'win-64':
        args.tk_base = args.tk_base.replace('unix', 'win')

    # Fix TK versions (Linux-only at vb015; Windows-only at vb020)
    if args.release == 'vb020' and args.platform != 'win-64':
        args.release = 'vb015'
    elif args.release == 'vb015' and args.platform == 'win-64':
        args.release = 'vb020'

    # Directory for TK libraries
    lib_root = 'swat/lib/{}'.format({'linux-64': 'linux',
                                     'osx-64': 'mac',
                                     'linux-ppc64le': 'linux',
                                     'win-64': 'win'}[args.platform])
    lib_root = os.path.join(args.root, lib_root)

    # Create output directory
    os.makedirs(lib_root, exist_ok=True)

    if args.platform.lower() in ("linux-64", "win-64"):
        print_err("***** main : binary supported on platform {}".format(args.platform))
        is_installed = get_packages(lib_root, args.tk_base, args.release,
                                    args.platform, TK_PKGS)

        if not is_installed:
            print_err(("***** main : get_packages failed to get TK packages.  "
                       "Retrying with just placeholder files."))
            get_packages(lib_root, args.tk_base, args.release, 'linux-64',
                         [TK_PACKAGE_NAME], versions_only=True)
    else:
        print_err("***** main : REST ONLY platform {}".format(args.platform))
        is_installed = False
        create_restonly_placeholders(lib_root)

    update_tk_version(args.root, is_installed and args.release or 'none')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip(),
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('root', type=str, nargs='?', default='.',
                        help='root directory of Python package')

    parser.add_argument('-p', '--platform', type=str, metavar='<platform>',
                        default=get_platform(),
                        choices=['linux-64', 'osx-64', 'linux-ppc64le', 'win-64'],
                        help='platform libraries to install')
    parser.add_argument('-r', '--release', type=str, metavar='<release>',
                        default='vbviya', help='TK release')
    parser.add_argument('--tk-base', type=str, required=True, metavar='<url>',
                        help='base URL / path for TK repository')

    args = parser.parse_args()

    sys.exit(main(args) or 0)
