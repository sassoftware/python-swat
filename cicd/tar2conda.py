#!/usr/bin/env python

'''
Convert a tar.gz Python package distribution to a conda package

This tool takes a `tar.gz` of the SWAT package source, C extensions, and TK
files and converts it to a set of conda files using `conda build`. One conda
file is created for each supported Python version on each platform.

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import argparse
import contextlib
import glob
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
try:
    from urllib.request import urlretrieve, urlcleanup
except ImportError:
    from urllib import urlretrieve, urlcleanup

try:
    execfile
except NameError:
    def execfile(filename, global_vars, local_vars):
        with open(filename) as f:
            code = compile(f.read(), filename, 'exec')
            exec(code, global_vars, local_vars)


def print_err(*args, **kwargs):
    ''' Print a message to stderr '''
    sys.stderr.write(*args, **kwargs)
    sys.stderr.write('\n')


def tar_filter(tar_name, tar_info):
    '''
    Filter out compiled pieces of tar file

    Parameters
    ----------
    tar_name : string
        Basename of the tar file
    tar_info : TarInfo
        The tar information structure

    Returns
    -------
    :class:`TarInfo`

    '''
    if tar_info.name.endswith('.so'):
        return None
    if tar_info.name.endswith('.dll'):
        return None
    if tar_info.name.endswith('.dylib'):
        return None
    if tar_info.name.endswith('.pyd'):
        return None
    if tar_info.name == '__pycache__':
        return None
    tar_info.name = re.sub(r'^[^\\/]+', tar_name, tar_info.name)
    return tar_info


def update_recipe(recipe, **kwargs):
    '''
    Update recipe file with parameters

    Parameters
    ----------
    recipe : basestring
        Path to the conda recipe file
    **kwargs : keyword arguments, optional
        Substitution variables for fields in recipe

    '''
    # Add recipe filename as needed
    if os.path.isdir(recipe):
        recipe = os.path.join(recipe, 'meta.yaml')

    params = kwargs.copy()
    for name, value in params.items():
        if name == 'url':
            url = value
            continue

        if name == 'version':
            value = re.sub(r'-dev', r'.dev0', value)

        params[name] = value

    # Write variables to recipe
    out = []
    with open(recipe, 'r') as recipe_file:
        for line in recipe_file:

            if 'sha256' in line:
                continue

            if url:
                url = url.replace('\\', '/')
                if os.path.isdir(url):
                    line = re.sub(r'''^(\s+)(?:url|path):.*?(\s*#\s*\[.+?\]\s*)?$''',
                                  r'''\1path: %s\2''' % url, line)
                else:
                    line = re.sub(r'''^(\s+)(?:url|path):.*?(\s*#\s*\[.+?\]\s*)?$''',
                                  r'''\1url: %s\2''' % url, line)

            for key, value in params.items():
                if key == 'url':
                    continue
                line = re.sub(r'''^(\{%%\s*set\s+%s\s*=\s*)'[^']+'(\s*%%\}\s*)$''' % key,
                              r'''\1'%s'\2''' % value, line)

            out.append(line.rstrip())

    # Write recipe file
    with open(recipe, 'w') as recipe_file:
        recipe_file.write('\n'.join(out))
        recipe_file.write('\n')


def find_packages():
    ''' Dummy find_packages for setup.py:setup function '''
    return []


def get_version(pkg_dir):
    '''  Retrieve version number from setup.py '''

    def setup(**kwargs):
        ''' Override definition of setup() to print key/values to stdout '''
        kwargs['version'] = re.sub(r'\.dev$', '.dev0',
                                   kwargs['version'].replace('-dev', '.dev'))
        print(json.dumps(kwargs))

    # Write text metadata information
    metadata = io.StringIO()

    __file__ = os.path.join(pkg_dir, 'setup.py')
    glbls = dict(globals())
    glbls['__file__'] = __file__
    lcls = dict(locals())

    with TemporaryDirectory() as temp:
        tmp_setup = os.path.join(temp, 'setup.py')
        with open(__file__, 'r', encoding='utf-8') as setup_file:
            with open(tmp_setup, 'w', encoding='utf-8') as setup_file_out:
                setup_file_out.write(
                    re.sub(r'from setuptools .*', r'', setup_file.read()))
            with redirect_stdout(metadata):
                try:
                    execfile(tmp_setup.encode('utf-8'), glbls, lcls)
                except NameError:
                    with open(tmp_setup, encoding='utf-8') as f:
                        code = compile(f.read(), tmp_setup, 'exec')
                        exec(code, glbls, lcls)

    metadata = json.loads(metadata.getvalue())

    return metadata['version']


class TemporaryDirectory(object):
    '''
    Context manager for tempfile.mkdtemp()

    This class is available in python +v3.2.

    '''
    def __enter__(self):
        self.dir_name = tempfile.mkdtemp()
        return self.dir_name

    def __exit__(self, exc_type, exc_value, traceback):
        import atexit
        atexit.register(shutil.rmtree, self.dir_name)


@contextlib.contextmanager
def redirect_stdout(target):
    ''' Redirect stdout to given file-like object '''
    original = sys.stdout
    sys.stdout = target
    yield
    sys.stdout = original


def ensure_dot_separator(version):
    ''' Make sure that Python versions have a dot separator '''
    if '.' not in version:
        return '%s.%s' % (version[0], version[1:])
    return version


open = io.open


def main(url, args):
    ''' Convert given tar file to conda packages '''

    orig_url = url

    cwd = os.getcwd()

    args.output_folder = os.path.abspath(args.output_folder)

    os.makedirs(args.output_folder, exist_ok=True)

    args.recipe_dir = os.path.abspath(args.recipe_dir)
    if os.path.isfile(args.recipe_dir):
        args.recipe_dir = os.path.dirname(args.recipe_dir)

    download = False
    if url.startswith('http:') or url.startswith('https:'):
        print_err('> download %s' % url)
        download = True
        url, headers = urlretrieve(url)
    elif os.path.exists(url):
        url = os.path.abspath(url)

    with TemporaryDirectory() as temp:

        with tarfile.open(url, url.endswith('tar') and 'r:' or 'r:gz') as tar:
            names = tar.getnames()
            tar.extractall(temp)

        # Clean up
        if download:
            urlcleanup()

        os.chdir(temp)

        # Locate pyswat extensions
        versions = []
        platform = 'any'

        # Platform overrides
        if '-osx' in orig_url:
            platform = 'mac'

        for name in names:
            m = re.search(r'(\w+)[\\/](_py(\d*)swat(w?)\.\w+)$', name)
            if m:
                # Anaconda doesn't do narrow character builds; just use _pyswatw for 2.7
                if m.group(2).split('.')[0] == '_pyswat':
                    continue
                platform = m.group(1)
                versions.append(dict(extension=m.group(2),
                                     pyversion='.'.join(list(m.group(3) or '27'))))

#       # Filter version list
#       if not versions:
#           for ver in re.split(r'[,\s+]', args.python):
#               versions.append(dict(pyversion=ensure_dot_separator(ver)))
#       else:
#           arg_versions = [ensure_dot_separator(x)
#                           for x in re.split(r'[,\s+]', args.python)]
#           new_versions = []
#           for ver in versions:
#               if ver['pyversion'] not in arg_versions:
#                   continue
#               new_versions.append(ver)
#           versions = new_versions

        # Anaconda only has one build for Python 2.7
        versions = [x for x in versions if x['pyversion'] != '2.7u']

        url = os.path.join(temp, glob.glob('python-swat*')[0])
        update_recipe(args.recipe_dir, url=url, version=get_version(url))

        with TemporaryDirectory() as tmpext:

            # Move SWAT extensions to temporary location
            for ext in glob.glob(os.path.join('swat', 'lib', '*', '_py*swat*.*')):
                shutil.move(ext, tmpext)

            # Create wheel for each extension found
            for info in sorted(versions, key=lambda x: float(x['pyversion'])):
                cmd = ['conda', 'build', '-q', '--no-test']
                cmd.extend(['--python', info['pyversion']])
                if args.output_folder:
                    cmd.extend(['--output-folder', args.output_folder])
                if args.override_channels:
                    cmd.append('--override-channels')
                if args.channel:
                    for chan in args.channel:
                        cmd.extend(['--channel', chan])
                cmd.append(args.recipe_dir)

                extbase = '_py{}swat'.format(info['pyversion'].replace('.', '')
                                                              .replace('27', ''))
                for ext in glob.glob(os.path.join(tmpext, extbase + '.*')):
                    print_err('> copy %s' % ext)
                    shutil.copy(ext, os.path.join('swat', 'lib', platform, ext))

                print_err('>' + ' '.join(cmd))
                try:
                    print_err(subprocess.check_output(cmd).decode('utf-8'))
                except subprocess.CalledProcessError as exc:
                    out = exc.output.decode('utf-8')
                    print_err(out)
                    # Conda build fails intermittently on Windows when cleaning
                    # up at the end. Ignore these errors on Windows.
                    if not ('WinError 32' in out and 'used by another process' in out):
                        raise

                for ext in glob.glob(os.path.join(tmpext, extbase + '.*')):
                    print_err('> remove %s' % ext)
                    os.remove(os.path.join('swat', 'lib', platform, ext))

    os.chdir(cwd)


if __name__ == '__main__':

    opts = argparse.ArgumentParser(description=__doc__.strip(),
                                   formatter_class=argparse.RawTextHelpFormatter)

    opts.add_argument('url', type=str,
                      help='input file / url')

    opts.add_argument('--build', '-b', default=0, type=int,
                      help='build number')
    opts.add_argument('--channel', '-c', type=str, nargs='*',
                      help='additional chanel to search')
    opts.add_argument('--debug', action='store_true',
                      help='enable conda build debug logging')
    opts.add_argument('--output-folder', type=str, default='',
                      help='folder to create the output package in')
    opts.add_argument('--override-channels', action='store_true', default=False,
                      help='disable searching default or .condarc channels')
    opts.add_argument('--recipe-dir', '-r', required=True, type=str,
                      help='path to recipe file')
#   opts.add_argument('--python', default='2.7,2.7u,3.5,3.6,3.7,3.8', type=str,
#                     help='python package versions (for filtering packages with '
#                          'extensions or forcing for non-binary platforms)')

    args = opts.parse_args()

    main(args.url, args)
