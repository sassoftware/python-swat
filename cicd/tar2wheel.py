#!/usr/bin/env python

''' Convert a tar.gz Python package distribution to a wheel '''

from __future__ import print_function, division, absolute_import, unicode_literals

import argparse
import contextlib
import functools
import gzip
import glob
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import time
import zipfile
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


def setup2json(**kwargs):
    ''' Render setup parameters as JSON '''
    print(json.dumps(kwargs))


def setup2str(**kwargs):
    '''
    Print metadata from setup.py:setup(...) call

    The metadata specification is at the following URL:
    https://packaging.python.org/specifications/core-metadata/

    '''
    print('Metadata-Version: 2.1')
    print('Name: %s' % kwargs['name'])
    print('Version: %s' % re.sub(r'\.dev$', '.dev0',
                                 kwargs['version'].replace('-dev', '.dev')))
    print('Summary: %s' % kwargs['description'])
    print('Home-page: %s' % kwargs['url'])
    if 'author' in kwargs:
        print('Author: %s' % kwargs['author'])
    if 'author_email' in kwargs:
        print('Author-email: %s' % kwargs['author_email'])
    if 'maintainer' in kwargs:
        print('Maintainer: %s' % kwargs['maintainer'])
    if 'maintainer_email' in kwargs:
        print('Maintainer-email: %s' % kwargs['maintainer_email'])
    print('License: %s' % kwargs['license'])

    for item in kwargs['install_requires']:
        item = item.strip()
        if ' ' in item:
            print('Requires-Dist: %s (%s)' % tuple(item.split(' ', 1)))
        else:
            print('Requires-Dist: %s' % item)

    print('Platform: any')

    for item in kwargs['classifiers']:
        print('Classifier: %s' % item)

    if 'python_requires' in kwargs:
        print('Requires-Python: %s' % kwargs['python_requires'])

    if 'long_description_content_type' in kwargs:
        print('Description-Content-Type: %s' % kwargs['long_description_content_type'])

    print('')
    print(kwargs['long_description'].strip())


def find_packages():
    ''' Dummy find_packages for setup.py:setup function '''
    return []


def sha256_file(filename):
    ''' Create sha256 of file contents '''
    hash = hashlib.sha256()
    with open(filename, 'rb', buffering=0) as infile:
        for data in iter(lambda: infile.read(128 * 1024), b''):
            hash.update(data)
    return hash.hexdigest()


def sha256_string(data):
    ''' Create sha256 of data '''
    hash = hashlib.sha256()
    if hasattr(data, 'encode'):
        hash.update(data.encode('utf-8'))
    else:
        hash.update(data)
    return hash.hexdigest()


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


def dir2pypi(directory, pkg_name='swat'):
    '''
    Convert directory of wheels/tgzs to pypi structure

    Parameters
    ----------
    directory : string
        Name of the directory to convert

    '''
    os.makedirs(os.path.join(directory, 'simple', pkg_name), exist_ok=True)

    with open(os.path.join(directory, 'simple', 'index.html'), 'w') as index:
        index.write('''<html><head>''')
        index.write('''<title>Simple Index</title>''')
        index.write('''<meta name='api-version' value='2' />''')
        index.write(''''</head><body>\n''')
        index.write('''<a href='{0}/'>{0}</a><br />\n'''.format(pkg_name))
        index.write('''</body></html>\n''')

    with open(os.path.join(directory, 'simple', pkg_name, 'index.html'), 'w') as index:
        for f in sorted(glob.glob(os.path.join(directory, '*.tar.gz'))
                        + glob.glob(os.path.join(directory, '*.whl'))):
            index.write('''<a href='{0}'>{0}</a><br />\n'''.format(os.path.basename(f)))
            shutil.move(f, os.path.join(directory, 'simple', pkg_name))


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
    original = sys.stdout
    sys.stdout = target
    yield
    sys.stdout = original


open = io.open


def main(url, args):
    ''' Convert given tar file to wheel '''

    cwd = os.getcwd()

    orig_url = url

    download = False
    if url.startswith('http:') or url.startswith('https:'):
        print('Downloading %s' % url, file=sys.stderr)
        download = True
        url, headers = urlretrieve(url)

    with TemporaryDirectory() as temp:

        with tarfile.open(url, url.endswith('tar') and 'r:' or 'r:gz') as tar:
            names = tar.getnames()
            tar.extractall(temp)

        # Clean up
        if download:
            urlcleanup()

        outdir = os.path.abspath(args.dir)

        os.makedirs(outdir, exist_ok=True)

        os.chdir(temp)

        # Get root directory name
        root = names.pop(0)

        # Write text metadata information
        setup = setup2str
        metadata = io.StringIO()
        __file__ = os.path.join(temp, root, 'setup.py')
        tmp_setup = __file__.replace('setup.py', '.setup.py')
        glbls = dict(globals())
        glbls['__file__'] = __file__
        lcls = dict(locals())
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
        metadata = metadata.getvalue()

        # Write JSON metadata information
        setup = setup2json
        metadata_json = io.StringIO()
        __file__ = os.path.join(temp, root, 'setup.py')
        tmp_setup = __file__.replace('setup.py', '.setup.py')
        glbls = dict(globals())
        glbls['__file__'] = __file__
        lcls = dict(locals())
        with open(__file__, 'r', encoding='utf-8') as setup_file:
            with open(tmp_setup, 'w', encoding='utf-8') as setup_file_out:
                setup_file_out.write(
                    re.sub(r'from setuptools .*', r'', setup_file.read()))
            with redirect_stdout(metadata_json):
                try:
                    execfile(tmp_setup.encode('utf-8'), glbls, lcls)
                except NameError:
                    with open(tmp_setup, encoding='utf-8') as f:
                        code = compile(f.read(), tmp_setup, 'exec')
                        exec(code, glbls, lcls)
        metadata_json = metadata_json.getvalue()

        # Get version
        version = re.search(r'^Version:\s+(\S+)', metadata, flags=re.M).group(1)

        # Get top-level directory name
        for item in names:
            match = re.match(r'%s[\\/]([\w_]+)[\\/]__init__.py' % root, item)
            if match:
                top_level = match.group(1)
                break

        # Create source distribution
        if args.source_dist:

            tar_name = '%s-%s' % (top_level, version)

            print('Creating %s.tar.gz' % tar_name, file=sys.stderr)

            with tarfile.TarFile('%s.tar' % tar_name, mode='w') as tar:
                tar.add(root, recursive=True,
                        filter=functools.partial(tar_filter, tar_name))

                # Add metadata
                pkg_info = io.BytesIO(metadata.encode('utf-8'))
                info = tarfile.TarInfo(
                    name=os.path.join('%s-%s', 'PKG-INFO') % (top_level, version))
                info.size = len(metadata)
                info.mtime = int(time.time())
                tar.addfile(info, pkg_info)

                # Add metadata.json
                pkg_info = io.BytesIO(metadata_json.encode('utf-8'))
                info = tarfile.TarInfo(
                    name=os.path.join('%s-%s', 'PKG-INFO') % (top_level, version))
                info.size = len(metadata_json)
                info.mtime = int(time.time())
                tar.addfile(info, pkg_info)

            with open('%s.tar' % tar_name, 'rb') as tar_in:
                with gzip.open(
                        os.path.join(outdir, '%s.tar.gz' % tar_name), 'wb') as tgz_out:
                    shutil.copyfileobj(tar_in, tgz_out)

        # Locate pyswat extensions
        versions = []
        platform = 'any'
        arch = 'x86'

        # Platform overrides
        if '-ppc' in orig_url:
            arch = 'ppc'
        if '-osx' in orig_url:
            platform = 'mac'

        for name in names:
            m = re.search(r'(\w+)[\\/](_py(\d*)swat(w?)\.\w+)$', name)
            if m:
                platform = m.group(1)
                versions.append(dict(extension=m.group(2),
                                     pyversion='cp%s' % (m.group(3) or '27'),
                                     abi='cp%sm%s' % ((m.group(3) or '27'),
                                                      m.group(4) and 'u' or '')))
                if int(versions[-1]['pyversion'].replace('cp', '')) >= 38:
                    versions[-1]['abi'] = versions[-1]['abi'].replace('m', '')

#       # Filter version list
#       if not versions:
#           for ver in re.split(r'[,\s+]', args.python):
#               m = re.search(r'(\d\.?\d)(u?)', ver)
#               if m:
#                   versions.append(
#                       dict(pyversion='cp%s' % m.group(1).replace('.', ''),
#                            abi='cp%sm%s' % (m.group(1).replace('.', ''), m.group(2))))
#                   if int(versions[-1]['pyversion'].replace('cp', '')) >= 38:
#                       versions[-1]['abi'] = versions[-1]['abi'].replace('m', '')
#       else:
#           arg_versions = ['cp{}'.format(x.replace('.', ''))
#                           for x in re.split(r'[,\s+]', args.python)]
#           new_versions = []
#           for ver in versions:
#               if ver['pyversion'] not in arg_versions:
#                   continue
#               new_versions.append(ver)
#           versions = new_versions

        # Setup platform tag
        tag = 'py2.py3-none-any'
        if platform == 'linux':
            if arch == 'ppc':
                tag = '%(pyversion)s-%(abi)s-manylinux2014_ppc64le'
            else:
                tag = '%(pyversion)s-%(abi)s-manylinux1_x86_64'
        elif platform == 'win':
            tag = '%(pyversion)s-%(abi)s-win_amd64'
        elif platform == 'mac':
            tag = '%(pyversion)s-%(abi)s-macosx_10_9_x86_64'

        wheel = '%s\n' % '\n'.join([
            'Wheel-Version: 1.0',
            'Generator: tar2wheel (0.1.0)',
            'Root-Is-Purelib: %s' % (platform == 'any' and 'true' or 'false'),
            'Tag: %s' % tag,
            'Build: %s' % args.build,
        ])

        # Create wheel for each extension found
        for pyver in sorted(versions, key=lambda x: x['abi']):

            # Create wheel file
            zip_name = '%s-%s-%s-%s.whl' % (top_level, version, args.build, tag % pyver)
            zip_name = os.path.join(outdir, zip_name)

            print('Creating %s' % zip_name, file=sys.stderr)

            with zipfile.ZipFile(zip_name, 'w', compression=zipfile.ZIP_DEFLATED) as zip:

                # Add files and create record information
                record = []
                for name in names:
                    # Always use forward slash; even on Windows.
                    if not name.startswith('%s/%s' % (root, top_level)):
                        continue
                    if name.endswith('.pyc'):
                        continue
                    if re.search(r'[\\/]_py\d*swatw?\.\w+$', name):
                        if not name.endswith(pyver['extension']):
                            continue
                    if os.path.isfile(name):
                        # Always use forward slash; even on Windows.
                        zip.write(name, name.split('/', 1)[-1])
                        record.append('%s,sha256=%s,%s' % (name.split(os.sep, 1)[-1],
                                                           sha256_file(name),
                                                           os.path.getsize(name)))

                # Add metadata files
                dist_info = '%s-%s.dist-info' % (top_level, version)

                record.append('%s,sha=%s,%s' % (os.path.join(dist_info, 'METADATA'),
                                                sha256_string(metadata),
                                                len(metadata)))
                record.append('%s,sha=%s,%s' % (os.path.join(dist_info, 'metadata.json'),
                                                sha256_string(metadata_json),
                                                len(metadata_json)))
                record.append('%s,sha=%s,%s' % (os.path.join(dist_info, 'WHEEL'),
                                                sha256_string(wheel % pyver),
                                                len(wheel % pyver)))
                record.append('%s,sha=%s,%s' % (os.path.join(dist_info, 'top_level.txt'),
                                                sha256_string(top_level),
                                                len(top_level)))
                record.append(
                    '%s,,' % os.path.join(dist_info, 'RECORD').split(os.sep, 1)[-1])

                record = '%s\n' % '\n'.join(record)

                zip.writestr(os.path.join(dist_info, 'top_level.txt'), top_level)
                zip.writestr(os.path.join(dist_info, 'METADATA'),
                             metadata.encode('utf-8'))
                zip.writestr(os.path.join(dist_info, 'metadata.json'),
                             metadata_json.encode('utf-8'))
                zip.writestr(os.path.join(dist_info, 'WHEEL'), wheel % pyver)
                zip.writestr(os.path.join(dist_info, 'RECORD'), record)

    # Convert directory to pypi form if requested
    if args.pypi:
        dir2pypi(outdir)

    os.chdir(cwd)


if __name__ == '__main__':

    opts = argparse.ArgumentParser(description=__doc__.strip())

    opts.add_argument('urls', type=str, nargs='+',
                      help='input files / urls')

    opts.add_argument('--dir', '-d', default='.', type=str, metavar='<directory>',
                      help='output directory')
    opts.add_argument('--build', '-b', default=0, type=int, metavar='#',
                      help='build number')
    opts.add_argument('--source-dist', '-s', action='store_true',
                      help='create source distribution in addition')
    opts.add_argument('--pypi', action='store_true',
                      help='generate a PyPI directory structure')
#   opts.add_argument('--python', default='2.7,2.7u,3.5,3.6,3.7,3.8',
#                     type=str, metavar='<X.Y<,X.Z...>>',
#                     help='python versions (for filtering packages with extensions '
#                          'or forcing for non-binary platforms)')

    args = opts.parse_args()

    for url in args.urls:
        main(url, args)
        print(file=sys.stderr)
        args.source_dist = False
