#!/usr/bin/env python
# encoding: utf-8
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

'''
Utilities for testing

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import os
import re
import sys
import unittest
import pandas as pd
import warnings
from six.moves.urllib.parse import urlparse
from swat.config import OptionWarning
from swat.cas.connection import CAS

UUID_RE = r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'

RE_TYPE = type(re.compile(r''))


warnings.filterwarnings('ignore', category=OptionWarning)
warnings.filterwarnings('ignore', category=RuntimeWarning)


class TestCase(unittest.TestCase):
    ''' TestCase with SWAT Customizations '''

    def assertRegex(self, *args, **kwargs):
        ''' Compatibility across unittest versions '''
        if hasattr(unittest.TestCase, 'assertRegex'):
            return unittest.TestCase.assertRegex(self, *args, **kwargs)
        return self.assertRegexpMatches(*args, **kwargs)

    def assertNotEqual(self, *args, **kwargs):
        ''' Compatibility across unittest versions '''
        if hasattr(unittest.TestCase, 'assertNotEqual'):
            return unittest.TestCase.assertNotEqual(self, *args, **kwargs)
        return self.assertNotEquals(*args, **kwargs)

    def assertEqual(self, *args, **kwargs):
        ''' Compatibility across unittest versions '''
        if hasattr(unittest.TestCase, 'assertEqual'):
            return unittest.TestCase.assertEqual(self, *args, **kwargs)
        return self.assertEquals(*args, **kwargs)

    def assertContainsMessage(self, results, expectedMsg):
        ''' See if expected message is in results '''
        if isinstance(expectedMsg, RE_TYPE):
            for msg in results.messages:
                if expectedMsg.match(msg):
                    return
        else:
            for i in range(len(results.messages)):
                if expectedMsg in results.messages[i]:
                    return
        raise ValueError('Expected message not found: ' + expectedMsg)

    def replaceNaN(self, row, nan):
        ''' Replace NaNs in a iterable with specified value '''
        row = list(row)
        for i, x in enumerate(row):
            if pd.isnull(x):
                row[i] = nan
        return row

    def assertTablesEqual(self, a, b, fillna=-999999, sortby=None, precision=None):
        ''' Compare DataFrames / CASTables '''
        if hasattr(a, 'to_frame'):
            a = a.to_frame()
        if hasattr(b, 'to_frame'):
            b = b.to_frame()
        if sortby:
            a = a.sort_values(sortby, na_position='first')
            b = b.sort_values(sortby, na_position='first')
        self.assertEqual(list(a.columns), list(b.columns))
        a = a.fillna(value=fillna)
        b = b.fillna(value=fillna)
        if precision is not None:
            a = a.round(decimals=precision)
            b = b.round(decimals=precision)
        for lista, listb in zip(list(a.to_records(index=False)),
                                list(b.to_records(index=False))):
            self.assertEqual(list(lista), list(listb))

    def assertColsEqual(self, a, b, fillna=-999999, sort=False, precision=None):
        ''' Compare Series / CASColumns '''
        if hasattr(a, 'to_series'):
            a = a.to_series()
        if hasattr(b, 'to_series'):
            b = b.to_series()
        a = a.fillna(value=fillna)
        b = b.fillna(value=fillna)
        if precision is not None:
            a = a.round(decimals=precision)
            b = b.round(decimals=precision)
        if sort:
            a = list(sorted(a.tolist()))
            b = list(sorted(b.tolist()))
        else:
            a = a.tolist()
            b = b.tolist()
        self.assertEqual(a, b)


def get_casout_lib(server_type):
    ''' Get the name of the output CASLib '''
    out_lib = os.environ.get('CAS_OUT_LIB',
                             os.environ.get('CASOUTLIB', 'CASUSER'))
    if '.mpp' in server_type:
        out_lib = os.environ.get('CAS_MPP_OUT_LIB',
                                 os.environ.get('CASMPPOUTLIB', out_lib))
    return out_lib


def get_cas_data_lib(server_type):
    ''' Get the name of data CASLib '''
    data_lib = os.environ.get('CAS_DATA_LIB',
                              os.environ.get('CASDATALIB', 'CASTestTmp'))
    if '.mpp' in server_type:
        data_lib = os.environ.get('CAS_MPP_DATA_LIB',
                                  os.environ.get('CASMPPDATALIB', 'HPS'))
    return data_lib


def get_user_pass():
    '''
    Get the username and password from the environment if possible

    If the environment does not contain a username and password,
    they will be retrieved from a ~/.authinfo file.

    '''
    username = None
    password = None
    if 'CAS_USER' in os.environ:
        username = os.environ['CAS_USER']
    elif 'CASUSER' in os.environ:
        username = os.environ['CASUSER']
    if 'CAS_TOKEN' in os.environ:
        password = os.environ['CAS_TOKEN']
    elif 'CASTOKEN' in os.environ:
        password = os.environ['CASTOKEN']
    elif 'CAS_PASSWORD' in os.environ:
        password = os.environ['CAS_PASSWORD']
    elif 'CASPASSWORD' in os.environ:
        password = os.environ['CASPASSWORD']
    return username, password


def get_host_port_proto():
    '''
    Get the host, port and protocol from a .casrc file

    NOTE: .casrc files are written in Lua

    Returns
    -------
    (cashost, casport, casprotocol)

    '''
    cashost = None
    casport = None
    casprotocol = None

    url = None
    for name in ['CAS_URL', 'CASURL', 'CAS_HOST', 'CASHOST',
                 'CAS_HOSTNAME', 'CASHOSTNAME']:
        if name in os.environ:
            url = os.environ[name]
            break

    casport = casport or os.environ.get('CAS_PORT',
                                        os.environ.get('CASPORT'))
    if casport:
        casport = int(casport)

    casprotocol = casprotocol or os.environ.get('CAS_PROTOCOL',
                                                os.environ.get('CASPROTOCOL'))

    if url:
        cashost, casport, username, password, casprotocol = \
            CAS._get_connection_info(url, casport, casprotocol, None, None, None)

    if cashost and casport:
        return cashost, casport, casprotocol

    # If there is no host or port in the environment, look for .casrc
    casrc = None
    rcname = '.casrc'
    homepath = os.path.abspath(os.path.normpath(
        os.path.join(os.path.expanduser(os.environ.get('HOME', '~')), rcname)))
    upath = os.path.join(r'u:', rcname)
    cfgfile = os.path.abspath(os.path.normpath(rcname))

    while not os.path.isfile(cfgfile):
        if os.path.dirname(homepath) == os.path.dirname(cfgfile):
            break
        newcfgfile = os.path.abspath(os.path.normpath(rcname))
        if os.path.dirname(cfgfile) == os.path.dirname(newcfgfile):
            break

    if os.path.isfile(cfgfile):
        casrc = cfgfile
    elif os.path.exists(homepath):
        casrc = homepath
    elif os.path.exists(upath):
        casrc = upath
    else:
        return cashost, casport, casprotocol

    return _read_casrc(casrc)


def _read_casrc(path):
    '''
    Read the .casrc file using Lua

    Parameters
    ----------
    path : string
        Path to the .casrc file

    Returns
    -------
    (cashost, casport, casprotocol)

    '''
    cashost = None
    casport = None
    casprotocol = None

    if not os.path.isfile(path):
        return cashost, casport, casprotocol

    try:
        from lupa import LuaRuntime
        lua = LuaRuntime()
        lua.eval('dofile("%s")' % path)
        lg = lua.globals()

    except ImportError:
        import subprocess
        import tempfile

        lua_script = tempfile.TemporaryFile(mode='w')
        lua_script.write('''
            if arg[1] then
                dofile(arg[1])
                for name, value in pairs(_G) do
                    if name:match('cas') then
                        print(name .. ' ' .. tostring(value))
                    end
                end
            end
        ''')
        lua_script.seek(0)

        class LuaGlobals(object):
            pass

        lg = LuaGlobals()

        config = None
        try:
            config = subprocess.check_output('lua - %s' % path, stdin=lua_script,
                                             shell=True).strip().decode('utf-8')
        except (OSError, IOError, subprocess.CalledProcessError):
            pass
        finally:
            lua_script.close()

        if config:
            for name, value in re.findall(r'^\s*(cas\w+)\s+(.+?)\s*$', config, re.M):
                setattr(lg, name, value)
        else:
            # Try to parse manually
            config = re.sub(r'\-\-.*?$', r' ', open(path, 'r').read(), flags=re.M)
            for name, value in re.findall(r'\b(cas\w+)\s*=\s*(\S+)(?:\s+|\s*$)', config):
                setattr(lg, name, eval(value))

    try:
        cashost = str(lg.cashost)
    except Exception:
        sys.sterr.write('ERROR: Could not access cashost setting\n')
        sys.exit(1)

    try:
        casport = int(lg.casport)
    except Exception:
        sys.sterr.write('ERROR: Could not access casport setting\n')
        sys.exit(1)

    try:
        if lg.casprotocol:
            casprotocol = str(lg.casprotocol)
    except Exception:
        pass

    return cashost, casport, casprotocol


def load_data(conn, path, server_type, casout=None, importoptions=None):
    '''
    If data exists on the server, use it.  Otherwise, upload the data set.

    Parameters
    ----------
    conn : CAS
        The CAS connection
    path : string
        The relative path to the data file
    server_type : string
        The type of CAS server in the form platform.mpp|smp[.nohdfs]
    casout : dict
        The CAS output table specification

    Returns
    -------
    CASResults of loadtable / upload action

    '''
    import swat.tests as st
    import platform

    # Get location of data and casout
    data_lib = get_cas_data_lib(server_type)
    out_lib = get_casout_lib(server_type)

    if casout is None:
        casout = dict(caslib=out_lib)

    if 'caslib' not in casout and 'casLib' not in casout:
        casout['caslib'] = out_lib

    if 'name' not in casout:
        casout['name'] = re.sub('/', '.', os.path.splitext(path)[0])

    # Try to load server version first
    if 'win' in server_type:
        res = conn.loadtable(caslib=data_lib, path=path.replace('/', '\\'),
                             casout=casout, importoptions=importoptions)
    else:
        res = conn.loadtable(caslib=data_lib, path=path, casout=casout,
                             importoptions=importoptions)

    # If server version doesn't exist, upload local copy
    if 'tableName' not in res or not res['tableName']:
        # sys.stderr.write('NOTE: Uploading local data file.')
        plat = platform.system().lower()
        if 'win' in plat and 'darwin' not in plat:
            res = conn.upload(os.path.join(os.path.dirname(st.__file__),
                                           path.replace('/', '\\')), casout=casout,
                              importoptions=importoptions)
        else:
            res = conn.upload(os.path.join(os.path.dirname(st.__file__),
                                           path), casout=casout,
                              importoptions=importoptions)

    return res


def runtests(xmlrunner=False):
    ''' Run unit tests '''
    import sys

    profile_opt = [x for x in sys.argv
                   if x == '--profile' or x.startswith('--profile=')]
    sys.argv = [x for x in sys.argv
                if x != '--profile' and not x.startswith('--profile=')]

    if profile_opt:
        import cProfile as profile
        import os
        import pstats

        profile_opt = profile_opt[-1]

        if '=' in profile_opt:
            name = profile_opt.split('=')[-1]
        else:
            name = '%s.prof' % ([os.path.splitext(os.path.basename(x))[0]
                                 for x in sys.argv if x.endswith('.py')][0])

        if xmlrunner:
            import xmlrunner as xr
            profile.run('unittest.main(testRunner=xr.XMLTestRunner('
                        'output=\'test-reports\', verbosity=2))', name)
        else:
            profile.run('unittest.main()', name)

        stats = pstats.Stats(name)
        # stats.strip_dirs()
        stats.sort_stats('cumulative', 'calls')
        stats.print_stats(25)
        stats.sort_stats('time', 'calls')
        stats.print_stats(25)

    elif xmlrunner:
        import xmlrunner as xr
        unittest.main(testRunner=xr.XMLTestRunner(output='test-reports', verbosity=2))

    else:
        unittest.main()


def get_cas_host_type(conn):
    ''' Return a server type indicator '''
    out = conn.about()
    ostype = out['About']['System']['OS Family']
    stype = 'mpp'
    htype = 'nohdfs'
    if out['server'].iloc[0]['nodes'] == 1:
        stype = 'smp'
    if ostype.startswith('LIN'):
        ostype = 'linux'
    elif ostype.startswith('WIN'):
        ostype = 'windows'
    elif ostype.startswith('OSX'):
        ostype = 'mac'
    else:
        raise ValueError('Unknown OS type: ' + ostype)

    # Check to see if HDFS is present
    out = conn.table.querycaslib(caslib='CASUSERHDFS')
    for key, value in list(out.items()):
        if 'CASUSERHDFS' in key and value:
            # Default HDFS caslib for user exists
            htype = ''

    if stype == 'mpp' and (len(htype) > 0):
        return ostype + '.' + stype + '.' + htype
    else:
        return ostype + '.' + stype


def get_cas_version(conn):
    ''' Return CAS version '''
    return tuple([int(x) for x in conn.about()['About']['Version'].split('.')])


getcashosttype = get_cas_host_type
