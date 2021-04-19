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
import numpy as np

UUID_RE = r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'

RE_TYPE = type(re.compile(r''))

enabled = ['yes', 'y', 'on', 't', 'true', '1']

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

def get_cas_superuser_mode():
    ''' Return True if user has specified the CAS_SUPERUSER_MODE environment variable '''
    if os.environ.get('CAS_SUPERUSER_MODE', '').lower() in enabled:
        return True
    else:
        return False

def get_cas_action_test_mode():
    ''' Return True if user has specified the CAS_ACTION_TEST_MODE environment variable '''
    if os.environ.get('CAS_ACTION_TEST_MODE', '').lower() in enabled:
        return True
    else:
        return False

getcashosttype = get_cas_host_type

def get_generated_vc1():
    ''' Auto-generates the data from the vc1.csv file in the form of a dataFrame. '''
    
    # Values for the rowLen column
    rowLen_list = [71078, 106595, 60851, 85342, 103881, 175126, 138595, 125114, 106627, 102233, 205031, 165948, 161677, 85382, 141625, 125577, 142535, 185014, 73791, 56031, 
     67327, 108169, 137538, 56758, 128900, 77781, 39866, 52386, 57347, 141494, 105846, 33210, 132764, 83736, 141774, 88059, 192619, 71561, 89101, 125397, 
     77836, 137852, 141574, 89512, 166352, 173677, 124789, 81370, 171151, 71079, 71079, 106596, 60852, 85343, 103882, 175127, 138596, 125115, 106628, 102233, 
     205031, 165948, 161677, 85382, 141625, 125577, 142535, 185014, 73791, 56031, 67327, 108169, 137538, 56758, 128900, 77781, 39866, 52386, 57347, 141494, 
     105846, 33210, 132764, 83736, 141774, 88059, 192619, 71561, 89101, 125397, 77836, 137852, 141574, 89512, 166352, 173677, 124789, 81370, 171151, 71080]
    
    # Values for the length columns for the 9 varchar columns
    Vryn01Len_list = [79, 25, 0, 82, 92, 96, 26, 54, 47, 22, 8, 36, 17, 32, 59, 74, 88, 18, 64, 60, 
     68, 15, 35, 26, 73, 15, 73, 42, 94, 34, 30, 15, 4, 29, 39, 73, 57, 73, 36, 76, 
     4, 11, 64, 0, 86, 71, 78, 61, 65, 79, 79, 25, 0, 82, 92, 96, 26, 54, 47, 22, 
     8, 36, 17, 32, 59, 74, 88, 18, 64, 60, 68, 15, 35, 26, 73, 15, 73, 42, 94, 34, 
     30, 15, 4, 29, 39, 73, 57, 73, 36, 76, 4, 11, 64, 0, 86, 71, 78, 61, 65, 79]
    
    Vryn02Len_list = [566, 815, 378, 149, 490, 3, 405, 892, 309, 285, 568, 740, 304, 31, 497, 56, 959, 853, 60, 206, 
     958, 317, 951, 456, 118, 506, 582, 855, 22, 552, 444, 604, 874, 890, 894, 587, 786, 974, 738, 331, 
     883, 79, 893, 601, 419, 928, 198, 585, 581, 566, 566, 815, 378, 149, 490, 3, 405, 892, 309, 285, 
     568, 740, 304, 31, 497, 56, 959, 853, 60, 206, 958, 317, 951, 456, 118, 506, 582, 855, 22, 552, 
     444, 604, 874, 890, 894, 587, 786, 974, 738, 331, 883, 79, 893, 601, 419, 928, 198, 585, 581, 566]
    
    Vryn03Len_list = [8491, 2989, 6462, 6590, 2362, 3032, 4514, 4790, 607, 4097, 6482, 7456, 7297, 8239, 1446, 4294, 116, 5029, 4005, 4533, 
     2542, 3286, 6251, 8845, 7728, 4528, 8886, 2638, 9554, 5194, 2839, 9087, 7257, 6020, 8587, 1530, 4443, 4935, 9836, 8845, 
     4219, 8111, 1554, 3457, 5963, 9244, 7107, 7126, 6599, 8491, 8491, 2989, 6462, 6590, 2362, 3032, 4514, 4790, 607, 4097, 
     6482, 7456, 7297, 8239, 1446, 4294, 116, 5029, 4005, 4533, 2542, 3286, 6251, 8845, 7728, 4528, 8886, 2638, 9554, 5194, 
     2839, 9087, 7257, 6020, 8587, 1530, 4443, 4935, 9836, 8845, 4219, 8111, 1554, 3457, 5963, 9244, 7107, 7126, 6599, 8491]
    
    Vryn04Len_list = [42237, 63569, 12720, 5068, 27176, 75546, 67227, 99350, 31975, 80308, 94442, 93644, 53516, 3738, 37464, 49333, 73579, 94748, 18004, 30194, 
     41888, 60121, 33457, 32044, 50851, 51123, 10066, 26613, 30718, 41362, 70108, 4645, 37242, 51575, 72717, 65562, 86212, 42418, 24098, 75840, 
     7318, 80414, 71606, 42544, 69536, 77445, 68778, 41088, 68511, 42237, 42237, 63569, 12720, 5068, 27176, 75546, 67227, 99350, 31975, 80308, 
     94442, 93644, 53516, 3738, 37464, 49333, 73579, 94748, 18004, 30194, 41888, 60121, 33457, 32044, 50851, 51123, 10066, 26613, 30718, 41362, 
     70108, 4645, 37242, 51575, 72717, 65562, 86212, 42418, 24098, 75840, 7318, 80414, 71606, 42544, 69536, 77445, 68778, 41088, 68511, 42237]
    
    Vryn05Len_list = [17418, 35903, 34288, 63763, 65402, 91688, 59173, 10456, 71312, 9865, 99907, 58628, 94148, 64252, 94214, 67743, 66914, 73258, 47938, 19613, 
     12034, 42772, 91143, 10358, 66300, 16485, 14113, 17119, 14868, 88908, 30561, 16246, 77198, 20333, 50600, 15377, 99136, 20960, 53074, 32021, 
     55845, 47568, 66362, 38334, 84723, 75187, 42023, 24843, 90852, 17418, 17418, 35903, 34288, 63763, 65402, 91688, 59173, 10456, 71312, 9865, 
     99907, 58628, 94148, 64252, 94214, 67743, 66914, 73258, 47938, 19613, 12034, 42772, 91143, 10358, 66300, 16485, 14113, 17119, 14868, 88908, 
     30561, 16246, 77198, 20333, 50600, 15377, 99136, 20960, 53074, 32021, 55845, 47568, 66362, 38334, 84723, 75187, 42023, 24843, 90852, 17418]
    
    Vryn06Len_list = [90, 14, 93, 36, 23, 41, 2, 86, 59, 78, 85, 84, 3, 30, 72, 80, 16, 59, 27, 88, 
     43, 64, 84, 8, 63, 28, 39, 47, 30, 26, 87, 84, 46, 51, 5, 92, 39, 59, 36, 85, 
     62, 92, 85, 79, 76, 10, 83, 17, 21, 90, 90, 14, 93, 36, 23, 41, 2, 86, 59, 78, 
     85, 84, 3, 30, 72, 80, 16, 59, 27, 88, 43, 64, 84, 8, 63, 28, 39, 47, 30, 26, 
     87, 84, 46, 51, 5, 92, 39, 59, 36, 85, 62, 92, 85, 79, 76, 10, 83, 17, 21, 90]
    
    Vryn07Len_list = [110, 417, 212, 426, 291, 528, 23, 855, 975, 904, 394, 758, 186, 983, 16, 110, 182, 921, 64, 879, 
     551, 211, 31, 170, 708, 733, 641, 824, 317, 387, 257, 131, 211, 809, 107, 204, 199, 282, 965, 366, 
     451, 986, 610, 582, 657, 664, 928, 31, 231, 110, 110, 417, 212, 426, 291, 528, 23, 855, 975, 904, 
     394, 758, 186, 983, 16, 110, 182, 921, 64, 879, 551, 211, 31, 170, 708, 733, 641, 824, 317, 387, 
     257, 131, 211, 809, 107, 204, 199, 282, 965, 366, 451, 986, 610, 582, 657, 664, 928, 31, 231, 110]
    
    Vryn08Len_list = [1909, 2697, 6561, 9111, 7848, 4072, 7062, 8493, 1217, 6555, 2984, 4434, 6016, 7962, 7707, 3723, 581, 9977, 3440, 275, 
     9136, 1247, 5480, 4655, 2949, 4175, 5363, 4097, 1583, 4917, 1387, 2268, 9787, 3909, 8720, 4477, 1585, 1681, 131, 7673, 
     8944, 401, 292, 3813, 4719, 9959, 5486, 7478, 4134, 1909, 1909, 2697, 6561, 9111, 7848, 4072, 7062, 8493, 1217, 6555, 
     2984, 4434, 6016, 7962, 7707, 3723, 581, 9977, 3440, 275, 9136, 1247, 5480, 4655, 2949, 4175, 5363, 4097, 1583, 4917, 
     1387, 2268, 9787, 3909, 8720, 4477, 1585, 1681, 131, 7673, 8944, 401, 292, 3813, 4719, 9959, 5486, 7478, 4134, 1909]
    
    Vryn09Len_list = [78, 65, 38, 18, 96, 21, 64, 37, 26, 18, 60, 66, 89, 16, 49, 63, 1, 49, 90, 83, 
     7, 34, 6, 96, 9, 87, 3, 50, 61, 12, 31, 30, 44, 19, 5, 56, 60, 78, 87, 58, 
     11, 90, 8, 3, 71, 67, 7, 41, 55, 78, 78, 65, 38, 18, 96, 21, 64, 37, 26, 18, 
     60, 66, 89, 16, 49, 63, 1, 49, 90, 83, 7, 34, 6, 96, 9, 87, 3, 50, 61, 12, 
     31, 30, 44, 19, 5, 56, 60, 78, 87, 58, 11, 90, 8, 3, 71, 67, 7, 41, 55, 78]
    
    V1val_list=[]
    V2val_list=[]
    V3val_list=[]
    V4val_list=[]
    V5val_list=[]
    V6val_list=[]
    V7val_list=[]
    V8val_list=[]
    V9val_list=[]
    email_list = []
    
    # Generate the data for the userEmail column
    for u in range (1,101):
        email_list.append("Some.User{:>03d}@mycomp.com".format(u))
    
    # for each varchar column, generate the data based on the length fields for the column
    for v in Vryn01Len_list:
        V1val_list.append("!" * v)        
    for v in Vryn02Len_list:
        V2val_list.append("@" * v)
    for v in Vryn03Len_list:
        V3val_list.append("#" * v)
    for v in Vryn04Len_list:
        V4val_list.append("$" * v)
    for v in Vryn05Len_list:
        V5val_list.append("%" * v)
    for v in Vryn06Len_list:
        V6val_list.append("^" * v)
    for v in Vryn07Len_list:
        V7val_list.append("&" * v)
    for v in Vryn08Len_list:
        V8val_list.append("*" * v)
    for v in Vryn09Len_list:
        V9val_list.append("?" * v)
        
    vc1_dict = {'rowLen': rowLen_list,
                'rowId': range(1,101),
                'userEmail': email_list,
                'Vryn01Len': Vryn01Len_list,
                'Vryn01Val': V1val_list,
                'Vryn02Len': Vryn02Len_list,
                'Vryn02Val': V2val_list,
                'Vryn03Len': Vryn03Len_list,
                'Vryn03Val': V3val_list,
                'Vryn04Len': Vryn04Len_list,
                'Vryn04Val': V4val_list,
                'Vryn05Len': Vryn05Len_list,
                'Vryn05Val': V5val_list,
                'Vryn06Len': Vryn06Len_list,
                'Vryn06Val': V6val_list,
                'Vryn07Len': Vryn07Len_list,
                'Vryn07Val': V7val_list,
                'Vryn08Len': Vryn08Len_list,
                'Vryn08Val': V8val_list,
                'Vryn09Len': Vryn09Len_list,
                'Vryn09Val': V9val_list}
    
    return pd.DataFrame(vc1_dict,columns=['rowLen', 'rowId', 'userEmail', 
                                          'Vryn01Len','Vryn01Val','Vryn02Len','Vryn02Val',
                                          'Vryn03Len','Vryn03Val','Vryn04Len','Vryn04Val', 
                                          'Vryn05Len','Vryn05Val','Vryn06Len','Vryn06Val',
                                          'Vryn07Len','Vryn07Val','Vryn08Len', 'Vryn08Val',
                                          'Vryn09Len','Vryn09Val'])