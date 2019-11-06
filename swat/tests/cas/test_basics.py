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

# NOTE: This test requires a running CAS server.  You must use an ~/.authinfo
#       file to specify your username and password.  The CAS host and port must
#       be specified using the CASHOST and CASPORT environment variables.
#       A specific protocol ('cas', 'http', 'https', or 'auto') can be set using
#       the CASPROTOCOL environment variable.

import datetime
import os
import pandas
import six
import swat
import swat.utils.testing as tm
import sys
import numpy as np
import unittest

from swat.utils.compat import patch_pandas_sort
from swat.utils.testing import UUID_RE, get_cas_host_type, load_data

patch_pandas_sort()

# Pick sort keys that will match across SAS and Pandas sorting orders
SORT_KEYS = ['Origin', 'MSRP', 'Horsepower', 'Model']

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestBasics(tm.TestCase):

    server_type = None
    
    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = True
        swat.options.cas.missing.int64 = -999999

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if type(self).server_type is None:
            type(self).server_type = get_cas_host_type(self.s)

        self.srcLib = tm.get_casout_lib(self.server_type)

        r = load_data(self.s, 'datasources/cars_single.sashdat', self.server_type)

        self.tablename = r['tableName']
        self.assertNotEqual(self.tablename, None)

    def tearDown(self):
        # tear down tests
        self.s.endsession()
        del self.s
        swat.reset_option()

    def test_basic_connection(self):
        self.assertEqual(self.s._hostname, HOST)
        self.assertEqual(self.s._port, PORT)
        self.assertRegex(self.s._session, UUID_RE)
        if self.s._protocol == 'http':
            self.assertEqual(self.s._soptions, 'protocol=http')
        elif self.s._protocol == 'https':
            self.assertEqual(self.s._soptions, 'protocol=https')
        else:
            self.assertIn(self.s._soptions, ['', 'protocol=cas'])

    def test_connection_failure(self):
        user, passwd = tm.get_user_pass()
        with self.assertRaises(swat.SWATError):
           swat.CAS(HOST, 1999, USER, PASSWD, protocol=PROTOCOL)

    def test_copy_connection(self):
        s2 = self.s.copy()

        self.assertEqual(s2._hostname, self.s._hostname)
        self.assertEqual(s2._port, self.s._port)
        self.assertEqual(s2._username, self.s._username)
        self.assertNotEqual(s2._session, self.s._session)
        self.assertRegex(s2._session, UUID_RE)
        if self.s._protocol == 'http':
            self.assertEqual(self.s._soptions, 'protocol=http')
        elif self.s._protocol == 'https':
            self.assertEqual(self.s._soptions, 'protocol=https')
        else:
            self.assertIn(self.s._soptions, ['', 'protocol=cas'])

        s2.endsession()

    def test_fork_connection(self):
        slist = self.s.fork(3)

        self.assertEqual(len(slist), 3)

        self.assertEqual(slist[0]._hostname, self.s._hostname)
        self.assertEqual(slist[0]._port, self.s._port)
        self.assertEqual(slist[0]._username, self.s._username)
        self.assertEqual(slist[0]._session, self.s._session)
        self.assertRegex(slist[0]._session, UUID_RE)
        self.assertEqual(slist[0]._soptions, self.s._soptions)

        self.assertEqual(slist[1]._hostname, self.s._hostname)
        self.assertEqual(slist[1]._port, self.s._port)
        self.assertEqual(slist[1]._username, self.s._username)
        self.assertNotEqual(slist[1]._session, self.s._session)
        self.assertRegex(slist[1]._session, UUID_RE)
        self.assertEqual(slist[1]._soptions, self.s._soptions)

        self.assertEqual(slist[2]._hostname, self.s._hostname)
        self.assertEqual(slist[2]._port, self.s._port)
        self.assertEqual(slist[2]._username, self.s._username)
        self.assertNotEqual(slist[2]._session, self.s._session)
        self.assertRegex(slist[2]._session, UUID_RE)
        self.assertEqual(slist[2]._soptions, self.s._soptions)

        self.assertNotEqual(slist[0]._session, slist[1]._session)
        self.assertNotEqual(slist[1]._session, slist[2]._session)

        for i in range(len(slist)):
            if slist[i]._session != self.s._session:
                slist[i].endsession()

    def test_connect_existing_session(self):      
        user, passwd = tm.get_user_pass()
        t = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL, session=self.s._session)

        self.assertEqual(t._hostname, HOST)
        self.assertEqual(t._port, PORT)
        self.assertEqual(t._session, self.s._session)
        if self.s._protocol == 'http':
            self.assertIn(t._soptions, ['session=%s protocol=http' % self.s._session,
                                        'protocol=http session=%s' % self.s._session])
        else:
            self.assertIn(t._soptions, ['session=%s' % self.s._session,
                                        'session=%s protocol=cas' % self.s._session,
                                        'protocol=cas session=%s' % self.s._session])

    def test_connect_with_bad_session(self):
        user, passwd = tm.get_user_pass()
        with self.assertRaises(swat.SWATError):
           swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL, session='bad-session')

    def test_set_session_locale(self):
        user, passwd = tm.get_user_pass()
        u = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL, locale='es_US')
        if self.s._protocol == 'http':
            self.assertIn(u._soptions, ['locale=es_US protocol=http',
                                       'protocol=http locale=es_US'])
        else:
            self.assertIn(u._soptions, ['locale=es_US', 'locale=es_US protocol=cas',
                                                       'protocol=cas locale=es_US'])
        u.endsession()

    def test_set_bad_session_locale(self):
        user, passwd = tm.get_user_pass()
        with self.assertRaises(swat.SWATError):
           swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL, locale='bad-locale')

    def test_echo(self):
        out = self.s.builtins.echo(a=10, b=12.5, c='string value',
                                   d=[1,2,3], e={'x':100, 'y':'y-value', 'z':[20.5, 1.75]})

        d = out
        self.assertEqual(d['a'], 10)
        self.assertEqual(d['b'], 12.5)
        self.assertEqual(d['c'], 'string value')
        self.assertEqual(d['d'], [1,2,3])
        self.assertEqual(d['e'], {'x':100, 'y':'y-value', 'z':[20.5, 1.75]})

    def test_echo_using_dict(self):
        out = self.s.builtins.echo(a=10, b=12.5, c='string value',
                                   d=[1,2,3], e=dict(x=100, y='y-value', z=[20.5, 1.75]))

        d = out
        self.assertEqual(d['a'], 10)
        self.assertEqual(d['b'], 12.5)
        self.assertEqual(d['c'], 'string value')
        self.assertEqual(d['d'], [1,2,3])
        self.assertEqual(d['e'], {'x':100, 'y':'y-value', 'z':[20.5, 1.75]})

    def test_summary(self):
        out = self.s.loadactionset(actionset='simple')
        out = self.s.summary(table=self.s.CASTable(self.tablename, caslib=self.srcLib))

        summ = out['Summary']

        myLabel = 'Descriptive Statistics for ' + self.tablename
        myTitle = 'Descriptive Statistics for ' + self.tablename

        self.assertEqual(summ.name, 'Summary')
        self.assertEqual(summ.label, myLabel)
        self.assertEqual(summ.title, myTitle)
        self.assertTrue(len(summ.columns) >= 15)

        self.assertEqual(summ.columns[0], 'Column')
        self.assertEqual(summ.colinfo['Column'].name, 'Column')
        self.assertEqual(summ.colinfo['Column'].label, 'Analysis Variable')
        self.assertIn(summ.colinfo['Column'].dtype, ['char', 'varchar'])
        self.assertEqual(summ.colinfo['Column'].width, 11)
        self.assertEqual(summ.colinfo['Column'].format, None)

        self.assertEqual(summ.columns[1], 'Min')
        self.assertEqual(summ.colinfo['Min'].name, 'Min')
        self.assertIn(summ.colinfo['Min'].label, ['Min', 'Minimum'])
        self.assertEqual(summ.colinfo['Min'].dtype, 'double')
        self.assertEqual(summ.colinfo['Min'].width, 8)

        self.assertEqual(summ.columns[4], 'NMiss')
        self.assertEqual(summ.colinfo['NMiss'].name, 'NMiss')
        self.assertIn(summ.colinfo['NMiss'].label, ['N Miss', 'Number Missing'])
        self.assertEqual(summ.colinfo['NMiss'].dtype, 'double')
        self.assertEqual(summ.colinfo['NMiss'].width, 8)
        self.assertEqual(summ.colinfo['NMiss'].format, 'BEST10.')

        data = summ

        self.assertEqual(data['Column'].tolist(), 
                         ['MSRP','Invoice','EngineSize','Cylinders',
                          'Horsepower','MPG_City','MPG_Highway','Weight','Wheelbase','Length'])
        self.assertEqual(data['Min'].tolist(), 
                         [10280.0, 9875.0, 1.3, 3.0, 73.0, 10.0, 12.0, 1850.0, 89.0, 143.0])
        self.assertEqual(data['NMiss'].tolist(),
                         [0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    def test_alltypes(self):
        srcLib = tm.get_casout_lib(self.server_type)
        out = self.s.loadactionset(actionset='actionTest')
        out = self.s.alltypes(casout=dict(caslib=srcLib, name='typestable'))
        out = self.s.fetch(table=self.s.CASTable('typestable', caslib=srcLib), sastypes=False)

        data = out['Fetch']

        self.assertEqual(data['Double'].iloc[0], 42.42)
        self.assertEqual(type(data['Double'].iloc[0]), np.float64)

        self.assertEqual(data['Char'].iloc[0], u'AbC\u2782\u2781\u2780')
        if (sys.version_info >= (3, 0)):
            self.assertEqual(type(data['Char'].iloc[0]), type('aString'))
        else:
            self.assertEqual(type(data['Char'].iloc[0]), unicode)

        self.assertEqual(data['Varchar'].iloc[0],
                         u'This is a test of the Emergency Broadcast System. This is only a test. BEEEEEEEEEEEEEEEEEEP WHAAAA SCREEEEEEEEEEEECH. \u2789\u2788\u2787\u2786\u2785\u2784\u2783\u2782\u2781\u2780 Blastoff!')
        if (sys.version_info >= (3, 0)):
            self.assertEqual(type(data['Varchar'].iloc[0]), type('aString'))
        else:
            self.assertEqual(type(data['Varchar'].iloc[0]), unicode)

        self.assertEqual(data['Int32'].iloc[0], 42)
        self.assertIn(type(data['Int32'].iloc[0]), [np.int32, np.int64])

        # REST interface can sometimes overflow the JSON float
        if np.isnan(data['Int64'].iloc[0]):
            self.assertEqual(type(data['Int64'].iloc[0]), np.float64)
        else:
            self.assertEqual(data['Int64'].iloc[0], 9223372036854775807)
            self.assertEqual(type(data['Int64'].iloc[0]), np.int64)

        self.assertEqual(data['Date'].iloc[0], datetime.date(1963, 5, 19))
        self.assertEqual(type(data['Date'].iloc[0]), datetime.date)
        #self.assertEqual(type(data['Date'].iloc[0]), datetime.Date)

        self.assertEqual(data['Time'].iloc[0], datetime.time(11, 12, 13, 141516))
        self.assertEqual(type(data['Time'].iloc[0]), datetime.time)
        #self.assertEqual(type(data['Time'].iloc[0]), datetime.Time)

        self.assertEqual(data['Datetime'].iloc[0], pandas.to_datetime('1963-05-19 11:12:13.141516'))
        self.assertEqual(type(data['Datetime'].iloc[0]), pandas.Timestamp)
        #self.assertEqual(type(data['Datetime'].iloc[0]), datetime.Datetime)

        self.assertEqual(data['DecSext'].iloc[0], '12345678901234567890.123456789')
        if (sys.version_info >= (3, 0)):
            self.assertEqual(type(data['DecSext'].iloc[0]), type('aString'))
        else:
            self.assertEqual(type(data['DecSext'].iloc[0]), unicode)
        #self.assertEqual(type(data['DecSext'].iloc[0]), Decimal)

        self.assertEqual(type(data['Binary'].iloc[0]), bytes)
        self.assertTrue(len(data['Binary'].iloc[0]) > 0)

        self.assertEqual(type(data['Varbinary'].iloc[0]), bytes)
        #import binascii
        #print(len(data['Varbinary'].iloc[0]))
        #print(binascii.hexlify(data['Varbinary'].iloc[0]))
        self.assertTrue(len(data['Varbinary'].iloc[0]) > 0)

    def test_array_types(self):
        r = load_data(self.s, 'datasources/summary_array.sashdat', self.server_type)

        tablename = r['tableName']
        self.assertNotEqual(tablename, None)

        out = self.s.fetch(table=dict(name=tablename, caslib=self.srcLib), sastypes=False)

        data = out['Fetch']

        for i in range(15):
           self.assertEqual(data['_Min_'].iloc[i], data['myArray1'].iloc[i])
           self.assertEqual(data['_Max_'].iloc[i], data['myArray2'].iloc[i])
           self.assertEqual(data['_N_'].iloc[i], data['myArray3'].iloc[i])
           self.assertEqual(data['_NMiss_'].iloc[i], data['myArray4'].iloc[i])
           self.assertEqual(data['_Mean_'].iloc[i], data['myArray5'].iloc[i])
           self.assertEqual(data['_Sum_'].iloc[i], data['myArray6'].iloc[i])
           self.assertEqual(data['_Std_'].iloc[i], data['myArray7'].iloc[i])
           self.assertEqual(data['_StdErr_'].iloc[i], data['myArray8'].iloc[i])
           self.assertEqual(data['_Var_'].iloc[i], data['myArray9'].iloc[i])
           self.assertEqual(data['_USS_'].iloc[i], data['myArray10'].iloc[i])
           self.assertEqual(data['_CSS_'].iloc[i], data['myArray11'].iloc[i])
           self.assertEqual(data['_CV_'].iloc[i], data['myArray12'].iloc[i])
           self.assertEqual(data['_T_'].iloc[i], data['myArray13'].iloc[i])
           self.assertEqual(data['_PRT_'].iloc[i], data['myArray14'].iloc[i])

        self.s.droptable(caslib=self.srcLib, table=tablename)

    def test_multiple_connection_retrieval(self):
        f = self.s.fork(3)

        self.assertEqual(len(f), 3)
        self.assertRegex(f[0]._session, UUID_RE)
        self.assertRegex(f[1]._session, UUID_RE)
        self.assertRegex(f[2]._session, UUID_RE)
        self.assertNotEqual(f[0]._session, f[1]._session)
        self.assertNotEqual(f[1]._session, f[2]._session)

        f[0].loadactionset(actionset='actionTest')
        f[1].loadactionset(actionset='actionTest')
        f[2].loadactionset(actionset='actionTest')

        f[0].invoke('testsleep', duration=6000)
        f[1].invoke('testsleep', duration=11000)
        f[2].invoke('testsleep', duration=500)

        order = []
        for resp, conn in swat.getnext(f):
            if resp.messages and len(resp.messages) > 0:
                if '500 milliseconds' in resp.messages[0]:
                   order.append(f[2]._session)
                elif '6000 milliseconds' in resp.messages[0]:
                   order.append(f[0]._session)
                elif '11000 milliseconds' in resp.messages[0]:
                   order.append(f[1]._session)

        self.assertEqual(len(order), 3)

        # 10/29/2014: Asserting the responses come back in a particular order
        # fails intermittently. Make sure that all the responses are there for
        # now rather than have it fail randomly.
        #
        #self.assertEqual(order[0], f[2]._session)
        #self.assertEqual(order[1], f[0]._session)
        #self.assertEqual(order[2], f[1]._session)

        f1Found = False
        f2Found = False
        f3Found = False

        for i in range(len(f)):
            if order[i] == f[0]._session:
                f1Found = True
            elif order[i] == f[1]._session:
                f2Found = True
            elif order[i] == f[2]._session:
                f3Found = True

        self.assertTrue(f1Found)
        self.assertTrue(f2Found)
        self.assertTrue(f3Found)

        for i in range(len(f)):
            if f[i]._session != self.s._session:
                f[i].endsession()

    def test_addtable(self):
        if self.s._protocol in ['http', 'https']:
            tm.TestCase.skipTest(self, 'REST does not support addtable')        

        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        cars = pandas.io.parsers.read_csv(myFile)

        dmh = swat.datamsghandlers.CSV(myFile, nrecs=20)

        # Use the default caslib. Get it from the results, and use it in later actions.
        out = self.s.addtable(table='cars', **dmh.args.addtable)
        srcLib = out['caslib']

        out = self.s.tableinfo(caslib=srcLib, table='cars')
        data = out['TableInfo']

        self.assertEqual(data['Name'].iloc[0], 'CARS')
        self.assertEqual(data['Rows'].iloc[0], 428)
        self.assertEqual(data['Columns'].iloc[0], 15)

        out = self.s.columninfo(table=self.s.CASTable('cars', caslib=srcLib))
        data = out['ColumnInfo']

        self.assertEqual(len(data), 15)
        self.assertEqual(data['Column'].tolist(),
                         'Make,Model,Type,Origin,DriveTrain,MSRP,Invoice,EngineSize,Cylinders,Horsepower,MPG_City,MPG_Highway,Weight,Wheelbase,Length'.split(','))
        self.assertEqual(data['Type'].tolist(), ['varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'int64', 'int64', 'double', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64'])

        self.assertTablesEqual(cars, self.s.CASTable('cars', caslib=srcLib), sortby=SORT_KEYS)

        self.s.droptable(caslib=srcLib, table='cars')

    def test_attr_or_key(self):
        out = self.s.loadactionset(actionset='simple')
        out = self.s.reflect(actionset='simple')
        self.assertEqual(out[0]['name'], 'simple')

        out = self.s.columninfo(table=self.s.CASTable(self.tablename, caslib=self.srcLib))
        self.assertIsInstance(out['ColumnInfo'], swat.SASDataFrame)

    def test_responsefunc(self):
        self.s.loadactionset(actionset='datapreprocess')

        tbl = self.s.CASTable(self.tablename, caslib=self.srcLib)

        def myfunc(response, connection, userdata):
           if userdata is None:
              userdata = {}
           for key, value in response:
              userdata[key] = value
           return userdata

        userdata = tbl.histogram(responsefunc=myfunc, vars={'mpg_highway','mpg_city'})

        self.assertEqual(sorted(userdata.keys()), ['BinDetails'])
        self.assertEqual(userdata['BinDetails']['Variable'].tolist(),
                         [u'MPG_City']*11 + [u'MPG_Highway']*12)

    def test_resultfunc(self):
        self.s.loadactionset(actionset='datapreprocess')

        tbl = self.s.CASTable(self.tablename, caslib=self.srcLib)

        def myfunc(key, value, response, connection, userdata):
           if userdata is None:
              userdata = {}
           userdata[key] = value
           return userdata

        userdata = tbl.histogram(resultfunc=myfunc, vars={'mpg_highway','mpg_city'})

        self.assertEqual(sorted(userdata.keys()), ['BinDetails'])
        self.assertEqual(userdata['BinDetails']['Variable'].tolist(),
                         [u'MPG_City']*11 + [u'MPG_Highway']*12)

    def test_attrs(self):        
        self.s.loadactionset(actionset='simple')

        tbl = self.s.CASTable(self.tablename, caslib=self.srcLib)

        out = tbl.summary()['Summary']

        self.assertEqual(out.attrs['Action'], 'summary')
        self.assertEqual(out.attrs['Actionset'], 'simple')
        self.assertNotEqual(out.attrs['CreateTime'], 0)
        
    def test_epdf_server_options_are_hidden(self):
        # Verify datafeeder options are hidden
        
        # Hidden arguments are not visible in optimized images even when showHidden=True;
        box = os.environ.get('SDSBOX', 'LAXNO').upper()
        if (box == 'LAXNO' ) | (box == 'LAXDO') | (box == 'WX6NO') | (box == 'WX6DO'):
            # Do nothing in an optimized image
            return

        # Load the Server Properties actionSet that contains the datafeeder options
        r = self.s.builtins.loadactionset(actionset='configuration')
        if r.severity != 0:
            self.pp.pprint(r.messages)
            self.assertEquals( r.status, None )

        # List the actionSet and include all hidden actions. 
        # The setServOpt is supposed to be hidden but get and list are not. 
        out = self.s.builtins.help(actionSet='configuration', showHidden=True)
        cfg = out['configuration']
        self.assertTrue(cfg is not None)
        self.assertEqual(len(cfg.columns), 2)
        self.assertGreaterEqual(len(cfg), 3)
        self.assertEqual(cfg.columns[0], 'name')
        self.assertEqual(cfg.columns[1], 'description')
        item = cfg.iloc[0].tolist()
        self.assertTrue(item == ['setServOpt','sets a server option'] or
                        item == ['setServOpts','sets a server option'])
        self.assertEqual(cfg.iloc[1].tolist(),
                         ['getServOpt','displays the value of a server option'])
        self.assertEqual(cfg.iloc[2].tolist(),
                         ['listServOpts','Displays the server options and server values'])
        
        # List a hidden action 
        act = self.s.builtins.help(action='setServOpt', showHidden=True)
        has_set_serv_opts = False
        if not act:
            act = self.s.builtins.help(action='setServOpts', showHidden=True)
            self.assertTrue(act is not None)
            self.assertTrue(act['setServOpts'])
            has_set_serv_opts = True
        else:
            self.assertTrue(act is not None)
            self.assertTrue(act['setServOpt'])
        
        # 03/09/2016: bosout: Didn't expect hidden action to be returned. Developers notified.
        # Comment out until we know what to expect.
        #
        # Try to list a hidden action when hidden ones are not supposed to be shown
        act = self.s.builtins.help(action='setServOpt', showHidden=False)
        #self.assertTrue(act is None) 
            
        # Expect non-hidden actions to be returned       
        act = self.s.builtins.help(action='getServOpt', showHidden=False)
        self.assertTrue(act is not None)
        self.assertTrue(act['getServOpt'])
        
        act = self.s.builtins.help(action='listServOpts', showHidden=False)
        self.assertTrue(act is not None)
        self.assertTrue(act['listServOpts'])         
        
        # See if we can set a hidden datafeeder option
        if has_set_serv_opts:
            out = self.s.configuration.setServOpts(epdfsslusesni="NO")
            self.assertEqual( out.status, "Error parsing action parameters." )
        else:
            with self.assertRaises(AttributeError):
                self.s.configuration.setServOpts(epdfsslusesni="NO")
        
        # See if we can get a hidden datafeeder option
        with self.assertRaises(AttributeError):
            self.s.configuration.getServOpts(name='epdfsslusesni')
        
        # See if hidden datafeeder options appear in the option list. 
        opts = self.s.configuration.listServOpts()
        self.assertTrue(opts is not None)                      
        
        # Verify the datafeeder options don't appear since they are hidden
        self.assertFalse('NOTE:    int32 nThreads=0 (0 <= value <= 64),' in opts.messages)


if __name__ == '__main__':
   tm.runtests()
