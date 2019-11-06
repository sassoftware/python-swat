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

import numpy as np
import os
import pandas as pd
import six
import swat
import swat.utils.testing as tm
import sys
import unittest

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestConnection(tm.TestCase):
    
    # Create a class attribute to hold the cas host type
    server_type = None    

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.cas.trace_actions = False
        swat.options.cas.trace_ui_actions = False
        swat.options.interactive_mode = False

        user, passwd = tm.get_user_pass()

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if type(self).server_type is None: 
            type(self).server_type = tm.get_cas_host_type(self.s)        

        self.srcLib = tm.get_casout_lib(self.server_type)

        r = tm.load_data(self.s, 'datasources/cars_single.sashdat', self.server_type)

        self.tablename = r['tableName']
        self.assertNotEqual(self.tablename, None)
        self.table = r['casTable']

    def tearDown(self):
        # tear down tests
        try:
            self.s.endsession()
        except swat.SWATError:
            pass
        del self.s
        swat.reset_option()

    @unittest.skip('Need TKCal support')
    def test_multihost(self):
        hosts = [HOST, 'rdcgrd001', 'snap001', 'cas01']

        hostlist = list(sorted(set(hosts)))

        def str2hostlist(hoststr):
            return list(sorted(set(hoststr.split())))

        user, passwd = tm.get_user_pass()

        self.s = swat.CAS(hosts, PORT, USER, PASSWD)
        self.assertEqual(str2hostlist(self.s._hostname), hostlist)
        self.assertEqual(self.s._port, PORT)
#       self.assertEqual(self.s._username, USERNAME)

        self.s = swat.CAS(set(hosts), PORT, USER, PASSWD)
        self.assertEqual(str2hostlist(self.s._hostname), hostlist)
        self.assertEqual(self.s._port, PORT)
#       self.assertEqual(self.s._username, USERNAME)

        self.s = swat.CAS(tuple(hosts), PORT, USER, PASSWD)
        self.assertEqual(str2hostlist(self.s._hostname), hostlist)
        self.assertEqual(self.s._port, PORT)
#       self.assertEqual(self.s._username, USERNAME)

    def test_copy(self):
        s2 = self.s.copy()

        self.assertEqual(s2._hostname, self.s._hostname)
        self.assertEqual(s2._port, self.s._port)
        self.assertEqual(s2._username, self.s._username)
        self.assertNotEqual(s2._session, self.s._session)
        self.assertRegex(s2._session, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        if self.s._protocol == 'http':
            self.assertEqual(s2._soptions, 'protocol=http')
        elif self.s._protocol == 'cas':
            self.assertEqual(s2._soptions, 'protocol=cas')

        s2.endsession()

    def test_name(self):
        user, passwd = tm.get_user_pass()
        s = swat.CAS(HOST, PORT, USER, PASSWD, name='My Connection')
        name = list(s.sessionid().keys())[0].split(':')[0]
        self.assertEqual(s._name, name)

    def test_actionsets(self):
        self.assertTrue('decisiontree' not in self.s.get_actionset_names())

        self.s.loadactionset('decisiontree')

        actionsets = self.s.get_actionset_names()
        actions = self.s.get_action_names()

        self.assertTrue('decisiontree' in actionsets)
        self.assertTrue('dtreeprune' in actions)
        self.assertTrue('dtreemerge' in actions)
        self.assertTrue('decisiontree.dtreeprune' in actions)
        self.assertTrue('decisiontree.dtreemerge' in actions)

        info = list(self.s.actionsetinfo()['setinfo']['actionset'].values)
        for item in info:
            self.assertTrue(item.lower() in actionsets)

    def test_dir(self):
        dirout = self.s.__dir__()

        self.assertTrue('builtins.loadactionset' in dirout)
        self.assertTrue('table.loadtable' in dirout)
        self.assertTrue('elasticsearch.index' not in dirout)
        self.assertTrue('sandindex' not in dirout)
        self.assertTrue('elasticsearch.sandindex' not in dirout)

        self.s.loadactionset('elasticsearch')

        dirout = self.s.__dir__()

        self.assertTrue('builtins.loadactionset' in dirout)
        self.assertTrue('table.loadtable' in dirout)
        self.assertTrue('elasticsearch.index' in dirout)
        self.assertTrue('sandindex' in dirout)
        self.assertTrue('elasticsearch.sandindex' in dirout)

    def test_str(self):
        s = str(self.s)

        self.assertTrue(type(s) == str)
        self.assertTrue(HOST in s)
#       self.assertTrue(USERNAME in s)
        self.assertTrue(str(PORT) in s)

        r = repr(self.s)

        self.assertTrue(type(r) == str)
        self.assertTrue(HOST in r)
#       self.assertTrue(USERNAME in r)
        self.assertTrue(str(PORT) in r)

    def test_formatter(self):
        f = self.s.SASFormatter()

        self.assertTrue(f.format(10, 'F10.3'), '10.000')

    def test_results_hooks(self):
        data = {}

        # Add a hook
        def summary_hook(conn, results):
            data['summary'] = results

        self.s.add_results_hook('simple.summary', summary_hook)

        self.assertTrue(len(data) == 0)

        self.s.loadactionset('simple')
        self.table.summary()

        # Check that hook worked
        self.assertEqual(list(sorted(data.keys())), ['summary'])

        data.clear()
        self.assertTrue(len(data) == 0)

        # Add another hook to same action
        def summary_hook_2(conn, results):
            data['summary2'] = results
        
        self.s.add_results_hook('simple.summary', summary_hook_2)

        self.table.summary()

        self.assertEqual(list(sorted(data.keys())), ['summary', 'summary2'])

        # Delete one hook
        self.s.del_results_hook('simple.summary', summary_hook) 

        data.clear()
        self.assertTrue(len(data) == 0)

        self.table.summary()

        self.assertEqual(list(sorted(data.keys())), ['summary2'])
      
        data.clear()
        self.assertTrue(len(data) == 0)

        # Delete all hooks
        self.s.del_results_hooks('simple.summary')

        self.table.summary()

        self.assertEqual(list(sorted(data.keys())), [])

        # Delete hooks that don't exist
        self.s.del_results_hook('simple.foo', summary_hook)
        self.s.del_results_hook('simple.summary', summary_hook)
        self.s.del_results_hook('simple.summary', summary_hook_2)
        self.s.del_results_hooks('simple.summary')

    def test_close(self):
        self.s.listnodes()
        self.s.close()
        with self.assertRaises(swat.SWATError):
            self.s.listnodes()

    def test_bad_option(self):
        if self.s._protocol in ['http', 'https']:
            unittest.TestCase.skipTest(self, 'REST does not support options')        
        
        self.s._set_option(print_messages=True)
        self.s._set_option(print_messages=False)
        self.s._set_option(print_messages=0)
        self.s._set_option(print_messages=1)

        with self.assertRaises(swat.SWATError):
            self.s._set_option(print_messages=5)

        # TODO: Need tests for other options types, but no other options exist yet.

    def test_fork_connection(self):
        slist = self.s.fork(3)

        self.assertEqual(len(slist), 3)

        self.assertEqual(slist[0]._hostname, self.s._hostname)
        self.assertEqual(slist[0]._port, self.s._port)
        self.assertEqual(slist[0]._username, self.s._username)
        self.assertEqual(slist[0]._session, self.s._session)
        self.assertRegex(slist[0]._session, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        self.assertEqual(slist[0]._soptions, self.s._soptions)

        self.assertEqual(slist[1]._hostname, self.s._hostname)
        self.assertEqual(slist[1]._port, self.s._port)
        self.assertEqual(slist[1]._username, self.s._username)
        self.assertNotEqual(slist[1]._session, self.s._session)
        self.assertRegex(slist[1]._session, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        self.assertEqual(slist[1]._soptions, self.s._soptions)

        self.assertEqual(slist[2]._hostname, self.s._hostname)
        self.assertEqual(slist[2]._port, self.s._port)
        self.assertEqual(slist[2]._username, self.s._username)
        self.assertNotEqual(slist[2]._session, self.s._session)
        self.assertRegex(slist[2]._session, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        self.assertEqual(slist[2]._soptions, self.s._soptions)

        self.assertNotEqual(slist[0]._session, slist[1]._session)
        self.assertNotEqual(slist[1]._session, slist[2]._session)

        for i in range(len(slist)):
            if slist[i]._session != self.s._session:
                slist[i].endsession()

    def test_upload(self):
        import swat.tests as st

        numtbls = len(self.s.tableinfo().get('TableInfo', []))
        
        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        out = self.s.tableinfo().get('TableInfo')
        if out is not None:
            self.assertFalse('CARS' in out['Name'].tolist())

        out = self.s.upload(myFile)
        tbl = out['casTable']

        out = self.s.tableinfo()['TableInfo']
        self.assertEqual(len(out), numtbls + 1)
        self.assertTrue('CARS' in out['Name'].tolist())

        tbl = self.s.upload(myFile, casout={'replace':'True'})['casTable']

        out = self.s.tableinfo()['TableInfo']
        self.assertEqual(len(out), numtbls + 1)
        self.assertTrue('CARS' in out['Name'].tolist())

        tbl = self.s.upload(myFile, casout={'name':'global_cars', 'promote':True})['casTable']

        out = self.s.tableinfo()
        out = out['TableInfo']
        self.assertEqual(len(out), numtbls + 2)
        self.assertTrue('CARS' in out['Name'].tolist())
        self.assertTrue('GLOBAL_CARS' in out['Name'].tolist())

        self.s.droptable('global_cars')
        self.s.droptable('cars')

        # URLs
        tbl = self.s.upload('https://raw.githubusercontent.com/sassoftware/sas-viya-programming/master/data/class.csv', casout=dict(replace=True))['casTable']

        out = self.s.tableinfo()['TableInfo']
        self.assertEqual(len(out), numtbls + 1)
        self.assertTrue('CLASS' in out['Name'].tolist())
        self.assertEqual(len(tbl), 19)

        tbl.droptable()

        # DataFrame
        df = pd.read_csv(myFile)
        tbl = self.s.upload(df, casout=dict(replace=True, name='cars'))['casTable']

        out = self.s.tableinfo()['TableInfo']
        self.assertEqual(len(out), numtbls + 1)
        self.assertTrue('CARS' in out['Name'].tolist())
        self.assertEqual(len(tbl), 428)

        tbl.droptable()

    def test_upload_file(self):
        import swat.tests as st
        
        numtbls = len(self.s.tableinfo().get('TableInfo', []))

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        out = self.s.tableinfo().get('TableInfo')
        if out is not None:
            self.assertFalse('CARS' in out['Name'].tolist())

        tbl = self.s.upload_file(myFile)

        out = self.s.tableinfo()['TableInfo']
        self.assertEqual(len(out), numtbls + 1)
        self.assertTrue('CARS' in out['Name'].tolist())

        with self.assertRaises(swat.SWATError):
           self.s.upload_file(myFile)

        tbl.droptable()

    def test_upload_frame(self):
        import swat.tests as st
        
        numtbls = len(self.s.tableinfo().get('TableInfo', []))

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        out = self.s.tableinfo().get('TableInfo')
        if out is not None:
            self.assertFalse('CARS' in out['Name'].tolist())

        tbl = self.s.upload_frame(pd.read_csv(myFile), casout=dict(name='cars'))

        out = self.s.tableinfo()['TableInfo']
        self.assertEqual(len(out), numtbls + 1)
        self.assertTrue('CARS' in out['Name'].tolist())

        with self.assertRaises(swat.SWATError):
           self.s.upload_frame(pd.read_csv(myFile), casout=dict(name='cars'))

        # Test data types
        cars = pd.read_csv(myFile)
        cars['Horsepower'] = cars['Horsepower'].astype('int32')
        cars['MPG_City'] = cars['MPG_City'].astype('int32')
        cars['MPG_Highway'] = cars['MPG_Highway'].astype('int32')

        tbl = self.s.upload_frame(cars, casout=dict(name='cars', replace=True))

        if 'csv-ints' in self.s.server_features:
            self.assertEqual(tbl['Make'].dtype, 'varchar')
            self.assertEqual(tbl['Model'].dtype, 'varchar')
            self.assertEqual(tbl['Horsepower'].dtype, 'int32')
            self.assertEqual(tbl['MPG_City'].dtype, 'int32')
            self.assertEqual(tbl['MPG_Highway'].dtype, 'int32')
        else:
            self.assertEqual(tbl['Make'].dtype, 'varchar')
            self.assertEqual(tbl['Model'].dtype, 'varchar')
            self.assertEqual(tbl['Horsepower'].dtype, 'double')
            self.assertEqual(tbl['MPG_City'].dtype, 'double')
            self.assertEqual(tbl['MPG_Highway'].dtype, 'double')

        # Test importoptions.vars=
        tbl = self.s.upload_frame(cars, casout=dict(name='cars', replace=True),
                                  importoptions=dict(vars=dict(Make=dict(type='char', length=20),
                                                               Model=dict(type='char', length=40))))

        if 'csv-ints' in self.s.server_features:
            self.assertEqual(tbl['Make'].dtype, 'char')
            self.assertEqual(tbl['Model'].dtype, 'char')
            self.assertEqual(tbl['Horsepower'].dtype, 'int32')
            self.assertEqual(tbl['MPG_City'].dtype, 'int32')
            self.assertEqual(tbl['MPG_Highway'].dtype, 'int32')
        else:
            self.assertEqual(tbl['Make'].dtype, 'char')
            self.assertEqual(tbl['Model'].dtype, 'char')
            self.assertEqual(tbl['Horsepower'].dtype, 'double')
            self.assertEqual(tbl['MPG_City'].dtype, 'double')
            self.assertEqual(tbl['MPG_Highway'].dtype, 'double')

        tbl.droptable()

    def test_castable(self):
        # CASTable as table name
        out = self.s.tableinfo(table=self.table)['TableInfo']
        self.assertEqual(out['Name'][0], 'DATASOURCES.CARS_SINGLE')
        self.assertEqual(out['Rows'][0], 428) 

        # CASTable as normal parameter (the table= parameter doesn't use the
        # isTableDef flag because it doesn't want to load the table)
        out = self.s.columninfo(table=self.table)['ColumnInfo']
        self.assertEqual(len(out), 15)
        self.assertEqual(out['Column'][0], 'Make')
        self.assertEqual(out['Column'][1], 'Model')
        self.assertEqual(out['Column'][2], 'Type')
        self.assertEqual(out['Column'][14], 'Length')

        # CASTable as table definition
        self.s.loadactionset('simple')
        out = self.s.summary(table=self.table)['Summary']
        self.assertEqual(len(out), 10)
        self.assertEqual(out['Column'][0], 'MSRP')

        # String as CASTable
        self.s.setsessopt(caslib=self.table.params.caslib)
        out = self.s.summary(table=self.table.params.name)['Summary']
        self.assertEqual(len(out), 10)
        self.assertEqual(out['Column'][0], 'MSRP')

        # CASTable as CASLib
        out = self.s.caslibinfo(caslib=self.table)['CASLibInfo']
        self.assertEqual(len(out), 1)
        self.assertTrue(out['Name'][0].upper().startswith(self.srcLib.upper()))

        # CASTable as output table
        outtable = self.s.CASTable('summout', caslib=self.table.params.caslib)
        out = self.s.summary(table=self.table, casout=outtable)
        out = out['OutputCasTables']
        self.assertTrue(out['casLib'][0].upper().startswith(self.srcLib.upper()))
        self.assertEqual(out['Name'][0], 'summout')

    def test_invoke(self):
        out = self.s.invoke('tableinfo', table=self.table)
        self.assertTrue(out is self.s)
        for resp in out:
            for k, v in resp:
                self.assertEqual(k, 'TableInfo')

    def test_json(self):
        import json
        self.s.loadactionset('simple')
        out = self.s.summary(_json='{"table":%s}' % json.dumps(self.table.to_params()))['Summary']
        self.assertEqual(len(out), 10)
        self.assertEqual(out['Column'][0], 'MSRP')

    def test_datamsghandler(self):
        if self.s._protocol in ['http', 'https']:
            unittest.TestCase.skipTest(self, 'REST does not support data messages')
        
        import swat.tests as st
        
        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        cars = pd.io.parsers.read_csv(myFile)

        dmh = swat.datamsghandlers.CSV(myFile, nrecs=20)

        # Use the default caslib. Get it from the results, and use it in later actions.
        tbl = self.s.addtable(table='cars', **dmh.args.addtable)['casTable']

        out = tbl.tableinfo()['TableInfo']

        self.assertEqual(out['Name'].iloc[0], 'CARS')
        self.assertEqual(out['Rows'].iloc[0], 428)
        self.assertEqual(out['Columns'].iloc[0], 15)

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
        self.assertEqual(userdata['BinDetails']['Variable'].tolist(), [u'MPG_City']*11 + [u'MPG_Highway']*12)

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
        self.assertEqual(userdata['BinDetails']['Variable'].tolist(), [u'MPG_City']*11 + [u'MPG_Highway']*12)

    def test_action_class(self):
        self.s.loadactionset('simple')
        summ = self.s.Summary(table=self.table)

        out = summ()['Summary']
        self.assertEqual(len(out), 10)
        self.assertEqual(out['Column'][0], 'MSRP')

        summ = self.s.simple.Summary(table=self.table)

        out = summ()['Summary']
        self.assertEqual(len(out), 10)
        self.assertEqual(out['Column'][0], 'MSRP')

        s1 = self.s.simple
        s2 = self.s.simple

        self.assertTrue(s1 is not s2)

        s1 = self.s.simple.Summary
        s2 = self.s.simple.Summary

        self.assertTrue(s1 is s2)

    def test_action_class_2(self):
        self.s.loadactionset('simple')

        s1 = self.s.Summary
        s2 = self.s.Summary

        self.assertTrue(s1 is s2)

    def test_action_class_3(self):
        self.s.loadactionset('simple')

        s1 = self.s.summary
        s2 = self.s.summary

        self.assertTrue(s1 is not s2)

    def test_multiple_connection_retrieval(self):
        f = self.s.fork(3)

        self.assertEqual(len(f), 3)
        self.assertRegex(f[0]._session, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        self.assertRegex(f[1]._session, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        self.assertRegex(f[2]._session, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        self.assertNotEqual(f[0]._session, f[1]._session)
        self.assertNotEqual(f[1]._session, f[2]._session)

        f[0].loadactionset(actionset='actionTest')
        f[1].loadactionset(actionset='actionTest')
        f[2].loadactionset(actionset='actionTest')

        f[0].invoke('testsleep', duration=6000)
        f[1].invoke('testsleep', duration=11000)
        act = f[2].Testsleep(duration=500)

        order = []
        for resp, conn in swat.getnext([f[0], f[1], act]):
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

    @unittest.skip('Timeouts don\'t seem to work in the event watcher')
    def test_timeout(self):
        self.s.loadactionset('actiontest')

        sleep = self.s.Testsleep(duration=10000)

        responses = []
        connections = []
        for resp, conn in swat.getnext(sleep, timeout=1):
            responses.append(resp)
            connections.append(conn)

        self.assertTrue([] in responses)
        self.assertTrue(conn in connections)

    def test_bad_host(self):
        with self.assertRaises(swat.SWATError):
            swat.CAS('junk-host-name', PORT, USER, PASSWD)

    def test_context_manager(self):
        with swat.CAS(HOST, PORT, USER, PASSWD) as s:
            self.assertTrue(len(s.serverstatus()) > 0)

        with self.assertRaises(swat.SWATError):
            s.serverstatus()

    def test_session_context(self):
        self.assertEqual(self.s.getsessopt('locale').locale, 'en_US')
        with self.s.session_context(locale='fr'):
            self.assertEqual(self.s.getsessopt('locale').locale, 'fr')
        self.assertEqual(self.s.getsessopt('locale').locale, 'en_US')

    def test_multiple_hosts(self):
        user, passwd = tm.get_user_pass()
        if self.s._protocol in ['http', 'https']:
            unittest.TestCase.skipTest(self, 'REST does not support multiple hosts yet')        
        with swat.CAS([HOST, 'foo', 'bar'], PORT, USER, PASSWD) as s:
            self.assertTrue(len(s.serverstatus()) > 0)

#   def test_authinfo(self):
#       with swat.CAS(authinfo='~/.authinfo') as s:
#           self.assertTrue(len(s.serverstatus()) > 0)

#       with swat.CAS(authinfo=['~/.missing-authinfo', '~/.authinfo'],
#                     protocol=PROTOCOL) as s:
#           self.assertTrue(len(s.serverstatus()) > 0)

    def test_get_action(self):
        from swat.cas.actions import CASAction
        pct = self.s.get_action('percentile')
        self.assertTrue(isinstance(pct, CASAction))
        self.assertTrue('Percentile' in pct.__class__.__name__)

    def test_get_action_class(self):
        from swat.cas.actions import CASAction
        pct = self.s.get_action_class('percentile')
        self.assertTrue(issubclass(pct, CASAction))
        self.assertTrue('Percentile' in pct.__name__)

    def test_get_actionset(self):
        from swat.cas.actions import CASActionSet
        pct = self.s.get_actionset('percentile')
        self.assertTrue(isinstance(pct, CASActionSet))
        self.assertTrue('Percentile' in pct.__class__.__name__)

    def test_fetchvars(self):
        stbl = self.table.sort_values('MSRP')[['Make', 'Model', 'MSRP']]
        self.assertEqual(stbl.fetch(to=1).Fetch.MSRP[0], 10280)

        stbl = self.table.sort_values('MSRP')[['Make', 'Model', 'MSRP']]
        self.assertNotEqual(stbl.fetch(to=1, sortby='Make').Fetch.MSRP[0], 10280)
        self.assertEqual(stbl.fetch(to=1, sortby='Make').Fetch.Make[0], 'Acura')

    def test_apply_importoptions_vars(self):
        df = pd.DataFrame([[10, 'Hello', 'World']], columns=['foo', 'bar', 'baz'])
        df['foo'] = df['foo'].astype('int32')
        df_dtypes = self.s._extract_dtypes(df)

        # importoptions= fully defined, dtypes won't override
        importoptions = dict(vars=[dict(name='foo', type='double', format='best8'),
                                   dict(name='bar', type='varchar'),
                                   dict(name='baz', type='char')])

        self.s._apply_importoptions_vars(importoptions, df_dtypes)

        self.assertEqual(importoptions,
                         dict(vars=[dict(name='foo', type='double', format='best8'),
                                    dict(name='bar', type='varchar'),
                                    dict(name='baz', type='char')]))

        # importoptions= missing a column
        importoptions = dict(vars=[dict(name='foo', type='double', format='best8'),
                                   dict(),
                                   dict(name='baz', type='char')])

        self.s._apply_importoptions_vars(importoptions, df_dtypes)

        self.assertEqual(importoptions,
                         dict(vars=[dict(name='foo', type='double', format='best8'),
                                    dict(name='bar', type='varchar'),
                                    dict(name='baz', type='char')]))

        # importoptions= missing a data type
        importoptions = dict(vars=[dict(name='foo', format='best8'),
                                   dict(name='bar'),
                                   dict(name='baz', type='char')])

        self.s._apply_importoptions_vars(importoptions, df_dtypes)

        if 'csv-ints' in self.s.server_features:
            self.assertEqual(importoptions,
                             dict(vars=[dict(name='foo', type='int32', format='best8'),
                                        dict(name='bar', type='varchar'),
                                        dict(name='baz', type='char')]))
        else:
            self.assertEqual(importoptions,
                             dict(vars=[dict(name='foo', type='double', format='best8'),
                                        dict(name='bar', type='varchar'),
                                        dict(name='baz', type='char')]))

        # importoptions= missing columns at end
        importoptions = dict(vars=[dict(name='foo', type='double', format='best8')])

        self.s._apply_importoptions_vars(importoptions, df_dtypes)

        self.assertEqual(importoptions,
                         dict(vars=[dict(name='foo', type='double', format='best8'),
                                    dict(name='bar', type='varchar'),
                                    dict(name='baz', type='varchar')]))

        # importoptions= dict fully defined, dtypes won't override
        importoptions = dict(vars=dict(foo=dict(type='double', format='best8'),
                                       bar=dict(type='varchar'),
                                       baz=dict(type='char')))

        self.s._apply_importoptions_vars(importoptions, df_dtypes)

        self.assertEqual(importoptions,
                         dict(vars=dict(foo=dict(type='double', format='best8'),
                                        bar=dict(type='varchar'),
                                        baz=dict(type='char'))))

        # importoptions= dict missing dtypes
        importoptions = dict(vars=dict(foo=dict(format='best8'),
                                       bar=dict(),
                                       baz=dict(type='char')))

        self.s._apply_importoptions_vars(importoptions, df_dtypes)

        if 'csv-ints' in self.s.server_features:
            self.assertEqual(importoptions,
                             dict(vars=dict(foo=dict(type='int32', format='best8'),
                                            bar=dict(type='varchar'),
                                            baz=dict(type='char'))))
        else:
            self.assertEqual(importoptions,
                             dict(vars=dict(foo=dict(type='double', format='best8'),
                                            bar=dict(type='varchar'),
                                            baz=dict(type='char'))))

        # importoptions= dict missing key
        importoptions = dict(vars=dict(foo=dict(type='double', format='best8'),
                                       baz=dict(type='char')))

        self.s._apply_importoptions_vars(importoptions, df_dtypes)

        self.assertEqual(importoptions,
                         dict(vars=dict(foo=dict(type='double', format='best8'),
                                        bar=dict(type='varchar'),
                                        baz=dict(type='char'))))

    def test_has_action(self):
        self.assertTrue(self.s.has_action('table.loadtable'))
        self.assertTrue(self.s.has_action('table.LoadTable'))
        self.assertTrue(self.s.has_action('table.loadTable'))

        self.assertTrue(self.s.has_action('loadtable'))
        self.assertTrue(self.s.has_action('LoadTable'))
        self.assertTrue(self.s.has_action('loadTable'))

        self.assertFalse(self.s.has_action('table.unknownAction'))
        self.assertFalse(self.s.has_action('table.unknownaction'))

    def test_has_actionset(self):
        self.assertTrue(self.s.has_actionset('table'))
        self.assertTrue(self.s.has_actionset('Table'))
        self.assertTrue(self.s.has_actionset('builtins'))
        self.assertTrue(self.s.has_actionset('BuiltIns'))

        self.assertFalse(self.s.has_actionset('unknownActionSet'))
        self.assertFalse(self.s.has_actionset('unknownactionset'))


if __name__ == '__main__':
   import xmlrunner
   unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports', verbosity=2))

#  import profile, pstats

#  profile.run("unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))", 'stats.txt')

#  stats = pstats.Stats('stats.txt')
#  stats.strip_dirs()
#  stats.sort_stats('cumulative', 'calls')
#  stats.print_stats(25)
#  stats.sort_stats('time', 'calls')
#  stats.print_stats(25)
