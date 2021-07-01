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


import copy
import io
import os
import matplotlib
import numpy as np
import pandas as pd
import pytz
import re
import six
import swat
import swat.utils.testing as tm
import sys
import unittest
import warnings
from swat.cas.table import concat

from PIL import Image
from swat.utils.compat import patch_pandas_sort

matplotlib.use('Agg')
patch_pandas_sort()

pd_version = tuple([int(x) for x in re.match(r'^(\d+)\.(\d+)\.(\d+)',
                                             pd.__version__).groups()])

# Pick sort keys that will match across SAS and Pandas sorting orders
SORT_KEYS = ['Origin', 'MSRP', 'Horsepower', 'Model']

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestCASTable(tm.TestCase):

    # Create a class attribute to hold the cas host type
    server_type = None
    server_version = None

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.cas.trace_actions = False
        swat.options.cas.trace_ui_actions = False
        swat.options.interactive_mode = False
        swat.options.cas.missing.int64 = -999999

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if type(self).server_type is None:
            # Set once per class and have every test use it.
            # No need to change between tests.
            type(self).server_type = tm.get_cas_host_type(self.s)

        if type(self).server_version is None:
            type(self).server_version = tm.get_cas_version(self.s)

        self.srcLib = tm.get_casout_lib(self.server_type)

        r = tm.load_data(self.s, 'datasources/cars_single.sashdat', self.server_type)

        self.tablename = r['tableName']
        self.assertNotEqual(self.tablename, None)
        self.table = r['casTable']

    def get_cars_df(self, casout=None, all_doubles=True):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        df = pd.read_csv(myFile)
        df['Model'] = ' ' + df['Model']

        if all_doubles:
            for name in ['MSRP', 'Invoice', 'EngineSize', 'Cylinders', 'Horsepower',
                         'MPG_City', 'MPG_Highway', 'Weight', 'Wheelbase', 'Length']:
                df[name] = df[name].astype(float)

        return df

    def tearDown(self):
        # tear down tests
        try:
            self.s.endsession()
        except swat.SWATError:
            pass
        del self.s
        swat.reset_option()

    def test_copy(self):
        t1 = self.s.CASTable('cars', caslib=self.srcLib, vars=['a', 'b', 'c'])
        t2 = t1.copy()

        self.assertTrue(t1 is not t2)
        self.assertEqual(t1, t2)

        self.assertTrue(t1.params.vars == t2.params.vars)

        t3 = copy.deepcopy(t1)

        self.assertTrue(t1 is not t3)
        self.assertEqual(t1, t3)

        self.assertTrue(t1.params.vars is not t3.params.vars)
        self.assertTrue(t1.params.vars == t3.params.vars)

        t3.params.vars = [1, 2, 3]

        self.assertTrue(t1.params.vars is not t3.params.vars)
        self.assertFalse(t1.params.vars == t3.params.vars)

    def test_to_params(self):
        t1 = self.s.CASTable('cars', caslib=self.srcLib, vars=['a', 'b', 'c'])
        t2 = copy.deepcopy(t1)

        del t2.params.vars

        self.assertNotEqual(t1, t2)

        t2.params.vars[0] = 'a'
        t2.params.vars[1] = 'b'
        t2.params.vars[2] = 'c'

        self.assertTrue(t1.to_params() == t2.to_params())

        t3 = self.s.CASTable('cars')

        t3.caslib = self.srcLib
        t3.vars[0] = 'a'
        t3.vars[1] = 'b'
        t3.vars[2] = 'c'

        self.assertEqual(t1.to_params(), t3.to_params())

    def test_actionsets(self):
        self.assertTrue('decisiontree' not in self.table.get_actionset_names())

        self.s.loadactionset('decisiontree')

        actionsets = self.table.get_actionset_names()
        actions = self.table.get_action_names()

        self.assertTrue('decisiontree' in actionsets)
        self.assertTrue('dtreeprune' in actions)
        self.assertTrue('dtreemerge' in actions)
        self.assertTrue('decisiontree.dtreeprune' in actions)
        self.assertTrue('decisiontree.dtreemerge' in actions)

        info = list(self.s.actionsetinfo()['setinfo']['actionset'].values)
        for item in info:
            self.assertTrue(item.lower() in actionsets)

    def test_dir(self):
        dirout = self.table.__dir__()

        self.assertTrue('builtins.loadactionset' in dirout)
        self.assertTrue('session.timeout' in dirout)
        self.assertTrue('sessionid' in dirout)
        self.assertTrue('session.sessionid' in dirout)
        self.assertTrue('autotune' not in dirout)
        self.assertTrue('autotune.tuneall' not in dirout)
        self.assertTrue('tunesvm' not in dirout)
        self.assertTrue('autotune.tunesvm' not in dirout)

        self.s.loadactionset('autotune')

        dirout = self.table.__dir__()

        self.assertTrue('builtins.loadactionset' in dirout)
        self.assertTrue('session.timeout' in dirout)
        self.assertTrue('sessionid' in dirout)
        self.assertTrue('session.sessionid' in dirout)
        self.assertTrue('autotune.tuneall' in dirout)
        self.assertTrue('tunesvm' in dirout)
        self.assertTrue('autotune.tunesvm' in dirout)

        # Whack connection
        self.table.set_connection(None)

        dirout = self.table.__dir__()

        self.assertTrue('builtins.loadactionset' not in dirout)
        self.assertTrue('session.sessionid' not in dirout)
        self.assertTrue('dataquality.parse' not in dirout)

    def test_str(self):
        s = str(self.table)

        self.assertTrue(type(s) == str)
        self.assertTrue('DATASOURCES.CARS_SINGLE' in s)
        self.assertRegex(s, r"^.+? caslib=u?'%s(\([^\)]+\))?'.+?" % self.srcLib.upper())

        r = repr(self.table)

        self.assertTrue(type(r) == str)
        self.assertTrue('DATASOURCES.CARS_SINGLE' in s)
        self.assertRegex(s, r"^.+? caslib=u?'%s(\([^\)]+\))?'.+?" % self.srcLib.upper())

        # Test sort indicator
        newtbl = self.table.sort_values('Make')
        self.assertTrue(repr(newtbl).endswith(".sort_values('Make')"))

        newtbl.sort_values('Model', inplace=True)
        self.assertTrue(repr(newtbl).endswith(".sort_values('Model')"))

        newtbl.sort_values(['MSRP', 'Invoice'], ascending=False, inplace=True)
        self.assertTrue(repr(newtbl).endswith(".sort_values(['MSRP', 'Invoice'], "
                                              "ascending=False)"))

        newtbl = self.table.sort_values(['Make', 'Model'], ascending=[True, False])
        self.assertTrue(repr(newtbl).endswith(".sort_values(['Make', 'Model'], "
                                              "ascending=[True, False])"))

        newtbl = self.table.sort_values(['Make', 'Model'], ascending=False)
        self.assertTrue(repr(newtbl).endswith(".sort_values(['Make', 'Model'], "
                                              "ascending=False)"))

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
        self.assertTrue(out['Name'].iloc[-1].upper().startswith(self.srcLib.upper()))

        # CASTable as output table
        outtable = self.s.CASTable('summout', caslib=self.table.params.caslib)
        out = self.s.summary(table=self.table, casout=outtable)['OutputCasTables']
        self.assertTrue(out['casLib'][0].upper().startswith(self.srcLib.upper()))
        self.assertEqual(out['Name'][0], 'summout')

    def test_action_class(self):
        out = self.table.loadactionset('simple')
        summ = self.table.Summary(table=self.table)

        out = summ()['Summary']
        self.assertEqual(len(out), 10)
        self.assertEqual(out['Column'][0], 'MSRP')

        summ = self.table.simple.Summary(table=self.table)

        out = summ()['Summary']
        self.assertEqual(len(out), 10)
        self.assertEqual(out['Column'][0], 'MSRP')

        s1 = self.table.simple
        s2 = self.table.simple

        self.assertTrue(s1 is not s2)

        s1 = self.table.simple.Summary
        s2 = self.table.simple.Summary

        self.assertTrue(s1 is not s2)

    def test_action_class_2(self):
        self.table.loadactionset('simple')

        s1 = self.table.Summary
        s2 = self.table.Summary

        self.assertTrue(s1 is not s2)

    def test_action_class_3(self):
        self.table.loadactionset('simple')

        s1 = self.table.summary
        s2 = self.table.summary

        self.assertTrue(s1 is not s2)

    def test_get_connection(self):
        self.assertTrue(self.table.get_connection(), self.s)

        self.table.set_connection(None)

        with self.assertRaises(swat.SWATError):
            self.table.get_connection()

    def test_to_table(self):
        tbl = self.s.CASTable('foo', caslib=self.srcLib, replace=True, singlepass=True)
        intbl = self.s.CASTable('foo', caslib=self.srcLib, singlepass=True)
        outtbl = self.s.CASTable('foo', caslib=self.srcLib, replace=True)

        intbl2 = tbl.to_table()
        self.assertTrue(isinstance(intbl2, type(tbl)))
        self.assertEqual(intbl, intbl2)

        outtbl2 = tbl.to_outtable()
        self.assertTrue(isinstance(outtbl2, type(tbl)))
        self.assertEqual(outtbl, outtbl2)

    def test_attrs(self):
        tbl = self.s.CASTable('foo', caslib=self.srcLib, replace=True, singlepass=True)

        self.assertEqual(tbl.params, dict(name='foo', caslib=self.srcLib,
                                          replace=True, singlepass=True))

        tbl.caslib = 'MyCaslib'
        tbl.importoptions.filetype = 'xlsx'

        self.assertEqual(tbl.params, dict(name='foo', caslib='MyCaslib',
                                          replace=True, singlepass=True,
                                          importoptions=dict(filetype='xlsx')))

        tbl.nonparam = 'value'

        self.assertEqual(tbl.params, dict(name='foo', caslib='MyCaslib',
                                          replace=True, singlepass=True,
                                          importoptions=dict(filetype='xlsx')))

        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(tbl.caslib, 'MyCaslib')
        # NOTE: This will return the `replace` method
        # self.assertEqual(tbl.replace, True)
        self.assertEqual(tbl.SinglePass, True)
        self.assertEqual(tbl.importOptions, dict(filetype='xlsx'))

        with self.assertRaises(AttributeError):
            tbl.nonexist

        del tbl.importoptions

        self.assertEqual(tbl.params, dict(name='foo', caslib='MyCaslib',
                                          replace=True, singlepass=True))

        del tbl.Replace

        self.assertEqual(tbl.params, dict(name='foo', caslib='MyCaslib', singlepass=True))

    def test_invoke(self):
        self.table.loadactionset('simple')
        conn = self.table.invoke('summary')
        for resp in conn:
            for k, v in resp:
                self.assertEqual(k, 'Summary')

    def test_retrieve(self):
        self.table.loadactionset('simple')
        out = self.table.retrieve('summary')
        self.assertEqual(list(out.keys()), ['Summary'])

    def test_columns(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']

        out = self.table.columns
        self.assertEqual(type(out), pd.Index)
        self.assertEqual(out.tolist(), columns)
        self.assertEqual(self.table.head().columns.tolist(), columns)

        # Add computed columns
        self.table['MakeModel'] = (self.table.Make + self.table['Model']).str.lower()
        self.table['One'] = 1
        out = self.table.columns
        columns.extend(['MakeModel', 'One'])
        self.assertEqual(out.tolist(), columns)
        self.assertEqual(self.table.head().columns.tolist(), columns)

        # Add them a second time
        self.table['MakeModel'] = (self.table.Make + self.table['Model']).str.lower()
        self.table['One'] = 1
        out = self.table.columns
        self.assertEqual(out.tolist(), columns)
        self.assertEqual(self.table.head().columns.tolist(), columns)

        # Delete columns
        del self.table['Type']
        del self.table['MSRP']
        out = self.table.columns
        columns = [x for x in columns if x not in ['Type', 'MSRP']]
        self.assertEqual(out.tolist(), columns)
        self.assertEqual(self.table.head().columns.tolist(), columns)

        # Add them a third time under a different name
        self.table['MakeModel_2'] = (self.table.Make + self.table['Model']).str.lower()
        self.table['One_2'] = 1
        out = self.table.columns
        columns.extend(['MakeModel_2', 'One_2'])
        self.assertEqual(out.tolist(), columns)
        self.assertEqual(self.table.head().columns.tolist(), columns)

    def test_index(self):
        index = self.table.index
        self.assertTrue(index is None)

    def test_as_matrix(self):
        matrix = self.table.as_matrix()

        self.assertTrue(isinstance(matrix, np.ndarray))

        self.assertEqual(
            matrix.tolist()[:3],
            [[u'Acura', u' MDX', u'SUV', u'Asia', u'All', 36945.0, 33337.0, 3.5,
              6.0, 265.0, 17.0, 23.0, 4451.0, 106.0, 189.0],
             [u'Acura', u' RSX Type S 2dr', u'Sedan', u'Asia', u'Front', 23820.0,
              21761.0, 2.0, 4.0, 200.0, 24.0, 31.0, 2778.0, 101.0, 172.0],
             [u'Acura', u' TSX 4dr', u'Sedan', u'Asia', u'Front', 26990.0, 24647.0,
              2.4, 4.0, 200.0, 22.0, 29.0, 3230.0, 105.0, 183.0]])

        self.assertEqual(
            matrix.tolist()[-1],
            [u'Volvo', u' XC70', u'Wagon', u'Europe', u'All', 35145.0, 33112.0,
             2.5, 5.0, 208.0, 20.0, 27.0, 3823.0, 109.0, 186.0])

        # Subset
        matrix = self.table.as_matrix(columns=['Make', 'MSRP', 'Invoice'])

        self.assertEqual(matrix.tolist()[:3],
                         [[u'Acura', 36945.0, 33337.0],
                          [u'Acura', 23820.0, 21761.0],
                          [u'Acura', 26990.0, 24647.0]])

        self.assertEqual(matrix.tolist()[-1], [u'Volvo', 35145.0, 33112.0])

        # Double subset
        tbl = self.table.copy()
        tbl.params.vars = ['Make', 'Model', 'MSRP', 'Invoice']

        self.assertEqual(tbl.columns.tolist(), ['Make', 'Model', 'MSRP', 'Invoice'])

        matrix = tbl.as_matrix(columns=['Make', 'MSRP'])

        self.assertEqual(matrix.tolist()[:3],
                         [[u'Acura', 36945.0],
                          [u'Acura', 23820.0],
                          [u'Acura', 26990.0]])

        # Make sure the table didn't get modified
        self.assertEqual(tbl.columns.tolist(), ['Make', 'Model', 'MSRP', 'Invoice'])

    def test_types(self):
        columns = ['Double', 'Char', 'Varchar', 'Int32', 'Int64', 'Date', 'Time',
                   'Datetime', 'DecSext', 'Varbinary', 'Binary']
        dtypes = ['double', 'char', 'varchar', 'int32', 'int64', 'date', 'time',
                  'datetime', 'decsext', 'varbinary', 'binary']
        ftypes = ['double:dense', 'char:dense', 'varchar:dense', 'int32:dense',
                  'int64:dense', 'date:dense', 'time:dense',
                  'datetime:dense', 'decsext:dense', 'varbinary:dense', 'binary:dense']

        srcLib = self.srcLib
        out = self.s.loadactionset(actionset='actionTest')
        if out.severity != 0:
            self.skipTest("actionTest failed to load")

        out = self.s.alltypes(casout=dict(caslib=srcLib, name='typestable'))

        tbl = self.s.CASTable('typestable', caslib=srcLib)
        data = self.s.fetch(table=tbl, sastypes=False).Fetch

        self.assertEqual(tbl.dtypes.index.tolist(), columns)
        self.assertEqual(tbl.dtypes.tolist(), dtypes)

        self.assertEqual(tbl.ftypes.index.tolist(), columns)
        self.assertEqual(tbl.ftypes.tolist(), ftypes)

        # Compare to real DataFrame
        self.assertEqual(type(data.dtypes), type(tbl.dtypes))
        self.assertEqual(type(data.dtypes.index), type(tbl.dtypes.index))
        self.assertEqual(data.dtypes.index.tolist(), columns)

        if pd_version[0] < 1:
            self.assertEqual(data.ftypes.index.tolist(), columns)

    def test_type_counts(self):
        # alltypes
        index = sorted(['double', 'char', 'varchar', 'int32', 'int64', 'date', 'time',
                        'datetime', 'decsext', 'varbinary', 'binary'])
        findex = sorted(['double:dense', 'char:dense', 'varchar:dense', 'int32:dense',
                         'int64:dense', 'date:dense', 'time:dense', 'datetime:dense',
                         'decsext:dense', 'varbinary:dense', 'binary:dense'])
        counts = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

        srcLib = self.srcLib
        out = self.s.loadactionset(actionset='actionTest')
        if out.severity != 0:
            self.skipTest("actionTest failed to load")

        out = self.s.alltypes(casout=dict(caslib=srcLib, name='typestable'))

        tbl = self.s.CASTable('typestable', caslib=srcLib)

        dtype_counts = tbl.get_dtype_counts()
        ftype_counts = tbl.get_ftype_counts()

        self.assertTrue(isinstance(dtype_counts, pd.Series))
        self.assertEqual(dtype_counts.tolist(), counts)

        self.assertTrue(isinstance(ftype_counts, pd.Series))
        self.assertEqual(ftype_counts.tolist(), counts)

        self.assertTrue(isinstance(dtype_counts.index, pd.Index))
        self.assertEqual(dtype_counts.index.tolist(), index)

        self.assertTrue(isinstance(ftype_counts.index, pd.Index))
        self.assertEqual(ftype_counts.index.tolist(), findex)

        # cars_single
        index = ['char', 'double']
        findex = ['char:dense', 'double:dense']
        counts = [5, 10]

        tbl = self.table

        dtype_counts = tbl.get_dtype_counts()
        ftype_counts = tbl.get_ftype_counts()

        self.assertTrue(isinstance(dtype_counts, pd.Series))
        self.assertEqual(dtype_counts.tolist(), counts)

        self.assertTrue(isinstance(ftype_counts, pd.Series))
        self.assertEqual(ftype_counts.tolist(), counts)

        self.assertTrue(isinstance(dtype_counts.index, pd.Index))
        self.assertEqual(dtype_counts.index.tolist(), index)

        self.assertTrue(isinstance(ftype_counts.index, pd.Index))
        self.assertEqual(ftype_counts.index.tolist(), findex)

    def test_select_dtypes(self):
        srcLib = self.srcLib
        out = self.s.loadactionset(actionset='actionTest')
        if out.severity != 0:
            self.skipTest("actionTest failed to load")

        out = self.s.alltypes(casout=dict(caslib=srcLib, name='typestable'))

        tbl = self.s.CASTable('typestable', caslib=srcLib)

        # Preserves original order
        subtbl = tbl.select_dtypes(include=['int32', 'int64', 'double'])
        self.assertEqual(subtbl.columns.tolist(), ['Double', 'Int32', 'Int64'])
        self.assertEqual(subtbl.head().columns.tolist(), ['Double', 'Int32', 'Int64'])

        subtbl = tbl.select_dtypes(exclude=['int32', 'int64', 'double'])
        self.assertEqual(subtbl.columns.tolist(),
                         ['Char', 'Varchar', 'Date', 'Time',
                          'Datetime', 'DecSext', 'Varbinary', 'Binary'])
        self.assertEqual(subtbl.head().columns.tolist(),
                         ['Char', 'Varchar', 'Date', 'Time',
                          'Datetime', 'DecSext', 'Varbinary', 'Binary'])

        char_names = ['Char', 'Varchar', 'Varbinary', 'Binary']
        num_names = ['Double', 'Int32', 'Int64', 'Date', 'Time', 'Datetime', 'DecSext']
        float_names = ['Double', 'DecSext']
        int_names = ['Int32', 'Int64', 'Date', 'Time', 'Datetime']

        # Numeric types
        subtbl = tbl.select_dtypes(include=['number'])
        self.assertEqual(subtbl.columns.tolist(), num_names)
        self.assertEqual(subtbl.head().columns.tolist(), num_names)

        subtbl = tbl.select_dtypes(include=['numeric'])
        self.assertEqual(subtbl.columns.tolist(), num_names)
        self.assertEqual(subtbl.head().columns.tolist(), num_names)

        subtbl = tbl.select_dtypes(exclude=['character'])
        self.assertEqual(subtbl.columns.tolist(), num_names)
        self.assertEqual(subtbl.head().columns.tolist(), num_names)

        # Character types
        subtbl = tbl.select_dtypes(include=['character'])
        self.assertEqual(subtbl.columns.tolist(), char_names)
        self.assertEqual(subtbl.head().columns.tolist(), char_names)

        subtbl = tbl.select_dtypes(exclude=['numeric'])
        self.assertEqual(subtbl.columns.tolist(), char_names)
        self.assertEqual(subtbl.head().columns.tolist(), char_names)

        subtbl = tbl.select_dtypes(exclude=['number'])
        self.assertEqual(subtbl.columns.tolist(), char_names)
        self.assertEqual(subtbl.head().columns.tolist(), char_names)

        # Float types
        subtbl = tbl.select_dtypes(include=['floating'])
        self.assertEqual(subtbl.columns.tolist(), float_names)
        self.assertEqual(subtbl.head().columns.tolist(), float_names)

        # Integer types
        subtbl = tbl.select_dtypes(include=['integer'])
        self.assertEqual(subtbl.columns.tolist(), int_names)
        self.assertEqual(subtbl.head().columns.tolist(), int_names)

        subtbl = tbl.select_dtypes(exclude=['integer', 'character'])
        self.assertEqual(subtbl.columns.tolist(), float_names)
        self.assertEqual(subtbl.head().columns.tolist(), float_names)

        # Mixed include and exclude
        subtbl = tbl.select_dtypes(include=['number'], exclude=['int32', 'int64'])
        self.assertEqual(subtbl.columns.tolist(),
                         [x for x in num_names if x not in ['Int32', 'Int64']])
        self.assertEqual(subtbl.head().columns.tolist(),
                         [x for x in num_names if x not in ['Int32', 'Int64']])

        # In place
        subtbl = tbl.select_dtypes(include=['number'], inplace=True)
        self.assertTrue(subtbl is None)
        self.assertEqual(tbl.columns.tolist(), num_names)
        self.assertEqual(tbl.head().columns.tolist(), num_names)

    def test_values(self):
        data = self.table.values

        self.assertEqual(
            data.tolist()[:3],
            [[u'Acura', u' MDX', u'SUV', u'Asia', u'All', 36945.0, 33337.0,
              3.5, 6.0, 265.0, 17.0, 23.0, 4451.0, 106.0, 189.0],
             [u'Acura', u' RSX Type S 2dr', u'Sedan', u'Asia', u'Front', 23820.0,
              21761.0, 2.0, 4.0, 200.0, 24.0, 31.0, 2778.0, 101.0, 172.0],
             [u'Acura', u' TSX 4dr', u'Sedan', u'Asia', u'Front', 26990.0, 24647.0,
              2.4, 4.0, 200.0, 22.0, 29.0, 3230.0, 105.0, 183.0]])

        self.assertEqual(
            data.tolist()[-1],
            [u'Volvo', u' XC70', u'Wagon', u'Europe', u'All', 35145.0, 33112.0,
             2.5, 5.0, 208.0, 20.0, 27.0, 3823.0, 109.0, 186.0])

    def test_column_values(self):
        df = self.get_cars_df()['Model']
        tbl = self.table['Model']

        self.assertEqual(df.values.tolist(), tbl.values.tolist())

    def test_dtypes(self):
        dtypes = self.table.dtypes
        self.assertEqual(dtypes.Model, 'char')
        self.assertEqual(dtypes.Type, 'char')
        self.assertEqual(dtypes.Origin, 'char')
        self.assertEqual(dtypes.DriveTrain, 'char')
        self.assertEqual(dtypes.MSRP, 'double')
        self.assertEqual(dtypes.Invoice, 'double')

    def test_column_dtypes(self):
        self.assertEqual(self.table.Model.dtype, 'char')
        self.assertEqual(self.table.Type.dtype, 'char')
        self.assertEqual(self.table.Origin.dtype, 'char')
        self.assertEqual(self.table.DriveTrain.dtype, 'char')
        self.assertEqual(self.table.MSRP.dtype, 'double')
        self.assertEqual(self.table.Invoice.dtype, 'double')

    def test_ftypes(self):
        ftypes = self.table.ftypes
        self.assertEqual(ftypes.Model, 'char:dense')
        self.assertEqual(ftypes.Type, 'char:dense')
        self.assertEqual(ftypes.Origin, 'char:dense')
        self.assertEqual(ftypes.DriveTrain, 'char:dense')
        self.assertEqual(ftypes.MSRP, 'double:dense')
        self.assertEqual(ftypes.Invoice, 'double:dense')

    def test_column_ftypes(self):
        self.assertEqual(self.table.Model.ftype, 'char:dense')
        self.assertEqual(self.table.Type.ftype, 'char:dense')
        self.assertEqual(self.table.Origin.ftype, 'char:dense')
        self.assertEqual(self.table.DriveTrain.ftype, 'char:dense')
        self.assertEqual(self.table.MSRP.ftype, 'double:dense')
        self.assertEqual(self.table.Invoice.ftype, 'double:dense')

    def test_axes(self):
        axes = self.table.axes
        # self.assertEqual(axes[0].tolist(), list(range(428)))
        self.assertEqual(axes[1].tolist(), list(self.table.columns))

    def test_ndim(self):
        ndim = self.table.head(n=10000).ndim
        self.assertEqual(self.table.ndim, ndim)

    def test_column_ndim(self):
        ndim = self.table['Model'].head(n=10000).ndim
        self.assertEqual(self.table['Model'].ndim, ndim)

    @unittest.skipIf(pd_version <= (0, 14, 0), 'Need newer version of Pandas')
    def test_size(self):
        size = self.table.head(n=10000).size
        self.assertEqual(self.table.size, size)

    @unittest.skipIf(pd_version <= (0, 14, 0), 'Need newer version of Pandas')
    def test_column_size(self):
        size = self.table['Model'].head(n=10000).size
        self.assertEqual(self.table['Model'].size, size)

    def test_column_itemsize(self):
        self.assertEqual(self.table['Make'].itemsize, 13)
        self.assertEqual(self.table['Model'].itemsize, 40)
        self.assertEqual(self.table['Type'].itemsize, 8)
        self.assertEqual(self.table['Origin'].itemsize, 6)
        self.assertEqual(self.table['DriveTrain'].itemsize, 5)
        self.assertEqual(self.table['MSRP'].itemsize, 8)
        self.assertEqual(self.table['Invoice'].itemsize, 8)

    def test_shape(self):
        shape = self.table.head(n=10000).shape
        self.assertEqual(self.table.shape, shape)

    def test_column_shape(self):
        shape = self.table['Make'].head(n=10000).shape
        self.assertEqual(self.table['Make'].shape, shape)

    def test_copy_with_params(self):
        tbl = self.table.copy()

        self.assertTrue(tbl is not self.table)
        self.assertEqual(tbl.columns.tolist(), self.table.columns.tolist())

        tbl.params.vars = ['Make', 'Model']

        tbl2 = tbl.copy(deep=False)

        self.assertTrue(tbl2.params.vars is tbl.params.vars)
        self.assertEqual(tbl2.params.vars, tbl.params.vars)

        tbl3 = tbl.copy()

        self.assertTrue(tbl3.params.vars is not tbl.params.vars)
        self.assertEqual(tbl3.params.vars, tbl.params.vars)

    def test_column_copy(self):
        make = self.table['Make']
        makecopy = make.copy(deep=False)

        self.assertEqual(make.__class__.__name__, 'CASColumn')
        self.assertEqual(makecopy.__class__.__name__, 'CASColumn')
        self.assertTrue(makecopy is not make)
        self.assertEqual(makecopy.columns.tolist(), make.columns.tolist())

        self.assertTrue(makecopy.get_inputs_param() is make.get_inputs_param())

        makecopy2 = self.table['Make'].copy()

        self.assertTrue(makecopy2.get_inputs_param() is not make.get_inputs_param())
        self.assertEqual(makecopy2.get_inputs_param(), make.get_inputs_param())

    def test_column_isnull(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(df['Cylinders'].isnull().tolist(),
                         tbl['Cylinders'].isnull().tolist())
        self.assertEqual(df['Cylinders'].notnull().tolist(),
                         tbl['Cylinders'].notnull().tolist())

        self.assertEqual(df['Make'].isnull().tolist(),
                         tbl['Make'].isnull().tolist())
        self.assertEqual(df['Make'].notnull().tolist(),
                         tbl['Make'].notnull().tolist())

    def test_head(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']

        # Table head with no args
        df = self.table.head()

        self.assertEqual(df.columns.tolist(), columns)
        self.assertEqual(df.shape, (5, len(columns)))

        self.assertEqual(df['Model'].tolist(),
                         [' MDX', ' RSX Type S 2dr', ' TSX 4dr',
                          ' TL 4dr', ' 3.5 RL 4dr'])

        # Table head with n=10
        df = self.table.head(10)

        self.assertEqual(df.columns.tolist(), columns)
        self.assertEqual(df.shape, (10, len(columns)))

        # Table head with n=10 and columns specified
        df = self.table.head(10, columns=['MSRP', 'Invoice'])

        self.assertEqual(df.columns.tolist(), ['MSRP', 'Invoice'])
        self.assertEqual(df.shape, (10, 2))

        # Column head with n=10
        self.assertEqual(df['MSRP'].head(10).tolist(),
                         self.table['MSRP'].head(10).tolist())

    def test_tail(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']

        # Table tail with no args
        df = self.table.tail()

        self.assertEqual(df.columns.tolist(), columns)
        self.assertEqual(df.shape, (5, len(columns)))

        self.assertEqual(df['Model'].tolist(),
                         [' C70 LPT convertible 2dr', ' C70 HPT convertible 2dr',
                          ' S80 T6 4dr', ' V40', ' XC70'])

        # Table tail with n=10
        df = self.table.tail(10)

        self.assertEqual(df.columns.tolist(), columns)
        self.assertEqual(df.shape, (10, len(columns)))

        # Table tail with n=10 and columns specified
        df = self.table.tail(10, columns=['MSRP', 'Invoice'])

        self.assertEqual(df.columns.tolist(), ['MSRP', 'Invoice'])
        self.assertEqual(df.shape, (10, 2))

        # Column tail with n=10
        self.assertEqual(df['MSRP'].tail(10).tolist(),
                         self.table['MSRP'].tail(10).tolist())

    def test_iter(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']
        data = []
        for col in self.table:
            data.append(col)
        self.assertEqual(data, columns)

    def test_column_iter(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(sorted(iter(df['Model'])),
                         sorted(iter(tbl['Model'])))
        self.assertEqual(len(list(iter(df['Model']))),
                         len(list(iter(tbl['Model']))))

    def test_iteritems(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']
        names = []
        for name, data in self.table.iteritems():
            names.append(name)
            self.assertEqual(data.__class__.__name__, 'CASColumn')
        self.assertEqual(names, columns)

        # Computed columns
        self.table['MakeModel'] = (self.table.Make + ' ' + self.table.Model)
        for item in self.table.iteritems():
            pass
        self.assertEqual(item[0], 'MakeModel')
        self.assertEqual(item[1].head().tolist(),
                         ['Acura  MDX',
                          'Acura  RSX Type S 2dr',
                          'Acura  TSX 4dr',
                          'Acura  TL 4dr',
                          'Acura  3.5 RL 4dr'])

    def test_column_iteritems(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(sorted(df['Model'].iteritems()),
                         sorted(tbl['Model'].iteritems()))
        self.assertEqual(len(list(df['Model'].iteritems())),
                         len(list(tbl['Model'].iteritems())))

    def test_iterrows(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']

        i = 0
        for idx, row in self.table.iterrows():
            i = idx + 1
            self.assertTrue(isinstance(row, pd.Series))
            self.assertEqual(row.index.tolist(), columns)

        self.assertEqual(i, self.table.shape[0])

    def test_itertuples(self):
        i = 0
        for row in self.table.itertuples():
            if i == 0:
                self.assertEqual(row,
                                 (0, u'Acura', u' MDX', u'SUV', u'Asia', u'All',
                                  36945.0, 33337.0, 3.5, 6.0, 265.0, 17.0, 23.0,
                                  4451.0, 106.0, 189.0))
            elif i == 1:
                self.assertEqual(row,
                                 (1, u'Acura', u' RSX Type S 2dr', u'Sedan', u'Asia',
                                  u'Front', 23820.0, 21761.0, 2.0, 4.0, 200.0, 24.0,
                                  31.0, 2778.0, 101.0, 172.0))
            elif i == 2:
                self.assertEqual(row,
                                 (2, u'Acura', u' TSX 4dr', u'Sedan', u'Asia', u'Front',
                                  26990.0, 24647.0, 2.3999999999999999, 4.0, 200.0, 22.0,
                                  29.0, 3230.0, 105.0, 183.0))
            i += 1
            self.assertTrue(isinstance(row, tuple))

        self.assertEqual(i, self.table.shape[0])

    @unittest.skipIf(pd_version >= (0, 21, 0), 'Deprecated in pandas')
    def test_get_value(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(df.get_value(10, 'Make'), tbl.get_value(10, 'Make'))
        self.assertEqual(df.get_value(99, 'Model'), tbl.get_value(99, 'Model'))

        with self.assertRaises(KeyError):
            tbl.get_value(99, 'Foo')

        with self.assertRaises(IndexError):
            tbl.get_value(500, 'Make')

    def test_column_get(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(df['Model'].get(10), tbl['Model'].get(10))
        self.assertEqual(df['Model'].get(99), tbl['Model'].get(99))
        self.assertEqual(df['Model'].get(500), tbl['Model'].get(500))

    def test_lookup(self):
        look = self.table.lookup([0, 5, 10], ['Make', 'Make', 'MSRP'])
        head = self.table.head(n=15).lookup([0, 5, 10], ['Make', 'Make', 'MSRP'])

        self.assertEqual(str(look.dtype), 'object')
        self.assertEqual(list(look), ['Acura', 'Acura', 33430.0])

        self.assertEqual(str(look.dtype), str(head.dtype))
        self.assertEqual(list(head), ['Acura', 'Acura', 33430.0])

        look = self.table.lookup([0, 5, 10], ['MSRP', 'MSRP', 'MSRP'])
        head = self.table.head(n=15).lookup([0, 5, 10], ['MSRP', 'MSRP', 'MSRP'])

        self.assertEqual(str(look.dtype), 'float64')
        self.assertEqual(str(head.dtype), 'float64')

    def test_pop(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']

        self.assertEqual(columns, self.table.columns.tolist())

        model = self.table.pop('Model')

        self.assertEqual(model.__class__.__name__, 'CASColumn')
        self.assertEqual(self.table.columns.tolist(),
                         [x for x in columns if x != 'Model'])
        self.assertEqual(self.table.head().columns.tolist(),
                         [x for x in columns if x != 'Model'])

        self.assertEqual(self.table.head().pop('Make').to_dict(),
                         self.table.pop('Make').head().to_dict())

        with self.assertRaises(KeyError):
            self.table.pop('Foo')

    def test_delitem(self):
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                   'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                   'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                   'Wheelbase', 'Length']

        self.assertEqual(columns, self.table.columns.tolist())

        del self.table['Model']
        del self.table['Length']

        self.assertEqual(self.table.columns.tolist(),
                         [x for x in columns if x not in ['Model', 'Length']])
        self.assertEqual(self.table.head().columns.tolist(),
                         [x for x in columns if x not in ['Model', 'Length']])

        with self.assertRaises(KeyError):
            del self.table['Foo']

    def test_corr(self):
        columns = ['MSRP', 'Invoice', 'EngineSize', 'Cylinders', 'Horsepower',
                   'MPG_City', 'MPG_Highway', 'Weight', 'Wheelbase', 'Length']

        corr = self.table.corr()
        dfcorr = self.get_cars_df().corr()

        self.assertEqual(corr.columns.tolist(), dfcorr.columns.tolist())
        self.assertEqual(corr.columns.tolist(), columns)
        self.assertEqual(re.sub(r'(\.\d{8})\d+', r'\1', corr.to_csv())
                           .replace('0.99999999', '1.0')
                           .replace('1.00000000', '1.0')
                           .split('\n'),
                         re.sub(r'(\.\d{8})\d+', r'\1', dfcorr.to_csv())
                           .replace('0.99999999', '1.0')
                           .replace('1.00000000', '1.0')
                           .split('\n'))

    def test_count(self):
        count = self.table.count()
        dfcount = self.get_cars_df().count()

        self.assertEqual(count.index.tolist(), dfcount.index.tolist())
        self.assertEqual(count.tolist(), dfcount.tolist())

        count = self.table.count(numeric_only=True)
        dfcount = self.get_cars_df().count(numeric_only=True)

        self.assertEqual(count.index.tolist(), dfcount.index.tolist())
        self.assertEqual(count.tolist(), dfcount.tolist())

    @unittest.skipIf(pd_version <= (0, 14, 0), 'Need newer version of Pandas')
    def test_describe(self):
        # if self.server_type == 'windows.smp':
        #     tm.TestCase.skipTest(self, 'Skip on WX6 until defect S1240339 fixed')

        df = self.get_cars_df()

        # Numeric data
        desc = self.table.describe()
        dfdesc = df.describe()

        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        # NOTE: Pandas returns slightly different values for the commented
        #       out lines below.

        self.assertEqual(desc.loc['count'].tolist(), dfdesc.loc['count'].tolist())
        for mean, dfmean in zip(desc.loc['mean'].tolist(), dfdesc.loc['mean'].tolist()):
            self.assertAlmostEqual(mean, dfmean, 5)
        for std, dfstd in zip(desc.loc['std'].tolist(), dfdesc.loc['std'].tolist()):
            self.assertAlmostEqual(std, dfstd, 5)
        self.assertEqual(desc.loc['min'].tolist(), dfdesc.loc['min'].tolist())
        # for pct, dpct in zip(desc.loc['25%'].tolist(), dfdesc.loc['25%'].tolist()):
        #     self.assertAlmostEqual(pct, dpct, 0)
        # for pct, dpct in zip(desc.loc['50%'].tolist(), dfdesc.loc['50%'].tolist()):
        #     self.assertAlmostEqual(pct, dpct, 0)
        # for pct, dpct in zip(desc.loc['75%'].tolist(), dfdesc.loc['75%'].tolist()):
        #     self.assertAlmostEqual(pct, dpct, 0)
        self.assertEqual(desc.loc['max'].tolist(), dfdesc.loc['max'].tolist())

        # Character data
        desc = self.table.describe(exclude=['number'])
        dfdesc = df.describe(exclude=['number'])

        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        self.assertEqual(desc.loc['count'].tolist(), dfdesc.loc['count'].tolist())
        self.assertEqual(desc.loc['unique'].tolist(), dfdesc.loc['unique'].tolist())
        # NOTE: The topk action sorts ties by formatted value, so it's different
        #       than what Pandas gets.
        # self.assertEqual(desc.loc['top'].tolist(), dfdesc.loc['top'].tolist())
        self.assertEqual(desc.loc['freq'].tolist(), dfdesc.loc['freq'].tolist())

        # Percentiles
        desc = self.table.describe(percentiles=[0.3, 0.7])
        dfdesc = df.describe(percentiles=[0.3, 0.7])
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        desc = self.table.describe(percentiles=0.4)
        dfdesc = df.describe(percentiles=[0.4])
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        desc = self.table.describe(percentiles=0.5)
        dfdesc = df.describe(percentiles=[0.5])
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        # Select by data type
        desc = self.table.describe(include=['floating'])
        dfdesc = df.describe(include=['floating'])
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        desc = self.table.describe(exclude=['floating'])
        dfdesc = df.describe(exclude=['floating'])
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        desc = self.table.describe(exclude=[object])
        dfdesc = df.describe(exclude=[object])
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        desc = self.table.describe(exclude=[np.number])
        dfdesc = df.describe(exclude=[np.number])
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        desc = self.table.describe(include='all')
        dfdesc = df.describe(include='all')
        self.assertEqual(desc.index.tolist(), dfdesc.index.tolist())
        self.assertEqual(desc.columns.tolist(), dfdesc.columns.tolist())

        # Select stats
        desc = self.table.describe(stats=['count', 'unique'])
        self.assertEqual(desc.index.tolist(), ['count', 'unique'])

        desc = self.table.describe(stats='all')
        labels = desc.index.tolist()
        if 'skewness' in labels:
            self.assertEqual(
                labels,
                ['count', 'unique', 'mean', 'std', 'min', '25%', '50%', '75%']
                + ['max', 'nmiss', 'sum', 'stderr', 'var', 'uss']
                + ['cv', 'tvalue', 'probt', 'css', 'kurtosis', 'skewness'])
        else:
            self.assertEqual(
                labels,
                ['count', 'unique', 'mean', 'std', 'min', '25%', '50%', '75%']
                + ['max', 'nmiss', 'sum', 'stderr', 'var', 'uss']
                + ['cv', 'tvalue', 'probt', 'css'])

        desc = self.table.describe(include='all', stats='all')
        labels = desc.index.tolist()
        if 'skewness' in labels:
            self.assertEqual(
                labels,
                ['count', 'unique', 'top', 'freq', 'mean']
                + ['std', 'min', '25%', '50%', '75%']
                + ['max', 'nmiss', 'sum', 'stderr', 'var', 'uss']
                + ['cv', 'tvalue', 'probt', 'css', 'kurtosis', 'skewness'])
        else:
            self.assertEqual(
                labels,
                ['count', 'unique', 'top', 'freq', 'mean']
                + ['std', 'min', '25%', '50%', '75%']
                + ['max', 'nmiss', 'sum', 'stderr', 'var', 'uss']
                + ['cv', 'tvalue', 'probt', 'css'])

        # Test all character data
        chardf = df[['Make', 'Model', 'Type', 'Origin']]
        chardfdesc = chardf.describe()
        chardesc = self.table.datastep('keep Make Model Type Origin').describe()

        self.assertEqual(len(chardfdesc.index), len(chardesc.index))
        self.assertEqual(chardfdesc.loc['count'].tolist(),
                         chardesc.loc['count'].tolist())
        self.assertEqual(chardfdesc.loc['unique'].tolist(),
                         chardesc.loc['unique'].tolist())
        # TODO: Pandas sorts characters differently
        # self.assertEqual(chardfdesc.loc['top'].tolist(),
        #                  chardesc.loc['top'].tolist())
        self.assertEqual(chardfdesc.loc['freq'].tolist(),
                         chardesc.loc['freq'].tolist())

        # All character / all stats
        chardfdesc = chardf.describe()
        chardesc = self.table.datastep('keep Make Model Type Origin')\
                       .describe(stats='all')
        self.assertEqual(chardfdesc.loc['count'].tolist(),
                         chardesc.loc['count'].tolist())
        self.assertEqual(chardfdesc.loc['unique'].tolist(),
                         chardesc.loc['unique'].tolist())
        # self.assertEqual(chardfdesc.loc['min'].tolist(),
        #                  chardesc.loc['max'].tolist())
        # self.assertEqual(chardfdesc.loc['min'].tolist(),
        #                  chardesc.loc['max'].tolist())

        # All character / manual stats
        chardfdesc = chardf.describe()
        chardesc = self.table.datastep('keep Make Model Type Origin')\
                       .describe(stats=['count', 'unique'])
        self.assertEqual(chardfdesc.loc['count'].tolist(),
                         chardesc.loc['count'].tolist())
        self.assertEqual(chardfdesc.loc['unique'].tolist(),
                         chardesc.loc['unique'].tolist())

        # Test groupby

        grptbl = self.table.groupby(['Make'])
        grpdf = df.groupby(['Make'])

        tbldesc = grptbl.describe(percentiles=[.5])
        dfdesc = grpdf.describe(percentiles=[.5])

        self.assertEqual(tbldesc.index.tolist()[:50],
                         dfdesc.index.tolist()[:50])
        self.assertEqual(set(tbldesc.columns.tolist()),
                         set(dfdesc.columns.tolist()))

        tbllist = tbldesc['MPG_City']
        dflist = dfdesc['MPG_City']
        if isinstance(dflist, pd.Series):
            tbllist = tbllist.tolist()[:50]
            dflist = dflist.tolist()[:50]
            for i in range(50):
                self.assertAlmostEqual(tbllist[i], dflist[i], 2)
        else:
            self.assertTablesEqual(tbldesc['MPG_City'],
                                   dfdesc['MPG_City'], precision=6)

        # Test missing character values
        tbl2 = self.table.replace({'Make': {'BMW': ''}})
        df2 = df.replace({'Make': {'BMW': np.nan}})

        self.assertColsEqual(tbl2.describe(include='all').loc['count'],
                             df2.describe(include='all').loc['count'])

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_max(self):
        # if self.server_type == 'windows.smp':
        #     tm.TestCase.skipTest(self, 'Skip on WX6 until defect S1240339 fixed')

        df = self.get_cars_df()
        tbl = self.table

        out = tbl.max().tolist()
        # dfout = df.max().tolist()

        # TODO: The DataFrame result looks wrong.  It converts all values to
        #       strings and they get truncated.
        # self.assertEqual(out, dfout)

        self.assertEqual(out, ['Volvo', ' Z4 convertible 3.0i 2dr', 'Wagon', 'USA',
                               'Rear', 192465.0, 173560.0, 8.3000000000000007, 12.0,
                               500.0, 60.0, 66.0, 7190.0, 144.0, 238.0])

        # Numeric only
        out = tbl.max(numeric_only=True).tolist()

        self.assertEqual(out, [192465.0, 173560.0, 8.3000000000000007, 12.0,
                               500.0, 60.0, 66.0, 7190.0, 144.0, 238.0])

        # Column max
        self.assertEqual(df['Make'].max(), tbl['Make'].max())
        self.assertEqual(df['MSRP'].max(), tbl['MSRP'].max())

        # Only character columns
        # chardf = df[['Make', 'Model', 'Type', 'Origin']]
        out = tbl.datastep('keep Make Model Type Origin').max().tolist()
        self.assertEqual(out, ['Volvo', ' Z4 convertible 3.0i 2dr', 'Wagon', 'USA'])

        # Groupby variables

        dfgrp = df.groupby(['Make', 'Cylinders'])
        grp = tbl.groupby(['Make', 'Cylinders'])

        # Pandas uses a different sort order for Type
        if pd_version < (1, 0, 0):
            dfmax = dfgrp.max().drop('Type', axis=1).drop('Model', axis=1)
            max = grp.max().drop('Type', axis=1).drop('Model', axis=1)
            self.assertEqual(dfmax[:30].to_csv(), max[:30].to_csv())

        # Column max
        self.assertEqual(dfgrp['Origin'].max().tolist()[:40],
                         grp['Origin'].max().tolist()[:40])
        self.assertEqual(dfgrp['Horsepower'].max().tolist()[:40],
                         grp['Horsepower'].max().tolist()[:40])

    def test_mean(self):
        # if self.server_type == 'windows.smp':
        #     tm.TestCase.skipTest(self, 'Skip on WX6 until defect S1240339 fixed')

        mean = self.table.mean()
        dfmean = self.get_cars_df().mean()

        self.assertAlmostEqual(mean.loc['MSRP'], dfmean.loc['MSRP'], 4)
        self.assertAlmostEqual(mean.loc['Invoice'], dfmean.loc['Invoice'], 4)
        self.assertAlmostEqual(mean.loc['EngineSize'], dfmean.loc['EngineSize'], 4)
        self.assertAlmostEqual(mean.loc['Cylinders'], dfmean.loc['Cylinders'], 4)
        self.assertAlmostEqual(mean.loc['Horsepower'], dfmean.loc['Horsepower'], 4)
        self.assertAlmostEqual(mean.loc['MPG_City'], dfmean.loc['MPG_City'], 4)
        self.assertAlmostEqual(mean.loc['MPG_Highway'], dfmean.loc['MPG_Highway'], 4)
        self.assertAlmostEqual(mean.loc['Weight'], dfmean.loc['Weight'], 4)
        self.assertAlmostEqual(mean.loc['Wheelbase'], dfmean.loc['Wheelbase'], 4)
        self.assertAlmostEqual(mean.loc['Length'], dfmean.loc['Length'], 4)

        mean = self.table.mean(numeric_only=True)
        dfmean = self.get_cars_df().mean(numeric_only=True)

        self.assertAlmostEqual(mean.loc['MSRP'], dfmean.loc['MSRP'], 4)
        self.assertAlmostEqual(mean.loc['Invoice'], dfmean.loc['Invoice'], 4)
        self.assertAlmostEqual(mean.loc['EngineSize'], dfmean.loc['EngineSize'], 4)
        self.assertAlmostEqual(mean.loc['Cylinders'], dfmean.loc['Cylinders'], 4)
        self.assertAlmostEqual(mean.loc['Horsepower'], dfmean.loc['Horsepower'], 4)
        self.assertAlmostEqual(mean.loc['MPG_City'], dfmean.loc['MPG_City'], 4)
        self.assertAlmostEqual(mean.loc['MPG_Highway'], dfmean.loc['MPG_Highway'], 4)
        self.assertAlmostEqual(mean.loc['Weight'], dfmean.loc['Weight'], 4)
        self.assertAlmostEqual(mean.loc['Wheelbase'], dfmean.loc['Wheelbase'], 4)
        self.assertAlmostEqual(mean.loc['Length'], dfmean.loc['Length'], 4)

        # Only character columns
        with self.assertRaises(swat.SWATError):
            self.table.datastep('keep Make Model Type Origin').mean().tolist()

    @unittest.skipIf(pd_version < (0, 18, 0), 'Need newer version of Pandas')
    def test_skew(self):
        try:
            skew = self.table.skew()
        except KeyError:
            return unittest.skip('CAS server does not support skew')
        dfskew = self.get_cars_df().skew()
        self.assertTablesEqual(skew, dfskew, precision=4)

        skew = self.table.skew(numeric_only=True)
        dfskew = self.get_cars_df().skew(numeric_only=True)
        self.assertTablesEqual(skew, dfskew, precision=4)

        # Only character columns
        with self.assertRaises(swat.SWATError):
            self.table.datastep('keep Make Model Type Origin').skew().tolist()

        skew = self.table.groupby('Origin').skew()
        dfskew = self.get_cars_df().groupby('Origin').skew()
        # Pandas messes up the column order
        dfskew = dfskew[[x for x in self.table.columns if x in dfskew.columns]]
        self.assertTablesEqual(skew, dfskew, precision=4)

    @unittest.skipIf(pd_version < (0, 17, 0), 'Need newer version of Pandas')
    def test_kurt(self):
        try:
            kurt = self.table.kurt()
        except KeyError:
            return unittest.skip('CAS server does not support kurtosis')
        dfkurt = self.get_cars_df().kurt()
        self.assertTablesEqual(kurt, dfkurt, precision=4)

        kurt = self.table.kurt(numeric_only=True)
        dfkurt = self.get_cars_df().kurt(numeric_only=True)
        self.assertTablesEqual(kurt, dfkurt, precision=4)

        # Only character columns
        with self.assertRaises(swat.SWATError):
            self.table.datastep('keep Make Model Type Origin').kurt().tolist()

        # Not supported by Pandas
        # kurt = self.table.groupby('Origin').kurt()
        # dfkurt = self.get_cars_df().groupby('Origin').kurt()
        # Pandas messes up the column order
        # dfkurt = dfkurt[[x for x in self.table.columns if x in dfskew.columns]]
        # self.assertTablesEqual(kurt, dfkurt, precision=4)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_min(self):
        # if self.server_type == 'windows.smp':
        #     tm.TestCase.skipTest(self, 'Skip on WX6 until defect S1240339 fixed')

        df = self.get_cars_df()
        tbl = self.table

        out = tbl.min().tolist()
        dfout = df.min().tolist()

        # TODO: The DataFrame result looks wrong.  It converts all values to
        #       strings and they get truncated.
        # self.assertEqual(out, dfout)

        self.assertEqual(out, ['Acura', ' 3.5 RL 4dr', 'Hybrid', 'Asia', 'All',
                               10280.0, 9875.0, 1.3, 3.0, 73.0, 10.0, 12.0,
                               1850.0, 89.0, 143.0])

        # Numeric only
        out = tbl.min(numeric_only=True).tolist()
        dfout = df.min(numeric_only=True).tolist()

        self.assertEqual(out, dfout)

        # Only character columns
        # chardf = df[['Make', 'Model', 'Type', 'Origin']]
        out = tbl.datastep('keep Make Model Type Origin').min().tolist()
        self.assertEqual(out, ['Acura', ' 3.5 RL 4dr', 'Hybrid', 'Asia'])

        # Column min
        self.assertEqual(df['Make'].min(), tbl['Make'].min())
        self.assertEqual(df['MSRP'].min(), tbl['MSRP'].min())

        # Groupby variables

        dfgrp = df.groupby(['Make', 'Cylinders'])
        grp = tbl.groupby(['Make', 'Cylinders'])

        # Pandas uses a different sort order for Type
        if pd_version < (1, 0, 0):
            dfmin = dfgrp.min().drop('Type', axis=1)
            min = grp.min().drop('Type', axis=1)
            self.assertEqual(dfmin[:30].to_csv(), min[:30].to_csv())

        # Column min
        self.assertEqual(dfgrp['Model'].min().tolist()[:40],
                         grp['Model'].min().tolist()[:40])
        self.assertEqual(dfgrp['Horsepower'].min().tolist()[:40],
                         grp['Horsepower'].min().tolist()[:40])

    def test_mode(self):
        df = self.get_cars_df()
        tbl = self.table

        dfmode = df.mode()
        tblmode = tbl.mode()

        self.assertTablesEqual(dfmode, tblmode)

        # Numeric only
        dfmode = df.mode(numeric_only=True)
        tblmode = tbl.mode(numeric_only=True)
        self.assertTablesEqual(dfmode, tblmode)

        # Character only
        dfmode = df[['Make', 'Model', 'Type', 'Origin']].mode()
        tblmode = tbl[['Make', 'Model', 'Type', 'Origin']].mode()
        self.assertTablesEqual(dfmode, tblmode)

        # Column mode
        self.assertEqual(df['Make'].mode().tolist(), tbl['Make'].mode().tolist())
        self.assertEqual(df['MSRP'].mode().tolist(), tbl['MSRP'].mode().tolist())

        # Groupby variables

        dfgrp = df.groupby(['Make', 'Cylinders'])
        tblgrp = tbl.groupby(['Make', 'Cylinders'])

        # TODO: Pandas mode sets columns with all unique values to NaN
        self.assertEqual(
            re.sub(r'^,+$', dfgrp.get_group(('Acura', 6.0))
                                 .mode()[['Type', 'Origin',
                                          'EngineSize', 'MPG_City']]
                                 .to_csv(index=False), r'', re.M),
            re.sub(r'^,+$', tblgrp.mode().loc[('Acura', 6.0),
                                              ['Type', 'Origin',
                                               'EngineSize', 'MPG_City']]
                                  .dropna(how='all').to_csv(index=False), r'', re.M))

        dfgrp = df[['Make', 'Type']].groupby(['Make'])
        tblgrp = tbl[['Make', 'Type']].groupby(['Make'])

        # TODO: Pandas mode sets columns with all unique values to NaN
        self.assertEqual(
            dfgrp.get_group('Acura').mode()[['Type']].to_csv(index=False),
            tblgrp.mode().loc['Acura', ['Type']].dropna(how='all').to_csv(index=False))

        dfgrp = df[['Cylinders', 'MPG_City']].groupby(['Cylinders'])
        tblgrp = tbl[['Cylinders', 'MPG_City']].groupby(['Cylinders'])

        # TODO: Pandas mode sets columns with all unique values to NaN
        self.assertEqual(
            dfgrp.get_group(6.0).mode()[['MPG_City']].to_csv(index=False),
            tblgrp.mode().loc[6.0, ['MPG_City']].dropna(how='all').to_csv(index=False))

    def test_median(self):
        df = self.get_cars_df()
        tbl = self.table

        dfmed = df.median()
        tblmed = tbl.median()

        self.assertEqual(dfmed.tolist(), tblmed.tolist())

    @unittest.skipIf(pd_version < (0, 18, 0), 'Need newer version of Pandas')
    @unittest.skipIf(tuple([int(x) for x in np.__version__.split('.')[:2]])
                     < (1, 9), 'Need newer version of numpy')
    def test_quantile(self):
        df = self.get_cars_df()
        tbl = self.table

        # Single quantile
        dfqnt = df.quantile(interpolation='nearest')
        tblqnt = tbl.quantile()

        # NOTE: These differ slightly from Pandas
        dfqnt.drop(['MSRP', 'Invoice', 'Weight', 'Cylinders'], inplace=True)
        tblqnt.drop(['MSRP', 'Invoice', 'Weight', 'Cylinders'], inplace=True)

        self.assertEqual(dfqnt.tolist(), tblqnt.tolist())

        # Multiple quantiles
        dfqnt = df.quantile([0.1, 0.5, 1], interpolation='nearest')
        tblqnt = tbl.quantile([0.1, 0.5, 1])

        # NOTE: These differ slightly from Pandas
        dfqnt.drop(['MSRP', 'Invoice', 'Weight', 'Cylinders'], axis=1, inplace=True)
        tblqnt.drop(['MSRP', 'Invoice', 'Weight', 'Cylinders'], axis=1, inplace=True)

        self.assertEqual(dfqnt.to_csv(), tblqnt.to_csv())

        # Numeric only
        dfqnt = df.quantile([0.1, 0.5, 1], numeric_only=True, interpolation='nearest')
        tblqnt = tbl.quantile([0.1, 0.5, 1], numeric_only=True)

        # NOTE: These differ slightly from Pandas
        dfqnt.drop(['MSRP', 'Invoice', 'Weight', 'Cylinders'], axis=1, inplace=True)
        tblqnt.drop(['MSRP', 'Invoice', 'Weight', 'Cylinders'], axis=1, inplace=True)

        self.assertEqual(dfqnt.to_csv(), tblqnt.to_csv())

        # Columns
        self.assertEqual(df['MSRP'].quantile(), tbl['MSRP'].quantile())
        self.assertEqual(df['Horsepower'].quantile([0.1, 0.5, 1],
                                                   interpolation='nearest').tolist(),
                         tbl['Horsepower'].quantile([0.1, 0.5, 1]).tolist())

        # Newer versions of pandas have behavior changes that make checking quantiles
        # with groupby extremely difficult to compare.

        # Groupby variables

        # dfgrp = df.groupby(['Make', 'Cylinders'])
        # tblgrp = tbl.groupby(['Make', 'Cylinders'])

        # dfqnt = dfgrp[['EngineSize']].quantile(interpolation='nearest')
        # tblqnt = tblgrp.quantile()[['EngineSize']]

        # self.assertEqual(dfqnt[1:10].to_csv(), tblqnt[1:10].to_csv())

        # dfqnt = dfgrp[['EngineSize']].quantile([0.5, 1], interpolation='nearest')
        # tblqnt = tblgrp.quantile([0.5, 1])[['EngineSize']]

        # self.assertEqual(dfqnt[1:10].to_csv(), tblqnt[1:10].to_csv())

        # Groupby column

        # dfqnt = dfgrp['EngineSize'].quantile(interpolation='nearest')
        # tblqnt = tblgrp['EngineSize'].quantile()

        # self.assertEqual(dfqnt[1:10].tolist(), tblqnt[1:10].tolist())

        # dfqnt = dfgrp['EngineSize'].quantile([0.5, 1], interpolation='nearest')
        # tblqnt = tblgrp['EngineSize'].quantile([0.5, 1])

        # self.assertEqual(dfqnt[1:10].tolist(), tblqnt[1:10].tolist())

    @unittest.skipIf(int(pd.__version__.split('.')[1]) >= 19,
                     'Bug in Pandas 19 returns too many results')
    def test_nlargest(self):
        if not hasattr(pd.DataFrame, 'nlargest'):
            tm.TestCase.skipTest(self, 'DataFrame does not support nlargest')

        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(df.nlargest(5, ['Cylinders', 'MSRP']).to_csv(index=False),
                         tbl.nlargest(5, ['Cylinders', 'MSRP']).to_csv(index=False))

        self.assertEqual(df.nlargest(5, 'Invoice').to_csv(index=False),
                         tbl.nlargest(5, 'Invoice').to_csv(index=False))

    @unittest.skipIf(int(pd.__version__.split('.')[1]) >= 19,
                     'Bug in Pandas 19 returns too many results')
    def test_nsmallest(self):
        if not hasattr(pd.DataFrame, 'nsmallest'):
            tm.TestCase.skipTest(self, 'DataFrame does not support nsmallest')

        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(df.nsmallest(6, ['MPG_City', 'Invoice']).to_csv(index=False),
                         tbl.nsmallest(6, ['MPG_City', 'Invoice']).to_csv(index=False))

        self.assertEqual(df.nsmallest(5, 'Invoice').to_csv(index=False),
                         tbl.nsmallest(5, 'Invoice').to_csv(index=False))

    def test_sum(self):
        summ = self.table.sum()
        dfsumm = self.get_cars_df().sum(numeric_only=True)

        self.assertAlmostEqual(summ.loc['MSRP'], dfsumm.loc['MSRP'], 4)
        self.assertAlmostEqual(summ.loc['Invoice'], dfsumm.loc['Invoice'], 4)
        self.assertAlmostEqual(summ.loc['EngineSize'], dfsumm.loc['EngineSize'], 4)
        self.assertAlmostEqual(summ.loc['Cylinders'], dfsumm.loc['Cylinders'], 4)
        self.assertAlmostEqual(summ.loc['Horsepower'], dfsumm.loc['Horsepower'], 4)
        self.assertAlmostEqual(summ.loc['MPG_City'], dfsumm.loc['MPG_City'], 4)
        self.assertAlmostEqual(summ.loc['MPG_Highway'], dfsumm.loc['MPG_Highway'], 4)
        self.assertAlmostEqual(summ.loc['Weight'], dfsumm.loc['Weight'], 4)
        self.assertAlmostEqual(summ.loc['Wheelbase'], dfsumm.loc['Wheelbase'], 4)
        self.assertAlmostEqual(summ.loc['Length'], dfsumm.loc['Length'], 4)

    def test_std(self):
        std = self.table.std().tolist()
        dfstd = self.get_cars_df().std(numeric_only=True).tolist()
        for tbl, df in zip(std, dfstd):
            self.assertAlmostEqual(tbl, df, 3)

    def test_var(self):
        var = self.table.var().tolist()
        dfvar = self.get_cars_df().var(numeric_only=True).tolist()
        for tbl, df in zip(var, dfvar):
            self.assertAlmostEqual(tbl, df, 3)

    def test_nmiss(self):
        out = self.table.nmiss()

        self.assertEqual(out.loc['Invoice'], 0)
        self.assertEqual(out.loc['EngineSize'], 0)
        self.assertEqual(out.loc['Cylinders'], 2)
        self.assertEqual(out.loc['Horsepower'], 0)
        self.assertEqual(out.loc['MPG_City'], 0)
        self.assertEqual(out.loc['MPG_Highway'], 0)
        self.assertEqual(out.loc['Weight'], 0)
        self.assertEqual(out.loc['Wheelbase'], 0)
        self.assertEqual(out.loc['Length'], 0)

        self.assertEqual(out.dtype, np.int64)

    def test_stderr(self):
        out = self.table.stderr()

        self.assertAlmostEqual(out.loc['MSRP'], 939.267478, 4)
        self.assertAlmostEqual(out.loc['Invoice'], 852.763949, 4)
        self.assertAlmostEqual(out.loc['EngineSize'], 0.053586, 4)
        self.assertAlmostEqual(out.loc['Cylinders'], 0.075507, 4)
        self.assertAlmostEqual(out.loc['Horsepower'], 3.472326, 4)
        self.assertAlmostEqual(out.loc['MPG_City'], 0.253199, 4)
        self.assertAlmostEqual(out.loc['MPG_Highway'], 0.277511, 4)
        self.assertAlmostEqual(out.loc['Weight'], 36.686838, 4)
        self.assertAlmostEqual(out.loc['Wheelbase'], 0.401767, 4)
        self.assertAlmostEqual(out.loc['Length'], 0.694020, 4)

    def test_uss(self):
        out = self.table.uss()

        self.assertEqual(out.loc['MSRP'], 620985422112.0)
        self.assertEqual(out.loc['Invoice'], 518478936590.0)
        self.assertAlmostEqual(out.loc['EngineSize'], 4898.54, 1)
        self.assertEqual(out.loc['Cylinders'], 15400.0)
        self.assertEqual(out.loc['Horsepower'], 22151103.0)
        self.assertEqual(out.loc['MPG_City'], 183958.0)
        self.assertEqual(out.loc['MPG_Highway'], 322479.0)
        self.assertEqual(out.loc['Weight'], 5725124540.0)
        self.assertEqual(out.loc['Wheelbase'], 5035958.0)
        self.assertEqual(out.loc['Length'], 14952831.0)

    def test_css(self):
        out = self.table.css()

        self.assertAlmostEqual(out.loc['MSRP'], 161231618703.0, 0)
        self.assertAlmostEqual(out.loc['Invoice'], 132901324092.0, 0)
        self.assertAlmostEqual(out.loc['EngineSize'], 524.775420561, 4)
        self.assertAlmostEqual(out.loc['Cylinders'], 1032.21596244, 4)
        self.assertAlmostEqual(out.loc['Horsepower'], 2203497.39019, 4)
        self.assertAlmostEqual(out.loc['MPG_City'], 11716.4205607, 4)
        self.assertAlmostEqual(out.loc['MPG_Highway'], 14074.5116822, 4)
        self.assertAlmostEqual(out.loc['Weight'], 245975707.065, 3)
        self.assertAlmostEqual(out.loc['Wheelbase'], 29499.8224299, 4)
        self.assertAlmostEqual(out.loc['Length'], 88026.8668224, 4)

    def test_cv(self):
        out = self.table.cv()

        self.assertAlmostEqual(out.loc['MSRP'], 59.2884898823, 4)
        self.assertAlmostEqual(out.loc['Invoice'], 58.7782559912, 4)
        self.assertAlmostEqual(out.loc['EngineSize'], 34.6790337271, 4)
        self.assertAlmostEqual(out.loc['Cylinders'], 26.8349459075, 4)
        self.assertAlmostEqual(out.loc['Horsepower'], 33.275058732, 4)
        self.assertAlmostEqual(out.loc['MPG_City'], 26.1117767219, 4)
        self.assertAlmostEqual(out.loc['MPG_Highway'], 21.3877091729, 4)
        self.assertAlmostEqual(out.loc['Weight'], 21.2127760515, 4)
        self.assertAlmostEqual(out.loc['Wheelbase'], 7.68515005441, 4)
        self.assertAlmostEqual(out.loc['Length'], 7.70434945771, 4)

    def test_tvalue(self):
        out = self.table.tvalue()

        self.assertAlmostEqual(out.loc['MSRP'], 34.8940593809, 4)
        self.assertAlmostEqual(out.loc['Invoice'], 35.1969627487, 4)
        self.assertAlmostEqual(out.loc['EngineSize'], 59.6561052663, 4)
        self.assertAlmostEqual(out.loc['Cylinders'], 76.9137657728, 4)
        self.assertAlmostEqual(out.loc['Horsepower'], 62.1731761082, 4)
        self.assertAlmostEqual(out.loc['MPG_City'], 79.2292347086, 4)
        self.assertAlmostEqual(out.loc['MPG_Highway'], 96.7292041348, 4)
        self.assertAlmostEqual(out.loc['Weight'], 97.5268904709, 4)
        self.assertAlmostEqual(out.loc['Wheelbase'], 269.196576763, 4)
        self.assertAlmostEqual(out.loc['Length'], 268.525733148, 4)

    def test_probt(self):
        out = self.table.probt()

        self.assertAlmostEqual(out.loc['MSRP'], 4.16041192748e-127, 131)
        self.assertAlmostEqual(out.loc['Invoice'], 2.68439770392e-128, 132)
        self.assertAlmostEqual(out.loc['EngineSize'], 3.13374452967e-209, 213)
        self.assertAlmostEqual(out.loc['Cylinders'], 1.51556887239e-251, 255)
        self.assertAlmostEqual(out.loc['Horsepower'], 4.18534404473e-216, 220)
        self.assertAlmostEqual(out.loc['MPG_City'], 1.86628363939e-257, 261)
        self.assertAlmostEqual(out.loc['MPG_Highway'], 1.66562083762e-292, 296)
        self.assertAlmostEqual(out.loc['Weight'], 5.81254663578e-294, 298)
        self.assertAlmostEqual(out.loc['Wheelbase'], 0, 0)
        self.assertAlmostEqual(out.loc['Length'], 0, 0)

    def test_datastep(self):
        # if self.server_type == 'windows.smp':
        #     tm.TestCase.skipTest(self, 'Skip on WX6 until defect S1240339 fixed')

        out = self.table.datastep('MakeModel = trim(Make) || trim(Model)')

        self.assertEqual(out.columns[-1], 'MakeModel')
        self.assertEqual(out.head()['MakeModel'].tolist(),
                         ['Acura MDX', 'Acura RSX Type S 2dr', 'Acura TSX 4dr',
                          'Acura TL 4dr', 'Acura 3.5 RL 4dr'])

        # casout=
        out = self.table.datastep('MakeModel = trim(Make) || trim(Model)',
                                  casout='newcars')

        self.assertEqual(out.get_param('name'), 'newcars')

        # casout= + caslib=
        out = self.table.datastep('MakeModel = trim(Make) || trim(Model)',
                                  casout=dict(name='newcars2', caslib=self.srcLib))

        self.assertEqual(out.get_param('name'), 'newcars2')

    def test_getattr(self):
        make = self.table.Make
        self.assertEqual(make.__class__.__name__, 'CASColumn')
        self.assertEqual(make.name, 'Make')
        self.assertEqual(make.head(1)[0], 'Acura')

        # Non-existent
        with self.assertRaises(AttributeError):
            self.table.Makes

        # Computed columns
        self.table['MakeModel'] = self.table.Make + self.table.Model
        makemodel = self.table.MakeModel
        self.assertEqual(makemodel.__class__.__name__, 'CASColumn')
        self.assertEqual(makemodel.name, 'MakeModel')
        self.assertEqual(makemodel.head(1)[0], 'Acura MDX')

        # Columns in vars
        newtbl = self.table.select_dtypes('numeric')
        msrp = newtbl.MSRP
        self.assertEqual(msrp.__class__.__name__, 'CASColumn')
        self.assertEqual(msrp.name, 'MSRP')
        self.assertEqual(msrp.head(1)[0], 36945)

    def test__loadactionset(self):
        if 'fedSql' in self.s.actionsetinfo().setinfo['actionset'].tolist():
            tm.TestCase.skipTest(self, 'fedsql is already loaded')

        with self.assertRaises(AttributeError):
            self.table.fedsql

        self.table._loadactionset('fedsql')

        self.assertTrue(self.table.fedsql is not None)

    def test__numcolumns(self):
        self.table.params.computedvars = 'MakeModel'
        self.table.params.computedvarsprogram = 'MakeModel = trim(Make) || trim(Model)'

        self.assertEqual(self.table._numcolumns, 16)

    def test__summary(self):
        summ = self.table._summary()
        if len(summ.index) == 14:
            self.assertEqual(list(summ.index),
                             ['min', 'max', 'count', 'nmiss', 'mean', 'sum', 'std',
                              'stderr', 'var', 'uss', 'css', 'cv', 'tvalue', 'probt'])
        else:
            self.assertEqual(list(summ.index),
                             ['min', 'max', 'count', 'nmiss', 'mean', 'sum', 'std',
                              'stderr', 'var', 'uss', 'css', 'cv', 'tvalue', 'probt',
                              'skewness', 'kurtosis'])
        self.assertEqual(len(summ.columns), 10)

    def test_disconnected(self):
        self.s.close()

        self.table.to_table()
        with self.assertRaises(swat.SWATError):
            self.table.head()

        self.table.to_outtable()
        with self.assertRaises(swat.SWATError):
            self.table.head()

    def test_to_params2(self):
        self.table.params.singlepass = True
        self.table.params.replace = True

        self.assertEqual(set(self.table.to_table_params().keys()),
                         {'name', 'caslib', 'singlepass'})
        self.assertEqual(set(self.table.to_outtable_params().keys()),
                         {'name', 'caslib', 'replace'})

    def test_intersect_columns(self):
        self.assertEqual(self.table.get_param('vars', []), [])

        out = self.table._intersect_columns(['Make', 'Model', 'MSRP'])
        self.assertEqual(self.table.get_param('vars', []), [])
        self.assertEqual(out, ['Make', 'Model', 'MSRP'])

        self.table._intersect_columns(['Make', 'Model', 'MSRP'], inplace=True)
        self.assertEqual(self.table.get_inputs_param(), ['Make', 'Model', 'MSRP'])

        out = self.table._intersect_columns(['Horsepower'])
        self.assertEqual(out, [])
        self.assertEqual(self.table.get_inputs_param(), ['Make', 'Model', 'MSRP'])

        out = self.table._intersect_columns(['Model'])
        self.assertEqual(out, ['Model'])
        self.assertEqual(self.table.get_inputs_param(), ['Make', 'Model', 'MSRP'])

        out = self.table._intersect_columns(['MSRP', 'Model'])
        self.assertEqual(out, ['Model', 'MSRP'])
        self.assertEqual(self.table.get_inputs_param(), ['Make', 'Model', 'MSRP'])

        self.table._intersect_columns(['MSRP', 'Model'], inplace=True)
        self.assertEqual(self.table.get_inputs_param(), ['Model', 'MSRP'])

        # No columns passed in
        out = self.table._intersect_columns([])
        out = ['Model', 'MSRP']

        self.table._intersect_columns([], inplace=True)
        self.assertEqual(self.table.get_inputs_param(), ['Model', 'MSRP'])

    def test_to_view(self):
        self.assertEqual(self.table.columns.tolist(),
                         ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                          'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                          'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                          'Wheelbase', 'Length'])

        self.table.params.vars = ['Make', 'Model']

        out = self.table.to_view()

        self.assertEqual(self.table.columns.tolist(), ['Make', 'Model'])
        self.assertEqual(self.table.columninfo()['ColumnInfo'].to_dict(),
                         out.columninfo()['ColumnInfo'].to_dict())

        # Bad parameter
        with self.assertRaises(swat.SWATError):
            out = self.table.to_view(foo='bar')

    def test_iat(self):
        df = self.get_cars_df().sort_values(['Make', 'MSRP'])
        df.index = range(len(df))
        tbl = self.table.sort_values(['Make', 'MSRP'])

        with self.assertRaises(NotImplementedError):
            tbl.iat[0, 0]

        # self.assertEqual(df.iat[10, 5], tbl.iat[10, 5])
        # self.assertEqual(df.iat[99, 0], tbl.iat[99, 0])
        # self.assertEqual(df.iat[-5, 0], tbl.iat[-5, 0])
        # self.assertEqual(df.iat[-99, 0], tbl.iat[-99, 0])
        # self.assertEqual(df.iat[-99, -2], tbl.iat[-99, -2])

        # with self.assertRaises(IndexError):
        #     tbl.iat[500, 0]

        # with self.assertRaises(IndexError):
        #     tbl.iat[-500, 0]

#   def test_column_iat(self):
#       df = self.get_cars_df().sort_values(['Make', 'MSRP'])
#       df.index = range(len(df))
#       tbl = self.table.sort_values(['Make', 'MSRP'])
#
#       self.assertEqual(df['Model'].iat[10], tbl['Model'].iat[10])
#       self.assertEqual(df['Model'].iat[99], tbl['Model'].iat[99])
#       self.assertEqual(df['Model'].iat[-5], tbl['Model'].iat[-5])
#       self.assertEqual(df['Model'].iat[-99], tbl['Model'].iat[-99])
#
#       with self.assertRaises(IndexError):
#           tbl['Model'].iat[500]
#
#       with self.assertRaises(IndexError):
#           tbl['Model'].iat[-500]

    def test_at(self):
        df = self.get_cars_df().sort_values(['Make', 'MSRP'])
        df.index = range(len(df))
        tbl = self.table.sort_values(['Make', 'MSRP'])

        with self.assertRaises(NotImplementedError):
            tbl.at[0, 0]

        # self.assertEqual(df.iat[10, 5], tbl.at[10, 5])
        # self.assertEqual(df.iat[99, 0], tbl.at[99, 0])

        # with self.assertRaises(KeyError):
        #     tbl.at[-5, 0]

        # with self.assertRaises(KeyError):
        #     tbl.at[500, 0]

        # with self.assertRaises(KeyError):
        #     tbl.at[-500, 0]

    def test_column_at(self):
        df = self.get_cars_df().sort_values(['Make', 'MSRP'])
        df.index = range(len(df))
        tbl = self.table.sort_values(['Make', 'MSRP'])

        with self.assertRaises(NotImplementedError):
            tbl['Model'].iat[0]

        # self.assertEqual(df['Model'].iat[10], tbl['Model'].at[10])
        # self.assertEqual(df['Model'].iat[99], tbl['Model'].at[99])

        # with self.assertRaises(KeyError):
        #     tbl['Model'].at[-5]

        # with self.assertRaises(KeyError):
        #     tbl['Model'].at[500]

        # with self.assertRaises(KeyError):
        #     tbl['Model'].at[-500]

    def test_loc(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        def sortAssertEqual(a, b):
            self.assertEqual(sorted(a.tolist()), sorted(b.tolist()))

        # Row labels
#       self.assertEqual(df.loc[0].tolist(), tbl.loc[0].tolist())
#       self.assertEqual(df.loc[5].tolist(), tbl.loc[5].tolist())
#       self.assertEqual(df.loc[149].tolist(), tbl.loc[149].tolist())

        # Slicing that returns a CASTable is not allowed
#       with self.assertRaises(TypeError):
#           tbl.loc[0:1]

        with self.assertRaises(IndexError):
            tbl.loc[10:100, 'MSRP']

        with self.assertRaises(IndexError):
            tbl.loc[10, 'MSRP']

        # Row labels with single column name
        sortAssertEqual(df.loc[:, 'Make'],
                        tbl.loc[:, 'Make'])
        sortAssertEqual(df.loc[:, 'MSRP'],
                        tbl.loc[:, 'MSRP'])

        # Row labels with columns
        self.assertEqual(sorted(df.loc[:, ['Make']].itertuples(index=False)),
                         sorted(tbl.loc[:, ['Make']].itertuples(index=False)))
        self.assertEqual(list(df.loc[:, ['Make']].columns),
                         list(tbl.loc[:, ['Make']].columns))
        self.assertEqual(len(df.loc[:, ['Make']]), len(tbl.loc[:, ['Make']]))
        self.assertEqual(sorted(df.loc[:, ['Make', 'MSRP']].itertuples(index=False)),
                         sorted(tbl.loc[:, ['Make', 'MSRP']].itertuples(index=False)))
        self.assertEqual(list(df.loc[:, ['Make', 'MSRP']].columns),
                         list(tbl.loc[:, ['Make', 'MSRP']].columns))

        # Non-existent row
        # with self.assertRaises(KeyError):
        #     tbl.loc[500, ['Make', 'MSRP']]

        # Non-existent column
        # dfout = df.loc[:, ['Foo', 'MSRP']].values
        # tblout = tbl.loc[:, ['Foo', 'MSRP']].values
        # self.assertTrue(np.isnan(dfout[0, 0]) and np.isnan(tblout[0, 0]))
        # self.assertEqual(dfout[0, 1], tblout[0, 1])

        # Column slices
        self.assertTablesEqual(df.loc[:, 'Make':'MSRP'],
                               tbl.loc[:, 'Make':'MSRP'], sortby=None)

        # Use str here because np.nan won't compare equal to each other
        self.assertTablesEqual(df.loc[:, 'Make':], tbl.loc[:, 'Make':], sortby=None)
        self.assertTablesEqual(df.loc[:, :'MSRP'], tbl.loc[:, :'MSRP'], sortby=None)
        self.assertTablesEqual(df.loc[:, :], tbl.loc[:, :], sortby=None)

        # Indexes should not work for columns
        # with self.assertRaises((KeyError, TypeError)):
        #     tbl.loc[0, 5]

    # TODO: I don't know why this test doesn't work yet
    def test_column_loc(self):
        df = self.get_cars_df().sort_values(['Make', 'Model'])
        df.index = range(len(df))
        tbl = self.table.sort_values(['Make', 'Model'])

        with self.assertRaises(NotImplementedError):
            tbl['Model'].loc[0]

        # df = df['Model']
        # tbl = tbl['Model']

        # Row labels
        # self.assertEqual(df.loc[0], tbl.loc[0])

        # Slicing that returns a CASTable is not allowed
        # with self.assertRaises(TypeError):
        #     tbl.loc[0:1]

        # Non-existent row
        # with self.assertRaises(KeyError):
        #     tbl.loc[500]

        # with self.assertRaises(KeyError):
        #     tbl.loc[-2]

        # with self.assertRaises(TypeError):
        #     tbl.loc[[0, 2]]

    def test_iloc(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        with self.assertRaises(IndexError):
            tbl.iloc[0]

        with self.assertRaises(IndexError):
            tbl.iloc[0, 0]

        # Row indexes with single column name
        self.assertColsEqual(df.iloc[:, 0], tbl.iloc[:, 0])
        self.assertColsEqual(df.iloc[:, 3], tbl.iloc[:, 3])

        # Row indexes with columns
        self.assertTablesEqual(df.iloc[:, [0]],
                               tbl.iloc[:, [0]], sortby=None)
        self.assertTablesEqual(df.iloc[:, [0, 3]],
                               tbl.iloc[:, [0, 3]], sortby=None)
        self.assertTablesEqual(df.iloc[:, [-2]],
                               tbl.iloc[:, [-2]], sortby=None)
        self.assertTablesEqual(df.iloc[:, [2, -3]],
                               tbl.iloc[:, [2, -3]], sortby=None)
        self.assertTablesEqual(df.iloc[:, [2, -3, 4]],
                               tbl.iloc[:, [2, -3, 4]], sortby=None)

        # Row indexes
        # self.assertEqual(df.iloc[0].tolist(), tbl.iloc[0].tolist())
        # self.assertEqual(df.iloc[5].tolist(), tbl.iloc[5].tolist())
        # self.assertEqual(df.iloc[149].tolist(), tbl.iloc[149].tolist())
        # self.assertEqual(df.iloc[-1].tolist(), tbl.iloc[-1].tolist())
        # self.assertEqual(df.iloc[-423].tolist(), tbl.iloc[-423].tolist())
        # self.assertEqual(df.iloc[5].tolist(), tbl.iloc[-423].tolist())
        # self.assertEqual(df.iloc[-423].tolist(), tbl.iloc[5].tolist())

        # Slicing that returns a CASTable is not allowed
        # with self.assertRaises(TypeError):
        #     tbl.iloc[0:1]

        # Row indexes with single column name
        # self.assertEqual(df.iloc[0, 0], tbl.iloc[0, 0])
        # self.assertEqual(df.iloc[5, 3], tbl.iloc[5, 3])

        # Row indexes with columns
        # self.assertEqual(df.iloc[0, [0]].tolist(),
        #                  tbl.iloc[0, [0]].tolist())
        # self.assertEqual(df.iloc[0, [0, 3]].tolist(),
        #                  tbl.iloc[0, [0, 3]].tolist())
        # self.assertEqual(df.iloc[149, [0, 3]].tolist(),
        #                  tbl.iloc[149, [0, 3]].tolist())
        # self.assertEqual(df.iloc[0, [-2]].tolist(),
        #                  tbl.iloc[0, [-2]].tolist())
        # self.assertEqual(df.iloc[0, [2, -3]].tolist(),
        #                  tbl.iloc[0, [2, -3]].tolist())
        # self.assertEqual(df.iloc[149, [2, -3, 4]].tolist(),
        #                  tbl.iloc[149, [2, -3, 4]].tolist())

        # Non-existent row
        # with self.assertRaises(IndexError):
        #     tbl.iloc[500, [0, 3]]

        # Non-existent column
        # with self.assertRaises(IndexError):
        #     tbl.iloc[5, [100, 3]]

        # Column slices
        # self.assertEqual(df.iloc[0, 1:5].tolist(), tbl.iloc[0, 1:5].tolist())
        # self.assertEqual(df.iloc[0, 1:].tolist(), tbl.iloc[0, 1:].tolist())
        # self.assertEqual(df.iloc[0, :5].tolist(), tbl.iloc[0, :5].tolist())
        # self.assertEqual(df.iloc[0, :].tolist(), tbl.iloc[0, :].tolist())
        # self.assertEqual(df.iloc[0, 1:-5].tolist(), tbl.iloc[0, 1:-5].tolist())
        # self.assertEqual(df.iloc[0, 1:].tolist(), tbl.iloc[0, 1:].tolist())
        # self.assertEqual(df.iloc[0, :-5].tolist(), tbl.iloc[0, :-5].tolist())
        # self.assertEqual(df.iloc[0, :].tolist(), tbl.iloc[0, :].tolist())

        # Labels should not work for columns
        # with self.assertRaises(ValueError):
        #     tbl.iloc[0, 'Make']

    def test_column_iloc(self):
        df = self.get_cars_df().sort_values(['Make', 'Model'])
        df.index = range(len(df))
        tbl = self.table.sort_values(['Make', 'Model'])

        with self.assertRaises(NotImplementedError):
            tbl['Model'].iloc[0]

        # df = df['Model']
        # tbl = tbl['Model']

        # Row indexes
        # self.assertEqual(df.iloc[0], tbl.iloc[0])
        # self.assertEqual(df.iloc[5], tbl.iloc[5])
        # self.assertEqual(df.iloc[149], tbl.iloc[149])
        # self.assertEqual(df.iloc[-1], tbl.iloc[-1])
        # self.assertEqual(df.iloc[-423], tbl.iloc[-423])
        # self.assertEqual(df.iloc[5], tbl.iloc[-423])
        # self.assertEqual(df.iloc[-423], tbl.iloc[5])

        # Slicing that returns a CASTable is not allowed
        # with self.assertRaises(TypeError):
        #     tbl.iloc[0:1]

        # Row indexes with single column name
        # with self.assertRaises(TypeError):
        #     tbl.iloc[0, 0]

        # Row indexes with columns
        # with self.assertRaises(TypeError):
        #     tbl.iloc[[0]]

        # Labels should not work for columns
        # with self.assertRaises(TypeError):
        #     tbl.iloc['Make']

    @unittest.skipIf(pd_version >= (1, 0, 0), 'Need newer version of Pandas')
    def test_ix(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=FutureWarning)

            # Row indexes
            # self.assertEqual(df[0].tolist(), tbl.ix[0].tolist())
            # self.assertEqual(df.ix[5].tolist(), tbl.ix[5].tolist())
            # self.assertEqual(df.ix[149].tolist(), tbl.ix[149].tolist())

            # No negative indexing if the index column is numeric
            # with self.assertRaises(KeyError):
            #     tbl.ix[-1]

            # Slicing that returns a CASTable is not allowed
            # with self.assertRaises(TypeError):
            #     tbl.ix[0:1]

            with self.assertRaises(IndexError):
                tbl.ix[0, 0]

            with self.assertRaises(IndexError):
                tbl.ix[:3, 0]

            # Row indexes with single column name
            self.assertColsEqual(df.ix[:, 0], tbl.ix[:, 0])
            self.assertColsEqual(df.ix[:, 3], tbl.ix[:, 3])

            # Row indexes with columns
            self.assertTablesEqual(df.ix[:, [0]],
                                   tbl.ix[:, [0]], sortby=None)
            self.assertEqual(len(df.ix[:, [0]]),
                             len(tbl.ix[:, [0]]))
            self.assertTablesEqual(df.ix[:, [0, 3]],
                                   tbl.ix[:, [0, 3]], sortby=None)
            self.assertTablesEqual(df.ix[:, [-2]],
                                   tbl.ix[:, [-2]], sortby=None)
            self.assertTablesEqual(df.ix[:, [2, -3]],
                                   tbl.ix[:, [2, -3]], sortby=None)
            self.assertTablesEqual(df.ix[:, [2, -3, 4]],
                                   tbl.ix[:, [2, -3, 4]], sortby=None)

            # Non-existent row
            # with self.assertRaises(KeyError):
            #     tbl.ix[500, [0, 3]]

            # Non-existent column
            # with self.assertRaises(IndexError):
            #     tbl.ix[5, [100, 3]]

            # Column slices - use strings because nan won't compare equal
            self.assertTablesEqual(df.ix[:, 1:5], tbl.ix[:, 1:5], sortby=None)
            self.assertTablesEqual(df.ix[:, 1:], tbl.ix[:, 1:], sortby=None)
            self.assertTablesEqual(df.ix[:, :5], tbl.ix[:, :5], sortby=None)
            self.assertTablesEqual(df.ix[:, :], tbl.ix[:, :], sortby=None)
            self.assertTablesEqual(df.ix[:, 1:-5], tbl.ix[:, 1:-5], sortby=None)
            self.assertTablesEqual(df.ix[:, 1:], tbl.ix[:, 1:], sortby=None)
            self.assertTablesEqual(df.ix[:, :-5], tbl.ix[:, :-5], sortby=None)
            self.assertTablesEqual(df.ix[:, :], tbl.ix[:, :], sortby=None)

            # Row labels with single column name
            self.assertColsEqual(df.ix[:, 'Make'], tbl.ix[:, 'Make'])
            self.assertColsEqual(df.ix[:, 'MSRP'], tbl.ix[:, 'MSRP'])

            # Row labels with columns
            self.assertTablesEqual(df.ix[:, ['Make']],
                                   tbl.ix[:, ['Make']], sortby=None)
            self.assertTablesEqual(df.ix[:, ['Make', 'MSRP']],
                                   tbl.ix[:, ['Make', 'MSRP']], sortby=None)

            # Non-existent row
            # with self.assertRaises(KeyError):
            #     tbl.ix[500, ['Make', 'MSRP']]

            # Non-existent column
            try:
                dfout = df.ix[:, ['Foo', 'MSRP']].values
                tblout = tbl.ix[:, ['Foo', 'MSRP']].values
                self.assertTrue(np.isnan(dfout[0, 0]) and np.isnan(tblout[0, 0]))
                self.assertEqual(dfout[0, 1], tblout[0, 1])
            except KeyError:
                # Newer versions of pandas raise a KeyError.
                # If that happens, skip this test.
                pass

            # Column slices
            self.assertTablesEqual(df.ix[:, 'Make':'MSRP'],
                                   tbl.ix[:, 'Make':'MSRP'], sortby=None)
            self.assertTablesEqual(df.ix[:, 'Make':],
                                   tbl.ix[:, 'Make':], sortby=None)
            self.assertTablesEqual(df.ix[:, :'MSRP'],
                                   tbl.ix[:, :'MSRP'], sortby=None)
            self.assertTablesEqual(df.ix[:, :],
                                   tbl.ix[:, :], sortby=None)

    @unittest.skipIf(pd_version >= (1, 0, 0), 'Need newer version of Pandas')
    def test_column_ix(self):
        df = self.get_cars_df().sort_values(['Make', 'Model'])
        df.index = range(len(df))
        tbl = self.table.sort_values(['Make', 'Model'])

        with self.assertRaises(NotImplementedError):
            tbl['Model'].ix[0]

        # df = df['Model']
        # tbl = tbl['Model']

        # Row indexes
        # self.assertEqual(df.ix[0], tbl.ix[0])
        # self.assertEqual(df.ix[5], tbl.ix[5])
        # self.assertEqual(df.ix[149], tbl.ix[149])

        # No negative indexing if the index column is numeric
        # with self.assertRaises(KeyError):
        #     tbl.ix[-1]

        # Slicing that returns a CASTable is not allowed
        # with self.assertRaises(TypeError):
        #     tbl.ix[0:1]

        # Row indexes with columns
        # with self.assertRaises(TypeError):
        #     tbl.ix[[0]]

        # Non-existent row
        # with self.assertRaises(KeyError):
        #     tbl.ix[500]

        # Column slices
        # with self.assertRaises(TypeError):
        #     tbl.ix[1:5]

        # Row labels with single column name
        # with self.assertRaises(TypeError):
        #     tbl.ix['Make']

    def test_xs(self):
        df = self.get_cars_df()
        tbl = self.table

        # self.assertEqual(df.xs(5).tolist(), tbl.xs(5).tolist())
        self.assertEqual(sorted(df.xs('Model', axis=1).tolist()),
                         sorted(tbl.xs('Model', axis=1).tolist()))

        with self.assertRaises(IndexError):
            tbl.xs(0, axis=0)

        with self.assertRaises(swat.SWATError):
            tbl.xs(0, axis=2)

    def test_column_xs(self):
        tbl = self.table
        tbl = tbl['Model']

        with self.assertRaises(AttributeError):
            tbl.xs(5)

    def test_getitem(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        # Column names
        self.assertColsEqual(df['MSRP'], tbl['MSRP'])
        self.assertTablesEqual(df[['Make', 'MSRP']], tbl[['Make', 'MSRP']], sortby=None)

        # Column indexes
        if pd_version < (0, 20, 0):
            self.assertTablesEqual(df[[2, 5, 6]], tbl[[2, 5, 6]], sortby=None)

        # Column index
        with self.assertRaises(KeyError):
            tbl[5]

        # Row slice
        with self.assertRaises(IndexError):
            tbl[2:7]

    def test_sas_methods(self):
        tbl = self.table.sort_values(SORT_KEYS)

        self.assertEqual(tbl['MSRP'].sas.abs().head(1)[0], 10280)
        self.assertEqual(tbl['MSRP'].sas.airy().head(1)[0], 0)
        # self.assertEqual(tbl['MSRP'].sas.beta().head(1)[0], 10280)
        # self.assertEqual(tbl['MSRP'].sas.cnoct().head(1)[0], 10280)

        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('e').head(1)[0], 2.7182818284590451)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('euler').head(1)[0], 0.57721566490153287)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('pi').head(1)[0], 3.1415926535897931)
        self.assertEqual(
            tbl['MSRP'].sas.constant('exactint').head(1)[0], 9007199254740992)
        self.assertTrue(
            tbl['MSRP'].sas.constant('big').head(1)[0] > 1.7976931348600000e+308)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('logbig').head(1)[0], 709.78271289338397)
        self.assertTrue(
            tbl['MSRP'].sas.constant('sqrtbig').head(1)[0] > 1.3407807929000000e+154)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('small').head(1)[0], 2.2250738585072014e-308)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('logsmall').head(1)[0], -708.40183374461219, 1)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('sqrtsmall').head(1)[0], 1.4916681462400413e-154)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('maceps').head(1)[0], 2.2204460492503131e-16)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('logmaceps').head(1)[0], -36.043653389117154)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.constant('sqrtmaceps').head(1)[0], 1.4901161193847656e-08)

        self.assertAlmostEqual(tbl['MSRP'].sas.dairy().head(1)[0], -0.0)
        # self.assertAlmostEqual(tbl['MSRP'].sas.deviance().head(1)[0], -0.0)
        self.assertAlmostEqual(tbl['MSRP'].sas.digamma().head(1)[0], 9.237906900088305)
        self.assertAlmostEqual(tbl['MSRP'].sas.erf().head(1)[0], 1.0)
        # self.assertAlmostEqual(tbl['MSRP'].sas.erfc().head(1)[0], -0.0)
        # self.assertAlmostEqual(tbl['MSRP'].sas.exp().head(1)[0], -0.0)
        # self.assertAlmostEqual(tbl['MSRP'].sas.fact().head(1)[0], -0.0)
        # self.assertAlmostEqual(tbl['MSRP'].sas.fnoct().head(1)[0], -0.0)
        # self.assertAlmostEqual(tbl['MSRP'].sas.gamma().head(1)[0], -0.0)
        self.assertAlmostEqual(tbl['MSRP'].sas.lgamma().head(1)[0], 84682.482909884173)
        self.assertAlmostEqual(tbl['MSRP'].sas.log().head(1)[0], 9.2379555390091568)
        self.assertAlmostEqual(tbl['MSRP'].sas.log1px().head(1)[0], 9.2380528105427189)
        self.assertAlmostEqual(tbl['MSRP'].sas.log10().head(1)[0], 4.0119931146592567)
        self.assertAlmostEqual(tbl['MSRP'].sas.log2().head(1)[0], 13.327552644081241)
        # self.assertAlmostEqual(tbl['MSRP'].sas.logbeta().head(1)[0], -0.0)
        self.assertAlmostEqual(tbl['MSRP'].sas.mod(100).head(1)[0], 80)
        self.assertAlmostEqual(tbl['MSRP'].sas.modz(100).head(1)[0], 80)
        self.assertAlmostEqual(tbl['MSRP'].sas.sign().head(1)[0], 1)
        self.assertAlmostEqual(tbl['MSRP'].sas.sqrt().head(1)[0], 101.39033484509261)
        # self.assertAlmostEqual(tbl['MSRP'].sas.tnoct().head(1)[0], -0.0)
        self.assertAlmostEqual(
            tbl['MSRP'].sas.trigamma().head(1)[0], 9.7280996080681686e-05)

    @unittest.skipIf(pd_version <= (0, 16, 0), 'Need newer version of Pandas')
    def test_str_methods(self):
        df = self.get_cars_df()
        tbl = self.table

        def sortAssertEqual(a, b, strip=False):
            if strip:
                self.assertEqual(sorted(x.strip() for x in a.tolist()),
                                 sorted(x.strip() for x in b.tolist()))
            else:
                self.assertEqual(sorted(a.tolist()), sorted(b.tolist()))

        sortAssertEqual(df['Model'].str.capitalize().head(),
                        tbl['Model'].str.capitalize().head())
        sortAssertEqual(df['Make'].str.capitalize().head(),
                        tbl['Make'].str.capitalize().head())

        with self.assertRaises(TypeError):
            tbl['MSRP'].str.capitalize()

        # self.assertEqual(df['Model'].str.cat(sep=', ')[:100],
        #                  tbl['Model'].str.cat(sep=', ')[:100])
        # self.assertEqual(df['Make'].str.cat(sep=', ')[:100],
        #                  tbl['Make'].str.cat(sep=', ')[:100])
        # This method is implemented in DataFrames, so it doesn't work with numerics
        # with self.assertRaises(AttributeError):
        #     tbl['MSRP'].str.cat(sep=', ')

        # self.assertEqual(df['Model'].str.center(50), tbl['Model'].str.center(50))
        # self.assertEqual(df['Make'].str.center(50), tbl['Make'].str.center(50))
        # This method is implemented in DataFrames, so it doesn't work with numerics
        # with self.assertRaises(AttributeError):
        #     tbl['MSRP'].str.center(50)

        # Compare to string literal
        sortAssertEqual(df['Model'].str.contains('Quattro', regex=False),
                        tbl['Model'].str.contains('Quattro', regex=False))
        sortAssertEqual(df['Model'].str.contains('quattro', case=True, regex=False),
                        tbl['Model'].str.contains('quattro', case=True, regex=False))
        sortAssertEqual(df['Model'].str.contains('quattro', case=False, regex=False),
                        tbl['Model'].str.contains('quattro', case=False, regex=False))

        # Compare to string column
        # TODO: This causes multiple definitions of Quattro and LowQuattro in comppgm
        tbl['Quattro'] = 'Quattro'
        tbl['LowQuattro'] = 'quattro'
        self.assertEqual(
            len(tbl[tbl['Model'].str.contains(tbl['Quattro'],
                                              regex=False)].head(50)), 11)
        self.assertEqual(
            len(tbl[tbl['Model'].str.contains(tbl['LowQuattro'],
                                              case=True, regex=False)].head(50)), 0)
        self.assertEqual(
            len(tbl[tbl['Model'].str.contains(tbl['LowQuattro'],
                                              case=False, regex=False)].head(50)), 11)

        del tbl['Quattro']
        del tbl['LowQuattro']

        # Compare to regex
        self.assertEqual(
            df[df['Model'].str.contains('Quat+ro|TL',
                                        regex=True)].to_csv(index=False),
            tbl[tbl['Model'].str.contains('Quat+ro|TL',
                                          regex=True)].to_csv(index=False))
        self.assertEqual(
            df[df['Model'].str.contains('quat+ro|tl',
                                        case=True, regex=True)].to_csv(index=False),
            tbl[tbl['Model'].str.contains('quat+ro|tl',
                                          case=True, regex=True)].to_csv(index=False))
        self.assertEqual(
            df[df['Model'].str.contains('quat+ro|tl',
                                        case=False, regex=True)].to_csv(index=False),
            tbl[tbl['Model'].str.contains('quat+ro|tl',
                                          case=False, regex=True)].to_csv(index=False))

        # Compare to regex column
        # TODO: Deleting computed columns needs to delete the appropriate part
        #       of comppgm as well.
        tbl['ReQuattro'] = 'Quat+ro|TL'
        tbl['ReLowQuattro'] = 'quat+ro|tl'
        self.assertEqual(
            len(tbl[tbl['Model'].str.contains(tbl['ReQuattro'],
                                              regex=True)].head(50)), 12)
        self.assertEqual(
            len(tbl[tbl['Model'].str.contains(tbl['ReLowQuattro'],
                                              case=True, regex=True)].head(50)), 3)
        self.assertEqual(
            len(tbl[tbl['Model'].str.contains(tbl['ReLowQuattro'],
                                              case=False, regex=True)].head(50)), 15)

        del tbl['ReQuattro']
        del tbl['ReLowQuattro']

        # Count occurrences of string literal
        sortAssertEqual(df['Model'].str.count('4'),
                        tbl['Model'].str.count('4'))

        # Endswith
        sortAssertEqual(df['Model'].str.endswith('4dr'),
                        tbl['Model'].str.endswith('4dr'))

        # Startswith
        sortAssertEqual(df['Model'].str.endswith('A4'),
                        tbl['Model'].str.endswith('A4'))

        # Find
        sortAssertEqual(df['Model'].str.find('Quattro'),
                        tbl['Model'].str.find('Quattro'))
        sortAssertEqual(df['Model'].str.find('Quattro', 2),
                        tbl['Model'].str.find('Quattro', 2))
        sortAssertEqual(df['Model'].str.find('Quattro', 1, 3),
                        tbl['Model'].str.find('Quattro', 1, 3))

        # Index
        sortAssertEqual(
            df[df['Model'].str.contains('Quattro')]['Model'].str.index('ttro'),
            tbl[tbl['Model'].str.contains('Quattro')]['Model'].str.index('ttro'))
        with self.assertRaises(ValueError):
            tbl['Model'].str.index('Quattro')

        # Len
        sortAssertEqual(df['Model'].str.len(),
                        tbl['Model'].str.len())

        # Lower
        sortAssertEqual(df['Model'].str.lower(),
                        tbl['Model'].str.lower())

        # Lstrip
        sortAssertEqual(df['Model'].str.lstrip(),
                        tbl['Model'].str.lstrip())

        # Repeat
        sortAssertEqual(df['Model'].str.repeat(4),
                        tbl['Model'].str.repeat(4))

        # Replace
        sortAssertEqual(df['Model'].str.replace('A4', 'B105'),
                        tbl['Model'].str.replace('A4', 'B105'))
        sortAssertEqual(df['Model'].str.replace('A4', 'B'),
                        tbl['Model'].str.replace('A4', 'B'))

        # TODO: If the replacement is an empty string, all empty strings come back

        self.assertEqual(
            sorted(tbl['Model'].str.replace('4dr', tbl['Make']).tolist())[:12],
            [' 3.5 RL Acura', ' 3.5 RL w/Navigation Acura', ' 300M Chrysler',
             ' 300M Special Edition Chrysler', ' 325Ci 2dr',
             ' 325Ci convertible 2dr', ' 325i BMW', ' 325xi BMW',
             ' 325xi Sport', ' 330Ci 2dr', ' 330Ci convertible 2dr', ' 330i BMW'])

        # Rfind
        sortAssertEqual(df['Model'].str.rfind('Quattro'),
                        tbl['Model'].str.rfind('Quattro'))
        # start / end not supported yet
        # sortAssertEqual(df['Model'].str.rfind('Quattro', 2),
        #                 tbl['Model'].str.rfind('Quattro', 2))
        # sortAssertEqual(df['Model'].str.rfind('Quattro', 1, 3),
        #                 tbl['Model'].str.rfind('Quattro', 1, 3))

        # Rindex
        sortAssertEqual(
            df[df['Model'].str.contains('Quattro')]['Model'].str.rindex('ttro'),
            tbl[tbl['Model'].str.contains('Quattro')]['Model'].str.rindex('ttro'))
        with self.assertRaises(ValueError):
            tbl['Model'].str.rindex('Quattro')

        # Rstrip
        sortAssertEqual(df['Model'].str.rstrip(),
                        tbl['Model'].str.rstrip())

        # Slice
        sortAssertEqual(df['Model'].str.slice(2, 5),
                        tbl['Model'].str.slice(2, 5), strip=True)

        # Startswith
        sortAssertEqual(df['Model'].str.startswith('A4'),
                        tbl['Model'].str.startswith('A4'))

        # Strip
        sortAssertEqual(df['Model'].str.strip(),
                        tbl['Model'].str.strip())

        # Title
        self.assertEqual(tbl['Model'].str.title().tolist()[:5],
                         [' Mdx', ' Rsx Type S 2dr', ' Tsx 4dr',
                          ' Tl 4dr', ' 3.5 Rl 4dr'])

        # Upper
        sortAssertEqual(df['Model'].str.upper(),
                        tbl['Model'].str.upper())

        # Isalnum
        sortAssertEqual(df['Model'].str.isalnum(),
                        tbl['Model'].str.isalnum())

        # Isalpha
        sortAssertEqual(df['Model'].str.isalpha(),
                        tbl['Model'].str.isalpha())

        # Isdigit
        sortAssertEqual(df['Model'].str.isdigit(),
                        tbl['Model'].str.isdigit())

        # Isspace
        sortAssertEqual(df['Model'].str.isspace(),
                        tbl['Model'].str.isspace())

        # Islower
        sortAssertEqual(df['Model'].str.islower(),
                        tbl['Model'].str.islower())

        # Isupper
        sortAssertEqual(df['Model'].str.isupper(),
                        tbl['Model'].str.isupper())

        # Istitle
        # NOTE: title is implemented slightly differently than Python's
        self.assertEqual(len(tbl[tbl['Model'].str.istitle()]['Model'].tolist()), 133)

        # Isnumeric
        sortAssertEqual(df['Model'].str.isnumeric(),
                        tbl['Model'].str.isnumeric())

        # Isdecimal
        sortAssertEqual(df['Model'].str.isdecimal(),
                        tbl['Model'].str.isdecimal())

        # Soundslike
        # print(tbl['Make'].str.soundslike('board').tolist()[:100])

    def test_numeric_comparisons(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        self.assertTablesEqual(
            df[df['MSRP'] < 12360][['Model', 'MSRP']],
            tbl[tbl['MSRP'] < 12360][['Model', 'MSRP']],
            sortby=None)
        self.assertTablesEqual(
            df[df['MSRP'].lt(12360)][['Model', 'MSRP']],
            tbl[tbl['MSRP'].lt(12360)][['Model', 'MSRP']],
            sortby=None)

        self.assertTablesEqual(
            df[df['MSRP'] <= 12360][['Model', 'MSRP']],
            tbl[tbl['MSRP'] <= 12360][['Model', 'MSRP']],
            sortby=None)
        self.assertTablesEqual(
            df[df['MSRP'].le(12360)][['Model', 'MSRP']],
            tbl[tbl['MSRP'].le(12360)][['Model', 'MSRP']],
            sortby=None)

        self.assertTablesEqual(
            df[df['MSRP'] > 74995.0][['Model', 'MSRP']],
            tbl[tbl['MSRP'] > 74995.0][['Model', 'MSRP']],
            sortby=None)
        self.assertTablesEqual(
            df[df['MSRP'].gt(74995.0)][['Model', 'MSRP']],
            tbl[tbl['MSRP'].gt(74995.0)][['Model', 'MSRP']],
            sortby=None)

        self.assertTablesEqual(
            df[df['MSRP'] >= 74995.0][['Model', 'MSRP']],
            tbl[tbl['MSRP'] >= 74995.0][['Model', 'MSRP']],
            sortby=None)
        self.assertTablesEqual(
            df[df['MSRP'].ge(74995.0)][['Model', 'MSRP']],
            tbl[tbl['MSRP'].ge(74995.0)][['Model', 'MSRP']],
            sortby=None)

        self.assertTablesEqual(
            df[df['MSRP'] == 12360][['Model', 'MSRP']],
            tbl[tbl['MSRP'] == 12360][['Model', 'MSRP']],
            sortby=None)
        self.assertTablesEqual(
            df[df['MSRP'].eq(12360)][['Model', 'MSRP']],
            tbl[tbl['MSRP'].eq(12360)][['Model', 'MSRP']],
            sortby=None)

        self.assertEqual(
            len(df[df['MSRP'] != 12360][['Model', 'MSRP']]),
            len(tbl[tbl['MSRP'] != 12360][['Model', 'MSRP']]))
        self.assertEqual(
            len(df[df['MSRP'].ne(12360)][['Model', 'MSRP']]),
            len(tbl[tbl['MSRP'].ne(12360)][['Model', 'MSRP']]))

        self.assertTablesEqual(
            df[(df['MSRP'] < 12360) & (df['MSRP'] > 11000)][['Model', 'MSRP']],
            tbl[(tbl['MSRP'] < 12360) & (tbl['MSRP'] > 11000)][['Model', 'MSRP']],
            sortby=None)

        self.assertTablesEqual(
            df[(df['MSRP'] > 90000) | (df['MSRP'] < 11000)][['Model', 'MSRP']],
            tbl[(tbl['MSRP'] > 90000) | (tbl['MSRP'] < 11000)][['Model', 'MSRP']],
            sortby=None)

    def test_character_comparisons(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertColsEqual(df[df['Make'] < 'BMW']['Model'],
                             tbl[tbl['Make'] < 'BMW']['Model'])
        self.assertColsEqual(df[df['Make'].lt('BMW')]['Model'],
                             tbl[tbl['Make'].lt('BMW')]['Model'])

        self.assertColsEqual(df[df['Make'] <= 'BMW']['Model'],
                             tbl[tbl['Make'] <= 'BMW']['Model'])
        self.assertColsEqual(df[df['Make'].le('BMW')]['Model'],
                             tbl[tbl['Make'].le('BMW')]['Model'])

        self.assertColsEqual(df[df['Make'] > 'Volkswagen']['Model'],
                             tbl[tbl['Make'] > 'Volkswagen']['Model'])
        self.assertColsEqual(df[df['Make'].gt('Volkswagen')]['Model'],
                             tbl[tbl['Make'].gt('Volkswagen')]['Model'])

        self.assertColsEqual(df[df['Make'] >= 'Volkswagen']['Model'],
                             tbl[tbl['Make'] >= 'Volkswagen']['Model'])
        self.assertColsEqual(df[df['Make'].ge('Volkswagen')]['Model'],
                             tbl[tbl['Make'].ge('Volkswagen')]['Model'])

        self.assertColsEqual(df[df['Make'] == 'BMW']['Model'],
                             tbl[tbl['Make'] == 'BMW']['Model'])
        self.assertColsEqual(df[df['Make'].eq('BMW')]['Model'],
                             tbl[tbl['Make'].eq('BMW')]['Model'])

        self.assertColsEqual(df[df['Make'] != 'BMW']['Model'],
                             tbl[tbl['Make'] != 'BMW']['Model'])
        self.assertColsEqual(df[df['Make'].ne('BMW')]['Model'],
                             tbl[tbl['Make'].ne('BMW')]['Model'])

        self.assertColsEqual(
            df[(df['Make'] < 'BMW') & (df['Make'] > 'Audi')]['Model'],
            tbl[(tbl['Make'] < 'BMW') & (tbl['Make'] > 'Audi')]['Model'])

        self.assertColsEqual(
            df[(df['Make'] > 'Volkswagen') | (df['Make'] < 'BMW')]['Model'],
            tbl[(tbl['Make'] > 'Volkswagen') | (tbl['Make'] < 'BMW')]['Model'])

    def test_numeric_operators(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertColsEqual((df['MSRP'] + 123.45), (tbl['MSRP'] + 123.45))
        self.assertColsEqual((df['MSRP'] + -123.45), (tbl['MSRP'] + -123.45))
        self.assertColsEqual((df['MSRP'].add(123.45)), (tbl['MSRP'].add(123.45)))
        self.assertColsEqual((df['MSRP'].add(-123.45)), (tbl['MSRP'].add(-123.45)))

        self.assertColsEqual((df['MSRP'] + df['Invoice']),
                             (tbl['MSRP'] + tbl['Invoice']))
        self.assertColsEqual((df['MSRP'] + -df['Invoice']),
                             (tbl['MSRP'] + -tbl['Invoice']))
        self.assertColsEqual((df['MSRP'].add(df['Invoice'])),
                             (tbl['MSRP'].add(tbl['Invoice'])))
        self.assertColsEqual((df['MSRP'].add(-df['Invoice'])),
                             (tbl['MSRP'].add(-tbl['Invoice'])))

        self.assertColsEqual((df['MSRP'] - 123.45), (tbl['MSRP'] - 123.45))
        self.assertColsEqual((df['MSRP'] - -123.45), (tbl['MSRP'] - -123.45))
        self.assertColsEqual((df['MSRP'].sub(123.45)), (tbl['MSRP'].sub(123.45)))
        self.assertColsEqual((df['MSRP'].sub(-123.45)), (tbl['MSRP'].sub(-123.45)))

        self.assertColsEqual((df['MSRP'] - df['Invoice']),
                             (tbl['MSRP'] - tbl['Invoice']))
        self.assertColsEqual((df['MSRP'] - -df['Invoice']),
                             (tbl['MSRP'] - -tbl['Invoice']))
        self.assertColsEqual((df['MSRP'].sub(df['Invoice'])),
                             (tbl['MSRP'].sub(tbl['Invoice'])))
        self.assertColsEqual((df['MSRP'].sub(-df['Invoice'])),
                             (tbl['MSRP'].sub(-tbl['Invoice'])))

        def assertItemsAlmostEqual(list1, list2, decimals=4):
            for item1, item2 in zip(sorted(list1.tolist()), sorted(list2.tolist())):
                self.assertAlmostEqual(item1, item2, decimals)

        assertItemsAlmostEqual((df['MSRP'] * 123.45), (tbl['MSRP'] * 123.45))
        assertItemsAlmostEqual((df['MSRP'] * -123.45), (tbl['MSRP'] * -123.45))
        assertItemsAlmostEqual((df['MSRP'].mul(123.45)), (tbl['MSRP'].mul(123.45)))
        assertItemsAlmostEqual((df['MSRP'].mul(-123.45)), (tbl['MSRP'].mul(-123.45)))

        assertItemsAlmostEqual((df['MSRP'] * df['Invoice']),
                               (tbl['MSRP'] * tbl['Invoice']))
        assertItemsAlmostEqual((df['MSRP'] * -df['Invoice']),
                               (tbl['MSRP'] * -tbl['Invoice']))
        assertItemsAlmostEqual((df['MSRP'].mul(df['Invoice'])),
                               (tbl['MSRP'].mul(tbl['Invoice'])))
        assertItemsAlmostEqual((df['MSRP'].mul(-df['Invoice'])),
                               (tbl['MSRP'].mul(-tbl['Invoice'])))

        assertItemsAlmostEqual((df['MSRP'] / 123.45),
                               (tbl['MSRP'] / 123.45))
        assertItemsAlmostEqual((df['MSRP'] / -123.45),
                               (tbl['MSRP'] / -123.45))
        assertItemsAlmostEqual((df['MSRP'].div(123.45)),
                               (tbl['MSRP'].div(123.45)))
        assertItemsAlmostEqual((df['MSRP'].div(-123.45)),
                               (tbl['MSRP'].div(-123.45)))
        assertItemsAlmostEqual((df['MSRP'].truediv(123.45)),
                               (tbl['MSRP'].truediv(123.45)))
        assertItemsAlmostEqual((df['MSRP'].truediv(-123.45)),
                               (tbl['MSRP'].truediv(-123.45)))
        assertItemsAlmostEqual((df['MSRP'].floordiv(123.45)),
                               (tbl['MSRP'].floordiv(123.45)))
        assertItemsAlmostEqual((df['MSRP'].floordiv(-123.45)),
                               (tbl['MSRP'].floordiv(-123.45)))
        assertItemsAlmostEqual((df['MSRP'] // 123.45),
                               (tbl['MSRP'] // 123.45))
        assertItemsAlmostEqual((df['MSRP'] // -123.45),
                               (tbl['MSRP'] // -123.45))

        assertItemsAlmostEqual((df['MSRP'] / df['Invoice']),
                               (tbl['MSRP'] / tbl['Invoice']))
        assertItemsAlmostEqual((df['MSRP'] / -df['Invoice']),
                               (tbl['MSRP'] / -tbl['Invoice']))
        assertItemsAlmostEqual((df['MSRP'].div(df['Invoice'])),
                               (tbl['MSRP'].div(tbl['Invoice'])))
        assertItemsAlmostEqual((df['MSRP'].div(-df['Invoice'])),
                               (tbl['MSRP'].div(-tbl['Invoice'])))
        assertItemsAlmostEqual((df['MSRP'].truediv(df['Invoice'])),
                               (tbl['MSRP'].truediv(tbl['Invoice'])))
        assertItemsAlmostEqual((df['MSRP'].truediv(-df['Invoice'])),
                               (tbl['MSRP'].truediv(-tbl['Invoice'])))
        assertItemsAlmostEqual((df['MSRP'].floordiv(df['Invoice'])),
                               (tbl['MSRP'].floordiv(tbl['Invoice'])))
        assertItemsAlmostEqual((df['MSRP'].floordiv(-df['Invoice'])),
                               (tbl['MSRP'].floordiv(-tbl['Invoice'])))
        assertItemsAlmostEqual((df['MSRP'] // df['Invoice']),
                               (tbl['MSRP'] // tbl['Invoice']))
        assertItemsAlmostEqual((df['MSRP'] // -df['Invoice']),
                               (tbl['MSRP'] // -tbl['Invoice']))

        dfout = (df['MSRP'] % 123.45)
        tblout = (tbl['MSRP'] % 123.45)
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'] % -123.45).head(100).tolist()
        tblout = [(x - 123.45) for x in (tbl['MSRP'] % -123.45).head(100).tolist()]
        for dfo, tblo in zip(sorted(dfout), sorted(tblout)):
            self.assertAlmostEqual(dfo, tblo, 2)

        dfout = (df['MSRP'].mod(123.45))
        tblout = (tbl['MSRP'].mod(123.45))
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'].mod(-123.45))
        tblout = [(x - 123.45) for x in (tbl['MSRP'].mod(-123.45))]
        for dfo, tblo in zip(sorted(dfout), sorted(tblout)):
            self.assertAlmostEqual(dfo, tblo, 2)

        dfout = (df['MSRP'] % df['Horsepower'])
        tblout = (tbl['MSRP'] % tbl['Horsepower'])
        assertItemsAlmostEqual(dfout, tblout, 2)

        # dfout = (df['MSRP'] % -df['Horsepower'])
        # tblout = ((tbl['MSRP'] % -tbl['Horsepower']) + tbl['MSRP'])
        # assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'].mod(df['Horsepower']))
        tblout = (tbl['MSRP'].mod(tbl['Horsepower']))
        assertItemsAlmostEqual(dfout, tblout, 2)

        # dfout = (df['MSRP'].mod(-df['Horsepower']))
        # tblout = ((tbl['MSRP'].mod(-tbl['Horsepower'])) + tbl['MSRP'])
        # assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'] ** 1.2345)
        tblout = (tbl['MSRP'] ** 1.2345)
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'] ** -1.2345)
        tblout = (tbl['MSRP'] ** -1.2345)
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'].pow(1.2345))
        tblout = (tbl['MSRP'].pow(1.2345))
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'].pow(-1.2345))
        tblout = (tbl['MSRP'].pow(-1.2345))
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'] ** (df['EngineSize'] / 10))
        tblout = (tbl['MSRP'] ** (tbl['EngineSize'] / 10))
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'] ** -(df['EngineSize'] / 10))
        tblout = (tbl['MSRP'] ** -(tbl['EngineSize'] / 10))
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'].pow(df['EngineSize'] / 10))
        tblout = (tbl['MSRP'].pow(tbl['EngineSize'] / 10))
        assertItemsAlmostEqual(dfout, tblout, 2)

        dfout = (df['MSRP'].pow(-df['EngineSize'] / 10))
        tblout = (tbl['MSRP'].pow(-tbl['EngineSize'] / 10))
        assertItemsAlmostEqual(dfout, tblout, 2)

        self.assertColsEqual(123.45 + df['MSRP'], 123.45 + tbl['MSRP'])
        self.assertColsEqual(-123.45 + df['MSRP'], -123.45 + tbl['MSRP'])
        self.assertColsEqual((df['MSRP'].radd(123.45)),
                             (tbl['MSRP'].radd(123.45)))
        self.assertColsEqual((df['MSRP'].radd(-123.45)),
                             (tbl['MSRP'].radd(-123.45)))

        self.assertColsEqual((df['MSRP'].radd(df['EngineSize'])),
                             (tbl['MSRP'].radd(tbl['EngineSize'])))
        self.assertColsEqual((df['MSRP'].radd(-df['EngineSize'])),
                             (tbl['MSRP'].radd(-tbl['EngineSize'])))

        self.assertColsEqual(123.45 - df['MSRP'], 123.45 - tbl['MSRP'])
        self.assertColsEqual(-123.45 - df['MSRP'], -123.45 - tbl['MSRP'])
        self.assertColsEqual((df['MSRP'].rsub(123.45)),
                             (tbl['MSRP'].rsub(123.45)))
        self.assertColsEqual((df['MSRP'].rsub(-123.45)),
                             (tbl['MSRP'].rsub(-123.45)))

        self.assertColsEqual((df['MSRP'].rsub(df['EngineSize'])),
                             (tbl['MSRP'].rsub(tbl['EngineSize'])))
        self.assertColsEqual((df['MSRP'].rsub(-df['EngineSize'])),
                             (tbl['MSRP'].rsub(-tbl['EngineSize'])))

        assertItemsAlmostEqual(123.45 * df['MSRP'], 123.45 * tbl['MSRP'])
        assertItemsAlmostEqual(-123.45 * df['MSRP'], -123.45 * tbl['MSRP'])
        assertItemsAlmostEqual((df['MSRP'].rmul(123.45)),
                               (tbl['MSRP'].rmul(123.45)))
        assertItemsAlmostEqual((df['MSRP'].rmul(-123.45)),
                               (tbl['MSRP'].rmul(-123.45)))

        assertItemsAlmostEqual((df['MSRP'].rmul(df['EngineSize'])),
                               (tbl['MSRP'].rmul(tbl['EngineSize'])))
        assertItemsAlmostEqual((df['MSRP'].rmul(-df['EngineSize'])),
                               (tbl['MSRP'].rmul(-tbl['EngineSize'])))

        assertItemsAlmostEqual(123.45 / df['MSRP'], 123.45 / tbl['MSRP'])
        assertItemsAlmostEqual(-123.45 / df['MSRP'], -123.45 / tbl['MSRP'])
        assertItemsAlmostEqual((df['MSRP'].rdiv(123.45)),
                               (tbl['MSRP'].rdiv(123.45)))
        assertItemsAlmostEqual((df['MSRP'].rdiv(-123.45)),
                               (tbl['MSRP'].rdiv(-123.45)))
        assertItemsAlmostEqual((df['MSRP'].rtruediv(123.45)),
                               (tbl['MSRP'].rtruediv(123.45)))
        assertItemsAlmostEqual((df['MSRP'].rtruediv(-123.45)),
                               (tbl['MSRP'].rtruediv(-123.45)))

        assertItemsAlmostEqual(123.45 // df['MSRP'], 123.45 // tbl['MSRP'])
        assertItemsAlmostEqual(-123.45 // df['MSRP'], -123.45 // tbl['MSRP'])
        assertItemsAlmostEqual((df['MSRP'].rfloordiv(123.45)),
                               (tbl['MSRP'].rfloordiv(123.45)))
        assertItemsAlmostEqual((df['MSRP'].rfloordiv(-123.45)),
                               (tbl['MSRP'].rfloordiv(-123.45)))

        assertItemsAlmostEqual((df['MSRP'].rdiv(df['EngineSize'])),
                               (tbl['MSRP'].rdiv(tbl['EngineSize'])))
        assertItemsAlmostEqual((df['MSRP'].rdiv(-df['EngineSize'])),
                               (tbl['MSRP'].rdiv(-tbl['EngineSize'])))
        assertItemsAlmostEqual((df['MSRP'].rtruediv(df['EngineSize'])),
                               (tbl['MSRP'].rtruediv(tbl['EngineSize'])))
        assertItemsAlmostEqual((df['MSRP'].rtruediv(-df['EngineSize'])),
                               (tbl['MSRP'].rtruediv(-tbl['EngineSize'])))
        assertItemsAlmostEqual((df['MSRP'].rfloordiv(df['EngineSize'])),
                               (tbl['MSRP'].rfloordiv(tbl['EngineSize'])))
        assertItemsAlmostEqual((df['MSRP'].rfloordiv(-df['EngineSize'])),
                               (tbl['MSRP'].rfloordiv(-tbl['EngineSize'])))

        dfout = (123.45 % df['MSRP'])
        tblout = (123.45 % tbl['MSRP'])
        assertItemsAlmostEqual(dfout, tblout, 4)

        dfout = (df['MSRP'].rmod(123.45))
        tblout = (tbl['MSRP'].rmod(123.45))
        assertItemsAlmostEqual(dfout, tblout, 4)

        dfout = (df['MSRP'].rmod(-123.45)).head(100).tolist()
        tblout = ((tbl['MSRP'].rmod(-123.45)).head(100)
                  + tbl['MSRP'].head(100)).tolist()
        for dfo, tblo in zip(sorted(dfout), sorted(tblout)):
            self.assertAlmostEqual(dfo, tblo, 4)

        dfout = (df['MSRP'].rmod(df['EngineSize']))
        tblout = (tbl['MSRP'].rmod(tbl['EngineSize']))
        assertItemsAlmostEqual(dfout, tblout, 4)

        dfout = (df['MSRP'].rmod(-df['EngineSize'])).head(100).tolist()
        tblout = ((tbl['MSRP'].rmod(-tbl['EngineSize'])).head(100)
                  + tbl['MSRP'].head(100)).tolist()
        for dfo, tblo in zip(sorted(dfout), sorted(tblout)):
            self.assertAlmostEqual(dfo, tblo, 4)

        dfout = (0.12345 ** df['MSRP'])
        tblout = (0.12345 ** tbl['MSRP'])
        assertItemsAlmostEqual(dfout, tblout, 4)

        dfout = (df['MSRP'].rpow(0.12345))
        tblout = (tbl['MSRP'].rpow(0.12345))
        assertItemsAlmostEqual(dfout, tblout, 4)

        dfout = (df['MSRP'].rpow(-0.12345))
        tblout = (tbl['MSRP'].rpow(-0.12345))
        assertItemsAlmostEqual(dfout, tblout, 4)

        dfout = (df['MSRP'].rpow(df['EngineSize'] / 10))
        tblout = (tbl['MSRP'].rpow(tbl['EngineSize'] / 10))
        assertItemsAlmostEqual(dfout, tblout, 4)

        dfout = (df['MSRP'].rpow(-df['EngineSize'] / 10))
        tblout = (tbl['MSRP'].rpow(-tbl['EngineSize'] / 10))
        assertItemsAlmostEqual(dfout, tblout, 4)

        self.assertColsEqual(df['EngineSize'].round(0),
                             tbl['EngineSize'].round(0))

        self.assertColsEqual(df['EngineSize'].round(2),
                             tbl['EngineSize'].round(2))

        if [int(x) for x in np.__version__.split('.')[:2]] > [1, 8]:
            assertItemsAlmostEqual(np.floor(df['EngineSize']),
                                   np.floor(tbl['EngineSize']))
            assertItemsAlmostEqual(np.ceil(df['EngineSize']),
                                   np.ceil(tbl['EngineSize']))
            assertItemsAlmostEqual(np.trunc(df['EngineSize']),
                                   np.trunc(tbl['EngineSize']))

    def test_character_operators(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertColsEqual((df['Make'] + 'Foo'),
                             (tbl['Make'] + 'Foo'))
        self.assertColsEqual((df['Make'].add('Foo')),
                             (tbl['Make'].add('Foo')))
        self.assertColsEqual((df['Make'] + df['Model']),
                             (tbl['Make'] + tbl['Model']))
        self.assertColsEqual((df['Make'].add(df['Model'])),
                             (tbl['Make'].add(tbl['Model'])))

        with self.assertRaises(AttributeError):
            -tbl['Make']

        with self.assertRaises(AttributeError):
            tbl['Make'] - 'Foo'
        with self.assertRaises(AttributeError):
            tbl['Make'] - tbl['Model']
        with self.assertRaises(AttributeError):
            tbl['Make'].sub('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].sub(tbl['Model'])

        self.assertColsEqual((df['Make'] * 3), (tbl['Make'] * 3))
        self.assertColsEqual((df['Make'] * -3), (tbl['Make'] * -3))
        self.assertColsEqual((df['Make'].mul(3)), (tbl['Make'].mul(3)))
        self.assertColsEqual((df['Make'].mul(-3)), (tbl['Make'].mul(-3)))

        self.assertColsEqual((df['Make'] * df['EngineSize'].astype('int')),
                             (tbl['Make'] * tbl['EngineSize']))
        self.assertColsEqual((df['Make'].mul(df['EngineSize'].astype('int'))),
                             (tbl['Make'].mul(tbl['EngineSize'])))

        with self.assertRaises(AttributeError):
            tbl['Make'] / 'Foo'
        with self.assertRaises(AttributeError):
            tbl['Make'].div('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].truediv('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].floordiv('Foo')

        with self.assertRaises(AttributeError):
            tbl['Make'] / tbl['Model']
        with self.assertRaises(AttributeError):
            tbl['Make'].div(tbl['Model'])
        with self.assertRaises(AttributeError):
            tbl['Make'].truediv(tbl['Model'])
        with self.assertRaises(AttributeError):
            tbl['Make'].floordiv(tbl['Model'])

        with self.assertRaises(AttributeError):
            tbl['Make'] % 'Foo'
        with self.assertRaises(AttributeError):
            tbl['Make'].mod('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'] % tbl['Model']
        with self.assertRaises(AttributeError):
            tbl['Make'].mod(tbl['Model'])

        with self.assertRaises(AttributeError):
            tbl['Make'] ** 'Foo'
        with self.assertRaises(AttributeError):
            tbl['Make'].pow('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'] ** tbl['Model']
        with self.assertRaises(AttributeError):
            tbl['Make'].pow(tbl['Model'])

        self.assertColsEqual('Foo' + df['Make'], 'Foo' + tbl['Make'])
        self.assertColsEqual((df['Make'].radd(df['Model'].str.strip())),
                             (tbl['Make'].radd(tbl['Model'].str.strip())))
        self.assertColsEqual((df['Make'].radd('Foo')),
                             (tbl['Make'].radd('Foo')))
        self.assertColsEqual((df['Make'].radd(df['Model'].str.strip())),
                             (tbl['Make'].radd(tbl['Model'].str.strip())))

        with self.assertRaises(AttributeError):
            tbl['Make'].rsub('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].rsub(tbl['Model'])

        self.assertColsEqual(3 * df['Make'], 3 * tbl['Make'])
        self.assertColsEqual((df['Make'].rmul(3)),
                             (tbl['Make'].rmul(3)))
        self.assertColsEqual((df['Make'].rmul(df['EngineSize'].astype('int'))),
                             (tbl['Make'].rmul(tbl['EngineSize'])))

        with self.assertRaises(AttributeError):
            tbl['Make'].rdiv('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].rdiv(tbl['Model'])
        with self.assertRaises(AttributeError):
            tbl['Make'].rtruediv('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].rtruediv(tbl['Model'])
        with self.assertRaises(AttributeError):
            tbl['Make'].rfloordiv('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].rfloordiv(tbl['Model'])

        with self.assertRaises(AttributeError):
            tbl['Make'].rmod('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].rmod(tbl['Model'])

        with self.assertRaises(AttributeError):
            tbl['Make'].rpow('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].rpow(tbl['Model'])

        with self.assertRaises(AttributeError):
            tbl['Make'].round('Foo')
        with self.assertRaises(AttributeError):
            tbl['Make'].round(tbl['Model'])

        # with self.assertRaises(AttributeError):
        #     np.floor(tbl['Make'])
        # with self.assertRaises(AttributeError):
        #     np.ceil(tbl['Make'])
        # with self.assertRaises(AttributeError):
        #     np.trunc(tbl['Make'])

    def test_column_stats(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        self.assertColsEqual(df['MSRP'].abs(),
                             tbl['MSRP'].abs())
        self.assertColsEqual((df['MSRP'] * -1).abs(),
                             (tbl['MSRP'] * -1).abs())

        self.assertEqual(df['MSRP'].all(), tbl['MSRP'].all())
        self.assertEqual(df['Cylinders'].all(), tbl['Cylinders'].all())

        self.assertColsEqual(df['MSRP'].between(40000, 50000),
                             tbl['MSRP'].between(40000, 50000))

        self.assertColsEqual(df['MSRP'].clip(40000, 50000),
                             tbl['MSRP'].clip(40000, 50000))

        if pd_version < (1, 0, 0):
            self.assertColsEqual(df['MSRP'].clip_lower(40000),
                                 tbl['MSRP'].clip_lower(40000))
            self.assertColsEqual(df['MSRP'].clip_upper(40000),
                                 tbl['MSRP'].clip_upper(40000))

        self.assertAlmostEqual(df['MSRP'].corr(df['Invoice']),
                               tbl['MSRP'].corr(tbl['Invoice']), 4)

        self.assertEqual(df['Cylinders'].count(), tbl['Cylinders'].count())

        # Pandas doesn't like NaNs in here
        dfdesc = df['Cylinders'].dropna().describe()
        tbldesc = tbl['Cylinders'].describe()
        # self.assertEqual(dfdesc.name, tbldesc.name)
        self.assertEqual(dfdesc.loc['count'], tbldesc.loc['count'])
        self.assertAlmostEqual(dfdesc.loc['mean'], tbldesc.loc['mean'], 4)
        self.assertAlmostEqual(dfdesc.loc['std'], tbldesc.loc['std'], 4)
        self.assertAlmostEqual(dfdesc.loc['min'], tbldesc.loc['min'], 4)
        self.assertAlmostEqual(dfdesc.loc['25%'], tbldesc.loc['25%'], 4)
        self.assertAlmostEqual(dfdesc.loc['50%'], tbldesc.loc['50%'], 4)
        self.assertAlmostEqual(dfdesc.loc['75%'], tbldesc.loc['75%'], 4)
        self.assertAlmostEqual(dfdesc.loc['max'], tbldesc.loc['max'], 4)

        dfdesc = df['Make'].describe()
        tbldesc = tbl['Make'].describe()
        # self.assertEqual(dfdesc.name, tbldesc.name)
        self.assertEqual(dfdesc.loc['count'], tbldesc.loc['count'])
        self.assertEqual(dfdesc.loc['unique'], tbldesc.loc['unique'])
        self.assertEqual(dfdesc.loc['top'], tbldesc.loc['top'])
        self.assertEqual(dfdesc.loc['freq'], tbldesc.loc['freq'])

        self.assertEqual(df['MSRP'].max(), tbl['MSRP'].max())
        self.assertAlmostEqual(df['MSRP'].mean(), tbl['MSRP'].mean(), 4)
        self.assertEqual(df['MSRP'].min(), tbl['MSRP'].min())
        self.assertEqual(df['MSRP'].sum(), tbl['MSRP'].sum())
        self.assertAlmostEqual(df['MSRP'].std(), tbl['MSRP'].std(), 4)
        self.assertAlmostEqual(df['MSRP'].var(), tbl['MSRP'].var(), 4)
        self.assertEqual(tbl['Cylinders'].nmiss(), 2)
        self.assertAlmostEqual(tbl['MSRP'].stderr(), 939.267477664, 4)
        self.assertAlmostEqual(tbl['MSRP'].uss(), 620985422112.0, 1)
        self.assertAlmostEqual(tbl['MSRP'].css(), 161231618703.0, 1)
        self.assertAlmostEqual(tbl['MSRP'].cv(), 59.2884898823, 4)
        self.assertAlmostEqual(tbl['MSRP'].tvalue(), 34.8940593809, 4)
        self.assertAlmostEqual(tbl['MSRP'].probt(), 4.16041192748e-127, 127)

        self.assertColsEqual(df['MSRP'].nlargest(),
                             tbl['MSRP'].nlargest())
        self.assertColsEqual(df['MSRP'].nsmallest(),
                             tbl['MSRP'].nsmallest())

    def test_column_value_counts(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertColsEqual(df['Make'].value_counts(), tbl['Make'].value_counts())
        self.assertColsEqual(df['MSRP'].value_counts(), tbl['MSRP'].value_counts())

        self.assertColsEqual(df['Make'].value_counts(normalize=True),
                             tbl['Make'].value_counts(normalize=True))
        self.assertColsEqual(df['MSRP'].value_counts(normalize=True),
                             tbl['MSRP'].value_counts(normalize=True))

        self.assertColsEqual(df['Make'].value_counts(sort=True, ascending=True),
                             tbl['Make'].value_counts(sort=True, ascending=True))
        self.assertColsEqual(df['MSRP'].value_counts(sort=True, ascending=True),
                             tbl['MSRP'].value_counts(sort=True, ascending=True))

        # Test groupby variables
        tblgrp = tbl.groupby(['Make', 'Cylinders'])
        dfgrp = df.groupby(['Make', 'Cylinders'])

        self.assertColsEqual(dfgrp['Type'].value_counts(),
                             tblgrp['Type'].value_counts())
        self.assertColsEqual(dfgrp['Horsepower'].value_counts(),
                             tblgrp['Horsepower'].value_counts())

        self.assertColsEqual(dfgrp['Type'].value_counts(normalize=True),
                             tblgrp['Type'].value_counts(normalize=True))
        self.assertColsEqual(dfgrp['Horsepower'].value_counts(normalize=True),
                             tblgrp['Horsepower'].value_counts(normalize=True))

        # NOTE: Pandas doesn't seem to count NaNs regardless
        # self.assertEqual(dfgrp['Type'].value_counts(dropna=False).tolist(),
        #                  tblgrp['Type'].value_counts(dropna=False).tolist())
        # self.assertEqual(dfgrp['Horsepower'].value_counts(dropna=False).tolist(),
        #                  tblgrp['Horsepower'].value_counts(dropna=False).tolist())

    def test_column_unique(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(type(df['Make'].unique()), type(tbl['Make'].unique()))
        self.assertEqual(sorted(df['Make'].unique()), sorted(tbl['Make'].unique()))
        self.assertEqual(
            len([True for x in df['Cylinders'].unique() if np.isnan(x)]),
            len([True for x in tbl['Cylinders'].unique() if np.isnan(x)]))
        self.assertEqual(
            sorted([x for x in df['Cylinders'].unique() if not np.isnan(x)]),
            sorted([x for x in tbl['Cylinders'].unique() if not np.isnan(x)]))

        self.assertEqual(df['Make'].nunique(), tbl['Make'].nunique())
        self.assertEqual(df['Cylinders'].nunique(), tbl['Cylinders'].nunique())
        self.assertEqual(df['Cylinders'].nunique(dropna=False),
                         tbl['Cylinders'].nunique(dropna=False))

        self.assertFalse(tbl['Cylinders'].query('Make = "Ford"').is_unique)
        self.assertTrue(tbl['Model'].query('Make = "Ford"').is_unique)

        # Test groupby variables
        tblgrp = tbl.groupby(['Make', 'Cylinders'])
        dfgrp = df.groupby(['Make', 'Cylinders'])

        self.assertEqual(type(dfgrp['Type'].unique()), type(tblgrp['Type'].unique()))
        self.assertEqual(sorted(sorted(list(x)) for x in dfgrp['Type'].unique()),
                         sorted(sorted(list(x)) for x in tblgrp['Type'].unique()))
        # self.assertEqual(
        #     len([True for x in dfgrp['Horsepower'].unique() if np.isnan(x)]),
        #     len([True for x in tblgrp['Horsepower'].unique() if np.isnan(x)]))
        # self.assertEqual(
        #     sorted([x for x in dfgrp['Horsepower'].unique() if not np.isnan(x)]),
        #     sorted([x for x in tblgrp['Horsepower'].unique() if not np.isnan(x)]))

        self.assertColsEqual(dfgrp['Type'].nunique(),
                             tblgrp['Type'].nunique())
        self.assertColsEqual(dfgrp['Horsepower'].nunique(),
                             tblgrp['Horsepower'].nunique())
        # self.assertColsEqual(dfgrp['Horsepower'].nunique(dropna=False),
        #                      tblgrp['Horsepower'].nunique(dropna=False))

        self.assertTrue(
            x for x in tblgrp['Model'].query('Make = "Ford"').is_unique.tolist()
            if x is True)
        self.assertTrue(
            x for x in tblgrp['Type'].query('Make = "Ford"').is_unique.tolist()
            if x is False)

    @unittest.skip('Need way to verify the file exists on server')
    def test_load_path(self):
        df = self.get_cars_df()
        cars = self.s.load_path('datasources/cars_single.sashdat', caslib=self.srcLib,
                                casout=dict(replace=True))
        self.assertTablesEqual(df, cars)

    @unittest.skipIf(pd_version <= (0, 16, 0), 'Need newer version of Pandas')
    def test_read_pickle(self):
        df = self.get_cars_df(all_doubles=False)

        import pickle
        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        df.to_pickle(tmp.name)

        df2 = pd.read_pickle(tmp.name)
        tbl = self.s.read_pickle(tmp.name)

        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_pickle(tmp.name, use_addtable=True)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

        os.remove(tmp.name)

    def test_read_table(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.tsv')

        df = pd.read_table(myFile)
        tbl = self.s.read_table(myFile)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_table(myFile, use_addtable=True)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

    def test_read_csv(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        df = pd.read_csv(myFile)
        tbl = self.s.read_csv(myFile)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_csv(myFile, use_addtable=True)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

    def test_read_frame(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        df = pd.read_csv(myFile)
        tbl = self.s.read_frame(df)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_frame(df, use_addtable=True)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

    def test_read_fwf(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.fwf')

        df = pd.read_fwf(myFile)
        tbl = self.s.read_fwf(myFile)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_fwf(myFile, use_addtable=True)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

#   def test_read_clipboard(self):
#       ???

    def test_read_excel(self):
        # if self.s._protocol in ['http', 'https']:
        #     tm.TestCase.skipTest(self, 'REST does not support data messages')

        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.xls')

        df = pd.read_excel(myFile)
        tbl = self.s.read_excel(myFile)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_excel(myFile, use_addtable=True)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

#   @unittest.skip('Freezes on addtable')
#   def test_read_json(self):
#       if self.s._protocol in ['http', 'https']:
#           tm.TestCase.skipTest(self, 'REST does not support data messages')

#       import swat.tests as st

#       myFile = os.path.join(os.path.dirname(st.__file__),
#                             'datasources', 'pandas_issues.json')

#       df = tbl = None

#       with open(myFile, encoding='utf-8') as infile:
#           df = pd.read_json(infile)

#       with open(myFile, encoding='utf-8') as infile:
#           tbl = self.s.read_json(infile)

#       self.assertTablesEqual(df, tbl)

    @unittest.skipIf(pd_version <= (0, 16, 0), 'Need newer version of Pandas')
    def test_read_html(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.html')

        df = pd.read_html(myFile)[0]
        tbl = self.s.read_html(myFile)[0]

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_html(myFile, use_addtable=True)[0]

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

    @unittest.skip('Need way to verify HDF installation')
    def test_read_hdf(self):
        df = self.get_cars_df(all_doubles=False)

        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        try:
            df.to_hdf(tmp.name, key='cars')
        except ImportError:
            tm.TestCase.skipTest(self, 'Need PyTables installed')

        df2 = pd.read_hdf(tmp.name, key='cars')
        tbl = self.s.read_hdf(tmp.name, key='cars')

        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)
        if 'csv-ints' in self.s.server_features:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'varchar']))

        # Force addtable
        tbl = self.s.read_hdf(tmp.name, use_addtable=True)

        self.assertTablesEqual(df, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            if 'csv-ints' in self.s.server_features:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'int64', 'varchar']))
            else:
                self.assertEqual(set(tbl.dtypes.unique()),
                                 set(['double', 'varchar']))
        else:
            self.assertEqual(set(tbl.dtypes.unique()),
                             set(['double', 'int64', 'varchar']))

        os.remove(tmp.name)

# TODO: Getting an error about only XPORT is supported.
#   def test_read_sas(self):
#       myFile = 'calcmilk.sas7bdat'
#       cwd, filename = os.path.split(os.path.abspath(__file__))
#       if cwd != None:
#           myFile = os.path.join(cwd, myFile)

#       df = pd.read_sas(myFile, format='sas7bdat', encoding='utf-8')
#       print(df)
#       tbl = self.s.read_sas(myFile, format='sas7bdat', encoding='utf-8')

#       self.assertEqual(df.head(50).to_csv(index=False),
#                        tbl.head(50).to_csv(index=False))

    def test_column_to_frame(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dff = df['Make'].to_frame()
        tblf = tbl['Make'].to_frame()

        self.assertTablesEqual(dff, tblf, sortby=None)

    @unittest.skipIf(pd_version <= (0, 17, 0), 'Need newer version of Pandas')
    def test_column_to_xarray(self):
        try:
            import xarray
        except (ImportError, ValueError):
            tm.TestCase.skipTest(self, 'Need xarray installed')

        df = self.get_cars_df(all_doubles=False).sort_values(['Make'])
        tbl = self.table

        dfx = df['Make'].to_xarray()
        tblx = tbl['Make'].to_xarray(sort=True)

        self.assertEqual(list(dfx.values)[:200], list(tblx.values)[:200])

    @unittest.skip('Need way to verify HDF installation')
    def test_column_to_hdf(self):
        df = self.get_cars_df(all_doubles=False)
        tbl = self.table

        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        try:
            df['Make'].to_hdf(tmp.name, key='cars_df')
            tbl['Make'].to_hdf(tmp.name, key='cars_tbl')
        except ImportError:
            tm.TestCase.skipTest(self, 'Need PyTables installed')

        dfhdf = pd.read_hdf(tmp.name, key='cars_df')
        tblhdf = pd.read_hdf(tmp.name, key='cars_tbl')

        self.assertColsEqual(dfhdf, tblhdf)

        os.remove(tmp.name)

    def test_column_to_json(self):
        df = self.get_cars_df(all_doubles=False)
        tbl = self.table

        dfj = df['Make'].to_json()
        tblj = tbl['Make'].to_json()

        self.assertEqual(dfj, tblj)

    @unittest.skipIf(pd_version <= (0, 17, 0), 'Need newer version of Pandas')
    def test_column_to_string(self):
        df = self.get_cars_df(all_doubles=False)
        tbl = self.table

        dfs = df['Make'].to_string(index=False)
        tbls = tbl['Make'].to_string(index=False)

        self.assertEqual(sorted(dfs.split('\n')), sorted(tbls.split('\n')))

    @unittest.skipIf(pd_version >= (0, 21, 0), 'Deprecated in pandas')
    def test_column_from_csv(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'iicc.csv')

        series = self.get_cars_df(all_doubles=False)['Make']
        column = self.table['Make']

        s2 = series.from_csv(myFile, header=0, index_col=None)
        c2 = column.from_csv(self.s, myFile, header=0, index_col=None)

        self.assertColsEqual(s2, c2, sort=True)
        self.assertEqual(c2.dtype, 'double')

        # Force addtable
        s2 = series.from_csv(myFile, header=0, index_col=None)
        c2 = column.from_csv(self.s, myFile, header=0, index_col=None, use_addtable=True)

        self.assertColsEqual(s2, c2, sort=True)

        if self.s._protocol in ['http', 'https']:
            self.assertEqual(c2.dtype, 'double')
        else:
            self.assertEqual(c2.dtype, 'int64')

    def test_query(self):
        df = self.get_cars_df()
        tbl = self.table

        df2 = df.query('MSRP < 20000 and MPG_City > 25', engine='python')
        tbl2 = tbl.query('MSRP < 20000 and MPG_City > 25')

        self.assertEqual(len(df2), len(tbl2))
        self.assertEqual(set(df2['Model'].tolist()),
                         set(tbl2['Model'].tolist()))

#   def test_where(self):
#       df = self.get_cars_df().sort_values(['MSRP', 'Invoice'])
#       df.index = range(len(df))
#       tbl = self.table.sort_values(['MSRP', 'Invoice'])

#       df2 = df.where(df < 15000)
#       tbl2 = tbl.where(tbl < 15000)

#       self.assertTrue(tbl2 is not tbl)
#       self.assertEqual(set(df2.Model.tolist()), set(tbl2.Model.tolist()))
#
#       df3 = df.where(df < 15000, inplace=True)
#       tbl3 = tbl.where(tbl < 15000, inplace=True)

#       self.assertTrue(tbl3 is None)
#       self.assertEqual(set(df.Model.tolist()), set(tbl.Model.tolist()))

    @unittest.skipIf(pd_version <= (0, 14, 0), 'Need newer version of Pandas')
    def test_timezones(self):
        if self.s._protocol in ['http', 'https']:
            tm.TestCase.skipTest(self, 'REST does not support data messages')

        import swat.tests as st

        utc_tz = pytz.timezone('UTC')

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'datetime.csv')

        df = pd.read_csv(myFile, parse_dates=[0, 1, 2])
        df['date'] = df['date'].apply(lambda x: x.date())
        df['time'] = df['time'].apply(lambda x: x.time())
        df.sort_values(['datetime'], inplace=True)

        from swat.cas import datamsghandlers as dmh

        def add_table():
            pd_dmh = dmh.PandasDataFrame(
                df, dtype={'date': 'date', 'time': 'time'},
                formats={'date': 'nldate', 'time': 'nltime', 'datetime': 'nldatm'},
                labels={'date': 'Date', 'time': 'Time', 'datetime': 'Datetime'})

            return self.s.addtable(table='datetime', caslib=self.srcLib,
                                   replace=True, **pd_dmh.args.addtable).casTable

        #
        # Ensure date/times remain timezone-naive
        #
        swat.options.timezone = None

        tbl = add_table()
        tblf = tbl.to_frame()

        self.assertEqual(
            sorted([x.year for x in df.date]),
            sorted([x.year for x in tblf.date]))
        self.assertEqual(
            sorted(df.datetime.dt.year),
            sorted(tblf.datetime.dt.year))

        self.assertEqual(
            sorted([x.month for x in df.date]),
            sorted([x.month for x in tblf.date]))
        self.assertEqual(
            sorted(df.datetime.dt.month),
            sorted(tblf.datetime.dt.month))

        self.assertEqual(
            sorted([x.day for x in df.date]),
            sorted([x.day for x in tblf.date]))
        self.assertEqual(
            sorted(df.datetime.dt.day),
            sorted(tblf.datetime.dt.day))

        self.assertEqual(
            sorted([x.hour for x in df.time]),
            sorted([x.hour for x in tblf.time]))
        self.assertEqual(
            sorted(df.datetime.dt.hour),
            sorted(tblf.datetime.dt.hour))

        self.assertEqual(
            sorted([x.minute for x in df.time]),
            sorted([x.minute for x in tblf.time]))
        self.assertEqual(
            sorted(df.datetime.dt.minute),
            sorted(tblf.datetime.dt.minute))

        self.assertEqual(
            sorted([x.second for x in df.time]),
            sorted([x.second for x in tblf.time]))
        self.assertEqual(
            sorted(df.datetime.dt.second),
            sorted(tblf.datetime.dt.second))

        tzs = [None] * 4
        self.assertEqual([x.tzinfo for x in df.time], tzs)
        self.assertEqual([x.tzinfo for x in tblf.time], tzs)
        self.assertEqual([x.tzinfo for x in df.datetime], tzs)
        self.assertEqual([x.tzinfo for x in tblf.datetime], tzs)

        #
        # Ensure downloaded data is in correct timezone
        #
        swat.options.timezone = 'US/Pacific'

        tbl = add_table()
        tblf = tbl.to_frame()

        df_pac = df[:]
        df_pac['datetime'] = df_pac['datetime'].apply(
            lambda x: utc_tz.localize(x).astimezone(pytz.timezone('US/Pacific')))

        self.assertEqual(
            sorted([x.year for x in df_pac.date]),
            sorted([x.year for x in tblf.date]))
        self.assertEqual(
            sorted(df_pac.datetime.dt.year),
            sorted(tblf.datetime.dt.year))

        self.assertEqual(
            sorted([x.month for x in df_pac.date]),
            sorted([x.month for x in tblf.date]))
        self.assertEqual(
            sorted(df_pac.datetime.dt.month),
            sorted(tblf.datetime.dt.month))

        self.assertEqual(
            sorted([x.day for x in df_pac.date]),
            sorted([x.day for x in tblf.date]))
        self.assertEqual(
            sorted(df_pac.datetime.dt.day),
            sorted(tblf.datetime.dt.day))

        self.assertEqual(
            sorted([x.hour for x in df_pac.time]),
            sorted([x.hour for x in tblf.time]))
        self.assertEqual(
            sorted(df_pac.datetime.dt.hour),
            sorted(tblf.datetime.dt.hour))

        self.assertEqual(
            sorted([x.minute for x in df_pac.time]),
            sorted([x.minute for x in tblf.time]))
        self.assertEqual(
            sorted(df_pac.datetime.dt.minute),
            sorted(tblf.datetime.dt.minute))

        self.assertEqual(
            sorted([x.second for x in df_pac.time]),
            sorted([x.second for x in tblf.time]))
        self.assertEqual(
            sorted(df_pac.datetime.dt.second),
            sorted(tblf.datetime.dt.second))

        tzs = (['PST', 'PDT', 'PDT', 'PST'],
               ['PDT', 'PST', 'PST', 'PDT'])
        self.assertIn([x.tzname() for x in df_pac.datetime], tzs)
        self.assertIn([x.tzname() for x in tblf.datetime], tzs)

    @unittest.skipIf(pd_version <= (0, 14, 0), 'Need newer version of Pandas')
    def test_dt_methods(self):
        if self.s._protocol in ['http', 'https']:
            tm.TestCase.skipTest(self, 'REST does not support data messages')

        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'datetime.csv')

        df = pd.read_csv(myFile, parse_dates=[0, 1, 2])
        df.sort_values(['datetime'], inplace=True)

        from swat.cas import datamsghandlers as dmh

        pd_dmh = dmh.PandasDataFrame(
            df, dtype={'date': 'date', 'time': 'time'},
            formats={'date': 'nldate', 'time': 'nltime', 'datetime': 'nldatm'},
            labels={'date': 'Date', 'time': 'Time', 'datetime': 'Datetime'})

        tbl = self.s.addtable(table='datetime', caslib=self.srcLib,
                              **pd_dmh.args.addtable).casTable

        with self.assertRaises(TypeError):
            self.table['Model'].dt.year

        # year
        self.assertColsEqual(df.date.dt.year, tbl.date.dt.year, sort=True)
        self.assertColsEqual(df.time.dt.year, tbl.time.dt.year, sort=True)
        self.assertColsEqual(df.datetime.dt.year, tbl.datetime.dt.year, sort=True)

        # month
        self.assertColsEqual(df.date.dt.month, tbl.date.dt.month, sort=True)
        self.assertColsEqual(df.time.dt.month, tbl.time.dt.month, sort=True)
        self.assertColsEqual(df.datetime.dt.month, tbl.datetime.dt.month, sort=True)

        # day
        self.assertColsEqual(df.date.dt.day, tbl.date.dt.day, sort=True)
        self.assertColsEqual(df.time.dt.day, tbl.time.dt.day, sort=True)
        self.assertColsEqual(df.datetime.dt.day, tbl.datetime.dt.day, sort=True)

        # hour
        self.assertColsEqual(df.date.dt.hour, tbl.date.dt.hour, sort=True)
        self.assertColsEqual(df.time.dt.hour, tbl.time.dt.hour, sort=True)
        self.assertColsEqual(df.datetime.dt.hour, tbl.datetime.dt.hour, sort=True)

        # minute
        self.assertColsEqual(df.date.dt.minute, tbl.date.dt.minute, sort=True)
        self.assertColsEqual(df.time.dt.minute, tbl.time.dt.minute, sort=True)
        self.assertColsEqual(df.datetime.dt.minute, tbl.datetime.dt.minute, sort=True)

        # second
        self.assertColsEqual(df.date.dt.second, tbl.date.dt.second, sort=True)
        self.assertColsEqual(df.time.dt.second, tbl.time.dt.second, sort=True)
        self.assertColsEqual(df.datetime.dt.second, tbl.datetime.dt.second, sort=True)

        # microsecond
        # TODO: Needs to be implemented yet
        self.assertColsEqual(df.date.dt.microsecond,
                             tbl.date.dt.microsecond, sort=True)
        self.assertColsEqual(df.time.dt.microsecond,
                             tbl.time.dt.microsecond, sort=True)
        self.assertColsEqual(df.datetime.dt.microsecond,
                             tbl.datetime.dt.microsecond, sort=True)

        # nanosecond
        # NOTE: nanosecond precision is not supported
        self.assertColsEqual(df.date.dt.nanosecond,
                             tbl.date.dt.nanosecond, sort=True)
        self.assertColsEqual(df.time.dt.nanosecond,
                             tbl.time.dt.nanosecond, sort=True)
        self.assertColsEqual(df.datetime.dt.nanosecond,
                             tbl.datetime.dt.nanosecond, sort=True)

        # week
        self.assertColsEqual(df.date.dt.week,
                             tbl.date.dt.week, sort=True)
        self.assertColsEqual(df.time.dt.week,
                             tbl.time.dt.week, sort=True)
        self.assertColsEqual(df.datetime.dt.week,
                             tbl.datetime.dt.week, sort=True)

        # weekofyear
        self.assertColsEqual(df.date.dt.weekofyear,
                             tbl.date.dt.weekofyear, sort=True)
        self.assertColsEqual(df.time.dt.weekofyear,
                             tbl.time.dt.weekofyear, sort=True)
        self.assertColsEqual(df.datetime.dt.weekofyear,
                             tbl.datetime.dt.weekofyear, sort=True)

        # dayofweek
        self.assertColsEqual(df.date.dt.dayofweek,
                             tbl.date.dt.dayofweek, sort=True)
        self.assertColsEqual(df.time.dt.dayofweek,
                             tbl.time.dt.dayofweek, sort=True)
        self.assertColsEqual(df.datetime.dt.dayofweek,
                             tbl.datetime.dt.dayofweek, sort=True)

        # weekday
        self.assertColsEqual(df.date.dt.weekday,
                             tbl.date.dt.weekday, sort=True)
        self.assertColsEqual(df.time.dt.weekday,
                             tbl.time.dt.weekday, sort=True)
        self.assertColsEqual(df.datetime.dt.weekday,
                             tbl.datetime.dt.weekday, sort=True)

        # dayofyear
        self.assertColsEqual(df.date.dt.dayofyear,
                             tbl.date.dt.dayofyear, sort=True)
        self.assertColsEqual(df.time.dt.dayofyear,
                             tbl.time.dt.dayofyear, sort=True)
        self.assertColsEqual(df.datetime.dt.dayofyear,
                             tbl.datetime.dt.dayofyear, sort=True)

        # quarter
        self.assertColsEqual(df.date.dt.quarter,
                             tbl.date.dt.quarter, sort=True)
        self.assertColsEqual(df.time.dt.quarter,
                             tbl.time.dt.quarter, sort=True)
        self.assertColsEqual(df.datetime.dt.quarter,
                             tbl.datetime.dt.quarter, sort=True)

        # is_month_start
        self.assertColsEqual(df.date.dt.is_month_start,
                             tbl.date.dt.is_month_start, sort=True)
        self.assertColsEqual(df.time.dt.is_month_start,
                             tbl.time.dt.is_month_start, sort=True)
        self.assertColsEqual(df.datetime.dt.is_month_start,
                             tbl.datetime.dt.is_month_start, sort=True)

        # is_month_end
        self.assertColsEqual(df.date.dt.is_month_end,
                             tbl.date.dt.is_month_end, sort=True)
        self.assertColsEqual(df.time.dt.is_month_end,
                             tbl.time.dt.is_month_end, sort=True)
        self.assertColsEqual(df.datetime.dt.is_month_end,
                             tbl.datetime.dt.is_month_end, sort=True)

        # is_quarter_start
        self.assertColsEqual(df.date.dt.is_quarter_start,
                             tbl.date.dt.is_quarter_start, sort=True)
        self.assertColsEqual(df.time.dt.is_quarter_start,
                             tbl.time.dt.is_quarter_start, sort=True)
        self.assertColsEqual(df.datetime.dt.is_quarter_start,
                             tbl.datetime.dt.is_quarter_start, sort=True)

        # is_quarter_end
        self.assertColsEqual(df.date.dt.is_quarter_end,
                             tbl.date.dt.is_quarter_end, sort=True)
        self.assertColsEqual(df.time.dt.is_quarter_end,
                             tbl.time.dt.is_quarter_end, sort=True)
        self.assertColsEqual(df.datetime.dt.is_quarter_end,
                             tbl.datetime.dt.is_quarter_end, sort=True)

        # is_year_start
        self.assertColsEqual(df.date.dt.is_year_start,
                             tbl.date.dt.is_year_start, sort=True)
        self.assertColsEqual(df.time.dt.is_year_start,
                             tbl.time.dt.is_year_start, sort=True)
        self.assertColsEqual(df.datetime.dt.is_year_start,
                             tbl.datetime.dt.is_year_start, sort=True)

        # is_year_end
        self.assertColsEqual(df.date.dt.is_year_end,
                             tbl.date.dt.is_year_end, sort=True)
        self.assertColsEqual(df.time.dt.is_year_end,
                             tbl.time.dt.is_year_end, sort=True)
        self.assertColsEqual(df.datetime.dt.is_year_end,
                             tbl.datetime.dt.is_year_end, sort=True)

        # daysinmonth
        self.assertColsEqual(df.date.dt.daysinmonth,
                             tbl.date.dt.daysinmonth, sort=True)
        self.assertColsEqual(df.time.dt.daysinmonth,
                             tbl.time.dt.daysinmonth, sort=True)
        self.assertColsEqual(df.datetime.dt.daysinmonth,
                             tbl.datetime.dt.daysinmonth, sort=True)

        # days_in_month
        self.assertColsEqual(df.date.dt.days_in_month,
                             tbl.date.dt.days_in_month, sort=True)
        self.assertColsEqual(df.time.dt.days_in_month,
                             tbl.time.dt.days_in_month, sort=True)
        self.assertColsEqual(df.datetime.dt.days_in_month,
                             tbl.datetime.dt.days_in_month, sort=True)

    @unittest.skipIf(pd_version <= (0, 14, 0), 'Need newer version of Pandas')
    def test_sas_dt_methods(self):
        if self.s._protocol in ['http', 'https']:
            tm.TestCase.skipTest(self, 'REST does not support data messages')

        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'datetime.csv')

        df = pd.read_csv(myFile, parse_dates=[0, 1, 2])
        df.sort_values(['datetime'], inplace=True)
        df2 = df[:]

        from swat.cas.utils.datetime import (python2sas_date,
                                             python2sas_time,
                                             python2sas_datetime)
        df2['date'] = df2['date'].apply(python2sas_date)
        df2['datetime'] = df2['datetime'].apply(python2sas_datetime)
        df2['time'] = df2['time'].apply(python2sas_time)

        from swat.cas import datamsghandlers as dmh

        pd_dmh = dmh.PandasDataFrame(
            df2,
            formats={'date': 'nldate', 'time': 'nltime', 'datetime': 'nldatm'},
            labels={'date': 'Date', 'time': 'Time', 'datetime': 'Datetime'})

        tbl = self.s.addtable(table='datetime', caslib=self.srcLib,
                              **pd_dmh.args.addtable).casTable

        with self.assertRaises(TypeError):
            self.table['Model'].dt.year

        # year
        self.assertColsEqual(df.date.dt.year, tbl.date.dt.year, sort=True)
        self.assertColsEqual(df.time.dt.year, tbl.time.dt.year, sort=True)
        self.assertColsEqual(df.datetime.dt.year, tbl.datetime.dt.year, sort=True)

        # month
        self.assertColsEqual(df.date.dt.month, tbl.date.dt.month, sort=True)
        self.assertColsEqual(df.time.dt.month, tbl.time.dt.month, sort=True)
        self.assertColsEqual(df.datetime.dt.month, tbl.datetime.dt.month, sort=True)

        # day
        self.assertColsEqual(df.date.dt.day, tbl.date.dt.day, sort=True)
        self.assertColsEqual(df.time.dt.day, tbl.time.dt.day, sort=True)
        self.assertColsEqual(df.datetime.dt.day, tbl.datetime.dt.day, sort=True)

        # hour
        self.assertColsEqual(df.date.dt.hour, tbl.date.dt.hour, sort=True)
        self.assertColsEqual(df.time.dt.hour, tbl.time.dt.hour, sort=True)
        self.assertColsEqual(df.datetime.dt.hour, tbl.datetime.dt.hour, sort=True)

        # minute
        self.assertColsEqual(df.date.dt.minute, tbl.date.dt.minute, sort=True)
        self.assertColsEqual(df.time.dt.minute, tbl.time.dt.minute, sort=True)
        self.assertColsEqual(df.datetime.dt.minute, tbl.datetime.dt.minute, sort=True)

        # second
        self.assertColsEqual(df.date.dt.second, tbl.date.dt.second, sort=True)
        self.assertColsEqual(df.time.dt.second, tbl.time.dt.second, sort=True)
        self.assertColsEqual(df.datetime.dt.second, tbl.datetime.dt.second, sort=True)

        # microsecond
        # TODO: Needs to be implemented yet
        self.assertColsEqual(df.date.dt.microsecond,
                             tbl.date.dt.microsecond, sort=True)
        self.assertColsEqual(df.time.dt.microsecond,
                             tbl.time.dt.microsecond, sort=True)
        self.assertColsEqual(df.datetime.dt.microsecond,
                             tbl.datetime.dt.microsecond, sort=True)

        # nanosecond
        # NOTE: nanosecond precision is not supported
        self.assertColsEqual(df.date.dt.nanosecond,
                             tbl.date.dt.nanosecond, sort=True)
        self.assertColsEqual(df.time.dt.nanosecond,
                             tbl.time.dt.nanosecond, sort=True)
        self.assertColsEqual(df.datetime.dt.nanosecond,
                             tbl.datetime.dt.nanosecond, sort=True)

        # week
        self.assertColsEqual(df.date.dt.week,
                             tbl.date.dt.week, sort=True)
        self.assertColsEqual(df.time.dt.week,
                             tbl.time.dt.week, sort=True)
        self.assertColsEqual(df.datetime.dt.week,
                             tbl.datetime.dt.week, sort=True)

        # weekofyear
        self.assertColsEqual(df.date.dt.weekofyear,
                             tbl.date.dt.weekofyear, sort=True)
        self.assertColsEqual(df.time.dt.weekofyear,
                             tbl.time.dt.weekofyear, sort=True)
        self.assertColsEqual(df.datetime.dt.weekofyear,
                             tbl.datetime.dt.weekofyear, sort=True)

        # dayofweek
        self.assertColsEqual(df.date.dt.dayofweek,
                             tbl.date.dt.dayofweek, sort=True)
        self.assertColsEqual(df.time.dt.dayofweek,
                             tbl.time.dt.dayofweek, sort=True)
        self.assertColsEqual(df.datetime.dt.dayofweek,
                             tbl.datetime.dt.dayofweek, sort=True)

        # weekday
        self.assertColsEqual(df.date.dt.weekday,
                             tbl.date.dt.weekday, sort=True)
        self.assertColsEqual(df.time.dt.weekday,
                             tbl.time.dt.weekday, sort=True)
        self.assertColsEqual(df.datetime.dt.weekday,
                             tbl.datetime.dt.weekday, sort=True)

        # dayofyear
        self.assertColsEqual(df.date.dt.dayofyear,
                             tbl.date.dt.dayofyear, sort=True)
        self.assertColsEqual(df.time.dt.dayofyear,
                             tbl.time.dt.dayofyear, sort=True)
        self.assertColsEqual(df.datetime.dt.dayofyear,
                             tbl.datetime.dt.dayofyear, sort=True)

        # quarter
        self.assertColsEqual(df.date.dt.quarter,
                             tbl.date.dt.quarter, sort=True)
        self.assertColsEqual(df.time.dt.quarter,
                             tbl.time.dt.quarter, sort=True)
        self.assertColsEqual(df.datetime.dt.quarter,
                             tbl.datetime.dt.quarter, sort=True)

        # is_month_start
        self.assertColsEqual(df.date.dt.is_month_start,
                             tbl.date.dt.is_month_start, sort=True)
        self.assertColsEqual(df.time.dt.is_month_start,
                             tbl.time.dt.is_month_start, sort=True)
        self.assertColsEqual(df.datetime.dt.is_month_start,
                             tbl.datetime.dt.is_month_start, sort=True)

        # is_month_end
        self.assertColsEqual(df.date.dt.is_month_end,
                             tbl.date.dt.is_month_end, sort=True)
        self.assertColsEqual(df.time.dt.is_month_end,
                             tbl.time.dt.is_month_end, sort=True)
        self.assertColsEqual(df.datetime.dt.is_month_end,
                             tbl.datetime.dt.is_month_end, sort=True)

        # is_quarter_start
        self.assertColsEqual(df.date.dt.is_quarter_start,
                             tbl.date.dt.is_quarter_start, sort=True)
        self.assertColsEqual(df.time.dt.is_quarter_start,
                             tbl.time.dt.is_quarter_start, sort=True)
        self.assertColsEqual(df.datetime.dt.is_quarter_start,
                             tbl.datetime.dt.is_quarter_start, sort=True)

        # is_quarter_end
        self.assertColsEqual(df.date.dt.is_quarter_end,
                             tbl.date.dt.is_quarter_end, sort=True)
        self.assertColsEqual(df.time.dt.is_quarter_end,
                             tbl.time.dt.is_quarter_end, sort=True)
        self.assertColsEqual(df.datetime.dt.is_quarter_end,
                             tbl.datetime.dt.is_quarter_end, sort=True)

        # is_year_start
        self.assertColsEqual(df.date.dt.is_year_start,
                             tbl.date.dt.is_year_start, sort=True)
        self.assertColsEqual(df.time.dt.is_year_start,
                             tbl.time.dt.is_year_start, sort=True)
        self.assertColsEqual(df.datetime.dt.is_year_start,
                             tbl.datetime.dt.is_year_start, sort=True)

        # is_year_end
        self.assertColsEqual(df.date.dt.is_year_end,
                             tbl.date.dt.is_year_end, sort=True)
        self.assertColsEqual(df.time.dt.is_year_end,
                             tbl.time.dt.is_year_end, sort=True)
        self.assertColsEqual(df.datetime.dt.is_year_end,
                             tbl.datetime.dt.is_year_end, sort=True)

        # daysinmonth
        self.assertColsEqual(df.date.dt.daysinmonth,
                             tbl.date.dt.daysinmonth, sort=True)
        self.assertColsEqual(df.time.dt.daysinmonth,
                             tbl.time.dt.daysinmonth, sort=True)
        self.assertColsEqual(df.datetime.dt.daysinmonth,
                             tbl.datetime.dt.daysinmonth, sort=True)

        # days_in_month
        self.assertColsEqual(df.date.dt.days_in_month,
                             tbl.date.dt.days_in_month, sort=True)
        self.assertColsEqual(df.time.dt.days_in_month,
                             tbl.time.dt.days_in_month, sort=True)
        self.assertColsEqual(df.datetime.dt.days_in_month,
                             tbl.datetime.dt.days_in_month, sort=True)

    @unittest.skipIf(pd_version >= (0, 21, 0), 'Deprecated in pandas')
    def test_from_csv(self):
        df = self.get_cars_df()

        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        df.to_csv(tmp.name, index=False)

        df2 = df.from_csv(tmp.name, index_col=None)
        tbl = self.table.from_csv(self.s, tmp.name, index_col=None)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)
        self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'varchar']))

        # Force addtable
        df2 = df.from_csv(tmp.name, index_col=None)
        tbl = self.table.from_csv(self.s, tmp.name, index_col=None, use_addtable=True)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'varchar']))
        else:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'int64', 'varchar']))

        os.remove(tmp.name)

    def test_from_dict(self):
        df = self.get_cars_df()
        dfdict = df.to_dict()

        df2 = df.from_dict(dfdict)
        tbl = self.table.from_dict(self.s, dfdict)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)
        self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'varchar']))

        # Force addtable
        df2 = df.from_dict(dfdict)
        tbl = self.table.from_dict(self.s, dfdict, use_addtable=True)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'varchar']))
        else:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'int64', 'varchar']))

    @unittest.skipIf(pd_version >= (0, 23, 0), 'Deprecated in pandas')
    def test_from_items(self):
        df = self.get_cars_df()
        dfitems = tuple(df.to_dict().items())

        df2 = df.from_items(dfitems)
        tbl = self.table.from_items(self.s, dfitems)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)
        self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'varchar']))

        # Force addtable
        df2 = df.from_items(dfitems)
        tbl = self.table.from_items(self.s, dfitems, use_addtable=True)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'varchar']))
        else:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'int64', 'varchar']))

    def test_from_records(self):
        df = self.get_cars_df()
        dfrec = df.to_records()

        df2 = df.from_records(dfrec)
        tbl = self.table.from_records(self.s, dfrec)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)

        # Force addtable
        df2 = df.from_records(dfrec)
        tbl = self.table.from_records(self.s, dfrec, use_addtable=True)

        self.assertEqual(len(df2), len(tbl))
        self.assertTablesEqual(df2, tbl, sortby=SORT_KEYS)

        if self.s._protocol in ['http', 'https']:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'varchar']))
        else:
            self.assertTrue(set(tbl.dtypes.unique()), set(['double', 'int64', 'varchar']))

    def test_info(self):
        # df = self.get_cars_df()
        tbl = self.table

        try:
            from StringIO import StringIO
        except ImportError:
            from io import StringIO

        info = StringIO()
        tbl.info(buf=info)
        info.seek(0)
        info = info.read()

        self.assertRegex(info, r'Make\s+428\s+False\s+char')
        self.assertRegex(info, r'Cylinders\s+426\s+True\s+double')
        self.assertRegex(info, r'dtypes: char\(5\), double\(10\)')

        # No columns with missing values
        info = StringIO()
        tbl[['Make', 'Model']].info(buf=info)
        info.seek(0)
        info = info.read()

        self.assertRegex(info, r'Make\s+428\s+False\s+char')
        self.assertRegex(info, r'Model\s+428\s+False\s+char')
        self.assertRegex(info, r'dtypes: char\(2\)')

        # max_cols=
        info = StringIO()
        tbl.info(buf=info, max_cols=2)
        info.seek(0)
        info = info.read()

        self.assertRegex(info, r'Make\s+428\s+False\s+char')
        self.assertRegex(info, r'Model\s+428\s+False\s+char')
        self.assertRegex(info, r'dtypes: char\(5\), double\(10\)')

        # verbose=False
        info = StringIO()
        tbl.info(buf=info, verbose=False)
        info.seek(0)
        info = info.read()

        self.assertRegex(info, r'Columns: 15 entries, Make to Length')
        self.assertRegex(info, r'dtypes: char\(5\), double\(10\)')

    def test_to_pickle(self):
        df = self.get_cars_df()
        tbl = self.table

        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        tbl.to_pickle(tmp.name)

        df2 = pd.read_pickle(tmp.name)

        self.assertTablesEqual(df, df2)

        os.remove(tmp.name)

    def test_to_csv(self):
        df = self.get_cars_df()
        tbl = self.table

        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        tbl.to_csv(tmp.name, index=False)

        df2 = pd.read_csv(tmp.name)

        self.assertTablesEqual(df, df2)

        os.remove(tmp.name)

    @unittest.skip('Need way to verify HDF installation')
    def test_to_hdf(self):
        df = self.get_cars_df()
        tbl = self.table

        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        try:
            tbl.to_hdf(tmp.name, 'cars')
        except ImportError:
            tm.TestCase.skipTest(self, 'Need PyTables installed')

        df2 = pd.read_hdf(tmp.name, 'cars')

        self.assertTablesEqual(df, df2)

        os.remove(tmp.name)

    def test_to_excel(self):
        df = self.get_cars_df()
        tbl = self.table

        import tempfile

        tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        tmp.close()

        try:
            tbl.to_excel(tmp.name)
        except Exception:
            tm.TestCase.skipTest(self, 'Need openpyxl installed')

        try:
            df2 = pd.read_excel(tmp.name)
        except Exception:
            tm.TestCase.skipTest(self, 'Need openpyxl installed')

        self.assertEqual(
            sorted(re.split(df.to_csv(index=False).replace('.0', ''), r'[\r\n]+')),
            sorted(re.split(df2.to_csv(index=False).replace('.0', ''), r'[\r\n]+')))

        os.remove(tmp.name)

    def test_to_json(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        df.index = range(len(df))
        tbl = self.table.sort_values(SORT_KEYS)

        import re
        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        tbl.to_json(tmp.name)

        df2 = pd.read_json(tmp.name)[['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                                      'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                                      'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                                      'Wheelbase', 'Length']]
        df2.sort_values(SORT_KEYS, inplace=True)
        df2.index = range(len(df2))

        csv = re.sub(r'\.0(,|\n|\r)', r'\1', df.head(100).to_csv(index=False))
        csv2 = re.sub(r'\.0(,|\n|\r)', r'\1', df2.head(100).to_csv(index=False))
        csv2 = re.sub(r'00000+\d+(,|\n|\r)', r'\1', csv2)
        self.assertEqual(sorted(csv.split('\n')),
                         sorted(csv2.split('\n')))

        os.remove(tmp.name)

    def test_to_html(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        df.index = range(len(df))
        tbl = self.table.sort_values(SORT_KEYS)

        html = tbl.to_html(index=False)

        df2 = pd.read_html(html)[0]

        df['Model'] = df['Model'].str.strip()

        import re

        csv = re.sub(r'\.0(,|\n)', r'\1', df.head(100).to_csv(index=False))
        csv2 = re.sub(r'\.0(,|\n)', r'\1', df2.head(100).to_csv(index=False))
        csv2 = re.sub(r'00000+\d+(,|\n)', r'\1', csv2)
        self.assertEqual(csv, csv2)

    def test_to_latex(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        df.index = range(len(df))
        tbl = self.table.sort_values(SORT_KEYS)

        tblltx = tbl.to_latex(index=False)
        dfltx = df.to_latex(index=False)

        self.assertEqual(dfltx[:1000], tblltx[:1000])

    def test_to_stata(self):
        df = self.get_cars_df()
        tbl = self.table

        import tempfile

        tmp = tempfile.NamedTemporaryFile(suffix='.dta', delete=False)
        tmp.close()

        tbl.to_stata(tmp.name)

        df2 = pd.read_stata(tmp.name)[['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                                       'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                                       'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                                       'Wheelbase', 'Length']]

        self.assertTablesEqual(df, df2)

        os.remove(tmp.name)

    @unittest.skipIf(pd_version >= (1, 0, 0), 'Need newer version of Pandas')
    def test_to_msgpack(self):
        df = self.get_cars_df()
        df.index = range(len(df))
        tbl = self.table

        import tempfile

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        tbl.to_msgpack(tmp.name)

        df2 = pd.read_msgpack(tmp.name)

        self.assertTablesEqual(tbl, df2, sortby=SORT_KEYS)

        os.remove(tmp.name)

    def test_to_records(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblrec = [list(x) for x in list(tbl.to_records(index=False))]
        dfrec = [list(x) for x in list(df.to_records(index=False))]

        self.assertEqual(dfrec[:90], tblrec[:90])

    def test_to_string(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblstr = re.sub(r'^.*(Make)', r'\1', tbl.to_string(index=False), flags=re.S)
        dfstr = re.sub(r'^.*(Make)', r'\1', df.to_string(index=False), flags=re.S)

        self.assertEqual(len(tblstr), len(dfstr))
        self.assertEqual(dfstr[:5000], tblstr[:5000])

    def test_column_abs(self):
        df = self.get_cars_df()
        tbl = self.table

        df['NegMSRP'] = -df['MSRP']
        tbl['NegMSRP'] = -tbl['MSRP']

        self.assertColsEqual(df['NegMSRP'], tbl['NegMSRP'])

        with self.assertRaises(TypeError):
            df['Make'].abs()

    def test_column_any(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(df['Cylinders'].any(), tbl['Cylinders'].any())

        # TODO: The where clause generated by this doesn't resolve yet
#       self.assertEqual(df['Make'].any(), tbl['Make'].any())

    def test_column_all(self):
        df = self.get_cars_df()
        tbl = self.table

        self.assertEqual(df['Cylinders'].all(), tbl['Cylinders'].all())

        # TODO: The where clause generated by this doesn't resolve yet
#       self.assertEqual(df['Make'].any(), tbl['Make'].any())

    def test_append_orderby(self):
        # Test appending
        self.table.orderby = []
        self.assertEqual(self.table.orderby, [])

        out = self.table.append_orderby('Make', inplace=False)
        self.assertEqual(self.table.orderby, [])
        self.assertEqual(out, [dict(name='Make')])

        out = self.table.append_orderby('Make')
        self.assertTrue(out is None)
        self.assertEqual(self.table.orderby, [dict(name='Make')])

        self.table.append_orderby(dict(name='Model'))
        self.assertEqual(self.table.orderby, [dict(name='Make'), dict(name='Model')])

        self.table.append_orderby([dict(name='MSRP'), 'Invoice'])
        self.assertEqual(self.table.orderby, [dict(name='Make'), dict(name='Model'),
                                              dict(name='MSRP'), dict(name='Invoice')])

        # Test setting scalar values
        self.table.orderby = 'Make'
        self.assertEqual(self.table.orderby, 'Make')

        out = self.table.append_orderby('Model')
        self.assertTrue(out is None)
        self.assertEqual(self.table.orderby, [dict(name='Make'), dict(name='Model')])

        self.table.orderby = dict(name='Make')
        self.assertEqual(self.table.orderby, dict(name='Make'))

        out = self.table.append_orderby(dict(name='Model'))
        self.assertTrue(out is None)
        self.assertEqual(self.table.orderby, [dict(name='Make'), dict(name='Model')])

        # Test setting empty values
        self.table.orderby = []
        self.assertEqual(self.table.orderby, [])

        out = self.table.append_orderby([None, 'Model'])
        self.assertTrue(out is None)
        self.assertEqual(self.table.orderby, [dict(name='Model')])

        out = self.table.append_orderby('')
        self.assertTrue(out is None)
        self.assertEqual(self.table.orderby, [dict(name='Model')])

    def test_groupby(self):
        df = self.get_cars_df()
        tbl = self.table

        tbl.groupby('Make')
        df.groupby('Make')

    def test_groupby_iter(self):
        df = self.get_cars_df()
        tbl = self.table

        for dfgrp, tblgrp in zip(sorted(df.groupby(['Make', 'MPG_City'])),
                                 sorted(tbl.groupby(['Make', 'MPG_City']))):
            self.assertTrue('CASTable' in tblgrp[1].__class__.__name__)
            self.assertEqual(dfgrp[0], tblgrp[0])

    def test_groupby_get_group(self):
        df = self.get_cars_df()
        tbl = self.table

        dfgrp = df.groupby(['Make', 'MPG_City'])
        tblgrp = tbl.groupby(['Make', 'MPG_City'])

        self.assertEqual(dfgrp.get_group(('Acura', 22)).to_csv(index=False),
                         tblgrp.get_group(('Acura', 22)).to_csv(index=False))

    def test_drop(self):
        tbl = self.table

        all_cols = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain',
                    'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                    'Horsepower', 'MPG_City', 'MPG_Highway', 'Weight',
                    'Wheelbase', 'Length']

        self.assertEqual(list(tbl.columns), all_cols)

        # Drop 1
        tbl2 = tbl.drop('Make', axis=1)

        self.assertEqual(list(tbl.columns), all_cols)
        self.assertEqual(list(tbl2.columns), all_cols[1:])

        # Drop multiple
        tbl2 = tbl.drop(['MPG_City', 'MPG_Highway'], axis=1)

        self.assertEqual(list(tbl.columns), all_cols)
        self.assertEqual(list(tbl2.columns),
                         [x for x in all_cols if x not in ['MPG_City', 'MPG_Highway']])

        # Drop from selected list
        tbl2 = tbl[['Make', 'Model', 'Type', 'Origin']]
        tbl3 = tbl2.drop('Type', axis=1)

        self.assertEqual(list(tbl2.columns), ['Make', 'Model', 'Type', 'Origin'])
        self.assertEqual(list(tbl3.columns), ['Make', 'Model', 'Origin'])

        # In place
        out = tbl.drop(['Make', 'Model'], axis=1, inplace=True)

        self.assertTrue(out is None)
        self.assertEqual(list(tbl.columns), all_cols[2:])

        # Wrong axis
        with self.assertRaises(NotImplementedError):
            tbl.drop('Type', axis=0)

        # Non-existent column
        with self.assertRaises(IndexError):
            tbl.drop('Foo', axis=1)

    def test_isin(self):
        try:
            import numexpr
        except ImportError:
            tm.TestCase.skipTest(self, 'numexpr is not installed')

        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        self.assertColsEqual(df['Cylinders'].isin([6, 8]),
                             tbl['Cylinders'].isin([6, 8]))

        df = self.get_cars_df().sort_values('Make')
        tbl = self.table.sort_values('Make')

        self.assertColsEqual(df['Make'].isin(['Acura', 'BMW', 'Porsche']),
                             tbl['Make'].isin(['Acura', 'BMW', 'Porsche']))

        self.assertColsEqual(df['Make'].isin(('Acura', 'BMW', 'Porsche')),
                             tbl['Make'].isin(('Acura', 'BMW', 'Porsche')))

        self.assertColsEqual(df['Make'].isin({'Acura', 'BMW', 'Porsche'}),
                             tbl['Make'].isin({'Acura', 'BMW', 'Porsche'}))

        self.assertColsEqual(
            df['Make'].isin(df.query('Make == "Acura" or Make == "BMW" '
                                     'or Make == "Porsche"')['Make']),
            tbl['Make'].isin(tbl.query('Make = "Acura" or Make = "BMW" '
                                       'or Make = "Porsche"')['Make']))

        self.assertColsEqual(
            df['Make'].isin(df.query('Make == "Acura" or Make == "BMW" '
                                     'or Make == "Porsche"')['Make']),
            tbl['Make'].isin(df.query('Make == "Acura" or Make == "BMW" '
                                      'or Make == "Porsche"')['Make']))

    def test_droptable(self):
        import swat.tests as st

        pathname = os.path.join(os.path.dirname(st.__file__),
                                'datasources', 'cars_single.sashdat')

        cars1 = tm.load_data(self.s, pathname, self.server_type,
                             casout={'name': 'cars1', 'caslib': self.srcLib}).casTable
        cars2 = tm.load_data(self.s, pathname, self.server_type,  # noqa: F841
                             casout={'name': 'cars2', 'caslib': self.srcLib}).casTable
        cars3 = tm.load_data(self.s, pathname, self.server_type,
                             casout={'name': 'cars3', 'caslib': self.srcLib}).casTable

        out = self.s.tableinfo(caslib=self.srcLib).TableInfo['Name'].tolist()
        self.assertTrue('CARS1' in out)
        self.assertTrue('CARS2' in out)
        self.assertTrue('CARS3' in out)

        cars1.droptable('cars2', caslib=self.srcLib)

        out = self.s.tableinfo(caslib=self.srcLib).TableInfo['Name'].tolist()
        self.assertTrue('CARS1' in out)
        self.assertTrue('CARS2' not in out)
        self.assertTrue('CARS3' in out)

        cars3.droptable(name='cars1', caslib=self.srcLib)

        out = self.s.tableinfo(caslib=self.srcLib).TableInfo['Name'].tolist()
        self.assertTrue('CARS1' not in out)
        self.assertTrue('CARS2' not in out)
        self.assertTrue('CARS3' in out)

        cars3.droptable()

        out = self.s.tableinfo(caslib=self.srcLib).TableInfo['Name'].tolist()
        self.assertTrue('CARS1' not in out)
        self.assertTrue('CARS2' not in out)
        self.assertTrue('CARS3' not in out)

    def test_view_sort(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)

        sorttbl = self.table.sort_values(SORT_KEYS)

        view = self.table.to_view(name='tblview')
        sortview = view.sort_values(SORT_KEYS)

        self.assertTablesEqual(df.head(), sorttbl.head())

        # Just run this to make sure it doesn't blow up
        view.head()

        if self.server_version < (3, 5):
            with self.assertRaises(swat.SWATError):
                sortview.head()
        else:
            sortview.head()

    def test_to_frame_ordering(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        sorttbl = self.table.sort_values(SORT_KEYS).to_frame(maxrows=20)
        self.assertTablesEqual(df, sorttbl)

    def test_fillna(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        sorttbl = self.table.sort_values(SORT_KEYS)

        self.assertTablesEqual(df.fillna(value=50),
                               sorttbl.fillna(value=50))

        self.assertTablesEqual(df.fillna(value={'Cylinders': 50}),
                               sorttbl.fillna(value={'Cylinders': 50}))

        self.assertTablesEqual(df.fillna(value=pd.Series([50], index=['Cylinders'])),
                               sorttbl.fillna(value=pd.Series([50], index=['Cylinders'])))

        # TODO: This should work according to the Pandas doc, but I can't
        #       figure out what form it wants the arguments in.
        # self.assertTablesEqual(df.fillna(value=pd.DataFrame([[50, 40]],
        #                                  columns=['Cylinders', 'Foo'])),
        #                        sorttbl.fillna(value=pd.DataFrame([[50, 40]],
        #                                       columns=['Cylinders', 'Foo'])))

        df.fillna(value={'Cylinders': 50}, inplace=True),
        self.assertTrue(sorttbl.fillna(value={'Cylinders': 50}, inplace=True) is None)
        self.assertTablesEqual(df, sorttbl)

    def test_dropna(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        sorttbl = self.table.sort_values(SORT_KEYS)

        self.assertTablesEqual(df.dropna(), sorttbl.dropna())

        self.assertTablesEqual(df.dropna(how='all'), sorttbl.dropna(how='all'))

    def test_replace(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        sorttbl = self.table.sort_values(SORT_KEYS)

        # Scalars
        self.assertTablesEqual(df.replace('BMW', 'AAAAA'),
                               sorttbl.replace('BMW', 'AAAAA'))
        self.assertTablesEqual(df.replace(8, 1000),
                               sorttbl.replace(8, 1000))
        self.assertTablesEqual(df.replace(6.00, 2000),
                               sorttbl.replace(6.00, 2000))

        # Mixed types - Pandas automatically changes type, but data step can't
        df2 = df.replace(8.00, '3000')
        df2['Cylinders'] = df2['Cylinders'].astype(float)
        self.assertTablesEqual(df2, sorttbl.replace(8.00, '3000'))

        if pd_version < (0, 20, 0):
            self.assertTablesEqual(df.replace('6.00', 4000),
                                   sorttbl.replace('6.00', 4000))

        # Lists
        self.assertTablesEqual(df.replace(['BMW', 'Audi'], ['FFFFF', 'GGGGG']),
                               sorttbl.replace(['BMW', 'Audi'], ['FFFFF', 'GGGGG']))
        self.assertTablesEqual(df.replace([6, 8], [5000, 6000]),
                               sorttbl.replace([6, 8], [5000, 6000]))

        self.assertTablesEqual(df.replace(['BMW', 'Audi'], 'HHHHH'),
                               sorttbl.replace(['BMW', 'Audi'], 'HHHHH'))
        self.assertTablesEqual(df.replace([6, 8], 7000),
                               sorttbl.replace([6, 8], 7000))

        with self.assertRaises(TypeError):
            sorttbl.replace('BMW', ['IIIII'])
        with self.assertRaises(TypeError):
            sorttbl.replace(6, [8000])

# TODO: This works for data step, but I can't figure out how Pandas works
        # Series
#       before = pd.Series(['BMW', 'Audi'])
#       after = pd.Series(['IIIII', 'JJJJJ'])
#       self.assertTablesEqual(df.replace(before, after),
#                              sorttbl.replace(before, after))
#       before = pd.Series([6, 8])
#       after = pd.Series([8000, 9000])
#       self.assertTablesEqual(df.replace(before, after),
#                              sorttbl.replace(before, after))

        # Dictionaries
        self.assertTablesEqual(df.replace({'Make': {'BMW': 'CCCCC', 'Foo': 'Bar'}}),
                               sorttbl.replace({'Make': {'BMW': 'CCCCC', 'Foo': 'Bar'}}))

        self.assertTablesEqual(df.replace({'Make': {'BMW': 'DDDDD'},
                                           'Model': {'4dr': 'E'}}),
                               sorttbl.replace({'Make': {'BMW': 'DDDDD'},
                                                'Model': {'4dr': 'E'}}))

        # Mixed types - This doesn't work well.  Numbers get padded.
        df2 = df.replace({'Make': {'BMW': '           20'}})
        self.assertTablesEqual(df2, sorttbl.replace({'Make': {'BMW': 20}}))

    def test_replace_regex(self):
        if self.server_version < (3, 2):
            tm.TestCase.skipTest(self, 'Requires CAS version > 3.01.')

        df = self.get_cars_df().sort_values(SORT_KEYS)
        sorttbl = self.table.sort_values(SORT_KEYS)

        import re

        # Scalars
        self.assertTablesEqual(
            df.replace(r'B(\w+W)', r'Q\1AAA', regex=True),
            sorttbl.replace(r'B(\w+W)', r'Q\1AAA', regex=True))
        self.assertTablesEqual(
            df.replace(regex=r'B(\w+W)', value=r'Q\1BBB'),
            sorttbl.replace(regex=r'B(\w+W)', value=r'Q\1BBB'))
        self.assertTablesEqual(
            df.replace(re.compile(r'B(\w+W)', re.I), r'Q\1CCC'),
            sorttbl.replace(re.compile(r'B(\w+W)', re.I), r'Q\1CCC'))
        self.assertTablesEqual(
            df.replace(re.compile(r'b(\w+w)', re.I), r'Q\1DDD'),
            sorttbl.replace(re.compile(r'b(\w+w)', re.I), r'Q\1DDD'))
        self.assertTablesEqual(
            df.replace(regex=re.compile(r'b(\w+w)', re.I), value=r'Q\1EEE'),
            sorttbl.replace(regex=re.compile(r'b(\w+w)', re.I), value=r'Q\1EEE'))

        self.assertTablesEqual(df.replace(8, 1000, regex=True),
                               sorttbl.replace(8, 1000, regex=True)),
        self.assertTablesEqual(df.replace(6.00, 2000, regex=True),
                               sorttbl.replace(6.00, 2000, regex=True))

        # Mixed types - Pandas automatically changes type, but data step can't
        with self.assertRaises(TypeError):
            sorttbl.replace(8.00, '3000', regex=True)
        with self.assertRaises(TypeError):
            sorttbl.replace('6.00', 4000, regex=True)

        return

        # Lists
        self.assertTablesEqual(
            df.replace([re.compile(r'B(\w+)W'), 'Audi'], [r'F\1F', 'GGGGG']),
            sorttbl.replace([re.compile(r'B(\w+)W'), 'Audi'], [r'F\1F', 'GGGGG']))
        self.assertTablesEqual(
            df.replace([6, 8], [5000, 6000], regex=True),
            sorttbl.replace([6, 8], [5000, 6000], regex=True))

        self.assertTablesEqual(
            df.replace([re.compile(r'B(\w+)W'), re.compile(r'A[ud]*i')], 'HHHHH'),
            sorttbl.replace([re.compile(r'B(\w+)W'), re.compile(r'A[ud*]i')], 'HHHHH'))
        self.assertTablesEqual(df.replace([6, 8], 7000, regex=True),
                               sorttbl.replace([6, 8], 7000, regex=True))

        with self.assertRaises(TypeError):
            sorttbl.replace(re.compile(r'B\w+W'), ['IIIII'])
        with self.assertRaises(TypeError):
            sorttbl.replace(6, [8000], regex=True)

        # TODO: This works for data step, but I can't figure out how Pandas works
        # Series
        # before = pd.Series(['BMW', 'Audi'])
        # after = pd.Series(['IIIII', 'JJJJJ'])
        # self.assertTablesEqual(df.replace(before, after),
        #                        sorttbl.replace(before, after))
        # before = pd.Series([6, 8])
        # after = pd.Series([8000, 9000])
        # self.assertTablesEqual(df.replace(before, after),
        #                        sorttbl.replace(before, after))

        # Dictionaries
        self.assertTablesEqual(
            df.replace({'Make': {re.compile(r'B\w+W'): r'J\1J', 'Foo': 'Bar'}}),
            sorttbl.replace({'Make': {re.compile(r'B\w+W'): r'J\1J', 'Foo': 'Bar'}}))

        self.assertTablesEqual(
            df.replace({'Make': {'BMW': 'KKKKK'},
                        'Model': {re.compile(r'[456]dr'): 'L'}}),
            sorttbl.replace({'Make': {'BMW': 'KKKKK'},
                             'Model': {re.compile(r'[456]dr'): 'L'}}))

        # Mixed types - This doesn't work well.  Numbers get padded.
        df2 = df.replace({'Make': {re.compile(r'B\w+W'): '           20'}})
        self.assertTablesEqual(df2, sorttbl.replace({'Make': {re.compile(r'B\w+W'): 20}}))

    def test_partition_inputs(self):
        tbl = self.table

        tbl['One'] = 1
        tbl['Two'] = 2

        colinfo = tbl.columninfo()['ColumnInfo'].set_index('Column').T

        out = tbl.partition(casout=dict(name='test_partition_table', replace=True))
        pcolinfo = out.casTable.columninfo()['ColumnInfo'].set_index('Column').T
        self.assertTablesEqual(colinfo, pcolinfo)

        colinfo = colinfo.drop('ID', errors='ignore').drop('Label', errors='ignore')

        out = tbl[['Model', 'MSRP']].partition(casout=dict(name='test_partition_table',
                                                           replace=True))
        pcolinfo = out.casTable.columninfo()['ColumnInfo'].set_index('Column').T
        pcolinfo = pcolinfo.drop('ID', errors='ignore').drop('Label', errors='ignore')
        self.assertTablesEqual(colinfo[['Model', 'MSRP']], pcolinfo)

        out = tbl[['Two', 'Model', 'One', 'MSRP']].partition(
            casout=dict(name='test_partition_table', replace=True))
        pcolinfo = out.casTable.columninfo()['ColumnInfo'].set_index('Column').T
        pcolinfo = pcolinfo.drop('ID', errors='ignore').drop('Label', errors='ignore')
        self.assertTablesEqual(colinfo[['Two', 'Model', 'One', 'MSRP']], pcolinfo)

    def test_reset_index(self):
        tbl = self.table

        out = tbl.reset_index()
        self.assertTrue(out is not tbl)
        self.assertEqual(tbl.params['name'], out.params['name'])

        out = tbl.reset_index(inplace=True)
        self.assertTrue(out is tbl)

    def test_sample(self):
        tbl = self.table

        # Test n=
        out = tbl.sample(n=12)
        self.assertEqual(len(out), 12)
        self.assertNotEqual(tbl.params['name'], out.params['name'])
        out.droptable()

        # Test frac=
        out = tbl.sample(frac=0.02)
        self.assertEqual(len(out), 9)
        self.assertNotEqual(tbl.params['name'], out.params['name'])
        out.droptable()

        # Test n= and frac=
        with self.assertRaises(ValueError):
            tbl.sample(n=2, frac=0.02)

        # Test no params
        out = tbl.sample()
        self.assertEqual(len(out), 1)
        self.assertNotEqual(tbl.params['name'], out.params['name'])
        out.droptable()

        # Test random_state=
        out1 = tbl.sample(n=10, random_state=123)
        out2 = tbl.sample(n=10, random_state=123)
        self.assertEqual(len(out1), 10)
        self.assertEqual(len(out2), 10)
        self.assertTablesEqual(out1, out2)
        out1.droptable()
        out2.droptable()

    def test_sampling(self):
        tbl = self.table

        # Basic tests
        num_tables = len(tbl.tableinfo().TableInfo)
        samp = tbl.to_frame(sample_pct=0.01, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp), 4)
        self.assertEqual(list(samp.columns), ['Make', 'Model'])
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        num_tables = len(tbl.tableinfo().TableInfo)
        samp = tbl.to_frame(sample_pct=0.02, fetchvars=['Make', 'Model', 'Horsepower'])
        self.assertEqual(len(samp), 9)
        self.assertEqual(list(samp.columns), ['Make', 'Model', 'Horsepower'])
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        # Test to= / from= interactions
        num_tables = len(tbl.tableinfo().TableInfo)
        samp = tbl.to_frame(sample_pct=0.02, to=5, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp), 5)
        self.assertEqual(list(samp.columns), ['Make', 'Model'])
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        num_tables = len(tbl.tableinfo().TableInfo)
        samp = tbl.to_frame(sample_pct=0.02, from_=2, to=5, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp), 4)
        self.assertEqual(list(samp.columns), ['Make', 'Model'])
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        # Test swat.options.cas.datasets.max_rows_fetched
        swat.options.cas.dataset.max_rows_fetched = 5

        # to_frame forces everything to get pulled down, so use _fetch here.
        # All internal calls to pull data use _fetch and will obey the
        # max_rows_fetched option.
        num_tables = len(tbl.tableinfo().TableInfo)
        samp = tbl._fetch(sample_pct=0.02, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp), 5)
        self.assertEqual(list(samp.columns), ['Make', 'Model'])
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        swat.options.cas.dataset.max_rows_fetched = 10000

        # Test seed
        num_tables = len(tbl.tableinfo().TableInfo)
        samp1 = tbl.to_frame(sample_pct=0.01, sample_seed=123,
                             fetchvars=['Make', 'Model'])
        samp2 = tbl.to_frame(sample_pct=0.01, sample_seed=123,
                             fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp1), 4)
        self.assertEqual(len(samp2), 4)
        self.assertEqual(list(samp1.columns), ['Make', 'Model'])
        self.assertEqual(list(samp2.columns), ['Make', 'Model'])
        self.assertTablesEqual(samp1, samp2)
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        # Test sample= parameter
        num_tables = len(tbl.tableinfo().TableInfo)
        samp1 = tbl.to_frame(sample=True, to=5, fetchvars=['Make', 'Model'])
        samp2 = tbl.to_frame(sample=True, to=5, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp1), 5)
        self.assertEqual(len(samp2), 5)
        self.assertEqual(list(samp1.columns), ['Make', 'Model'])
        self.assertEqual(list(samp2.columns), ['Make', 'Model'])
        self.assertNotEqual(samp1.to_dict(), samp2.to_dict())
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        swat.options.cas.dataset.max_rows_fetched = 10

        num_tables = len(tbl.tableinfo().TableInfo)
        samp1 = tbl._fetch(sample=True, fetchvars=['Make', 'Model'])
        samp2 = tbl._fetch(sample=True, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp1), 10)
        self.assertEqual(len(samp2), 10)
        self.assertEqual(list(samp1.columns), ['Make', 'Model'])
        self.assertEqual(list(samp2.columns), ['Make', 'Model'])
        self.assertNotEqual(samp1.to_dict(), samp2.to_dict())
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        swat.reset_option('cas.dataset.max_rows_fetched')

        # Test out-of-bounds sample_pct=
        num_tables = len(tbl.tableinfo().TableInfo)
        with self.assertRaises(ValueError):
            tbl.to_frame(sample_pct=100)
        with self.assertRaises(ValueError):
            tbl.to_frame(sample_pct=0)
        with self.assertRaises(ValueError):
            tbl.to_frame(sample_pct=1)
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        # No options returns self
        self.assertTrue(tbl._sample() is tbl)

        # Test groupby
        tbl.params.groupby = ['Origin']

        num_tables = len(tbl.tableinfo().TableInfo)
        samp = tbl.to_frame(sample_pct=0.01, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp), 4)
        self.assertEqual(list(samp.columns), ['Origin', 'Make', 'Model'])
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

        del tbl.params.groupby

        num_tables = len(tbl.tableinfo().TableInfo)
        samp = tbl.groupby('Origin', as_index=True)\
                  .to_frame(sample_pct=0.01, fetchvars=['Make', 'Model'])
        self.assertEqual(len(samp), 4)
        self.assertEqual(list(samp.columns), ['Make', 'Model'])
        self.assertEqual(list(samp.index.names), ['Origin'])
        self.assertEqual(num_tables, len(tbl.tableinfo().TableInfo))

    def assertPlotsEqual(self, fig1, fig2):
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            tm.TestCase.skipTest(self, 'Need matplotlib to run this test')

        buf1 = io.BytesIO()
        buf2 = io.BytesIO()

        fig1.figure.savefig(buf1, format='png')
        out1 = buf1.getvalue()
        buf1.close()
        plt.close()

        fig2.figure.savefig(buf2, format='png')
        out2 = buf2.getvalue()
        buf2.close()
        plt.close()

        return self.assertEqual(out1, out2)

    def test_hist(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(tbl.hist()[0][0], df.hist()[0][0])
            self.assertPlotsEqual(tbl.hist()[0][1], df.hist()[0][1])
            self.assertPlotsEqual(tbl.hist()[0][2], df.hist()[0][2])

            self.assertPlotsEqual(tbl.hist()[1][0], df.hist()[1][0])
            self.assertPlotsEqual(tbl.hist()[1][1], df.hist()[1][1])
            self.assertPlotsEqual(tbl.hist()[1][2], df.hist()[1][2])

            self.assertPlotsEqual(tbl.hist()[2][0], df.hist()[2][0])
            self.assertPlotsEqual(tbl.hist()[2][1], df.hist()[2][1])
            self.assertPlotsEqual(tbl.hist()[2][2], df.hist()[2][2])

            self.assertPlotsEqual(tbl.hist()[3][0], df.hist()[3][0])
            self.assertPlotsEqual(tbl.hist()[3][1], df.hist()[3][1])
            self.assertPlotsEqual(tbl.hist()[3][2], df.hist()[3][2])

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_boxplot(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl[['MSRP', 'Invoice']].boxplot(return_type='axes'),
                df[['MSRP', 'Invoice']].boxplot(return_type='axes')
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or \
                    type(msg).__name__ in ['TclError'] or \
                    'FixedLocator' in ('%s' % msg):
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            # Basic plot
            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice'])
                   .plot('Make', ['MSRP', 'Invoice']),
                df.sort_values(['MSRP', 'Invoice'])
                  .plot('Make', ['MSRP', 'Invoice'])
            )

            # Must reset index here because it uses that as X axis
            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice'])
                   .plot(y=['MSRP', 'Invoice']),
                df.sort_values(['MSRP', 'Invoice'])
                  .reset_index().plot(y=['MSRP', 'Invoice'])
            )

            # Test kind= parameter
            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice'])
                   .plot('MSRP', 'Invoice', 'scatter'),
                df.sort_values(['MSRP', 'Invoice'])
                  .plot('MSRP', 'Invoice', 'scatter')
            )

            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice'])
                   .plot('MSRP', 'Invoice', kind='scatter'),
                df.sort_values(['MSRP', 'Invoice'])
                  .plot('MSRP', 'Invoice', kind='scatter')
            )

            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice'])
                   .plot('Make', ['MSRP', 'Invoice'], kind='bar'),
                df.sort_values(['MSRP', 'Invoice'])
                  .plot('Make', ['MSRP', 'Invoice'], kind='bar')
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_sampling(self):
        tbl = self.table

        try:
            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice'])
                   .plot('MSRP', 'Invoice', sample_pct=0.05, sample_seed=123),
                tbl._fetch(sample_pct=0.05, sample_seed=123)
                   .sort_values(['MSRP', 'Invoice']).plot('MSRP', 'Invoice')
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_area(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice'])
                   .plot.area('Make', ['MSRP', 'Invoice']),
                df.sort_values(['MSRP', 'Invoice'])
                  .plot.area('Make', ['MSRP', 'Invoice'])
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_bar(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl['Cylinders'].sort_values().plot.bar(),
                df['Cylinders'].sort_values().plot.bar()
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_barh(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl['Cylinders'].sort_values().plot.barh(),
                df['Cylinders'].sort_values().plot.barh()
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_box(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl[['MSRP', 'Invoice']].plot.box(),
                df[['MSRP', 'Invoice']].plot.box()
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_density(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl[['MSRP', 'Invoice']].plot.density(),
                df[['MSRP', 'Invoice']].plot.density()
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_hexbin(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl.plot.hexbin('MSRP', 'Horsepower'),
                df.plot.hexbin('MSRP', 'Horsepower')
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_hist(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl.plot.hist(),
                df.plot.hist(),
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_kde(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl[['MSRP', 'Invoice']].plot.kde(),
                df[['MSRP', 'Invoice']].plot.kde()
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_line(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl.sort_values(['MSRP', 'Invoice']).plot.line('MSRP', 'Invoice'),
                df.sort_values(['MSRP', 'Invoice']).plot.line('MSRP', 'Invoice')
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_pie(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl['Cylinders'].sort_values().plot.pie(),
                df['Cylinders'].sort_values().plot.pie()
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_plot_scatter(self):
        tbl = self.table
        df = self.get_cars_df()

        try:
            self.assertPlotsEqual(
                tbl.plot.scatter('MSRP', 'Horsepower'),
                df.plot.scatter('MSRP', 'Horsepower')
            )

        except Exception as msg:
            if isinstance(msg, ImportError) or type(msg).__name__ in ['TclError']:
                tm.TestCase.skipTest(self, '%s' % msg)
            raise

    def test_eval(self):
        tbl = self.table
        df = self.get_cars_df()

        dfcol = df.eval('(MPG_City + MPG_Highway) / 2')
        tblcol = tbl.eval('(MPG_City + MPG_Highway) / 2')
        self.assertColsEqual(dfcol, tblcol)
        self.assertEqual(list(df.columns), list(tbl.columns))

        if pd_version > (0, 18, 0):
            dfcol = df.eval('MPG_Avg = (MPG_City + MPG_Highway) / 2', inplace=True)
            tblcol = tbl.eval('MPG_Avg = (MPG_City + MPG_Highway) / 2', inplace=True)
            self.assertTrue(dfcol is None)
            self.assertTrue(tblcol is None)
            self.assertTablesEqual(df, tbl)

            dfcol = df.eval('MPG_Avg = (MPG_City + MPG_Highway) / 2', inplace=False)
            tblcol = tbl.eval('MPG_Avg = (MPG_City + MPG_Highway) / 2', inplace=False)
            self.assertTrue(dfcol is not None)
            self.assertTrue(tblcol is not None)
            self.assertTrue(dfcol is not df)
            self.assertTrue(tblcol is not tbl)
            self.assertTablesEqual(dfcol, tblcol)

    def _get_comp_data(self, limit=None):
        df = pd.read_csv(six.StringIO('''Origin,A,B,C,D,E,X,Y,Z
        Asia,0,-94,0,-70,,X,a
        Asia,34,-12,-33,74,,X,b,
        Asia,0,40,0,77,,X,c,
        Europe,0,69,0,7,,X,,
        Europe,0,88,0,-46,,X,e,
        Europe,56,6,89,,,X,f,
        USA,33,-33,-52,62,,X,,
        USA,99,-60,0,76,,X,,
        USA,0,-12,-80,-90,,X,,
        USA,39,15,-72,-42,,X,,'''))
        df['Y'] = df['Y'].fillna('')
        df['Z'] = df['Z'].fillna('')

        if limit is not None:
            df = df.iloc[:limit]

        tbl = self.s.upload_frame(df,
                                  importoptions=dict(vars=[
                                      dict(name='Origin', type='varchar'),
                                      dict(name='A', type='double'),
                                      dict(name='B', type='double'),
                                      dict(name='C', type='double'),
                                      dict(name='D', type='double'),
                                      dict(name='E', type='double'),
                                      dict(name='X', type='varchar'),
                                      dict(name='Y', type='varchar'),
                                      dict(name='Z', type='varchar')
                                  ]))

        return df, tbl

    def test_abs(self):
        df, tbl = self._get_comp_data()
        self.assertTablesEqual(
            df[['A', 'B', 'C', 'D', 'E']].abs().sort_values(['A', 'B']),
            tbl[['A', 'B', 'C', 'D', 'E']].abs().sort_values(['A', 'B']))

    def test_all(self):
        df, tbl = self._get_comp_data()

        self.assertColsEqual(df.all(), tbl.all())
        self.assertColsEqual(df.all(skipna=True), tbl.all(skipna=True))

        # When skipna=False, pandas doesn't use booleans anymore
        self.assertColsEqual(
            df.all(skipna=False).apply(lambda x: pd.isnull(x) and x or bool(x)),
            tbl.all(skipna=False))

        # By groups
        self.assertTablesEqual(df.groupby('Origin').all(),
                               tbl.groupby('Origin').all())
        self.assertTablesEqual(df.groupby('Origin').all(skipna=True),
                               tbl.groupby('Origin').all(skipna=True))

        # When skipna=False, pandas doesn't use booleans anymore
        # TODO: Pandas seems inconsitent here.  It uses True without by groups,
        # but if a column contains an NaN and by groups, it uses NaN as
        # the result... ?
        # all = df.groupby('Origin').all(skipna=False).applymap(
        #    lambda x: pd.isnull(x) and x or bool(x))
        # all.loc['Europe', 'D'] = True
        # self.assertTablesEqual(all, tbl.groupby('Origin').all(skipna=False))

    def test_any(self):
        df, tbl = self._get_comp_data()

        self.assertColsEqual(df.any(), tbl.any())
        self.assertColsEqual(df.any(skipna=True), tbl.any(skipna=True))

        # When skipna=False, pandas doesn't use booleans anymore
        self.assertColsEqual(
            df.any(skipna=False).apply(lambda x: pd.isnull(x) and x or bool(x)),
            tbl.any(skipna=False))

        # By groups
        self.assertTablesEqual(df.groupby('Origin').any(),
                               tbl.groupby('Origin').any())
        self.assertTablesEqual(df.groupby('Origin').any(skipna=True),
                               tbl.groupby('Origin').any(skipna=True))

        # When skipna=False, pandas doesn't use booleans anymore
        if pd_version < (0, 23, 0):
            self.assertTablesEqual(
                df.groupby('Origin').any(skipna=False).applymap(
                    lambda x: pd.isnull(x) and x or bool(x)),
                tbl.groupby('Origin').any(skipna=False))

    def test_clip(self):
        df, tbl = self._get_comp_data()

        cols = ['A', 'B', 'C', 'D', 'E']

        df = df[cols]
        tbl = tbl[cols]

        self.assertTablesEqual(df.clip(lower=-10).sort_values(cols),
                               tbl.clip(lower=-10).sort_values(cols))
        self.assertTablesEqual(df.clip(upper=15).sort_values(cols),
                               tbl.clip(upper=15).sort_values(cols))
        self.assertTablesEqual(df.clip(lower=-10, upper=15).sort_values(cols),
                               tbl.clip(lower=-10, upper=15).sort_values(cols))

        if pd_version < (1, 0, 0):
            self.assertTablesEqual(df.clip_lower(-5).sort_values(cols),
                                   tbl.clip_lower(-5).sort_values(cols))

            self.assertTablesEqual(df.clip_upper(30).sort_values(cols),
                                   tbl.clip_upper(30).sort_values(cols))

    def _get_merge_data(self):
        import swat.tests as st

        finance = os.path.join(os.path.dirname(st.__file__),
                               'datasources', 'merge_finance.csv')
        repertory = os.path.join(os.path.dirname(st.__file__),
                                 'datasources', 'merge_repertory.csv')

        df_finance = pd.read_csv(finance)
        df_repertory = pd.read_csv(repertory)

        tbl_finance = self.s.read_csv(
            finance, casout=dict(name='unittest.merge_finance',
                                 caslib=self.srcLib, replace=True))
        tbl_repertory = self.s.read_csv(
            repertory, casout=dict(name='unittest.merge_repertory',
                                   caslib=self.srcLib, replace=True))

        return (df_finance, df_repertory), (tbl_finance, tbl_repertory)

    @unittest.skipIf(pd_version < (0, 18, 0), 'Need newer version of Pandas')
    def test_merge(self):
        dfs, tbls = self._get_merge_data()

        df_finance, df_repertory = dfs
        tbl_finance, tbl_repertory = tbls

        def fill_char(frame):
            for key in frame.columns:
                if 'Id' in key:
                    frame[key] = frame[key].fillna('')
            frame['Play'] = frame['Play'].fillna('')
            frame['Role'] = frame['Role'].fillna('')
            frame['Name'] = frame['Name'].fillna('')
            if '_merge' in frame.columns:
                frame['_merge'] = frame['_merge'].astype(object)
            if 'Which' in frame.columns:
                frame['Which'] = frame['Which'].astype(object)
            return frame

        #
        # Join methods
        #

        # defaults / inner -- ((*))
        df_out = fill_char(df_finance.merge(df_repertory, on='IdNumber', indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber', indicator=True)
        try:
            self.assertTablesEqual(df_out, tbl_out, sortby=['IdNumber', 'Play'])
        finally:
            tbl_out.droptable()

        # inner -- ((*))
        df_out = fill_char(df_finance.merge(df_repertory, on='IdNumber',
                                            how='inner', indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                    how='inner', indicator=True)
        try:
            self.assertTablesEqual(df_out, tbl_out, sortby=['IdNumber', 'Play'])
        finally:
            tbl_out.droptable()

        # outer -- (*(*)*)
        # TODO: Windows server merge adds FinanceIds
        if self.server_type != 'windows.smp':
            df_out = fill_char(df_finance.merge(df_repertory, on='IdNumber',
                                                how='outer', indicator=True))
            tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                        how='outer', indicator=True)
            try:
                self.assertTablesEqual(df_out, tbl_out, sortby=['IdNumber', 'Play'])
            finally:
                tbl_out.droptable()

        # left -- (*(*))
        df_out = fill_char(df_finance.merge(df_repertory, on='IdNumber',
                                            how='left', indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                    how='left', indicator=True)
        try:
            self.assertTablesEqual(df_out, tbl_out, sortby=['IdNumber', 'Play'])
        finally:
            tbl_out.droptable()

        # right -- ((*)*)
        df_out = fill_char(df_finance.merge(df_repertory, on='IdNumber',
                                            how='right', indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                    how='right', indicator=True)
        try:
            self.assertTablesEqual(df_out, tbl_out, sortby=['IdNumber', 'Play'])
        finally:
            tbl_out.droptable()

        #
        # TODO: Not supported by pandas
        #

        # left-minus-right -- (*())
        tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                    how='left-minus-right', indicator='Which')
        try:
            self.assertEqual(len(tbl_out[tbl_out['Which'] == 'left_only']), len(tbl_out))
        finally:
            tbl_out.droptable()

        # right-minus-left -- (()*)
        tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                    how='right-minus-left', indicator='Which')
        try:
            self.assertEqual(len(tbl_out[tbl_out['Which'] == 'right_only']), len(tbl_out))
        finally:
            tbl_out.droptable()

        # outer-minus-inner -- (*()*)
        tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                    how='outer-minus-inner', indicator='Which')
        try:
            self.assertEqual(len(tbl_out[tbl_out['Which'] == 'right_only'])
                             + len(tbl_out[tbl_out['Which'] == 'left_only']),
                             len(tbl_out))
        finally:
            tbl_out.droptable()

        #
        # Join methods using different keys
        #

        # defaults / inner -- ((*))
        df_out = fill_char(df_finance.merge(df_repertory, left_on='FinanceId',
                                            right_on='RepId', indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, left_on='FinanceId',
                                    right_on='RepId', indicator=True)
        try:
            self.assertTablesEqual(df_out, tbl_out,
                                   sortby=['IdNumber_x', 'IdNumber_y', 'Play'])
        finally:
            tbl_out.droptable()

        # inner -- ((*))
        df_out = fill_char(df_finance.merge(df_repertory, left_on='FinanceId',
                                            right_on='RepId', how='inner',
                                            indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, left_on='FinanceId',
                                    right_on='RepId', how='inner', indicator=True)
        try:
            self.assertTablesEqual(
                df_out, tbl_out, sortby=['IdNumber_x', 'IdNumber_y', 'Play'])
        finally:
            tbl_out.droptable()

        # outer -- (*(*)*)
        df_out = fill_char(df_finance.merge(df_repertory, left_on='FinanceId',
                                            right_on='RepId', how='outer',
                                            indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, left_on='FinanceId',
                                    right_on='RepId', how='outer', indicator=True)
        try:
            self.assertTablesEqual(
                df_out, tbl_out, sortby=['_merge', 'Play', 'Role', 'Name'])
        finally:
            tbl_out.droptable()

        # left -- (*(*))
        df_out = fill_char(df_finance.merge(df_repertory, left_on='FinanceId',
                                            right_on='RepId', how='left',
                                            indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, left_on='FinanceId',
                                    right_on='RepId', how='left', indicator=True)
        try:
            self.assertTablesEqual(
                df_out, tbl_out, sortby=['IdNumber_x', 'IdNumber_y', 'Play'])
        finally:
            tbl_out.droptable()

        # right -- ((*)*)
        df_out = fill_char(df_finance.merge(df_repertory, left_on='FinanceId',
                                            right_on='RepId', how='right',
                                            indicator=True))
        tbl_out = tbl_finance.merge(tbl_repertory, left_on='FinanceId',
                                    right_on='RepId', how='right', indicator=True)
        try:
            self.assertTablesEqual(
                df_out, tbl_out, sortby=['IdNumber_x', 'IdNumber_y', 'Play'])
        finally:
            tbl_out.droptable()

        #
        # casout=
        #
        try:
            tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                        indicator=True, casout='join_out')
            self.assertEqual(tbl_out.params['name'], 'join_out')

            tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                        indicator=True,
                                        casout=dict(name='join_out', replace=True))
            self.assertEqual(tbl_out.params['name'], 'join_out')

            tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                        indicator=True,
                                        casout=self.s.CASTable('join_out', replace=True))
            self.assertEqual(tbl_out.params['name'], 'join_out')

            with self.assertRaises(swat.SWATError):
                tbl_out = tbl_finance.merge(tbl_repertory, on='IdNumber',
                                            indicator=True,
                                            casout=dict(name='join_out', replace=False))

        finally:
            tbl_out.droptable()

    def _get_concat_data(self):
        import swat.tests as st

        cars_a = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars_a.csv')
        cars_b = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars_b.csv')
        cars_c = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars_c.csv')

        df_a = pd.read_csv(cars_a)
        df_b = pd.read_csv(cars_b)
        df_c = pd.read_csv(cars_c)

        tbl_a = self.s.read_csv(cars_a, casout=dict(name='unittest.cars_a',
                                                    caslib=self.srcLib,
                                                    replace=True))
        tbl_b = self.s.read_csv(cars_b, casout=dict(name='unittest.cars_b',
                                                    caslib=self.srcLib,
                                                    replace=True))
        tbl_c = self.s.read_csv(cars_c, casout=dict(name='unittest.cars_c',
                                                    caslib=self.srcLib,
                                                    replace=True))

        return (df_a, df_b, df_c), (tbl_a, tbl_b, tbl_c)

    def test_concat(self):
        dfs, tbls = self._get_concat_data()

        try:
            df_out = pd.concat(dfs)
            tbl_out = concat(tbls)
            self.assertTablesEqual(df_out, tbl_out, sortby=SORT_KEYS)
        finally:
            tbl_out.droptable()

        try:
            df_out = pd.concat(dfs)
            tbl_out = concat(tbls, casout='unittest.concat')
            self.assertTablesEqual(df_out, tbl_out, sortby=SORT_KEYS)
            self.assertEqual(tbl_out.name, 'unittest.concat')
        finally:
            tbl_out.droptable()

    def test_with_params(self):
        tbl = self.s.CASTable('foo')
        tbl2 = tbl.with_params(replace=True, promote=True)
        self.assertTrue(set(tbl2.to_params().keys()), set(['name']))
        self.assertTrue(set(tbl2.to_params().keys()), set(['name', 'replace', 'promote']))

    def test_groupby_column(self):
        head1 = self.table.groupby(['Type', 'Cylinders']).head()
        head2 = self.table.groupby([self.table['Type'], self.table['Cylinders']]).head()
        head3 = self.table.groupby(['Type', self.table['Cylinders']]).head()
        self.assertTablesEqual(head1, head2)
        self.assertTablesEqual(head2, head3)


if __name__ == '__main__':
    tm.runtests()
