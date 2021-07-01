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
import datetime
import json
import numpy as np
import os
import pandas as pd
import re
import six
import swat
import swat.utils.testing as tm
import sys
import unittest
from swat.dataframe import SASDataFrame, reshape_bygroups
from swat.utils.compat import text_types
from bs4 import BeautifulSoup

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestDataFrame(tm.TestCase):

    # Create a class attribute to hold the cas host type
    server_type = None

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = False

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if type(self).server_type is None:
            # Set once per class and have every test use it.
            # No need to change between tests.
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

    def test_colspec_str(self):
        dtype = 'char'
        if self.s._protocol in ['http', 'https']:
            dtype = 'varchar'

        out = self.table.fetch()['Fetch']

        s = str(out.colinfo['Model'])
        s = re.sub(r'(\d+)L\b', r'\1', s)
        s = re.sub(r'\bu\'', '\'', s)

        self.assertTrue(isinstance(s, str))
        self.assertEqual(s, "SASColumnSpec(attrs=dict(), dtype='%s', " % dtype
                            + "name='Model', size=(1, 1), width=40)")

        s = repr(out.colinfo['Model'])
        s = re.sub(r'(\d+)L\b', r'\1', s)
        s = re.sub(r'\bu\'', '\'', s)

        self.assertTrue(isinstance(s, str))
        self.assertEqual(s, "SASColumnSpec(attrs=dict(), dtype='%s', " % dtype
                            + "name='Model', size=(1, 1), width=40)")

    def test_array_types(self):
        r = tm.load_data(self.s, 'datasources/summary_array.sashdat', self.server_type)

        tablename = r['tableName']
        self.assertNotEqual(tablename, None)

        data = self.s.fetch(table=swat.table(tablename, caslib=self.srcLib),
                            sastypes=False)['Fetch']

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

    def test_alltypes(self):
        from swat.dataframe import dtype_from_var

        srcLib = tm.get_casout_lib(self.server_type)

        out = self.s.loadactionset(actionset='actionTest')
        if out.severity != 0:
            self.skipTest("actionTest failed to load")

        out = self.s.alltypes(casout=dict(caslib=srcLib, name='typestable'))
        out = self.s.fetch(table=self.s.CASTable('typestable', caslib=srcLib),
                           sastypes=False)

        data = out['Fetch']

        self.assertEqual(data['Double'].iloc[0], 42.42)
        self.assertEqual(type(data['Double'].iloc[0]), np.float64)
        self.assertEqual(dtype_from_var(data.Double.iloc[0]), 'double')

        self.assertEqual(data['Char'].iloc[0], u'AbC\u2782\u2781\u2780')
        self.assertTrue(isinstance(data['Char'].iloc[0], text_types))
        self.assertEqual(dtype_from_var(data.Char.iloc[0]), 'varchar')

        self.assertEqual(data['Varchar'].iloc[0],
                         u'This is a test of the Emergency Broadcast System. '
                         u'This is only a test. BEEEEEEEEEEEEEEEEEEP WHAAAA '
                         u'SCREEEEEEEEEEEECH. \u2789\u2788\u2787\u2786\u2785'
                         u'\u2784\u2783\u2782\u2781\u2780 Blastoff!')
        self.assertTrue(isinstance(data['Varchar'].iloc[0], text_types))
        self.assertEqual(dtype_from_var(data.Varchar.iloc[0]), 'varchar')

        self.assertEqual(data['Int32'].iloc[0], 42)
        self.assertIn(type(data['Int32'].iloc[0]), [np.int32, np.int64])
        self.assertIn(dtype_from_var(data.Int32.iloc[0]), ['int32', 'int64'])

        # REST interface can sometimes overflow the JSON float
        if np.isnan(data['Int64'].iloc[0]):
            self.assertEqual(type(data['Int64'].iloc[0]), np.float64)
        else:
            self.assertEqual(data['Int64'].iloc[0], 9223372036854775807)
            self.assertEqual(type(data['Int64'].iloc[0]), np.int64)

        self.assertEqual(data['Date'].iloc[0], datetime.date(1963, 5, 19))
        self.assertEqual(type(data['Date'].iloc[0]), datetime.date)
        # self.assertEqual(type(data['Date'].iloc[0]), datetime.Date)
        self.assertEqual(dtype_from_var(data.Date.iloc[0]), 'date')

        self.assertEqual(data['Time'].iloc[0], datetime.time(11, 12, 13, 141516))
        self.assertEqual(type(data['Time'].iloc[0]), datetime.time)
        # self.assertEqual(type(data['Time'].iloc[0]), datetime.Time)
        self.assertEqual(dtype_from_var(data.Time.iloc[0]), 'time')

        self.assertEqual(data['Datetime'].iloc[0],
                         pd.to_datetime('1963-05-19 11:12:13.141516'))
        self.assertEqual(type(data['Datetime'].iloc[0]), pd.Timestamp)
        # self.assertEqual(type(data['Datetime'].iloc[0]), datetime.Datetime)
        self.assertEqual(dtype_from_var(data.Datetime.iloc[0]), 'datetime')

        self.assertEqual(data['DecSext'].iloc[0], '12345678901234567890.123456789')
        self.assertTrue(isinstance(data['DecSext'].iloc[0], text_types))
        # self.assertEqual(type(data['DecSext'].iloc[0]), Decimal)

        # self.assertEqual(data['Varbinary'].iloc[0], '???')
        # self.assertEqual(type(data['Varbinary'].iloc[0]), bytes)

        # self.assertEqual(data['Binary'].iloc[0], '???')
        # self.assertEqual(type(data['Binary'].iloc[0]), bytes)

        self.assertEqual(dtype_from_var(b''), 'varbinary')

        with self.assertRaises(TypeError):
            dtype_from_var(None)

    def test_dict_methods(self):
        out = self.table.fetch()['Fetch']

        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain', 'MSRP', 'Invoice',
                   'EngineSize', 'Cylinders', 'Horsepower', 'MPG_City', 'MPG_Highway',
                   'Weight', 'Wheelbase', 'Length']

        self.assertEqual(list(out.columns), columns)
        self.assertTrue('Model' in out.colinfo)

        # pop
        model = out.pop('Model')
        columns.pop(1)

        self.assertEqual(list(out.columns), columns)
        self.assertTrue('Model' not in out.colinfo)
        self.assertEqual(model.name, 'Model')
        self.assertTrue(isinstance(model, pd.Series))

        # setitem
        columns.append('New Model')

        out['New Model'] = model
        self.assertEqual(list(out.columns), columns)
        self.assertTrue('New Model' in out.colinfo)

        # slice
        df = out[3:7]
        self.assertTrue(isinstance(df, SASDataFrame))
        self.assertEqual(list(df.columns), list(out.columns))
        self.assertEqual(df.colinfo, out.colinfo)

        # insert
        length = out.pop('Length')
        columns.pop(columns.index('Length'))

        out.insert(1, 'New Length', length)
        columns.insert(1, 'New Length')
        self.assertEqual(list(out.columns), columns)

    def test_html(self):
        out = self.table.fetch()['Fetch']

        # No html rendering
        pd.set_option('display.notebook.repr_html', False)

        html = out._repr_html_()
        self.assertTrue(html is None)

        # Default rendering
        pd.set_option('display.notebook.repr_html', True)

        html = out._repr_html_()

        label = 'Selected Rows from Table DATASOURCES.CARS_SINGLE'
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain', 'MSRP', 'Invoice',
                   'EngineSize', 'Cylinders', 'Horsepower', 'MPG_City', 'MPG_Highway',
                   'Weight', 'Wheelbase', 'Length']
        thstr = [None] + columns

        soup = BeautifulSoup(html, 'html.parser')
        htbl = soup.find_all('table')[0]

        caption = [x.string for x in htbl.find_all('caption')]
        self.assertEqual(caption, [label])

        headers = [x.string for x in htbl.thead.find_all('th')]
        self.assertEqual(headers, thstr)

        index = [x.string for x in htbl.tbody.find_all('tr')]
        data = [x.string for x in htbl.tbody.find_all('td')]
        self.assertEqual(len(index), 20)
        self.assertTrue((len(data) == 300) | (len(data) == 320))

        self.assertFalse(re.search(r'\d+ rows x \d+ columns', html))

        # Add index name
        out.index.name = 'Index'

        html = out._repr_html_()

        thstr = [None] + columns + ['Index'] + [None] * 15

        soup = BeautifulSoup(html, 'html.parser')
        htbl = soup.find_all('table')[0]

        headers = [x.string for x in htbl.thead.find_all('th')]
        self.assertEqual(headers, thstr)

        index = [x.string for x in htbl.tbody.find_all('tr')]
        data = [x.string for x in htbl.tbody.find_all('td')]
        self.assertEqual(len(index), 20)
        self.assertTrue((len(data) == 300) | (len(data) == 320))

        self.assertFalse(re.search(r'\d+ rows x \d+ columns', html))

        # Truncate
        out.index.name = None
        pd.options.display.max_rows = 5

        html = out._repr_html_()

        thstr = [None] + columns

        soup = BeautifulSoup(html, 'html.parser')
        htbl = soup.find_all('table')[0]

        caption = [x.string for x in htbl.find_all('caption')]
        self.assertEqual(caption, [label])

        headers = [x.string for x in htbl.thead.find_all('th')]
        self.assertEqual(headers, thstr)

        index = [x.string for x in htbl.tbody.find_all('tr')]
        data = [x.string for x in htbl.tbody.find_all('td')]
        self.assertEqual(len(index), 5)
        self.assertTrue((len(data) == 75) | (len(data) == 80))
        self.assertTrue(data.count('...'), len(out.columns))

        self.assertTrue(re.search(r'\d+ rows \S \d+ columns', html))

        # Don't show dimensions
        pd.options.display.show_dimensions = False
        html = out._repr_html_()
        self.assertFalse(re.search(r'\d+ rows \S \d+ columns', html))

        # Only show dimensions on truncate
        pd.options.display.show_dimensions = 'truncate'
        html = out._repr_html_()
        self.assertTrue(re.search(r'\d+ rows \S \d+ columns', html))

        pd.options.display.max_rows = 100
        html = out._repr_html_()
        self.assertFalse(re.search(r'\d+ rows \S \d+ columns', html))

        # No colinfo
        swat.reset_option()
        out.colinfo.clear()

        html = out._repr_html_()

        label = 'Selected Rows from Table DATASOURCES.CARS_SINGLE'
        columns = ['Make', 'Model', 'Type', 'Origin', 'DriveTrain', 'MSRP', 'Invoice',
                   'EngineSize', 'Cylinders', 'Horsepower', 'MPG_City', 'MPG_Highway',
                   'Weight', 'Wheelbase', 'Length']
        thstr = [None] + columns

        soup = BeautifulSoup(html, 'html.parser')
        htbl = soup.find_all('table')[0]

        caption = [x.string for x in htbl.find_all('caption')]
        self.assertEqual(caption, [label])

        headers = [x.string for x in htbl.thead.find_all('th')]
        self.assertEqual(headers, thstr)

        index = [x.string for x in htbl.tbody.find_all('tr')]
        data = [x.string for x in htbl.tbody.find_all('td')]
        self.assertEqual(len(index), 20)
        self.assertTrue((len(data) == 300) | (len(data) == 320))

        self.assertFalse(re.search(r'\d+ rows x \d+ columns', html))

# NOTE: Javascript will not be supported at this time
#   def test_javascript(self):
#       swat.options.display.notebook.repr_javascript = True

#       out = self.table.fetch()['Fetch']

#       # No html rendering
#       swat.options.display.notebook.repr_javascript = False

#       js = out._repr_javascript_()
#       self.assertTrue(js is None)

#       # Default rendering
#       swat.options.display.notebook.repr_javascript = True

#       js = out._repr_javascript_()

#       label = 'Selected Rows from Table DATASOURCES.CARS_SINGLE'
#       thstr = ['', 'Make', 'Model', 'Type', 'Origin', 'DriveTrain',
#                'MSRP', 'Invoice', 'Engine Size (L)', 'Cylinders',
#                'Horsepower', 'MPG (City)', 'MPG (Highway)',
#                'Weight (LBS)', 'Wheelbase (IN)', 'Length (IN)']

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       self.assertEqual(jsdata['label'], label)
#
#       headers = [x['title'] for x in jsdata['columns']]
#       self.assertEqual(headers, thstr)

#       data = jsdata['data']
#       self.assertEqual(len(data), 20)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is None)
#       self.assertTrue(nrows is None)

#       # Add index name
#       out.index.name = 'Index'

#       js = out._repr_javascript_()

#       thstr[0] = 'Index'

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       self.assertEqual(jsdata['label'], label)

#       headers = [x['title'] for x in jsdata['columns']]
#       self.assertEqual(headers, thstr)

#       data = jsdata['data']
#       self.assertEqual(len(data), 20)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is None)
#       self.assertTrue(nrows is None)

#       # Truncate
#       out.index.name = None
#       swat.options.display.max_rows = 5

#       thstr[0] = ''

#       js = out._repr_javascript_()

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       self.assertEqual(jsdata['label'], label)

#       headers = [x['title'] for x in jsdata['columns']]
#       self.assertEqual(headers, thstr)

#       data = jsdata['data']
#       self.assertEqual(len(data), 5)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is 15)
#       self.assertTrue(nrows is 20)

#       # Don't show dimensions
#       swat.options.display.show_dimensions = False

#       js = out._repr_javascript_()

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is None)
#       self.assertTrue(nrows is None)

#       # Only show dimensions on truncate
#       swat.options.display.show_dimensions = 'truncate'

#       js = out._repr_javascript_()

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is 15)
#       self.assertTrue(nrows is 20)

#       swat.options.display.max_rows = 100

#       js = out._repr_javascript_()

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is None)
#       self.assertTrue(nrows is None)

#       # No colinfo
#       swat.reset_option()
#       swat.options.display.notebook.repr_javascript = True
#       out.colinfo.clear()

#       js = out._repr_javascript_()

#       label = 'Selected Rows from Table DATASOURCES.CARS_SINGLE'
#       thstr = ['', 'Make', 'Model', 'Type', 'Origin', 'DriveTrain',
#                'MSRP', 'Invoice', 'EngineSize', 'Cylinders',
#                'Horsepower', 'MPG_City', 'MPG_Highway',
#                'Weight', 'Wheelbase', 'Length']

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       self.assertEqual(jsdata['label'], label)

#       headers = [x['title'] for x in jsdata['columns']]
#       self.assertEqual(headers, thstr)

#       data = jsdata['data']
#       self.assertEqual(len(data), 20)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is None)
#       self.assertTrue(nrows is None)

#       # Add index name
#       out.index.name = 'Index'

#       js = out._repr_javascript_()

#       thstr[0] = 'Index'

#       jsdata = re.search(r"new swat\.SASDataFrame\(\$\('#[^']+?'\), "
#                          r"JSON\.parse\('(.+?)'\)\);",
#                          js, re.S).group(1)
#       jsdata = json.loads(jsdata)

#       self.assertEqual(jsdata['label'], label)

#       headers = [x['title'] for x in jsdata['columns']]
#       self.assertEqual(headers, thstr)

#       data = jsdata['data']
#       self.assertEqual(len(data), 20)

#       ncolumns = jsdata.get('ncolumns')
#       nrows = jsdata.get('nrows')

#       self.assertTrue(ncolumns is None)
#       self.assertTrue(nrows is None)

#       swat.options.repr_javascript = False

    def test_table_attrs(self):
        self.s.loadactionset('simple')
        out = self.table.summary()['Summary']

        self.assertEqual(out.attrs['Action'], 'summary')
        self.assertEqual(out.attrs['Actionset'], 'simple')
        self.assertTrue(isinstance(out.attrs['CreateTime'], float))

    def test_alltypes_html(self):
        srcLib = tm.get_casout_lib(self.server_type)

        out = self.s.loadactionset(actionset='actionTest')
        if out.severity != 0:
            self.skipTest("actionTest failed to load")

        out = self.s.alltypes(casout=dict(caslib=srcLib, name='typestable'))
        out = self.s.fetch(
            table=self.s.CASTable('typestable', caslib=srcLib,
                                  varlist=['Double', 'Char', 'Varchar', 'Int32',
                                           'Int64', 'Date', 'Time', 'Datetime',
                                           'DecSext']), sastypes=False)

        data = out['Fetch']

        data._repr_html_()

    def test_reshape_bygroups(self):
        out = self.table.groupby(['Origin', 'Cylinders']).summary(subset=['Min',
                                                                          'Max',
                                                                          'Mean'])
        columns = ['Column', 'Min', 'Max', 'Mean']

        fmt = out['ByGroup3.Summary']
        self.assertEqual(list(fmt.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(fmt.columns), columns)
        self.assertEqual(fmt.index.values[0], ('Asia', '4'))

        #
        # Formatted to any
        #

        # From formatted to none
        none = reshape_bygroups(fmt, bygroup_columns='none')
        self.assertEqual(list(none.index.names), [None])
        self.assertEqual(list(none.columns), columns)

        # From formatted to raw
        raw = reshape_bygroups(fmt, bygroup_columns='raw')
        self.assertEqual(list(raw.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(raw.columns), columns)
        self.assertEqual(raw.index.values[0], ('Asia', 4))

        # From formatted to formatted
        fmt2 = reshape_bygroups(fmt, bygroup_columns='formatted')
        self.assertEqual(list(fmt2.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(fmt2.columns), columns)
        self.assertEqual(fmt2.index.values[0], ('Asia', '4'))

        # From formatted to both
        both = reshape_bygroups(fmt, bygroup_columns='both')
        self.assertEqual(list(both.index.names), ['Origin', 'Origin_f',
                                                  'Cylinders', 'Cylinders_f'])
        self.assertEqual(list(both.columns), columns)
        self.assertEqual(both.index.values[0], ('Asia', 'Asia', 4, '4'))

        # raise ValueError('done')

        #
        # None to any
        #

        # From none to none
        none2 = reshape_bygroups(none, bygroup_columns='none')
        self.assertEqual(list(none2.index.names), [None])
        self.assertEqual(list(none2.columns), columns)

        # From none to raw
        raw = reshape_bygroups(none, bygroup_columns='raw')
        self.assertEqual(list(raw.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(raw.columns), columns)
        self.assertEqual(raw.index.values[0], ('Asia', 4))

        # From none to formatted
        fmt = reshape_bygroups(none, bygroup_columns='formatted')
        self.assertEqual(list(fmt.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(fmt.columns), columns)
        self.assertEqual(fmt.index.values[0], ('Asia', '4'))

        # From none to both
        both = reshape_bygroups(none, bygroup_columns='both')
        self.assertEqual(list(both.index.names), ['Origin', 'Origin_f',
                                                  'Cylinders', 'Cylinders_f'])
        self.assertEqual(list(both.columns), columns)
        self.assertEqual(both.index.values[0], ('Asia', 'Asia', 4, '4'))

        #
        # Raw to any
        #

        # From raw to none
        none = reshape_bygroups(raw, bygroup_columns='none')
        self.assertEqual(list(none.index.names), [None])
        self.assertEqual(list(none.columns), columns)

        # From raw to raw
        raw2 = reshape_bygroups(raw, bygroup_columns='raw')
        self.assertEqual(list(raw2.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(raw2.columns), columns)
        self.assertEqual(raw2.index.values[0], ('Asia', 4))

        # From raw to formatted
        fmt = reshape_bygroups(raw, bygroup_columns='formatted')
        self.assertEqual(list(fmt.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(fmt.columns), columns)
        self.assertEqual(fmt.index.values[0], ('Asia', '4'))

        # From raw to both
        both = reshape_bygroups(raw, bygroup_columns='both')
        self.assertEqual(list(both.index.names), ['Origin', 'Origin_f',
                                                  'Cylinders', 'Cylinders_f'])
        self.assertEqual(list(both.columns), columns)
        self.assertEqual(both.index.values[0], ('Asia', 'Asia', 4, '4'))

        #
        # Both to any
        #

        # From raw to none
        none = reshape_bygroups(both, bygroup_columns='none')
        self.assertEqual(list(none.index.names), [None])
        self.assertEqual(list(none.columns), columns)

        # From raw to raw
        raw = reshape_bygroups(both, bygroup_columns='raw')
        self.assertEqual(list(raw.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(raw.columns), columns)
        self.assertEqual(raw.index.values[0], ('Asia', 4))

        # From raw to formatted
        fmt = reshape_bygroups(both, bygroup_columns='formatted')
        self.assertEqual(list(fmt.index.names), ['Origin', 'Cylinders'])
        self.assertEqual(list(fmt.columns), columns)
        self.assertEqual(fmt.index.values[0], ('Asia', '4'))

        # From raw to both
        both2 = reshape_bygroups(both, bygroup_columns='both')
        self.assertEqual(list(both2.index.names), ['Origin', 'Origin_f',
                                                   'Cylinders', 'Cylinders_f'])
        self.assertEqual(list(both2.columns), columns)
        self.assertEqual(both2.index.values[0], ('Asia', 'Asia', 4, '4'))

        # Formatted with no index
        out = reshape_bygroups(fmt, bygroup_as_index=False)
        self.assertEqual(list(out.index.names), [None])
        self.assertEqual(list(out.columns), ['Origin', 'Cylinders'] + columns)
        self.assertEqual(out.Origin[0], 'Asia')
        self.assertEqual(out.Cylinders[0], '4')

        # Raw with no index
        out = reshape_bygroups(fmt, bygroup_columns='raw', bygroup_as_index=False)
        self.assertEqual(list(out.index.names), [None])
        self.assertEqual(list(out.columns), ['Origin', 'Cylinders'] + columns)
        self.assertEqual(out.Origin[0], 'Asia')
        self.assertEqual(out.Cylinders[0], 4)

        # None with no index
        out = reshape_bygroups(fmt, bygroup_columns='none', bygroup_as_index=False)
        self.assertEqual(list(out.index.names), [None])
        self.assertEqual(list(out.columns), columns)

        # Both with no index
        out = reshape_bygroups(fmt, bygroup_columns='both', bygroup_as_index=False)
        self.assertEqual(list(out.index.names), [None])
        self.assertEqual(list(out.columns), ['Origin', 'Origin_f', 'Cylinders',
                                             'Cylinders_f'] + columns)
        self.assertEqual(out.Origin[0], 'Asia')
        self.assertEqual(out.Origin_f[0], 'Asia')
        self.assertEqual(out.Cylinders[0], 4)
        self.assertEqual(out.Cylinders_f[0], '4')

        # Both with suffix and no index
        out = reshape_bygroups(fmt, bygroup_columns='both',
                               bygroup_as_index=False, bygroup_formatted_suffix='.1')
        self.assertEqual(list(out.index.names), [None])
        self.assertEqual(list(out.columns), ['Origin', 'Origin.1', 'Cylinders',
                                             'Cylinders.1'] + columns)
        self.assertEqual(out.Origin[0], 'Asia')
        self.assertEqual(out['Origin.1'][0], 'Asia')
        self.assertEqual(out.Cylinders[0], 4)
        self.assertEqual(out['Cylinders.1'][0], '4')

        # Raw with no index using list
        out = reshape_bygroups([fmt], bygroup_columns='raw', bygroup_as_index=False)
        self.assertEqual(list(out[0].index.names), [None])
        self.assertEqual(list(out[0].columns), ['Origin', 'Cylinders'] + columns)
        self.assertEqual(out[0].Origin[0], 'Asia')
        self.assertEqual(out[0].Cylinders[0], 4)

    def test_apply_labels(self):
        out = self.table.crosstab(col='Cylinders', row='Horsepower')['Crosstab']

        self.assertEqual(list(out.columns),
                         ['Horsepower', 'Col1', 'Col2', 'Col3', 'Col4',
                          'Col5', 'Col6', 'Col7'])

        newout = out.apply_labels()

        self.assertEqual(list(out.columns),
                         ['Horsepower', 'Col1', 'Col2', 'Col3', 'Col4',
                          'Col5', 'Col6', 'Col7'])
        self.assertEqual(list(newout.columns),
                         ['Horsepower', '3', '4', '5', '6', '8', '10', '12'])

        newout = out.apply_labels(inplace=True)

        self.assertTrue(newout is None)
        self.assertEqual(list(out.columns),
                         ['Horsepower', '3', '4', '5', '6', '8', '10', '12'])

    def test_apply_formats(self):
        swat.options.display.apply_formats = True

        out = self.table.to_frame(from_=1, to=1, sortby=['Make', 'Model'])
        out.colinfo['Cylinders'].format = 'int'
        out.colinfo['Horsepower'].format = 'int'
        out.colinfo['MPG_City'].format = 'int'
        out.colinfo['MPG_Highway'].format = 'int'
        out.colinfo['Weight'].format = 'best'
        out.colinfo['Wheelbase'].format = 'best'
        out.colinfo['Length'].format = 'best'

        f = ['Acura', '3.5', 'RL', '4dr', 'Sedan', 'Asia', 'Front', '$43,755',
             '$39,014', '3.5', '6', '225', '18', '24', '3880', '115', '197']
        ft = ['Acura', '3.5', 'RL', '4dr', 'Sedan', 'Asia', 'Front', '...',
              '18', '24', '3880', '115', '197']

        # __str__
        pd.set_option('display.max_columns', 10000)
        s = [re.split(r'\s+', x[1:].strip())
             for x in str(out).split('\n') if x.startswith('0')]
        s = [item for sublist in s for item in sublist]
        self.assertEqual(s, f)

        # truncated __str__
        pd.set_option('display.max_columns', 10)
        s = [re.split(r'\s+', x[1:].strip())
             for x in str(out).split('\n') if x.startswith('0')]
        s = [item for sublist in s for item in sublist]
        self.assertEqual(s, ft)

        pd.set_option('display.max_columns', 10000)

        # __repr__
        s = [re.split(r'\s+', x[1:].strip())
             for x in repr(out).split('\n') if x.startswith('0')]
        s = [item for sublist in s for item in sublist]
        self.assertEqual(s, f)

        # to_string
        s = [re.split(r'\s+', x[1:].strip())
             for x in out.to_string().split('\n') if x.startswith('0')]
        s = [item for sublist in s for item in sublist]
        self.assertEqual(s, f)

        f = ('''<tr> <td>0</td> <td>Acura</td> <td>3.5 RL 4dr</td> <td>Sedan</td> '''
             '''<td>Asia</td> <td>Front</td> <td>$43,755</td> <td>$39,014</td> '''
             '''<td>3.5</td> <td>6</td> <td>225</td> <td>18</td> <td>24</td> '''
             '''<td>3880</td> <td>115</td> <td>197</td> </tr>''')

        # to_html
        s = out.to_html()
        self.assertTrue(s is not None)
        s = re.sub(r'\s+', r' ', s).replace('th>', 'td>')
        self.assertTrue(f in s)

        # _repr_html_
        s = out._repr_html_()
        self.assertTrue(s is not None)
        s = re.sub(r'\s+', r' ', s).replace('th>', 'td>')
        self.assertTrue(f in s)


# NOTE: Javascript will not be supported at this time
#   def test_alltypes_javascript(self):
#       srcLib = tm.get_casout_lib(self.server_type)
#       out = self.s.loadactionset(actionset='actionTest')
#       out = self.s.alltypes(targetlib=srcLib, outtable='typestable')
#       out = self.s.fetch(table=self.s.CASTable('typestable', caslib=srcLib,
#                                varlist=['Double', 'Char', 'Varchar', 'Int32',
#                                         'Int64', 'Date', 'Time', 'Datetime',
#                                         'DecSext']), sastypes=False)

#       data = out['Fetch']

#       html = data._repr_javascript_()


if __name__ == '__main__':
    tm.runtests()
