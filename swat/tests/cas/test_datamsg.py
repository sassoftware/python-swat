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
import numpy as np
import pandas as pd
import os
import six
import swat
import swat.utils.testing as tm
import sys
import time
import unittest
from swat.cas.datamsghandlers import *  # noqa: F403

# Pick sort keys that will match across SAS and Pandas sorting orders
SORT_KEYS = ['Origin', 'MSRP', 'Horsepower', 'Model']

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestDataMsgHandlers(tm.TestCase):

    # Create a class attribute to hold the cas host type
    server_type = None

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = False
        swat.options.cas.missing.int64 = -999999

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if self.s._protocol in ['http', 'https']:
            tm.TestCase.skipTest(self, 'REST does not support data messages')

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

    def test_csv(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        cars = pd.io.parsers.read_csv(myFile)

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
                         ('Make,Model,Type,Origin,DriveTrain,MSRP,Invoice,'
                          'EngineSize,Cylinders,Horsepower,MPG_City,MPG_Highway,'
                          'Weight,Wheelbase,Length').split(','))
        self.assertEqual(data['Type'].tolist(),
                         ['varchar', 'varchar', 'varchar', 'varchar',
                          'varchar', 'int64', 'int64', 'double', 'int64',
                          'int64', 'int64', 'int64', 'int64', 'int64', 'int64'])

        self.assertTablesEqual(cars, self.s.CASTable('cars', caslib=srcLib),
                               sortby=SORT_KEYS)

        self.s.droptable(caslib=srcLib, table='cars')

    def test_dataframe(self):
        # Boolean
        s_bool_ = pd.Series([True, False], dtype=np.bool_)
        s_bool8 = pd.Series([True, False], dtype=np.bool8)

        # Integers
        s_byte = pd.Series([100, 999], dtype=np.byte)
        s_short = pd.Series([100, 999], dtype=np.short)
        s_intc = pd.Series([100, 999], dtype=np.intc)
        s_int_ = pd.Series([100, 999], dtype=np.int_)
        s_longlong = pd.Series([100, 999], dtype=np.longlong)
        s_intp = pd.Series([100, 999], dtype=np.intp)
        s_int8 = pd.Series([100, 999], dtype=np.int8)
        s_int16 = pd.Series([100, 999], dtype=np.int16)
        s_int32 = pd.Series([100, 999], dtype=np.int32)
        s_int64 = pd.Series([100, 999], dtype=np.int64)

        # Unsigned integers
        s_ubyte = pd.Series([100, 999], dtype=np.ubyte)
        s_ushort = pd.Series([100, 999], dtype=np.ushort)
        s_uintc = pd.Series([100, 999], dtype=np.uintc)
        s_uint = pd.Series([100, 999], dtype=np.uint)
        s_ulonglong = pd.Series([100, 999], dtype=np.ulonglong)
        s_uintp = pd.Series([100, 999], dtype=np.uintp)
        s_uint8 = pd.Series([100, 999], dtype=np.uint8)
        s_uint16 = pd.Series([100, 999], dtype=np.uint16)
        s_uint32 = pd.Series([100, 999], dtype=np.uint32)
        s_uint64 = pd.Series([100, 999], dtype=np.uint64)

        # Floating point
        s_half = pd.Series([12.3, 456.789], dtype=np.half)
        s_single = pd.Series([12.3, 456.789], dtype=np.single)
        s_double = pd.Series([12.3, 456.789], dtype=np.double)
        s_longfloat = pd.Series([12.3, 456.789], dtype=np.longfloat)
        s_float16 = pd.Series([12.3, 456.789], dtype=np.float16)
        s_float32 = pd.Series([12.3, 456.789], dtype=np.float32)
        s_float64 = pd.Series([12.3, 456.789], dtype=np.float64)
#       s_float96 = pd.Series([12.3, 456.789], dtype=np.float96)
        if hasattr(np, 'float128'):
            s_float128 = pd.Series([12.3, 456.789], dtype=np.float128)
        else:
            s_float128 = pd.Series([12.3, 456.789], dtype=np.float64)

        # Complex floating point
#       s_csingle = pd.Series(..., dtype=np.single)
#       s_complex_ = pd.Series(..., dtype=np.complex_)
#       s_clongfloat = pd.Series(..., dtype=np.clongfloat)
#       s_complex64 = pd.Series(..., dtype=np.complex64)
#       s_complex192 = pd.Series(..., dtype=np.complex192)
#       s_complex256 = pd.Series(..., dtype=np.complex256)

        # Python object
        s_object_ = pd.Series([('tuple', 'type'), ('another', 'tuple')], dtype=np.object_)
        s_str_ = pd.Series([u'hello', u'world'], dtype=np.str_)  # ASCII only
        s_unicode_ = pd.Series([u'hello', u'\u2603 (snowman)'], dtype=np.unicode_)
#       s_void = pd.Series(..., dtype=np.void)

        # Datetime
        s_datetime = pd.Series([np.datetime64('1979-03-22'), np.datetime64('1972-07-04')])

        # pd.options.display.max_columns = 100

        df = pd.DataFrame(dict(
            bool_=s_bool_,
            bool8=s_bool8,

            byte=s_byte,
            short=s_short,
            intc=s_intc,
            int_=s_int_,
            longlong=s_longlong,
            intp=s_intp,
            int8=s_int8,
            int16=s_int16,
            int32=s_int32,
            int64=s_int64,

            ubyte=s_ubyte,
            ushort=s_ushort,
            uintc=s_uintc,
            uint=s_uint,
            ulonglong=s_ulonglong,
            uintp=s_uintp,
            uint8=s_uint8,
            uint16=s_uint16,
            uint32=s_uint32,
            uint64=s_uint64,

            half=s_half,
            single=s_single,
            double=s_double,
            longfloat=s_longfloat,
            float16=s_float16,
            float32=s_float32,
            float64=s_float64,
            # float96=s_float96,
            float128=s_float128,

            object_=s_object_,
            str_=s_str_,
            unicode_=s_unicode_,

            datetime=s_datetime
        ))

        dmh = swat.datamsghandlers.PandasDataFrame(df)

        tbl = self.s.addtable(table='dtypes', **dmh.args.addtable).casTable

        data = tbl.fetch(sastypes=False).Fetch
        data = data.sort_values('bool8', ascending=False).reset_index()

        self.assertTrue(data['bool8'][0])
        self.assertFalse(data['bool8'][1])
        self.assertTrue(data['bool_'][0])
        self.assertFalse(data['bool_'][1])

        self.assertEqual(df['byte'].tolist(), data['byte'].tolist())
        self.assertEqual(df['short'].tolist(), data['short'].tolist())
        self.assertEqual(df['intc'].tolist(), data['intc'].tolist())
        self.assertEqual(df['int_'].tolist(), data['int_'].tolist())
        self.assertEqual(df['longlong'].tolist(), data['longlong'].tolist())
        self.assertEqual(df['intp'].tolist(), data['intp'].tolist())
        self.assertEqual(df['int8'].tolist(), data['int8'].tolist())
        self.assertEqual(df['int16'].tolist(), data['int16'].tolist())
        self.assertEqual(df['int32'].tolist(), data['int32'].tolist())
        self.assertEqual(df['int64'].tolist(), data['int64'].tolist())

        self.assertEqual(df['ubyte'].tolist(), data['ubyte'].tolist())
        self.assertEqual(df['ushort'].tolist(), data['ushort'].tolist())
        self.assertEqual(df['uintc'].tolist(), data['uintc'].tolist())
        self.assertEqual(df['uint'].tolist(), data['uint'].tolist())
        self.assertEqual(df['ulonglong'].tolist(), data['ulonglong'].tolist())
        self.assertEqual(df['uintp'].tolist(), data['uintp'].tolist())
        self.assertEqual(df['uint8'].tolist(), data['uint8'].tolist())
        self.assertEqual(df['uint16'].tolist(), data['uint16'].tolist())
        self.assertEqual(df['uint32'].tolist(), data['uint32'].tolist())
        self.assertEqual(df['uint64'].tolist(), data['uint64'].tolist())

        self.assertEqual(df['half'].tolist(), data['half'].tolist())
        self.assertEqual(df['single'].tolist(), data['single'].tolist())
        self.assertEqual(df['double'].tolist(), data['double'].tolist())
        self.assertEqual(df['longfloat'].tolist(), data['longfloat'].tolist())
        self.assertEqual(df['float16'].tolist(), data['float16'].tolist())
        self.assertEqual(df['float32'].tolist(), data['float32'].tolist())
        self.assertEqual(df['float64'].tolist(), data['float64'].tolist())
#       self.assertEqual(df['float96'].tolist(), data['float96'].tolist())
        self.assertEqual(df['float128'].tolist(), data['float128'].tolist())

#       self.assertEqual(df['object_'].tolist(), data['object_'].tolist())
        self.assertEqual(df['str_'].tolist(), data['str_'].tolist())
        self.assertEqual(df['unicode_'].tolist(), data['unicode_'].tolist())

        tbl.vars[0].name = 'datetime'
        tbl.vars[0].nfl = 20
        tbl.vars[0].nfd = 0

        data = tbl.fetch(sastypes=True, format=True,
                         sortby=[dict(name='datetime', formatted='raw',
                                      order='descending')]).Fetch

        self.assertEqual(
            [pd.to_datetime(x) for x in df['datetime'].tolist()],
            [pd.to_datetime(datetime.datetime.strptime(x, '  %d%b%Y:%H:%M:%S'))
             for x in data['datetime'].tolist()])

    def test_sasdataframe(self):
        df = self.table.fetch(sastypes=False).Fetch

        hpinfo = df.colinfo['Horsepower']
        hpinfo.label = 'How much power?'
        hpinfo.format = 'INT'
        hpinfo.width = 11

        dmh = swat.datamsghandlers.PandasDataFrame(df)

        tbl = self.s.addtable(table='dtypes', **dmh.args.addtable).casTable

        with swat.option_context('cas.dataset.index_name', 'Column'):
            data = tbl.columninfo().ColumnInfo

        hp = data.loc['Horsepower']

        self.assertEqual(hp.Label, hpinfo.label)
        self.assertEqual(hp.Format, hpinfo.format)
        self.assertEqual(hp.FormattedLength, hpinfo.width)

    def test_sas7bdat(self):
        try:
            import sas7bdat
        except ImportError:
            tm.TestCase.skipTest(self, 'sas7bdat package is not available')

        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__),
                              'datasources', 'cars.sas7bdat')

        dmh = swat.datamsghandlers.SAS7BDAT(myFile)

        tbl = self.s.addtable(table='cars', **dmh.args.addtable).casTable

        f = tbl.to_frame()
        s = sas7bdat.SAS7BDAT(myFile).to_data_frame()

        self.assertTablesEqual(f, s, sortby=SORT_KEYS)

    def test_text(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.tsv')

        dmh = swat.datamsghandlers.Text(myFile)

        tbl = self.s.addtable(table='cars', **dmh.args.addtable).casTable

        f = tbl.to_frame()
        s = pd.io.parsers.read_table(myFile)

        self.assertTablesEqual(f, s, sortby=SORT_KEYS)

    def test_json(self):
        df = self.table.to_frame()
        jsondf = df.to_json()

        dmh = swat.datamsghandlers.JSON(jsondf)

        tbl = self.s.addtable(table='cars', **dmh.args.addtable).casTable

        data = tbl.to_frame()

        self.assertTablesEqual(tbl, data, sortby=SORT_KEYS)

    def test_html(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.html')

        dmh = swat.datamsghandlers.HTML(myFile)

        tbl = self.s.addtable(table='cars', **dmh.args.addtable).casTable

        f = tbl.to_frame()
        s = pd.read_html(myFile)[0]

        self.assertTablesEqual(f, s, sortby=SORT_KEYS)

    def test_sql(self):
        try:
            import sqlite3 as lite
        except ImportError:
            tm.TestCase.skipTest(self, 'SQLite3 package is not available')

        import tempfile

        tmph, tmpf = tempfile.mkstemp(suffix='.db')

        with lite.connect(tmpf) as con:
            cur = con.cursor()
            cur.execute("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")
            cur.execute("INSERT INTO Cars VALUES(1,'Audi',52642)")
            cur.execute("INSERT INTO Cars VALUES(2,'Mercedes',57127)")
            cur.execute("INSERT INTO Cars VALUES(3,'Skoda',9000)")
            cur.execute("INSERT INTO Cars VALUES(4,'Volvo',29000)")
            cur.execute("INSERT INTO Cars VALUES(5,'Bentley',350000)")
            cur.execute("INSERT INTO Cars VALUES(6,'Citroen',21000)")
            cur.execute("INSERT INTO Cars VALUES(7,'Hummer',41400)")
            cur.execute("INSERT INTO Cars VALUES(8,'Volkswagen',21600)")

        from sqlalchemy import create_engine

        # SQLTable
        dmh = swat.datamsghandlers.SQLTable(
            'Cars',
            swat.datamsghandlers.SQLTable.create_engine('sqlite:///%s' % tmpf))

        tbl = self.s.addtable(table='Cars', **dmh.args.addtable).casTable
        data = tbl.fetch(sortby=['Id']).Fetch

        df = pd.io.sql.read_sql_table(
            'Cars', create_engine('sqlite:///%s' % tmpf)).sort_values(['Id'])

        for col in df.columns:
            self.assertEqual(df[col].tolist(), data[col].tolist())

        # SQLQuery
        dmh = swat.datamsghandlers.SQLQuery(
            'select * from cars where id < 5',
            swat.datamsghandlers.SQLQuery.create_engine('sqlite:///%s' % tmpf))

        tbl = self.s.addtable(table='Cars2', **dmh.args.addtable).casTable
        data = tbl.fetch(sortby=['Id']).Fetch

        for col in df.columns:
            self.assertEqual(df[col].tolist()[:4], data[col].tolist())

        try:
            os.remove(tmpf)
        except:  # noqa: E722
            pass

    def test_excel(self):
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.xls')

        dmh = swat.datamsghandlers.Excel(myFile)

        tbl = self.s.addtable(table='cars', **dmh.args.addtable).casTable

        f = tbl.to_frame()
        s = pd.read_excel(myFile, 0)

        self.assertTablesEqual(f, s, sortby=SORT_KEYS)

    def test_dbapi(self):
        try:
            import sqlite3 as lite
        except ImportError:
            tm.TestCase.skipTest(self, 'SQLite3 package is not available')

        import csv
        import tempfile
        import swat.tests as st

        myFile = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        tmph, tmpf = tempfile.mkstemp(suffix='.db')

        # Create database
        with lite.connect(tmpf) as con:
            cur = con.cursor()
            cur.execute(
                'CREATE TABLE Cars(Make TEXT, Model TEXT, Type TEXT, Origin TEXT, '
                'DriveTrain DOUBLE, MSRP DOUBLE, Invoice DOUBLE, EngineSize, '
                'Cylinders DOUBLE, Horsepower DOUBLE, MPG_City DOUBLE, '
                'MPG_Highway DOUBLE, Weight DOUBLE, Wheelbase DOUBLE, Length DOUBLE)')
            with open(myFile) as csv_data:
                # Skip header
                next(csv_data)
                reader = csv.reader(csv_data)
                for row in reader:
                    cur.execute(
                        'INSERT INTO Cars '
                        'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', row)

        # swat.options.cas.print_messages = True

        with lite.connect(tmpf) as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM Cars WHERE MSRP > 80000 AND Cylinders > 8")
            dmh = swat.datamsghandlers.DBAPI(lite, cur)
            tbl = self.s.addtable(table='cars_lite', **dmh.args.addtable).casTable
            tblinfo = tbl.tableinfo().TableInfo
            colinfo = tbl.columninfo().ColumnInfo

        self.assertEqual(tblinfo['Rows'][0], 3)
        self.assertEqual(tblinfo['Columns'][0], 15)

        self.assertEqual(
            colinfo['Column'].tolist(),
            ('Make,Model,Type,Origin,DriveTrain,MSRP,Invoice,EngineSize,'
             'Cylinders,Horsepower,MPG_City,MPG_Highway,Weight,'
             'Wheelbase,Length').split(','))

        self.assertEqual(
            sorted(tuple(x) for x in tbl.head().itertuples(index=False)),
            sorted([('Dodge', 'Viper SRT-10 convertible 2dr', 'Sports', 'USA',
                     'Rear', 81795.0, 74451.0, '8.3', 10.0, 500.0, 12.0, 20.0,
                     3410.0, 99.0, 176.0),
                    ('Mercedes-Benz', 'CL600 2dr', 'Sedan', 'Europe', 'Rear',
                     128420.0, 119600.0, '5.5', 12.0, 493.0, 13.0, 19.0, 4473.0,
                     114.0, 196.0),
                    ('Mercedes-Benz', 'SL600 convertible 2dr', 'Sports', 'Europe',
                     'Rear', 126670.0, 117854.0, '5.5', 12.0, 493.0, 13.0, 19.0,
                     4429.0, 101.0, 179.0)]))

        try:
            os.remove(tmpf)
        except:  # noqa: E722
            pass


if __name__ == '__main__':
    tm.runtests()
