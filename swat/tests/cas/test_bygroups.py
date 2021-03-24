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
import numpy as np
import os
import pandas as pd
import re
import six
import swat
import swat.utils.testing as tm
import sys
import unittest

from swat.utils.compat import patch_pandas_sort

patch_pandas_sort()

pd_version = tuple([int(x) for x in re.match(r'^(\d+)\.(\d+)\.(\d+)',
                                             pd.__version__).groups()])

# Pick sort keys that will match across SAS and Pandas sorting orders
SORT_KEYS = ['Origin', 'MSRP', 'Horsepower', 'Model']

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestByGroups(tm.TestCase):

    server_type = None

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
#       swat.options.cas.trace_actions = False
#       swat.options.cas.trace_ui_actions = False
        swat.options.interactive_mode = False

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if type(self).server_type is None:
            type(self).server_type = tm.get_cas_host_type(self.s)

        self.srcLib = tm.get_casout_lib(self.server_type)

        r = tm.load_data(self.s, 'datasources/cars_single.sashdat', self.server_type)

        self.tablename = r['tableName']
        self.assertNotEqual(self.tablename, None)
        self.table = r['casTable']

    def get_cars_df(self, all_doubles=True):
        import swat.tests as st

        cars_csv = os.path.join(os.path.dirname(st.__file__), 'datasources', 'cars.csv')

        df = pd.read_csv(cars_csv)
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

    def replaceNaN(self, row, nan):
        row = list(row)
        for i, x in enumerate(row):
            if pd.isnull(x):
                row[i] = nan
        return row

    def assertTablesEqual(self, a, b, fillna=-999999, sortby=SORT_KEYS,
                          include_index=False, decimals=None):
        if hasattr(a, 'to_frame'):
            a = a.to_frame()
        if hasattr(b, 'to_frame'):
            b = b.to_frame()
        if sortby:
            a = a.sort_values(sortby)
            b = b.sort_values(sortby)
        self.assertEqual(list(a.columns), list(b.columns))
        a = a.fillna(value=fillna)
        b = b.fillna(value=fillna)
        for lista, listb in zip(list(a.to_records(index=include_index)),
                                list(b.to_records(index=include_index))):
            lista = list(lista)
            listb = list(listb)
            if decimals is not None:
                for i, item in enumerate(lista):
                    if isinstance(item, (float, np.float64)):
                        lista[i] = float(('%%.%df' % decimals) % item)
                for i, item in enumerate(listb):
                    if isinstance(item, (float, np.float64)):
                        listb[i] = float(('%%.%df' % decimals) % item)
            self.assertEqual(lista, listb)

    def assertColsEqual(self, a, b, fillna=-999999, sort=False,
                        include_index=False, decimals=None):
        if hasattr(a, 'to_series'):
            a = a.to_series()
        if hasattr(b, 'to_series'):
            b = b.to_series()
        a = a.fillna(value=fillna)
        b = b.fillna(value=fillna)
        if sort:
            la = list(sorted(a.tolist()))
            lb = list(sorted(b.tolist()))
        else:
            la = a.tolist()
            lb = b.tolist()
        if decimals is not None:
            for i, item in enumerate(la):
                if isinstance(item, (float, np.float64)):
                    la[i] = float(('%%.%df' % decimals) % item)
            for i, item in enumerate(lb):
                if isinstance(item, (float, np.float64)):
                    lb[i] = float(('%%.%df' % decimals) % item)
        self.assertEqual(la, lb)
        if include_index:
            if sort:
                self.assertEqual(list(sorted(a.index.values)),
                                 list(sorted(b.index.values)))
            else:
                self.assertEqual(list(a.index.values), list(b.index.values))

    def test_groupby(self):
        df = self.get_cars_df().sort_values(['MSRP', 'Invoice'])
        df.index = range(len(df))
        self.table.sort_values(['MSRP', 'Invoice'])

        self.table.groupby('Make')
        df.groupby('Make')

        # TODO: Should some sort of comparison be done here?

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

    @unittest.skipIf(pd_version[:2] <= (0, 16), 'Need newer version of Pandas')
    def test_column_nlargest(self):
        df = self.get_cars_df()
        tbl = self.table

        # Pandas can't do this with groupby
        out = []
        for make, cyl in [('BMW', 6), ('BMW', 8), ('Porsche', 6), ('Porsche', 8)]:
            out.append(df.query('Make == "%s" and Cylinders == %s' % (make, cyl))
                       .nlargest(2, columns=['MSRP'])
                       [['Make', 'Cylinders', 'MSRP']]
                       .set_index(['Make', 'Cylinders'])['MSRP'])
        dfgrp = pd.concat(out)

        tblgrp = tbl['MSRP'].groupby(['Make', 'Cylinders'])\
            .query('Make in ("Porsche", "BMW")').nlargest(2)
        self.assertColsEqual(dfgrp, tblgrp, sort=None)

        tblgrp = tbl['MSRP'].groupby(['Make', 'Cylinders'], as_index=False)\
            .query('Make in ("Porsche", "BMW")').nlargest(2)
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        with self.assertRaises(NotImplementedError):
            tblgrp = tbl['MSRP'].groupby(['Make', 'Cylinders'], as_index=False)\
                .query('Make in ("Porsche", "BMW")').nlargest(2)
        # self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        # self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

    @unittest.skipIf(pd_version[:2] <= (0, 16), 'Need newer version of Pandas')
    def test_nlargest(self):
        df = self.get_cars_df()
        tbl = self.table

        # Pandas can't do this with groupby
        out = []
        for make, cyl in [('BMW', 6), ('BMW', 8), ('Porsche', 6), ('Porsche', 8)]:
            out.append(df.query('Make == "%s" and Cylinders == %s' % (make, cyl))
                       .nlargest(2, columns=['MSRP'])
                       [['Make', 'Cylinders', 'Model', 'MSRP', 'Horsepower']]
                       .set_index(['Make', 'Cylinders']))
        dfgrp = pd.concat(out)

        tblgrp = tbl[['Model', 'MSRP', 'Horsepower']]\
            .groupby(['Make', 'Cylinders'])\
            .query('Make in ("Porsche", "BMW")').nlargest(2, columns=['MSRP'])
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        tblgrp = tbl[['Model', 'MSRP', 'Horsepower']]\
            .groupby(['Make', 'Cylinders'], as_index=False)\
            .query('Make in ("Porsche", "BMW")').nlargest(2, columns=['MSRP'])
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        with self.assertRaises(NotImplementedError):
            tblgrp = tbl[['Model', 'MSRP', 'Horsepower']]\
                .groupby(['Make', 'Cylinders'], as_index=False)\
                .query('Make in ("Porsche", "BMW")').nlargest(2, columns=['MSRP'])
        # self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        # self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

    @unittest.skipIf(pd_version[:2] <= (0, 16), 'Need newer version of Pandas')
    def test_column_nsmallest(self):
        df = self.get_cars_df()
        tbl = self.table

        # Pandas can't do this with groupby
        out = []
        for make, cyl in [('BMW', 6), ('BMW', 8), ('Porsche', 6), ('Porsche', 8)]:
            out.append(df.query('Make == "%s" and Cylinders == %s' % (make, cyl))
                       .nsmallest(2, columns=['MSRP'])
                       [['Make', 'Cylinders', 'MSRP']]
                       .set_index(['Make', 'Cylinders'])['MSRP'])
        dfgrp = pd.concat(out)

        tblgrp = tbl['MSRP'].groupby(['Make', 'Cylinders'])\
            .query('Make in ("Porsche", "BMW")').nsmallest(2)
        self.assertColsEqual(dfgrp, tblgrp, sort=None)

        tblgrp = tbl['MSRP'].groupby(['Make', 'Cylinders'], as_index=False)\
            .query('Make in ("Porsche", "BMW")').nsmallest(2)
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['MSRP'].groupby(['Make', 'Cylinders'], as_index=False)\
            .query('Make in ("Porsche", "BMW")').nsmallest(2)
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

    @unittest.skipIf(pd_version[:2] <= (0, 16), 'Need newer version of Pandas')
    def test_nsmallest(self):
        df = self.get_cars_df()
        tbl = self.table

        # Pandas can't do this with groupby
        out = []
        for make, cyl in [('BMW', 6), ('BMW', 8), ('Porsche', 6), ('Porsche', 8)]:
            out.append(df.query('Make == "%s" and Cylinders == %s' % (make, cyl))
                       .nsmallest(2, columns=['MSRP'])
                       [['Make', 'Cylinders', 'Model', 'MSRP', 'Horsepower']]
                       .set_index(['Make', 'Cylinders']))
        dfgrp = pd.concat(out)

        tblgrp = tbl[['Model', 'MSRP', 'Horsepower']]\
            .groupby(['Make', 'Cylinders'])\
            .query('Make in ("Porsche", "BMW")').nsmallest(2, columns=['MSRP'])
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        tblgrp = tbl[['Model', 'MSRP', 'Horsepower']]\
            .groupby(['Make', 'Cylinders'], as_index=False)\
            .query('Make in ("Porsche", "BMW")').nsmallest(2, columns=['MSRP'])
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl[['Model', 'MSRP', 'Horsepower']]\
            .groupby(['Make', 'Cylinders'], as_index=False)\
            .query('Make in ("Porsche", "BMW")').nsmallest(2, columns=['MSRP'])
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_head(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = pd.concat([df.query('Origin == "Asia"').head(),
                           df.query('Origin == "Europe"').head(),
                           df.query('Origin == "USA"').head()])['MSRP']
        tblgrp = tbl['MSRP'].groupby('Origin').head()
        self.assertColsEqual(dfgrp, tblgrp, sort=None)

        dfgrp = pd.concat([df.query('Origin == "Asia"').head(),
                           df.query('Origin == "Europe"').head(),
                           df.query('Origin == "USA"').head()])
        dfgrp = dfgrp.set_index('Origin')['MSRP']
        tblgrp = tbl['MSRP'].groupby('Origin', as_index=True).head()
        self.assertColsEqual(dfgrp, tblgrp, sort=None, include_index=False)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['MSRP'].groupby('Origin').head(10)
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(list(tblgrp.columns), ['Origin', 'MSRP'])
        self.assertEqual(len(tblgrp), 30)

    def test_head(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').head()
        tblgrp = tbl.groupby('Origin').head()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        dfgrp = df.groupby('Origin').head(10)
        tblgrp = tbl.groupby('Origin').head(10)
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin').head(10)
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(list(tblgrp.columns), ['Origin', 'Make', 'Model', 'Type',
                                                'DriveTrain', 'MSRP', 'Invoice',
                                                'EngineSize', 'Cylinders', 'Horsepower',
                                                'MPG_City', 'MPG_Highway',
                                                'Weight', 'Wheelbase', 'Length'])
        self.assertEqual(len(tblgrp), 30)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_tail(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = pd.concat([df.query('Origin == "Asia"').tail(),
                           df.query('Origin == "Europe"').tail(),
                           df.query('Origin == "USA"').tail()])['MSRP']
        tblgrp = tbl['MSRP'].groupby('Origin').tail()
        self.assertColsEqual(dfgrp, tblgrp, sort=None)

        dfgrp = pd.concat([df.query('Origin == "Asia"').tail(),
                           df.query('Origin == "Europe"').tail(),
                           df.query('Origin == "USA"').tail()])
        dfgrp = dfgrp.set_index('Origin')['MSRP']
        tblgrp = tbl['MSRP'].groupby('Origin', as_index=True).tail()
        self.assertColsEqual(dfgrp, tblgrp, sort=None, include_index=True)

    def test_tail(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').tail()
        tblgrp = tbl.groupby('Origin').tail()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        dfgrp = df.groupby('Origin').tail(10)
        tblgrp = tbl.groupby('Origin').tail(10)
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_slice(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = pd.concat([df.query('Origin == "Asia"').iloc[5:9],
                           df.query('Origin == "Europe"').iloc[5:9],
                           df.query('Origin == "USA"').iloc[5:9]])
        tblgrp = tbl.groupby('Origin').slice(5, 9)
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        dfgrp = pd.concat([df.query('Origin == "Asia"').iloc[5:9],
                           df.query('Origin == "Europe"').iloc[5:9],
                           df.query('Origin == "USA"').iloc[5:9]]).set_index('Origin')
        tblgrp = tbl.groupby('Origin').slice(5, 9, bygroup_as_index=True)
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin').slice(5, 9)
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(list(tblgrp.columns), ['Origin', 'Make', 'Model', 'Type',
                                                'DriveTrain', 'MSRP', 'Invoice',
                                                'EngineSize', 'Cylinders',
                                                'Horsepower', 'MPG_City',
                                                'MPG_Highway', 'Weight',
                                                'Wheelbase', 'Length'])
        self.assertEqual(len(tblgrp), 12)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_slice(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = pd.concat([df.query('Origin == "Asia"').iloc[5:9],
                           df.query('Origin == "Europe"').iloc[5:9],
                           df.query('Origin == "USA"').iloc[5:9]])['MSRP']
        tblgrp = tbl['MSRP'].groupby('Origin').slice(5, 9)
        self.assertColsEqual(dfgrp, tblgrp, sort=None)

        dfgrp = pd.concat([df.query('Origin == "Asia"').iloc[5:9],
                           df.query('Origin == "Europe"').iloc[5:9],
                           df.query('Origin == "USA"').iloc[5:9]])
        dfgrp = dfgrp.set_index('Origin')['MSRP']
        tblgrp = tbl['MSRP'].groupby('Origin').slice(5, 9, bygroup_as_index=True)
        self.assertColsEqual(dfgrp, tblgrp, sort=None, include_index=True)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['MSRP'].groupby('Origin').slice(5, 9)
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(list(tblgrp.columns), ['Origin', 'MSRP'])
        self.assertEqual(len(tblgrp), 12)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_nth(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['MSRP'].nth(6)
        tblgrp = tbl.groupby('Origin')['MSRP'].nth(6)
        self.assertColsEqual(dfgrp, tblgrp, sort=True)

        dfgrp = df.groupby('Origin')['MSRP'].nth([5, 7])
        tblgrp = tbl.groupby('Origin')['MSRP'].nth([5, 7])
        self.assertColsEqual(dfgrp, tblgrp, sort=True)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin')['MSRP'].nth([5, 7])
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(list(tblgrp.columns), ['Origin', 'MSRP'])
        self.assertEqual(len(tblgrp), 6)

    def test_nth(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        columns = [x for x in df.columns if x != 'Origin']
        dfgrp = df.groupby('Origin').nth(6)[columns]
        tblgrp = tbl.groupby('Origin').nth(6)
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True)

        columns = [x for x in df.columns if x != 'Origin']
        dfgrp = df.groupby('Origin').nth([5, 7])[columns]
        tblgrp = tbl.groupby('Origin').nth([5, 7])
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin').nth([5, 7])
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(list(tblgrp.columns), ['Origin', 'Make', 'Model', 'Type',
                                                'DriveTrain', 'MSRP', 'Invoice',
                                                'EngineSize', 'Cylinders',
                                                'Horsepower', 'MPG_City',
                                                'MPG_Highway', 'Weight',
                                                'Wheelbase', 'Length'])
        self.assertEqual(len(tblgrp), 6)

    def test_column_unique(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['MSRP'].unique()
        tblgrp = tbl.groupby('Origin')['MSRP'].unique()
        self.assertEqual(len(dfgrp), len(tblgrp))
        self.assertEqual(sorted(dfgrp.iloc[0]), sorted(tblgrp.iloc[0]))
        self.assertEqual(sorted(dfgrp.iloc[1]), sorted(tblgrp.iloc[1]))
        self.assertEqual(sorted(dfgrp.iloc[2]), sorted(tblgrp.iloc[2]))

        dfgrp = df.groupby('Origin')['MSRP'].unique()
        tblgrp = tbl['MSRP'].groupby('Origin').unique()
        self.assertEqual(len(dfgrp), len(tblgrp))
        self.assertEqual(sorted(dfgrp.iloc[0]), sorted(tblgrp.iloc[0]))
        self.assertEqual(sorted(dfgrp.iloc[1]), sorted(tblgrp.iloc[1]))
        self.assertEqual(sorted(dfgrp.iloc[2]), sorted(tblgrp.iloc[2]))

        dfgrp = dfgrp.reset_index()
        tblgrp = tbl['MSRP'].groupby('Origin', as_index=False).unique()
        self.assertEqual(len(dfgrp), len(tblgrp))
        self.assertEqual(sorted(dfgrp.iloc[0, 1]), sorted(tblgrp.iloc[0, 1]))
        self.assertEqual(sorted(dfgrp.iloc[1, 1]), sorted(tblgrp.iloc[1, 1]))
        self.assertEqual(sorted(dfgrp.iloc[2, 1]), sorted(tblgrp.iloc[2, 1]))

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['MSRP'].groupby('Origin', as_index=False).unique()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        dfgrp = dfgrp.set_index('Origin')
        tblgrp = tblgrp.to_frame().set_index('Origin')
        self.assertEqual(sorted(dfgrp.loc['Asia']['MSRP']),
                         sorted(tblgrp.loc['Asia']['MSRP']))
        self.assertEqual(sorted(dfgrp.loc['Europe']['MSRP']),
                         sorted(tblgrp.loc['Europe']['MSRP']))
        self.assertEqual(sorted(dfgrp.loc['USA']['MSRP']),
                         sorted(tblgrp.loc['USA']['MSRP']))

#
# There is now a simple.unique action that overrides this behavior.
#
#   def test_unique(self):
#       tbl = self.table.sort_values(SORT_KEYS)

#       with self.assertRaises(AttributeError):
#           tbl.groupby('Origin').unique()

#       #
#       # Test casout threshold
#       #
#       swat.options.cas.dataset.bygroup_casout_threshold = 2

#       with self.assertRaises(AttributeError):
#           tbl.groupby('Origin').unique()

    def test_column_nunique(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby(['Origin', 'Cylinders'])['MSRP'].nunique()
        tblgrp = tbl.groupby(['Origin', 'Cylinders'])['MSRP'].nunique()
        self.assertColsEqual(dfgrp, tblgrp)

        tblgrp = tbl['MSRP'].groupby(['Origin', 'Cylinders']).nunique()
        self.assertColsEqual(dfgrp, tblgrp)

        tblgrp = tbl.groupby(['Origin', 'Cylinders'], as_index=False)['MSRP'].nunique()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        tblgrp = tbl['MSRP'].groupby(['Origin', 'Cylinders'], as_index=False).nunique()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2
        swat.options.cas.dataset.bygroup_columns = 'raw'

        tblgrp = tbl['MSRP'].groupby(['Origin', 'Cylinders'], as_index=False).nunique()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp,
                               sortby=['Origin', 'Cylinders', 'MSRP'])

    def test_nunique(self):
        tbl = self.table.sort_values(SORT_KEYS)

        with self.assertRaises(AttributeError):
            tbl.groupby('Origin').nunique()

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        with self.assertRaises(AttributeError):
            tbl.groupby('Origin').nunique()

    @unittest.skipIf(pd_version[:2] <= (0, 16), 'Need newer version of Pandas')
    def test_column_value_counts(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby(['Origin', 'Cylinders'])['EngineSize'].value_counts()
        tblgrp = tbl.groupby(['Origin', 'Cylinders'])['EngineSize'].value_counts()
        self.assertColsEqual(dfgrp, tblgrp, sort=True, include_index=True)

        dfgrp = df.groupby(['Origin', 'Cylinders'])['EngineSize'].value_counts()
        tblgrp = tbl['EngineSize'].groupby(['Origin', 'Cylinders']).value_counts()
        self.assertColsEqual(dfgrp, tblgrp, sort=True, include_index=True)

        tblgrp = tbl['EngineSize'].groupby(['Origin', 'Cylinders'],
                                           as_index=False).value_counts()
        dfgrp.name = None
        self.assertTablesEqual(dfgrp.reset_index().set_index('EngineSize'),
                               tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2
        swat.options.cas.dataset.bygroup_columns = 'raw'

        tblgrp = tbl['EngineSize'].groupby(['Origin', 'Cylinders'],
                                           as_index=False).value_counts()
        dfgrp = dfgrp.reset_index()[['Origin', 'Cylinders', 'EngineSize', 0]]
        dfgrp['_Frequency_'] = dfgrp[0]
        del dfgrp[0]
        dfgrp = dfgrp.sort_values(['Origin', 'Cylinders', '_Frequency_', 'EngineSize'])
        tblgrp = tblgrp.sort_values(['Origin', 'Cylinders', '_Frequency_', 'EngineSize'])
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

    def test_value_counts(self):
        tbl = self.table.sort_values(SORT_KEYS)

        with self.assertRaises(AttributeError):
            tbl.groupby('Origin').value_counts()

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        with self.assertRaises(AttributeError):
            tbl.groupby('Origin').value_counts()

    def test_column_max(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].max()
        tblgrp = tbl.groupby('Origin')['EngineSize'].max()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        dfgrp = df.groupby('Origin')['EngineSize'].max()
        tblgrp = tbl['EngineSize'].groupby('Origin').max()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).max()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).max()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp,
                               sortby=['Origin', 'EngineSize'])

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    @unittest.skipIf(pd_version >= (1, 0, 0), 'Raises AssertionError in Pandas 1')
    def test_max(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').max()
        tblgrp = tbl.groupby('Origin').max()
        # Drop Model since they get sorted differently
        self.assertTablesEqual(dfgrp.drop('Model', axis=1), tblgrp.drop('Model', axis=1),
                               sortby=None, include_index=True)

        dfgrp = df.groupby('Origin', as_index=False).max()
        tblgrp = tbl.groupby('Origin', as_index=False).max()
        # Drop Model since they get sorted differently
        self.assertTablesEqual(dfgrp.drop('Model', axis=1), tblgrp.drop('Model', axis=1),
                               sortby=None, include_index=True)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

#       dfgrp = df.groupby('Origin').max()
#       tblgrp = tbl.groupby('Origin').max()
#       # Drop Model since they get sorted differently
#       self.assertTablesEqual(dfgrp.drop('Model', axis=1), tblgrp.drop('Model', axis=1),
#                              sortby=None, include_index=True)

        dfgrp = df.groupby('Origin', as_index=False).max()
        tblgrp = tbl.groupby('Origin', as_index=False).max()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        # Drop Model since they get sorted differently
        self.assertTablesEqual(dfgrp.drop('Model', axis=1), tblgrp.drop('Model', axis=1),
                               sortby=['Origin', 'Make', 'Type', 'DriveTrain'])

    def test_column_min(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].min()
        tblgrp = tbl.groupby('Origin')['EngineSize'].min()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        dfgrp = df.groupby('Origin')['EngineSize'].min()
        tblgrp = tbl['EngineSize'].groupby('Origin').min()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).min()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).min()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp,
                               sortby=['Origin', 'EngineSize'])

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    @unittest.skipIf(pd_version >= (1, 0, 0), 'Raises AssertionError in Pandas 1')
    def test_min(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').min()
        tblgrp = tbl.groupby('Origin').min()
        # Drop Type since it gets sorted differently
        self.assertTablesEqual(dfgrp.drop('Type', axis=1), tblgrp.drop('Type', axis=1),
                               sortby=None, include_index=True)

        dfgrp = df.groupby('Origin', as_index=False).min()
        tblgrp = tbl.groupby('Origin', as_index=False).min()
        # Drop Type since it gets sorted differently
        self.assertTablesEqual(dfgrp.drop('Type', axis=1), tblgrp.drop('Type', axis=1),
                               sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

#       dfgrp = df.groupby('Origin').min()
#       tblgrp = tbl.groupby('Origin').min().to_frame()
#       # Drop Type since it gets sorted differently
#       self.assertTablesEqual(dfgrp.drop('Type', axis=1), tblgrp.drop('Type', axis=1),
#                              sortby=None)

        dfgrp = df.groupby('Origin', as_index=False).min()
        tblgrp = tbl.groupby('Origin', as_index=False).min()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        # Drop Type since it gets sorted differently
        self.assertTablesEqual(dfgrp.drop('Type', axis=1), tblgrp.drop('Type', axis=1),
                               sortby=['Origin', 'Make', 'Model'])

    def test_column_mean(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].mean()
        tblgrp = tbl.groupby('Origin')['EngineSize'].mean()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True, decimals=5)

        dfgrp = df.groupby('Origin')['EngineSize'].mean()
        tblgrp = tbl['EngineSize'].groupby('Origin').mean()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True, decimals=5)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).mean()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None, decimals=5)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).mean()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp,
                               sortby=['Origin', 'EngineSize'], decimals=5)

    @unittest.skipIf(sys.version_info.major < 3, 'Need newer version of Python')
    def test_mean(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').mean()
        tblgrp = tbl.groupby('Origin').mean()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True, decimals=5)

        dfgrp = df.groupby('Origin', as_index=False).mean()
        tblgrp = tbl.groupby('Origin', as_index=False).mean()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, decimals=5)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        dfgrp = df.groupby('Origin', as_index=False).mean()
        tblgrp = tbl.groupby('Origin', as_index=False).mean()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp, tblgrp,
                               sortby=['Origin', 'MSRP', 'Invoice'], decimals=5)

    def test_column_median(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].median()
        tblgrp = tbl.groupby('Origin')['EngineSize'].median()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        dfgrp = df.groupby('Origin')['EngineSize'].median()
        tblgrp = tbl['EngineSize'].groupby('Origin').median()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).median()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).median()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=['Origin'])

    @unittest.skipIf(sys.version_info.major < 3, 'Need newer version of Python')
    def test_median(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').median()
        tblgrp = tbl.groupby('Origin').median()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True)

        dfgrp = df.groupby('Origin', as_index=False).median()
        tblgrp = tbl.groupby('Origin', as_index=False).median()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        dfgrp = df.groupby('Origin', as_index=False).median()
        tblgrp = tbl.groupby('Origin', as_index=False).median()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp, tblgrp, sortby=['Origin'])

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_mode(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        asia = df.query('Origin == "Asia"').mode()
        asia['Origin'] = 'Asia'
        asia = asia.set_index('Origin', append=True).reorder_levels([1, 0])

        europe = df.query('Origin == "Europe"').mode()
        europe['Origin'] = 'Europe'
        europe = europe.set_index('Origin', append=True).reorder_levels([1, 0])

        asia = asia['EngineSize'].dropna()
        europe = europe['EngineSize'].dropna()
        dfgrp = pd.concat([asia, europe])

        tblgrp = tbl.query('Origin ^= "USA"').groupby('Origin')['EngineSize'].mode()
        self.assertColsEqual(dfgrp, tblgrp, sort=False, include_index=True)

        tblgrp = tbl['EngineSize'].query('Origin ^= "USA"').groupby('Origin').mode()
        self.assertColsEqual(dfgrp, tblgrp, sort=False, include_index=True)

        tblgrp = tbl['EngineSize'].query('Origin ^= "USA"')\
            .groupby('Origin', as_index=False).mode()
        self.assertTablesEqual(dfgrp.reset_index(level=0), tblgrp, sortby=None)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_mode(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        asia = df.query('Origin == "Asia"').mode()
        asia['Origin'] = 'Asia'
        asia = asia.set_index('Origin', append=True).reorder_levels([1, 0])

        europe = df.query('Origin == "Europe"').mode()
        europe['Origin'] = 'Europe'
        europe = europe.set_index('Origin', append=True).reorder_levels([1, 0])

#       usa = df.query('Origin == "USA"').mode()
#       usa['Origin'] = 'USA'
#       usa = usa.set_index('Origin', append=True).reorder_levels([1,0])

        dfgrp = pd.concat([asia, europe])
        tblgrp = tbl.query('Origin ^= "USA"').groupby('Origin').mode()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True)

        tblgrp = tbl.query('Origin ^= "USA"').groupby('Origin', as_index=False).mode()
        self.assertTablesEqual(dfgrp.reset_index('Origin'), tblgrp, sortby=None)

    def test_column_quantile(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].quantile()
        tblgrp = tbl.groupby('Origin')['EngineSize'].quantile()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        dfgrp = df.groupby('Origin')['EngineSize'].quantile()
        tblgrp = tbl['EngineSize'].groupby('Origin').quantile()
        self.assertColsEqual(dfgrp, tblgrp, include_index=True)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).quantile()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).quantile()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=['Origin'])

    @unittest.skipIf(sys.version_info.major < 3, 'Need newer version of Python')
    def test_quantile(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)
        numerics = ['MSRP', 'Invoice', 'EngineSize', 'Cylinders',
                    'Horsepower', 'MPG_City', 'MPG_Highway',
                    'Weight', 'Wheelbase', 'Length']

        dfgrp = df.groupby('Origin')[numerics].quantile()
        tblgrp = tbl.groupby('Origin').quantile()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True)

        dfgrp = df.groupby('Origin', as_index=False)[numerics].quantile()
        tblgrp = tbl.groupby('Origin', as_index=False).quantile()
        # For some reason some versions of Pandas drop this column,
        # but I think it should be there.
        try:
            dfgrp = dfgrp.drop('Origin', axis=1)
        except:  # noqa: E722
            pass
        tblgrp = tblgrp.drop('Origin', axis=1)
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        dfgrp = df.groupby('Origin', as_index=False)[numerics].quantile()
        tblgrp = tbl.groupby('Origin', as_index=False).quantile()
        # For some reason some versions of Pandas drop this column,
        # but I think it should be there.
        try:
            dfgrp = dfgrp.drop('Origin', axis=1)
        except:  # noqa: E722
            pass
        tblgrp = tblgrp.drop('Origin', axis=1)
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp, tblgrp, sortby=['EngineSize'])

    def test_column_sum(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].sum()
        tblgrp = tbl.groupby('Origin')['EngineSize'].sum()
        self.assertColsEqual(dfgrp, tblgrp, decimals=5)

        dfgrp = df.groupby('Origin')['EngineSize'].sum()
        tblgrp = tbl['EngineSize'].groupby('Origin').sum()
        self.assertColsEqual(dfgrp, tblgrp, decimals=5)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).sum()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None, decimals=5)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).sum()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp,
                               sortby=['Origin', 'EngineSize'], decimals=5)

    def test_sum(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').sum()
        tblgrp = tbl.groupby('Origin').sum()
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, decimals=5)

        dfgrp = df.groupby('Origin', as_index=False).sum()
        tblgrp = tbl.groupby('Origin', as_index=False).sum()
        self.assertTablesEqual(dfgrp, tblgrp, decimals=5, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        dfgrp = df.groupby('Origin', as_index=False).sum()
        tblgrp = tbl.groupby('Origin', as_index=False).sum()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp, tblgrp, decimals=5,
                               sortby=['Origin', 'MSRP', 'Invoice'])

    def test_column_std(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].std()
        tblgrp = tbl.groupby('Origin')['EngineSize'].std()
        self.assertColsEqual(dfgrp, tblgrp, decimals=5)

        dfgrp = df.groupby('Origin')['EngineSize'].std()
        tblgrp = tbl['EngineSize'].groupby('Origin').std()
        self.assertColsEqual(dfgrp, tblgrp, decimals=5)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).std()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, sortby=None, decimals=5)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).std()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp,
                               sortby=['Origin', 'EngineSize'], decimals=5)

    def test_std(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').std()
        tblgrp = tbl.groupby('Origin').std()
        self.assertTablesEqual(dfgrp, tblgrp, decimals=5, sortby=None)

        # dfgrp = df.groupby('Origin', as_index=False).std()
        tblgrp = tbl.groupby('Origin', as_index=False).std()
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, decimals=5, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        # dfgrp = df.groupby('Origin', as_index=False).std()
        tblgrp = tbl.groupby('Origin', as_index=False).std()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp.reset_index(), tblgrp, decimals=5,
                               sortby=['Origin', 'MSRP', 'Invoice'])

    def test_column_var(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].var()
        tblgrp = tbl.groupby('Origin')['EngineSize'].var()
        self.assertColsEqual(dfgrp, tblgrp, decimals=5)

        dfgrp = df.groupby('Origin')['EngineSize'].var()
        tblgrp = tbl['EngineSize'].groupby('Origin').var()
        self.assertColsEqual(dfgrp, tblgrp, decimals=5)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).var()
        # For some reason Pandas drops this column, but I think it should be there.
        tblgrp = tblgrp.drop('Origin', axis=1)
        self.assertTablesEqual(dfgrp, tblgrp, decimals=5, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).var()
        # For some reason Pandas drops this column, but I think it should be there.
        tblgrp = tblgrp.drop('Origin', axis=1)
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp, tblgrp, decimals=5, sortby=['EngineSize'])

    def test_var(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').var()
        tblgrp = tbl.groupby('Origin').var()
        self.assertTablesEqual(dfgrp, tblgrp, decimals=3, sortby=None)

        dfgrp = df.groupby('Origin', as_index=False).var()
        tblgrp = tbl.groupby('Origin', as_index=False).var()
        self.assertTablesEqual(dfgrp, tblgrp, decimals=3, sortby=None)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        dfgrp = df.groupby('Origin', as_index=False).var()
        tblgrp = tbl.groupby('Origin', as_index=False).var()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertTablesEqual(dfgrp, tblgrp, decimals=3,
                               sortby=['Origin', 'MSRP', 'Invoice'])

    def test_column_nmiss(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['Cylinders'].nmiss()
        self.assertEqual(len(tblgrp), 3)
        self.assertEqual(tblgrp.loc['Asia'], 2)
        self.assertEqual(tblgrp.loc['Europe'], 0)
        self.assertEqual(tblgrp.loc['USA'], 0)

        tblgrp = tbl['Cylinders'].groupby('Origin').nmiss()
        self.assertEqual(len(tblgrp), 3)
        self.assertEqual(tblgrp.loc['Asia'], 2)
        self.assertEqual(tblgrp.loc['Europe'], 0)
        self.assertEqual(tblgrp.loc['USA'], 0)

        tblgrp = tbl['Cylinders'].groupby('Origin', as_index=False).nmiss()
        self.assertEqual(len(tblgrp), 3)

        # Test character missing values
        tbl = self.table.replace({'Make': {'Buick': ''}})

        tblgrp = tbl.groupby('Origin')['Make'].nmiss()
        self.assertEqual(len(tblgrp), 3)
        self.assertEqual(tblgrp.loc['Asia'], 0)
        self.assertEqual(tblgrp.loc['Europe'], 0)
        self.assertEqual(tblgrp.loc['USA'], 9)

        tblgrp = tbl['Make'].groupby('Origin').nmiss()
        self.assertEqual(len(tblgrp), 3)
        self.assertEqual(tblgrp.loc['Asia'], 0)
        self.assertEqual(tblgrp.loc['Europe'], 0)
        self.assertEqual(tblgrp.loc['USA'], 9)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['Cylinders'].groupby('Origin').nmiss()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)
        tblgrp = tblgrp.to_frame().set_index('Origin')['Cylinders']
        self.assertEqual(tblgrp.loc['Asia'], 2)
        self.assertEqual(tblgrp.loc['Europe'], 0)
        self.assertEqual(tblgrp.loc['USA'], 0)

    def test_nmiss(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').nmiss()
        self.assertEqual(len(tblgrp), 3)
        self.assertEqual(tblgrp.loc['Asia', 'Cylinders'], 2)
        self.assertEqual(tblgrp.loc['Europe', 'Cylinders'], 0)
        self.assertEqual(tblgrp.loc['USA', 'Cylinders'], 0)

        tblgrp = tbl.groupby('Origin', as_index=False).nmiss()
        self.assertEqual(len(tblgrp), 3)

        # Test character missing values
        tbl = self.table.replace({'Make': {'Buick': ''}})
        tblgrp = tbl.groupby('Origin').nmiss()
        self.assertEqual(len(tblgrp), 3)
        self.assertEqual(tblgrp.loc['Asia', 'Make'], 0)
        self.assertEqual(tblgrp.loc['Europe', 'Make'], 0)
        self.assertEqual(tblgrp.loc['USA', 'Make'], 9)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin', as_index=False).nmiss()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_column_stderr(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['EngineSize'].stderr()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin').stderr()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).stderr()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).stderr()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_stderr(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').stderr()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl.groupby('Origin', as_index=False).stderr()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin', as_index=False).stderr()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_column_uss(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['EngineSize'].uss()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin').uss()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).uss()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).uss()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_uss(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').uss()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl.groupby('Origin', as_index=False).uss()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin', as_index=False).uss()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_column_css(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['EngineSize'].css()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin').css()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).css()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).css()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_css(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').css()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin').css()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_column_cv(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['EngineSize'].cv()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin').cv()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).cv()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).cv()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_cv(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').cv()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl.groupby('Origin', as_index=False).cv()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin', as_index=False).cv()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_column_tvalue(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['EngineSize'].tvalue()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin').tvalue()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).tvalue()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).tvalue()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_tvalue(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').tvalue()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl.groupby('Origin', as_index=False).tvalue()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).tvalue()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_column_probt(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['EngineSize'].probt()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin').probt()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).probt()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False).probt()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    def test_probt(self):
        # TODO: Not supported by Pandas; need comparison values
        # df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').probt()
        self.assertEqual(len(tblgrp), 3)

        tblgrp = tbl.groupby('Origin', as_index=False).probt()
        self.assertEqual(len(tblgrp), 3)

        #
        # Test casout threshold
        #
        swat.options.cas.dataset.bygroup_casout_threshold = 2

        tblgrp = tbl.groupby('Origin', as_index=False).probt()
        self.assertEqual(tblgrp.__class__.__name__, 'CASTable')
        self.assertEqual(len(tblgrp), 3)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_describe(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin')['EngineSize'].describe(percentiles=[0.5])
        tblgrp = tbl.groupby('Origin')['EngineSize'].describe(percentiles=[0.5])
        if isinstance(dfgrp, pd.Series):
            self.assertColsEqual(dfgrp, tblgrp, include_index=True, decimals=5)
        else:
            self.assertTablesEqual(dfgrp, tblgrp, sortby=None, decimals=5)

        dfgrp = df.groupby('Origin')['EngineSize'].describe(percentiles=[0.5])
        tblgrp = tbl['EngineSize'].groupby('Origin').describe(percentiles=[0.5])
        if isinstance(dfgrp, pd.Series):
            self.assertColsEqual(dfgrp, tblgrp, include_index=True, decimals=5)
        else:
            self.assertTablesEqual(dfgrp, tblgrp, sortby=None, decimals=5)

# NOTE: This just seems broken in pandas
#       dfgrp = df.groupby('Origin', as_index=False)['EngineSize']\
#           .describe(percentiles=[0.5])
#       tblgrp = tbl['EngineSize'].groupby('Origin', as_index=False)\
#           .describe(percentiles=[0.5])
#       # Pandas doesn't include this column, but it seems necessary
#       tblgrp = tblgrp.drop('Origin', axis=1)
#       self.assertTablesEqual(dfgrp, tblgrp, sortby=False, decimals=5)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_describe(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').describe(percentiles=[0.5])[
            ['MSRP', 'Invoice', 'EngineSize', 'Cylinders', 'Horsepower',
             'MPG_City', 'MPG_Highway', 'Weight', 'Wheelbase', 'Length']
        ]
        tblgrp = tbl.groupby('Origin').describe(percentiles=[0.5])
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, include_index=True, decimals=5)

        dfgrp = df.groupby('Origin', as_index=False).describe(percentiles=[0.5])
        tblgrp = tbl.groupby('Origin', as_index=False).describe(percentiles=[0.5])
        # Not sure why Pandas doesn't include this
        tblgrp = tblgrp.drop('Origin', axis=1)
        self.assertTablesEqual(dfgrp, tblgrp, sortby=None, decimals=5)

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_to_frame(self):
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['MSRP'].to_frame()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, ['Origin'])

        tblgrp = tbl['MSRP'].groupby('Origin').to_frame()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, ['Origin'])

        tblgrp = tbl['MSRP'].groupby('Origin', as_index=False).to_frame()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, [None])

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_to_frame(self):
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin').to_frame()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, ['Origin'])

        tblgrp = tbl.groupby('Origin', as_index=False).to_frame()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, [None])

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_column_to_series(self):
        tbl = self.table.sort_values(SORT_KEYS)

        tblgrp = tbl.groupby('Origin')['MSRP'].to_series()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, ['Origin'])

        tblgrp = tbl['MSRP'].groupby('Origin').to_series()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, ['Origin'])

    @unittest.skipIf(pd_version < (0, 16, 0), 'Need newer version of Pandas')
    def test_to_series(self):
        tbl = self.table.sort_values(SORT_KEYS)

        with self.assertRaises(ValueError):
            tblgrp = tbl.groupby('Origin').to_series()

        tblgrp = tbl[['MSRP']].groupby('Origin').to_series()
        self.assertEqual(len(tblgrp), 428)
        self.assertEqual(tblgrp.index.names, ['Origin'])

    def test___getattr__(self):
        df = self.get_cars_df().sort_values(SORT_KEYS)
        tbl = self.table.sort_values(SORT_KEYS)

        dfgrp = df.groupby('Origin').EngineSize.sum()
        tblgrp = tbl.groupby('Origin').EngineSize.sum()
        self.assertColsEqual(dfgrp, tblgrp, decimals=5)


if __name__ == '__main__':
    tm.runtests()
