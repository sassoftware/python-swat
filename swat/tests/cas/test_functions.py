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
import pandas as pd
import re
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


pd_version = tuple([int(x) for x in re.match(r'^(\d+)\.(\d+)\.(\d+)',
                                             pd.__version__).groups()])


class TestFunctions(tm.TestCase):

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

    def tearDown(self):
        # tear down tests
        self.s.endsession()
        del self.s
        swat.reset_option()

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

        # Use CASTables
        try:
            df_out = pd.concat(dfs)
            tbl_out = swat.concat(tbls)
            self.assertTablesEqual(df_out, tbl_out, sortby=SORT_KEYS)
        finally:
            tbl_out.droptable()

        try:
            df_out = pd.concat(dfs)
            tbl_out = swat.concat(tbls, casout='unittest.concat')
            self.assertTablesEqual(df_out, tbl_out, sortby=SORT_KEYS)
            self.assertEqual(tbl_out.name, 'unittest.concat')
        finally:
            tbl_out.droptable()

        # Use SASDataFrames
        cars_a = tbls[0].fetch(sastypes=False, to=1000)['Fetch']
        cars_b = tbls[1].fetch(sastypes=False, to=1000)['Fetch']
        cars_c = tbls[2].fetch(sastypes=False, to=1000)['Fetch']

        self.assertTablesEqual(pd.concat(dfs), swat.concat([cars_a, cars_b, cars_c]),
                               sortby=SORT_KEYS)

        # Standard DataFrames
        self.assertTablesEqual(
            pd.concat(dfs),
            swat.concat([pd.DataFrame(cars_a),
                         pd.DataFrame(cars_b),
                         pd.DataFrame(cars_c)]),
            sortby=SORT_KEYS)

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
                                 caslib=self.srcLib,
                                 replace=True))
        tbl_repertory = self.s.read_csv(
            repertory, casout=dict(name='unittest.merge_repertory',
                                   caslib=self.srcLib,
                                   replace=True))

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

        # Use CASTables
        df_out = fill_char(pd.merge(df_finance, df_repertory,
                                    on='IdNumber', indicator=True))
        tbl_out = swat.merge(tbl_finance, tbl_repertory,
                             on='IdNumber', indicator=True)
        try:
            self.assertTablesEqual(df_out, tbl_out, sortby=['IdNumber', 'Play'])
        finally:
            tbl_out.droptable()

        # Use SASDataFrames
        self.assertTablesEqual(
            df_out,
            fill_char(swat.merge(tbl_finance.fetch(to=1000, sastypes=False)['Fetch'],
                                 tbl_repertory.fetch(to=1000, sastypes=False)['Fetch'],
                                 on='IdNumber', indicator=True)),
            sortby=['IdNumber', 'Play'])

        # Use DataFrames
        self.assertTablesEqual(
            df_out,
            fill_char(swat.merge(pd.DataFrame(tbl_finance.to_frame()),
                                 pd.DataFrame(tbl_repertory.to_frame()),
                                 on='IdNumber', indicator=True)),
            sortby=['IdNumber', 'Play'])


if __name__ == '__main__':
    tm.runtests()
