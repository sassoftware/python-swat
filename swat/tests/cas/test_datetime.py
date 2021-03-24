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

import datetime
import pandas as pd
import numpy as np
import os
import re
import six
import swat
import swat.utils.testing as tm
import sys
import unittest
from swat.cas.utils.datetime import \
    (str2cas_timestamp, cas2python_timestamp,
     str2cas_date, cas2python_date, cas2sas_date, str2cas_time, cas2python_time,
     cas2sas_time, python2cas_datetime, python2cas_date, python2cas_time,
     str2sas_timestamp, sas2python_timestamp, sas2cas_timestamp, str2sas_date,
     sas2python_date, sas2python_time, python2sas_datetime, python2sas_date,
     python2sas_time, cas2sas_timestamp, sas2cas_date, str2sas_time, sas2cas_time)

from swat.utils.compat import patch_pandas_sort
from swat.utils.testing import UUID_RE, get_cas_host_type, load_data

patch_pandas_sort()

# Pick sort keys that will match across SAS and Pandas sorting orders
SORT_KEYS = ['Origin', 'MSRP', 'Horsepower', 'Model']

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestDateTime(tm.TestCase):

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

        self.dates = load_data(
            self.s, 'datasources/dates.csv', self.server_type,
            importoptions=dict(vars=dict(Region=dict(),
                                         Date=dict(format='DATE'))))['casTable']
        self.dates = self.dates.to_frame().set_index('Region')

        self.datetimes = load_data(
            self.s, 'datasources/datetimes.csv', self.server_type,
            importoptions=dict(vars=dict(Region=dict(),
                                         Datetime=dict(format='DATETIME'))))['casTable']
        self.datetimes = self.datetimes.to_frame().set_index('Region')

    def tearDown(self):
        # tear down tests
        self.s.endsession()
        del self.s
        swat.reset_option()

    def test_cas_datetime(self):
        self.assertEqual(str2cas_timestamp('19700101T12:00'), 315662400000000)
        self.assertEqual(cas2python_timestamp(315662400000000),
                         datetime.datetime(1970, 1, 1, 12, 0, 0))
        self.assertEqual(cas2sas_timestamp(315662400000000), 315662400)

        self.assertEqual(str2cas_date('19700101T12:00'), 3653)
        self.assertEqual(cas2python_date(3653),
                         datetime.date(1970, 1, 1))
        self.assertEqual(cas2sas_date(3653), 3653)

        self.assertEqual(str2cas_time('19700101T12:00'), 43200000000)
        self.assertEqual(cas2python_time(43200000000),
                         datetime.time(12, 0, 0))
        self.assertEqual(cas2sas_time(43200000000), 43200)

    def test_python2cas(self):
        self.assertEqual(python2cas_datetime(datetime.datetime(1970, 1, 1, 12, 0, 0)),
                         315662400000000)
        self.assertEqual(python2cas_date(datetime.date(1970, 1, 1)),
                         3653)
        self.assertEqual(python2cas_date(datetime.datetime(1970, 1, 1, 12, 0, 0)),
                         3653)
#       self.assertEqual(python2cas_date(datetime.time(12, 0, 0)),
#                        3653)
        self.assertEqual(python2cas_time(datetime.time(12, 0)),
                         43200000000)

    def test_sas_datetime(self):
        self.assertEqual(str2sas_timestamp('19700101T12:00'), 315662400)
        self.assertEqual(sas2python_timestamp(315662400),
                         datetime.datetime(1970, 1, 1, 12, 0, 0))
        self.assertEqual(sas2cas_timestamp(315662400), 315662400000000)

        self.assertEqual(str2sas_date('19700101T12:00'), 3653)
        self.assertEqual(sas2python_date(3653),
                         datetime.date(1970, 1, 1))
        self.assertEqual(sas2cas_date(3653), 3653)

        self.assertEqual(str2sas_time('19700101T12:00'), 43200)
        self.assertEqual(sas2python_time(43200),
                         datetime.time(12, 0, 0))
        self.assertEqual(sas2cas_time(43200), 43200000000)

    def test_python2sas(self):
        self.assertEqual(python2sas_datetime(datetime.datetime(1970, 1, 1, 12, 0, 0)),
                         315662400)
        self.assertEqual(python2sas_date(datetime.date(1970, 1, 1)),
                         3653)
        self.assertEqual(python2sas_date(datetime.datetime(1970, 1, 1, 12, 0, 0)),
                         3653)
#       self.assertEqual(python2sas_date(datetime.time(12, 0, 0)),
#                        3653)
        self.assertEqual(python2sas_time(datetime.time(12, 0)),
                         43200)

    def test_sas_date_conversion(self):
        self.assertEqual(self.dates.loc['N', 'Date'], datetime.date(1960, 1, 21))
        self.assertEqual(self.dates.loc['S', 'Date'], datetime.date(1960, 1, 31))
        self.assertEqual(self.dates.loc['E', 'Date'], datetime.date(1960, 10, 27))
        self.assertEqual(self.dates.loc['W', 'Date'], datetime.date(1961, 2, 4))
        self.assertTrue(pd.isnull(self.dates.loc['X', 'Date']))

    def test_sas_datetime_conversion(self):
        self.assertEqual(self.datetimes.loc['N', 'Datetime'],
                         datetime.datetime(2000, 3, 17, 0, 0, 0))
        self.assertEqual(self.datetimes.loc['S', 'Datetime'],
                         datetime.datetime(1991, 10, 17, 14, 45, 32))
        self.assertEqual(self.datetimes.loc['E', 'Datetime'],
                         datetime.datetime(1960, 1, 1, 0, 0, 0))
        self.assertEqual(self.datetimes.loc['W', 'Datetime'],
                         datetime.datetime(1959, 12, 31, 23, 59, 59))
        self.assertTrue(pd.isnull(self.datetimes.loc['X', 'Datetime']))


if __name__ == '__main__':
    tm.runtests()
