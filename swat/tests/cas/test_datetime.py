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
from swat.cas.utils.datetime import *


class TestDateTime(tm.TestCase):
    
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
                        

if __name__ == '__main__':
    tm.runtests()
