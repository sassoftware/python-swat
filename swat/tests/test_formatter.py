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
import re
import swat
import swat.utils.testing as tm
import unittest
from swat.utils.compat import int32, int64
from swat.formatter import SASFormatter

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestFormatter(tm.TestCase):

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

    def test_format(self):
        f = self.s.SASFormatter()

        out = f.format(np.int32(10))
        self.assertEqual(out, '10')

        out = f.format(int32(10))
        self.assertEqual(out, '10')

        out = f.format(np.int64(10))
        self.assertEqual(out, '10')

        out = f.format(int64(10))
        self.assertEqual(out, '10')

        out = f.format(u'hi there')
        self.assertEqual(out, u'hi there')

        out = f.format(b'hi there')
        self.assertEqual(out, 'hi there')

        out = f.format(None)
        self.assertEqual(out, '')

        out = f.format(np.float64(1.234))
        self.assertEqual(out, '1.234')

        out = f.format(float(1.234))
        self.assertEqual(out, '1.234')

        out = f.format(np.nan)
        self.assertEqual(out, 'nan')

        with self.assertRaises(TypeError):
            f.format({'hi': 'there'})

    def test_basic(self):
        f = SASFormatter()
        out = f.format(np.int32(10))
        self.assertEqual(out, '10')

    def test_locale(self):
        f = SASFormatter(locale='en_ES')
        out = f.format(np.int32(10))
        self.assertEqual(out, '10')

    def test_missing_format(self):
        f = self.s.SASFormatter()

        out = f.format(123.45678, sasfmt='foo7.2')
        self.assertEqual(out, '123.45678')

    def test_render_html(self):
        out = self.table.summary().Summary._render_html_()
        self.assertEqual(len(re.findall('<tr>', out)), 11)
        self.assertTrue(len(re.findall('<th ', out)) >= 16)


if __name__ == '__main__':
    tm.runtests()
