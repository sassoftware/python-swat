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

import os
import swat
import swat.utils.testing as tm
import unittest

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestCall(tm.TestCase):

    server_type = None

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = False

        user, passwd = tm.get_user_pass()

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if type(self).server_type is None:
            type(self).server_type = tm.get_cas_host_type(self.s)

        self.srcLib = tm.get_casout_lib(self.server_type)

    def tearDown(self):
        # tear down tests
        self.s.endsession()
        del self.s
        swat.reset_option()

    def test_dynamic_table_open(self):
        r = self.s.loadactionset(actionset='actionTest')
        if r.severity != 0:
            self.skipTest("actionTest failed to load")

        r = self.s.loadactionset(actionset='sessionProp')

        r = tm.load_data(self.s, 'datasources/cars_single.sashdat', self.server_type)

        self.tablename = r['tableName']
        self.assertNotEqual(self.tablename, None)

        r = self.s.sessionProp.setsessopt(caslib=self.srcLib)

        r = self.s.actionTest.testdynamictable(tableinfo=self.tablename)
        self.assertIn("NOTE: Table '" + self.tablename + "':", r.messages)
        self.assertIn("NOTE: -->Name: " + self.tablename, r.messages)
        self.assertIn("NOTE: -->nRecs: 428", r.messages)
        self.assertIn("NOTE: -->nVars: 15", r.messages)

        self.s.droptable(caslib=self.srcLib, table=self.tablename)

    def test_reflect(self):
        r = self.s.loadactionset(actionset='actionTest')
        if r.severity != 0:
            self.skipTest("actionTest failed to load")

        self.assertEqual(r, {'actionset': 'actionTest'})
        r = self.s.builtins.reflect(actionset="actionTest")
        self.assertEqual(r[0]['name'], 'actionTest')
        self.assertEqual(r[0]['label'], 'Test')
        if 'autoRetry' in r[0]['actions'][0]:
            del r[0]['actions'][0]['autoRetry']
        self.assertEqual(r[0]['actions'][0],
                         {'desc': 'Test function that calls other actions',
                          'name': 'testCall', 'params': []})

        self.assertEqual(r.status, None)
        self.assertNotEqual(r.performance, None)


if __name__ == '__main__':
    tm.runtests()
