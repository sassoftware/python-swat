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


import swat
import swat.utils.testing as tm
import unittest

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestDataPreprocess(tm.TestCase):

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

        r = self.s.loadactionset(actionset='table')
        self.assertEqual(r, {'actionset': 'table'})

        r = self.s.loadactionset(actionset='datapreprocess')
        self.assertEqual(r, {'actionset': 'datapreprocess'})

        r = tm.load_data(self.s, 'datasources/cars_single.sashdat', self.server_type)

        self.tablename = r['tableName']
        self.assertNotEqual(self.tablename, None)

    def tearDown(self):
        # tear down tests
        self.s.droptable(caslib=self.srcLib, table=self.tablename)
        self.s.endsession()
        del self.s
        self.pathname = None
        self.hdfs = None
        self.tablename = None
        swat.reset_option()

    def test_histogram(self):
        r = self.s.datapreprocess.histogram(table={'caslib': self.srcLib,
                                                   'name': self.tablename},
                                            vars={'MPG_City', 'MPG_Highway'})
        self.assertEqual(r.status, None)
        # self.assertEqualsBench(r, 'testdatapreprocess_histogram')


if __name__ == '__main__':
    tm.runtests()
