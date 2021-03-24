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
import json
import numpy as np
import os
import pandas as pd
import six
import swat
import swat.utils.testing as tm
import sys
import unittest
from bs4 import BeautifulSoup

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestCASResponse(tm.TestCase):

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

    def test_basic(self):
        self.table.loadactionset('simple')
        out = self.table.summary()

        self.assertEqual(len(out), 1)
        self.assertEqual(set(out.keys()), set(['Summary']))

        self.table.groupBy = {"Make"}

        self.table.loadactionset('datapreprocess')
        out = self.table.histogram()

        self.assertTrue('ByGroupInfo' in out)
        self.assertEqual(len(out), 39)
        for i in range(1, 39):
            self.assertTrue(('ByGroup%d.BinDetails' % i) in out)

    def test_disposition(self):
        # The default value of the logflushtime session option causes the response
        # messages to be flushed periodically during the action instead of all messages
        # flushed when the action completes. Flushing periodically caused this test
        # to fail because the test expects all the messages to arrive at once. Set
        # logflushtime so all messages will be flushed at once.
        self.s.sessionProp.setSessOpt(logflushtime=-1)

        conn = self.table.invoke('loadactionset', actionset='simple')

        messages = []
        disp = None
        perf = None
        for resp in conn:
            messages += resp.messages
            disp = resp.disposition
            perf = resp.performance

        self.assertIn("NOTE: Added action set 'simple'.", messages)
        if not messages[0].startswith('WARNING: License for feature'):
            self.assertEqual(disp.to_dict(), dict(debug=None, reason=None,
                                                  severity=0, status=None,
                                                  status_code=0))
        self.assertEqual(set(perf.to_dict().keys()),
                         set(['cpu_system_time', 'cpu_user_time', 'elapsed_time',
                              'memory', 'memory_os', 'memory_quota', 'system_cores',
                              'system_nodes', 'system_total_memory',
                              'data_movement_time', 'data_movement_bytes']))

    def test_str(self):
        conn = self.table.invoke('loadactionset', actionset='simple')

        for resp in conn:
            out = str(resp)
            self.assertTrue(isinstance(out, str))
            self.assertTrue('messages=' in out)
            self.assertTrue('performance=' in out)
            self.assertTrue('disposition=' in out)
            for item in ['cpu_system_time', 'cpu_user_time', 'elapsed_time', 'memory',
                         'memory_os', 'memory_quota', 'system_cores',
                         'system_nodes', 'system_total_memory']:
                self.assertTrue(('%s=' % item) in out)
            for item in ['severity', 'reason', 'status', 'debug']:
                self.assertTrue(('%s=' % item) in out)

            out = repr(resp)
            self.assertTrue(isinstance(out, str))
            self.assertTrue('messages=' in out)
            self.assertTrue('performance=' in out)
            self.assertTrue('disposition=' in out)
            for item in ['cpu_system_time', 'cpu_user_time', 'elapsed_time', 'memory',
                         'memory_os', 'memory_quota', 'system_cores',
                         'system_nodes', 'system_total_memory']:
                self.assertTrue(('%s=' % item) in out)
            for item in ['severity', 'reason', 'status', 'debug']:
                self.assertTrue(('%s=' % item) in out)


if __name__ == '__main__':
    tm.runtests()
