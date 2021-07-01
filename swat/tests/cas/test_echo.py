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


class TestEcho(tm.TestCase):

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = False
#       swat.options.trace_actions = True

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        out = self.s.loadactionset(actionset='actionTest')
        if out.severity != 0:
            self.skipTest("actionTest failed to load")

    def tearDown(self):
        # tear down tests
        self.s.endsession()
        del self.s
        swat.reset_option()

    def test_echo_null(self):
        r = self.s.actionTest.testecho()
        self.assertEqual(r, {})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_str(self):
        r = self.s.actionTest.testecho(x='a')
        self.assertEqual(r, {'x': 'a'})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_3(self):
        r = self.s.actionTest.testecho(x=3)
        self.assertEqual(r, {'x': 3})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_false(self):
        r = self.s.actionTest.testecho(x=False)
        self.assertEqual(r, {'x': 0})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_list(self):
        r = self.s.actionTest.testecho(w='a', x='b', y=3, z=False)
        self.assertEqual(r, {'w': 'a', 'x': 'b', 'y': 3, 'z': False})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_emptylist(self):
        r = self.s.actionTest.testecho(x=[])
        self.assertEqual(r, {'x': []})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_emptydict(self):
        r = self.s.actionTest.testecho(x={})
        self.assertEqual(r, {'x': []})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_emptytuple(self):
        r = self.s.actionTest.testecho(emptyTuple=())
        self.assertEqual(r, {'emptyTuple': []})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_singletuple(self):
        # A tuple with one item is constructed by following a value with a comma.
        # On output, tuples are always enclosed in parentheses.
        st = 7,
        r = self.s.actionTest.testecho(singleTuple=st)

        # Because of the way that results come back from the server,
        # there is no way to construct a list or tuple at the output.
        # There is always a possibility of mixed keys and non-keys,
        # so Python always has to use dictionaries for output objects.
        self.assertEqual(r, {'singleTuple': [7]})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_nest(self):
        mytuple = 12345, 54321, 'hello!'
        r = self.s.actionTest.testecho(w=3, x=4, y={5}, z=6, a=[7], t=mytuple)
        self.assertEqual(r, {'w': 3, 'x': 4, 'y': [5], 'z': 6, 'a': [7],
                             't': [12345, 54321, 'hello!']})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_nest_parms(self):
        r = self.s.actionTest.testecho(x=3, y={0: 5, 'alpha': 'beta'},
                                       test=4, orange=True, fred=6)
        self.assertEqual(r, {'x': 3, 'y': [5, 'beta'], 'test': 4,
                             'orange': True, 'fred': 6})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)


if __name__ == '__main__':
    tm.runtests()
