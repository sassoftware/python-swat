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


class TestCSess(tm.TestCase):

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = False

    def tearDown(self):
        swat.reset_option()

    # Create a session, loop through a bunch of echo actions
    # then terminate the session. Do this in a loop a few times.
    # Additional session tests are required.

    def conn(self):
        for j in range(1, 6):
            self.s = swat.CAS(HOST, PORT, USER, PASSWD)
            self.assertNotEqual(self.s, None)

            for i in range(1, 11):
                r, z = self.s.help(), self.s.help()
                if r.debug is not None:
                    print(r.debug)

                self.assertNotEqual(r, None)
                self.assertEqual(r.status, None)
                self.assertEqual(r.debug, None)
                self.assertNotEqual(z, None)

            self.s.endsession()
            del self.s

    def test_csess_help(self):
        self.conn()


if __name__ == '__main__':
    tm.runtests()
