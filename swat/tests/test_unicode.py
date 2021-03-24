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
import pandas as pd
import os
import numpy as np
import re
import six
import swat
import swat.utils.testing as tm
import unittest
from swat.utils.compat import text_types

os.environ['LANG'] = 'en_US.UTF-8'

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


def has_same_items(list1, list2):
    return list(sorted(list1)) == list(sorted(list2))


class TestUnicode(tm.TestCase):

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

    def tearDown(self):
        # tear down tests
        self.s.endsession()
        del self.s
        swat.reset_option()

    def test_connection_unicode_params(self):
        self.assertTrue(isinstance(self.s._hostname, text_types))
        self.assertTrue(isinstance(self.s._username, text_types))
        self.assertTrue(isinstance(self.s._session, text_types))
        self.assertTrue(isinstance(self.s._soptions, text_types))
        self.assertTrue(isinstance(self.s._name, text_types))

        s = swat.CAS(u'%s' % HOST, PORT, USER, PASSWD)

        self.assertTrue(isinstance(s._hostname, text_types))
        self.assertTrue(isinstance(s._username, text_types))
        self.assertTrue(isinstance(s._session, text_types))
        self.assertTrue(isinstance(s._soptions, text_types))
        self.assertTrue(isinstance(s._name, text_types))

        s = swat.CAS(u'%s' % HOST, PORT, USER, PASSWD, name=u'\u2603')

        self.assertTrue(isinstance(s._name, text_types))

    def test_connection_str(self):
        s = swat.CAS(u'%s' % HOST, PORT, USER, PASSWD, name=u'\u2603')
        out = str(s)
        self.assertRegex(out, r"CAS\(u?'[^']+', %d, u?'\w+(@\w+)?'," % PORT)
        self.assertTrue((u"name=u'\\u2603'" in out) or (u"name='\\u2603'" in out)
                        or (u"name=u'\u2603'" in out) or (u"name='\u2603'" in out))

    def test_connection_repr(self):
        s = swat.CAS(u'%s' % HOST, PORT, USER, PASSWD, name=u'\u2603')
        self.assertEqual(str(s), repr(s))

    def test_formatter(self):
        if self.s._protocol in ['http', 'https']:
            unittest.TestCase.skipTest(self, 'REST does not support SAS data formats')

        f = swat.SASFormatter()

        val = f.format(123.56, sasfmt='f10.5', width=20)
        self.assertTrue(isinstance(val, text_types))
        val = f.format(123.56, sasfmt='f10.5', width=20)
        self.assertEqual(val, '123.56000')

        val = f.format(123.56, sasfmt='BEST.', width=20)
        self.assertTrue(isinstance(val, text_types))
        val = f.format(123.56, sasfmt='BEST.', width=20)
        self.assertEqual(val, '123.56')

        val = f.format(123.56, sasfmt='DOLLAR20.2', width=20)
        self.assertTrue(isinstance(val, text_types))
        val = f.format(123.56, sasfmt='DOLLAR20.2', width=20)
        self.assertEqual(val, '$123.56')

        val = f.format(123456.78, sasfmt='EURO20.2', width=20)
        self.assertTrue(isinstance(val, text_types))
        val = f.format(123456.78, sasfmt='EURO20.2', width=20)
        self.assertEqual(val, u'\u20ac123,456.78')

        val = f.format(123456.78, sasfmt='NLMNY20.2', width=20)
        self.assertTrue(isinstance(val, text_types))
        val = f.format(123456.78, sasfmt='NLMNY20.2', width=20)
        self.assertEqual(val, u'$123,456.78')

        f = swat.SASFormatter(soptions='locale=fr-FR')

        val = f.format(123456.78, sasfmt='NLMNY20.2', width=20)
        self.assertTrue(isinstance(val, text_types))
        # Comment out the following line that is returning 'EUR' instead
        # of the 'Euro Sign' unicode character until the reason for the
        # diff is determined.  What _should_ we be getting back from python?
        # Should python be honoring the NLMNY format as documented for 9.4
        # or is it OK for python to return the NLMNYl format ('EUR') instead?
        #
        # self.assertEqual(f.format(123456.78, sasfmt='NLMNY20.2', width=20),
        #                  u'123\u00a0456,78 \u20ac')

    def test_unicode_params(self):
        r = self.s.echo(**{u'a': 1, u'\u2603': 2.3, u'foo': u'\u2603'})

        self.assertTrue(has_same_items(r.keys(), [u'a', u'foo', u'\u2603']))

        self.assertEqual(r[u'a'], 1)
        self.assertEqual(r['a'], 1)

        self.assertEqual(r[u'\u2603'], 2.3)

        self.assertEqual(r[u'foo'], u'\u2603')
        self.assertEqual(r['foo'], u'\u2603')
        self.assertTrue(isinstance(r[u'foo'], text_types))

    def test_unicode_list_params(self):
        r = self.s.echo(**{u'a': [1, 2, 3],
                           u'\u2603': [1.1, 2.3, 4],
                           u'foo': [u'\u2603', u'\u2600', u'\u2680']})

        self.assertTrue(has_same_items(r.keys(), [u'a', u'foo', u'\u2603']))

        self.assertEqual(r[u'a'], [1, 2, 3])
        self.assertEqual(r['a'], [1, 2, 3])

        self.assertEqual(r[u'\u2603'], [1.1, 2.3, 4])

        self.assertTrue(all(isinstance(x, text_types) for x in r[u'foo']))
        self.assertEqual(r[u'foo'], [u'\u2603', u'\u2600', u'\u2680'])
        self.assertEqual(r['foo'], [u'\u2603', u'\u2600', u'\u2680'])

    def test_unicode_tuple_params(self):
        r = self.s.echo(**{u'a': (1, 2, 3),
                           u'\u2603': (1.1, 2.3, 4),
                           u'foo': (u'\u2603', u'\u2600', u'\u2680')})

        self.assertTrue(has_same_items(r.keys(), [u'a', u'foo', u'\u2603']))

        self.assertEqual(r[u'a'], [1, 2, 3])
        self.assertEqual(r['a'], [1, 2, 3])

        self.assertEqual(r[u'\u2603'], [1.1, 2.3, 4])

        self.assertTrue(all(isinstance(x, text_types) for x in r[u'foo']))
        self.assertEqual(r[u'foo'], [u'\u2603', u'\u2600', u'\u2680'])
        self.assertEqual(r['foo'], [u'\u2603', u'\u2600', u'\u2680'])

    def test_unicode_set_params(self):
        r = self.s.echo(**{u'a': set([1, 2, 3]),
                           u'\u2603': set([1.1, 2.3, 4]),
                           u'foo': set([u'\u2603', u'\u2600', u'\u2680'])})

        self.assertTrue(has_same_items(r.keys(), [u'a', u'foo', u'\u2603']))

        self.assertTrue(has_same_items(r[u'a'], [1, 2, 3]))
        self.assertTrue(has_same_items(r['a'], [1, 2, 3]))

        self.assertTrue(has_same_items(r[u'\u2603'], [1.1, 2.3, 4]))

        self.assertTrue(all(isinstance(x, text_types) for x in r[u'foo']))
        self.assertTrue(has_same_items(r[u'foo'], [u'\u2603', u'\u2600', u'\u2680']))
        self.assertTrue(has_same_items(r['foo'], [u'\u2603', u'\u2600', u'\u2680']))

    def test_unicode_dict_params(self):
        r = self.s.echo(**{u'a': {u'x': 1, u'y': 2, u'z': 3},
                           u'\u2603': {u'\u2600': 1.1, u'\u2680': 2.3, u'\u2690': 4}})

        self.assertTrue(has_same_items(r.keys(), [u'a', u'\u2603']))

        self.assertTrue(all(isinstance(x, text_types) for x in r[u'a'].keys()))
        self.assertEqual(r[u'a'], {u'x': 1, u'y': 2, u'z': 3})
        self.assertEqual(r['a'], {u'x': 1, u'y': 2, u'z': 3})

        self.assertTrue(all(isinstance(x, text_types) for x in r[u'\u2603'].keys()))
        self.assertEqual(r[u'\u2603'], {u'\u2600': 1.1, u'\u2680': 2.3, u'\u2690': 4})

    def test_byte_params(self):
        if self.s._protocol in ['http', 'https']:
            unittest.TestCase.skipTest(self, 'REST does not support binary parameters')

        r = self.s.echo(**{'a': 1, 'b': b'\xc2\xa9'})

        self.assertTrue(has_same_items(r.keys(), [u'a', u'b']))

        self.assertEqual(r['a'], 1)

        self.assertEqual(r['b'], u'\xa9')
        self.assertTrue(isinstance(r['b'], text_types))

    def test_byte_list_params(self):
        if self.s._protocol in ['http', 'https']:
            unittest.TestCase.skipTest(self, 'REST does not support binary parameters')

        r = self.s.echo(**{'a': 1, 'b': [b'\xc2\xa9', b'\xe2\x98\x83']})

        self.assertTrue(has_same_items(r.keys(), [u'a', u'b']))

        self.assertEqual(r['a'], 1)

        self.assertEqual(r['b'], [u'\xa9', u'\u2603'])
        self.assertTrue(all(isinstance(x, text_types) for x in r['b']))

    def test_alltypes(self):
        srcLib = tm.get_casout_lib(self.server_type)
        out = self.s.loadactionset(actionset='actionTest')
        if out.severity != 0:
            self.skipTest("actionTest failed to load")

        out = self.s.alltypes(casout=dict(caslib=srcLib, name='typestable'))
        out = self.s.fetch(table=self.s.CASTable('typestable', caslib=srcLib),
                           sastypes=False)

        data = out['Fetch']

        self.assertEqual(data['Double'].iloc[0], 42.42)
        self.assertEqual(type(data['Double'].iloc[0]), np.float64)

        self.assertEqual(data['Char'].iloc[0], u'AbC\u2782\u2781\u2780')
        self.assertTrue(isinstance(data['Char'].iloc[0], text_types))

        self.assertEqual(data['Varchar'].iloc[0],
                         u'This is a test of the Emergency Broadcast System. '
                         u'This is only a test. BEEEEEEEEEEEEEEEEEEP WHAAAA '
                         u'SCREEEEEEEEEEEECH. \u2789\u2788\u2787\u2786\u2785'
                         u'\u2784\u2783\u2782\u2781\u2780 Blastoff!')
        self.assertTrue(isinstance(data['Varchar'].iloc[0], text_types))

        self.assertEqual(data['Int32'].iloc[0], 42)
        self.assertIn(type(data['Int32'].iloc[0]), [np.int32, np.int64])

        # REST interface can sometimes overflow the JSON float
        if np.isnan(data['Int64'].iloc[0]):
            self.assertEqual(type(data['Int64'].iloc[0]), np.float64)
        else:
            self.assertEqual(data['Int64'].iloc[0], 9223372036854775807)
            self.assertEqual(type(data['Int64'].iloc[0]), np.int64)

        self.assertEqual(data['Date'].iloc[0], datetime.date(1963, 5, 19))
        self.assertEqual(type(data['Date'].iloc[0]), datetime.date)
        # self.assertEqual(type(data['Date'].iloc[0]), datetime.Date)

        self.assertEqual(data['Time'].iloc[0], datetime.time(11, 12, 13, 141516))
        self.assertTrue(isinstance(data['Time'].iloc[0], datetime.time))
        # self.assertEqual(type(data['Time'].iloc[0]), datetime.Time)

        self.assertEqual(data['Datetime'].iloc[0],
                         pd.to_datetime('1963-05-19 11:12:13.141516'))
        self.assertTrue(isinstance(data['Datetime'].iloc[0], pd.Timestamp))
        # self.assertEqual(type(data['Datetime'].iloc[0]), datetime.Datetime)

        self.assertEqual(data['DecSext'].iloc[0], '12345678901234567890.123456789')
        self.assertTrue(isinstance(data['DecSext'].iloc[0], text_types))
        # self.assertEqual(type(data['DecSext'].iloc[0]), Decimal)

        # self.assertEqual(data['Varbinary'].iloc[0], '???')
        # self.assertEqual(type(data['Varbinary'].iloc[0]), bytes)

        # self.assertEqual(data['Binary'].iloc[0], '???')
        # self.assertEqual(type(data['Binary'].iloc[0]), bytes)


if __name__ == '__main__':
    tm.runtests()
