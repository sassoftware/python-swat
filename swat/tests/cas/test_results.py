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
import pandas as pd
import six
import swat
import swat.utils.testing as tm
import sys
import unittest
from bs4 import BeautifulSoup

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestCASResults(tm.TestCase):

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

    def test_basic(self):
        self.table.loadactionset('simple')
        out = self.table.summary()

        self.assertEqual(len(out), 1)
        self.assertEqual(set(out.keys()), set(['Summary']))

        self.table.groupBy = {"Make"}

        self.table.loadactionset('datapreprocess')
        out = self.table.histogram()

        self.assertEqual(len(out), 39)
        self.assertTrue('ByGroupInfo' in out)
        for i in range(1, 39):
            self.assertTrue(('ByGroup%d.BinDetails' % i) in out)

    def test_html(self):
        out = self.table.loadactionset('simple')

        html = out._repr_html_()
        soup = BeautifulSoup(html, 'html.parser')

        self.assertEqual(soup.find_all('div')[2].string, 'simple')

        out = self.table.summary()

        html = out._repr_html_()
        soup = BeautifulSoup(html, 'html.parser')
        htbl = soup.find_all('table')[0]

        headers = [x.string for x in htbl.thead.find_all('th')]
        if 'Skewness' in headers:
            self.assertEqual(
                headers,
                [None, 'Column', 'Min', 'Max', 'N', 'NMiss', 'Mean', 'Sum',
                 'Std', 'StdErr', 'Var', 'USS', 'CSS', 'CV', 'TValue', 'ProbT',
                 'Skewness', 'Kurtosis'])
        else:
            self.assertEqual(
                headers,
                [None, 'Column', 'Min', 'Max', 'N', 'NMiss', 'Mean', 'Sum',
                 'Std', 'StdErr', 'Var', 'USS', 'CSS', 'CV', 'TValue', 'ProbT'])

        caption = [x.string for x in htbl.find_all('caption')]
        index = [x.string for x in htbl.tbody.find_all('tr')]
        data = [x.string for x in htbl.tbody.find_all('td')]
        self.assertEqual(len(caption), 1)
        self.assertEqual(len(index), 10)
        self.assertTrue(len(data) >= 150)

        pd.set_option('display.notebook.repr_html', False)
        html = out._repr_html_()
        self.assertTrue(html is None)

    def test_concat_bygroups(self):
        # No By groups
        out = self.table.summary()

        cout = out.concat_bygroups()

        self.assertEqual(list(out.keys()), list(cout.keys()))
        self.assertEqual(list(out.keys()), ['Summary'])

        # By groups
        out = self.table.groupby(['Origin']).summary()

        names = ['ByGroupInfo', 'ByGroup1.Summary',
                 'ByGroup2.Summary', 'ByGroup3.Summary']

        # Return new CASResults object
        self.assertEqual(set(out.keys()), set(names))
        self.assertEqual(sum([len(out[x]) for x in names[1:]]), 30)

        cout = out.concat_bygroups()

        self.assertEqual(set(out.keys()), set(names))
        self.assertEqual(list(cout.keys()), ['Summary'])
        self.assertEqual(len(cout['Summary']), 30)

        # In-place
        iout = out.concat_bygroups(inplace=True)

        self.assertTrue(iout is None)
        self.assertEqual(list(out.keys()), ['Summary'])
        self.assertEqual(len(out['Summary']), 30)

        # By Group Sets
        out = self.table.mdsummary(sets=[dict(groupby=['Origin']),
                                         dict(groupby=['Make', 'Cylinders'])])

        names = ['ByGroupSet1.ByGroupInfo', 'ByGroupSet1.ByGroup1.MDSummary',
                 'ByGroupSet1.ByGroup2.MDSummary', 'ByGroupSet1.ByGroup3.MDSummary',
                 'ByGroupSet2.ByGroupInfo'] + \
                ['ByGroupSet2.ByGroup%d.MDSummary' % i for i in range(1, 88)]

        self.assertEqual(set(out.keys()), set(names))

        cout = out.concat_bygroups()

        self.assertEqual(set(out.keys()), set(names))
        self.assertEqual(set(cout.keys()), set(['ByGroupSet1.MDSummary',
                                                'ByGroupSet2.MDSummary']))
        self.assertEqual(len(cout['ByGroupSet1.MDSummary']), 30)
        self.assertEqual(len(cout['ByGroupSet2.MDSummary']), 870)

        # By Group Sets In-Place
        iout = out.concat_bygroups(inplace=True)

        self.assertTrue(iout is None)
        self.assertEqual(set(out.keys()), set(['ByGroupSet1.MDSummary',
                                               'ByGroupSet2.MDSummary']))
        self.assertEqual(len(out['ByGroupSet1.MDSummary']), 30)
        self.assertEqual(len(out['ByGroupSet2.MDSummary']), 870)

    def test_get_tables(self):
        # No By Groups
        out = self.table.topk()

        topk = out.get_tables('Topk')
        self.assertTrue(isinstance(topk, list))
        self.assertEqual(len(topk), 1)
        self.assertEqual(topk[0].name, 'Topk')

        topkmisc = out.get_tables('TopkMisc')
        self.assertTrue(isinstance(topkmisc, list))
        self.assertEqual(len(topkmisc), 1)
        self.assertEqual(topkmisc[0].name, 'TopkMisc')

        # By Groups
        out = self.table.groupby(['Origin']).topk()

        topk = out.get_tables('Topk')
        self.assertTrue(isinstance(topk, list))
        self.assertEqual(len(topk), 3)
        self.assertEqual([x.name for x in topk], ['Topk', 'Topk', 'Topk'])

        topkmisc = out.get_tables('TopkMisc')
        self.assertTrue(isinstance(topkmisc, list))
        self.assertEqual(len(topkmisc), 3)
        self.assertEqual([x.name for x in topkmisc], ['TopkMisc', 'TopkMisc', 'TopkMisc'])

        # Concat
        topkmisc = out.get_tables('TopkMisc', concat=True)
        self.assertEqual(topkmisc.name, 'TopkMisc')
        self.assertEqual(len(topkmisc), 45)

        # By Group Sets
        out = self.table.mdsummary(sets=[dict(groupby=['Origin']),
                                         dict(groupby=['Make', 'Cylinders'])])

        with self.assertRaises(ValueError):
            out.get_tables('MDSummary')

    def test_get_group(self):
        # No By Groups
        out = self.table.topk()

        with self.assertRaises(KeyError):
            out.get_group(('Origin',))

        # By Groups
        out = self.table.groupby(['Origin', 'Cylinders']).topk()

        # Raw values (note: By groups store formatted value by default)
        grp = out.get_group(('Asia', 4))

        self.assertEqual(set(grp.keys()), set(['Topk', 'TopkMisc']))
        self.assertEqual(set(grp.Topk.index.names), set(['Origin', 'Cylinders']))
        self.assertEqual(set(grp.Topk.index.values), set([('Asia', '4')]))

        # Formatted values
        grp = out.get_group(('Asia', '4'))

        self.assertEqual(set(grp.keys()), set(['Topk', 'TopkMisc']))
        self.assertEqual(set(grp.Topk.index.names), set(['Origin', 'Cylinders']))
        self.assertEqual(set(grp.Topk.index.values), set([('Asia', '4')]))

        # Non-existent
        with self.assertRaises(KeyError):
            out.get_group(('Asia', 10))

        with self.assertRaises(KeyError):
            out.get_group(('Asia',))

        # Key/value pairs with raw By values
        grp = out.get_group(Origin='Asia', Cylinders=4)

        self.assertEqual(set(grp.keys()), set(['Topk', 'TopkMisc']))
        self.assertEqual(set(grp.Topk.index.names), set(['Origin', 'Cylinders']))
        self.assertEqual(set(grp.Topk.index.values), set([('Asia', '4')]))

        # Key/value pairs with formatted By values
        grp = out.get_group(Cylinders='4', Origin='Asia')

        self.assertEqual(set(grp.keys()), set(['Topk', 'TopkMisc']))
        self.assertEqual(set(grp.Topk.index.names), set(['Origin', 'Cylinders']))
        self.assertEqual(set(grp.Topk.index.values), set([('Asia', '4')]))

        # Non-existent
        with self.assertRaises(KeyError):
            out.get_group(Origin='Asia', Cylinders=10)

        with self.assertRaises(KeyError):
            out.get_group(Origin='Asia', Make='Mazda')

        with self.assertRaises(KeyError):
            out.get_group(Origin='Asia')

        # By Group Sets
        out = self.table.mdsummary(sets=[dict(groupby=['Origin']),
                                         dict(groupby=['Make', 'Cylinders'])])

        with self.assertRaises(IndexError):
            out.get_group(('Asia',))

    def test_get_set(self):
        # No By Groups
        out = self.table.topk()
        with self.assertRaises(IndexError):
            out.get_set(2)

        # No By Group sets
        out = self.table.groupby('Origin').topk()
        with self.assertRaises(IndexError):
            out.get_set(2)

        # By Group Sets
        out = self.table.mdsummary(sets=[dict(groupby=['Origin']),
                                         dict(groupby=['Make', 'Cylinders'])])

        sout = out.get_set(1)
        self.assertEqual(set(sout.keys()),
                         set(['ByGroupInfo', 'ByGroup1.MDSummary',
                              'ByGroup2.MDSummary', 'ByGroup3.MDSummary']))

        sout = out.get_set(2)
        self.assertEqual(set(sout.keys()),
                         set(['ByGroupInfo']
                             + ['ByGroup%d.MDSummary' % i for i in range(1, 88)]))

        with self.assertRaises(IndexError):
            out.get_set(3)

    def test_str(self):
        out = self.table.groupby('Origin').topk()

        sout = str(out)

        self.assertTrue('[ByGroup1.TopkMisc]' in sout)
        self.assertTrue('[ByGroup2.TopkMisc]' in sout)
        self.assertTrue('[ByGroup3.TopkMisc]' in sout)
        self.assertTrue('[ByGroup1.Topk]' in sout)
        self.assertTrue('[ByGroup2.Topk]' in sout)
        self.assertTrue('[ByGroup3.Topk]' in sout)
        self.assertTrue('+ Elapsed: ' in sout or '+ Mem: ' in sout)

    def test_render_html(self):
        out = self.table.groupby('Origin').topk()

        rout = out._render_html_()

        self.assertTrue('<h3 class="byline">Origin=Asia</h3>' in rout)
        self.assertTrue('<h3 class="byline">Origin=Europe</h3>' in rout)
        self.assertTrue('<h3 class="byline">Origin=USA</h3>' in rout)

# NOTE: Javascript rendering will not be supported at this time
#   def test_javascript(self):
#       swat.options.display.notebook.repr_javascript = True

#       out = self.table.loadactionset('simple')
#       js = out._repr_javascript_()
#       self.assertTrue('new swat.CASResults(element, JSON.parse(' in js)

#       out = self.table.summary()
#       js = out._repr_javascript_()
#       self.assertTrue('new swat.CASResults(element, JSON.parse(' in js)

#       swat.options.cas.dataset.format = 'dataframe'
#       out = self.table.summary()
#       js = out._repr_javascript_()
#       self.assertTrue('new swat.CASResults(element, JSON.parse(' in js)

#       swat.options.display.notebook.repr_javascript = False
#       js = out._repr_javascript_()
#       self.assertTrue(js is None)

#   def test_json(self):
#       out = self.table.loadactionset('simple')
#       out = self.table.summary()

#       js = out._my_repr_json_()
#       data = json.loads(js)

#       self.assertEqual(set(['session', 'sessionname', 'performance',
#                             'signature','status_code', 'messages', 'debug',
#                             'status', 'reason', 'severity']),
#                        set(data.keys()))


if __name__ == '__main__':
    tm.runtests()
