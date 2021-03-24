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

import six
import swat
import swat.utils.testing as tm
import unittest
from swat.utils.compat import char_types

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestIMStat(tm.TestCase):

    # Create a class attribute to hold the cas host type
    server_type = None

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = False

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        if TestIMStat.server_type is None:
            # Set once per class and have every test use it.
            # No need to change between tests.
            TestIMStat.server_type = tm.get_cas_host_type(self.s)

        self.srcLib = tm.get_casout_lib(self.server_type)

        r = self.s.loadactionset(actionset='table')
        self.assertEqual(r, {'actionset': 'table'})

        r = self.s.loadactionset(actionset='simple')
        self.assertEqual(r, {'actionset': 'simple'})

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

    def test_nilfilename(self):
        # Expected: TypeError: table.name is a required argument
        r = self.s.summary(table={'caslib': self.srcLib})
        self.assertEqual(r.severity, 2)
        self.assertTrue('PARSE_ERROR' in r.debug)

    def test_badfilename(self):
        sum = self.s.summary(table={'caslib': self.srcLib,
                                    'name': '/hps/badfilename.sashdat'})
        self.assertEqual(sum, {})
        self.assertEqual(sum.status, 'A table could not be loaded.')
        self.assertContainsMessage(sum, "ERROR: Table '/hps/badfilename.sashdat' ")

    def test_badwhere(self):
        sum = self.s.summary(table={'caslib': self.srcLib,
                                    'name': self.tablename,
                                    'where': 'FOOBAR<5'})
        self.assertEqual(sum, {})
        self.assertContainsMessage(
            sum, "ERROR: The WHERE clause 'FOOBAR<5' could not be resolved.")

    def test_countwhere(self):
        sum = self.s.numrows(table={'caslib': self.srcLib,
                                    'name': self.tablename,
                                    'where': '1<5'})
        self.assertEqual(sum, {'numrows': 428})

        sum = self.s.numrows(table={'caslib': self.srcLib,
                                    'name': self.tablename,
                                    'where': '1>5'})
        self.assertEqual(sum, {'numrows': 0})

    def test_summary(self):
        sum = self.s.summary(table={'caslib': self.srcLib, 'name': self.tablename})
        self.assertEqual(sum.status, None)
        # self.assertEqualsBench(sum, 'testimstat_summary')

    def test_summary2(self):
        r = self.s.summary(table={'caslib': self.srcLib, 'name': self.tablename})

        self.assertEqual(list(r.keys())[0], 'Summary')
        tbl = r['Summary']
        cols = tbl.columns
        colinfo = tbl.colinfo
        self.assertEqual(tbl.label, "Descriptive Statistics for " + self.tablename)
        self.assertEqual(tbl.title, "Descriptive Statistics for " + self.tablename)
        self.assertEqual(len(tbl), 10)
        self.assertTrue(len(tbl.columns) >= 15)

        self.assertEqual(cols[0], 'Column')
        self.assertIn(colinfo['Column'].dtype, ['char', 'varchar'])
        self.assertIn(colinfo['Column'].width, [11, 12])
        self.assertEqual(colinfo['Column'].name, 'Column')
        self.assertEqual(colinfo['Column'].label, 'Analysis Variable')

        self.assertEqual(cols[1], 'Min')
        self.assertEqual(colinfo['Min'].dtype, 'double')
        self.assertEqual(colinfo['Min'].width, 8)
        self.assertEqual(colinfo['Min'].name, 'Min')

        self.assertEqual(cols[2], 'Max')
        self.assertEqual(colinfo['Max'].dtype, 'double')
        self.assertEqual(colinfo['Max'].width, 8)
        self.assertEqual(colinfo['Max'].name, 'Max')
        self.assertEqual(colinfo['Max'].format, 'D8.4')

        self.assertEqual(cols[3], 'N')
        self.assertEqual(colinfo['N'].dtype, 'double')
        self.assertEqual(colinfo['N'].width, 8)
        self.assertEqual(colinfo['N'].name, 'N')
        self.assertEqual(colinfo['N'].format, 'BEST10.')

        self.assertEqual(cols[4], 'NMiss')
        self.assertEqual(colinfo['NMiss'].dtype, 'double')
        self.assertEqual(colinfo['NMiss'].width, 8)
        self.assertEqual(colinfo['NMiss'].name, 'NMiss')
        self.assertEqual(colinfo['NMiss'].format, 'BEST10.')

        self.assertEqual(cols[5], 'Mean')
        self.assertEqual(colinfo['Mean'].dtype, 'double')
        self.assertEqual(colinfo['Mean'].width, 8)
        self.assertEqual(colinfo['Mean'].name, 'Mean')
        self.assertEqual(colinfo['Mean'].format, 'D8.4')

        self.assertEqual(cols[6], 'Sum')
        self.assertEqual(colinfo['Sum'].dtype, 'double')
        self.assertEqual(colinfo['Sum'].width, 8)
        self.assertEqual(colinfo['Sum'].name, 'Sum')
        self.assertEqual(colinfo['Sum'].format, 'BEST10.')

        self.assertEqual(cols[7], 'Std')
        self.assertEqual(colinfo['Std'].dtype, 'double')
        self.assertEqual(colinfo['Std'].width, 8)
        self.assertEqual(colinfo['Std'].name, 'Std')
        self.assertEqual(colinfo['Std'].format, 'D8.4')

        self.assertEqual(cols[8], 'StdErr')
        self.assertEqual(colinfo['StdErr'].dtype, 'double')
        self.assertEqual(colinfo['StdErr'].width, 8)
        self.assertEqual(colinfo['StdErr'].name, 'StdErr')
        self.assertEqual(colinfo['StdErr'].format, 'D8.4')

        self.assertEqual(cols[12], 'CV')
        self.assertEqual(colinfo['CV'].dtype, 'double')
        self.assertEqual(colinfo['CV'].width, 8)
        self.assertEqual(colinfo['CV'].name, 'CV')
        self.assertEqual(colinfo['CV'].format, 'D8.4')

        expected = [
            ['MSRP', 10280.0, 192465.0, 428.0, 0.0, 32774.85514018692,
             14027638, 19431.71667371752, 939.2674776639877, 377591612.88763154,
             620985422112, 161231618703.01868, 59.288489882267406,
             34.894059380933605, 4.160411927480192e-127],
            ['Invoice', 9875.0, 173560.0, 428.0, 0.0, 30014.70093457944,
             12846292, 17642.117750314756, 852.7639486634733, 311244318.715971,
             518478936590, 132901324091.7196, 58.77825599118186, 35.19696274874321,
             2.684397703921224e-128],
            ['EngineSize', 1.3, 8.3000000000000007, 428.0, 0.0, 3.1967289719626195,
             1368.2000000000012, 1.1085947183514742, 0.053585948289683515,
             1.2289822495567844, 4898.539999999997, 524.7754205607371,
             34.679033727118153, 59.6561052662767, 3.133744529671493e-209],
            ['Cylinders', 3.0, 12.0, 426.0, 2.0, 5.807511737089202,
             2474, 1.5584426332202244, 0.07550679229836678, 2.4287434410383866,
             15400, 1032.2159624413143, 26.834945907510733, 76.91376577276239,
             1.5155688723940396e-251],
            ['Horsepower', 73.0, 500.0, 428.0, 0.0, 215.88551401869159,
             92399.0, 71.836031583690726, 3.4723256480095284, 5160.415433693011,
             22151103, 2203497.390186917, 33.275058732042154, 62.17317610819295,
             4.18534404473185e-216],
            ['MPG_City', 10.0, 60.0, 428.0, 0.0, 20.060747663551403,
             8586.0, 5.238217638649048, 0.25319880644223236, 27.438924029854007,
             183958, 11716.420560747647, 26.111776721893694, 79.22923470860945,
             1.8662836393934126e-257],
            ['MPG_Highway', 12.0, 66.0, 428.0, 0.0, 26.843457943925234,
             11489.0, 5.7412007169842276, 0.27751141120218892, 32.96138567270021,
             322479, 14074.51168224297, 21.387709172854464, 96.72920413484424,
             1.6656208376181815e-292],
            ['Weight', 1850.0, 7190.0, 428.0, 0.0, 3577.9532710280373,
             1531364.0, 758.98321460987063, 36.686838406826887, 576055.520059533,
             5725124540, 245975707.0654211, 21.212776051482511, 97.52689047094972,
             5.812546635778978e-294],
            ['Wheelbase', 89.0, 144.0, 428.0, 0.0, 108.15420560747664,
             46290.0, 8.3118129910895107, 0.40176664543050045, 69.08623519884436,
             5035958, 29499.822429906577, 7.6851500544098297, 269.19657676307946, 0.0],
            ['Length', 143.0, 238.0, 428.0, 0.0, 186.36214953271028,
             79763.0, 14.357991256895621, 0.69401970287198034, 206.1519129330911,
             14952831, 88026.86682243086, 7.7043494577076173, 268.52573314779056, 0.0]
        ]

        actual = [
            tbl.iloc[0].tolist(),
            tbl.iloc[1].tolist(),
            tbl.iloc[2].tolist(),
            tbl.iloc[3].tolist(),
            tbl.iloc[4].tolist(),
            tbl.iloc[5].tolist(),
            tbl.iloc[6].tolist(),
            tbl.iloc[7].tolist(),
            tbl.iloc[8].tolist(),
            tbl.iloc[9].tolist()
        ]

        for rowIdx, rowVal in enumerate(actual):
            for colIdx, colVal in enumerate(rowVal):
                if len(expected[rowIdx]) > colIdx:
                    if isinstance(colVal, char_types):
                        self.assertEqual(colVal, expected[rowIdx][colIdx])
                    else:
                        self.assertAlmostEqual(colVal, expected[rowIdx][colIdx], places=2)

    def test_crosstab(self):
        r = self.s.crosstab(table={'caslib': self.srcLib, 'name': self.tablename},
                            row='Make', col='Cylinders')
        self.assertEqual(r.status, None)
        # self.assertEqualsBench(r, 'testimstat_crosstab')

    def test_regression(self):
        r = self.s.regression(table={'caslib': self.srcLib, 'name': self.tablename,
                                     'vars': ['MPG_City', 'MPG_Highway']})
        self.assertEqual(r.status, None)
        # self.assertEqualsBench(r, 'testimstat_regression')


if __name__ == '__main__':
    tm.runtests()
