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

import re
import swat
import swat.utils.testing as tm
import sys
import unittest
from datetime import datetime

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestParams(tm.TestCase):

    def setUp(self):
        swat.reset_option()
        swat.options.cas.print_messages = False
        swat.options.interactive_mode = False

        self.s = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        r = self.s.loadactionset(actionset='actionTest')
        if r.severity != 0:
            self.skipTest("actionTest failed to load")

        self.s.loadactionset(actionset='simple')

        server_type = tm.get_cas_host_type(self.s)

        self.trgLib = tm.get_casout_lib(server_type)

    def tearDown(self):
        # tear down tests
        self.s.endsession()
        del self.s
        swat.reset_option()

    def test_empty(self):
        r = self.s.actionTest.testparms()
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertNotEqual(r.performance, None)
        self.assertEqual(r, {})

    def test_badparm(self):
        r = self.s.actionTest.testparms(nuclear="fusion")
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertNotEqual(r.performance, None)
        self.assertEqual(r, {})

    def test_badlistparm(self):
        r = self.s.actionTest.testparms(dbl=5, dblList={3, 4, "foo"})
        self.assertEqual(r, {})
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertNotEqual(r.performance, None)
        # One version of python produces 'dblList[0]'; One produces 'dblList[1].
        self.assertContainsMessage(
            r, "ERROR: Error converting string parameter 'dblList[")

    def test_badparm2(self):
        r = self.s.actionTest.testparms("fusion")
        self.assertEqual(r.severity, 2)
        self.assertTrue('PARSE_ERROR' in r.debug)

    def test_wrongtype(self):
        r = self.s.actionTest.testparms(i32="xxx")
        self.assertEqual(r.status,
                         "A parameter was the wrong type and could not be converted.")
        self.assertNotEqual(r.performance, None)
        self.assertEqual(r, {})

    def test_wrongtype2(self):
        r = self.s.actionTest.testparms(dbl=5, i32="xxx")
        self.assertEqual(r.status,
                         "A parameter was the wrong type and could not be converted.")
        self.assertNotEqual(r.performance, None)
        self.assertEqual(r, {})
        self.assertContainsMessage(
            r,
            "ERROR: An attempt was made to convert parameter 'i32' from string "
            "to HEX4, but the conversion failed.")

    def test_minfailWithString(self):
        r = self.s.actionTest.testrangeparms(str="xxx", dblinclminmax=0)
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertEqual(r, {})
        self.assertContainsMessage(r, "ERROR: Parameter 'str' is not recognized.")

    def test_minfail(self):
        r = self.s.actionTest.testrangeparms(dblinclminmax=0)
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertEqual(r, {})
        self.assertContainsMessage(
            r,
            "ERROR: Value 0 was found for parameter 'dblInclMinMax', "
            "but the parameter must be greater than or equal to 1.")

    def test_mingood(self):
        r = self.s.actionTest.testrangeparms(dblinclminmax=5)
        self.assertEqual(
            r,
            {0: "dblInclMinMax=5, dblExclMinMax=5, dblInclMin=5, "
                "dblInclMax=5, dblExclMin=5, dblExclMax=5, dblBadRangeDefault=-1"})
        self.assertEqual(r.status, None)

    def test_exclmaxfailWithString(self):
        r = self.s.actionTest.testrangeparms(str="xxx", dblexclminmax=10)
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertEqual(r, {})
        self.assertContainsMessage(r, "ERROR: Parameter 'str' is not recognized.")

    def test_exclmaxfail(self):
        r = self.s.actionTest.testrangeparms(dblexclmax=10)
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertEqual(r, {})
        self.assertContainsMessage(
            r,
            "ERROR: Value 10 was found for parameter 'dblExclMax', "
            "but the parameter must be less than 10.")

    def test_inclmaxgood(self):
        r = self.s.actionTest.testrangeparms(dblinclmax=10)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "dblInclMinMax=5, dblExclMinMax=5, dblInclMin=5, dblInclMax=10, "
                "dblExclMin=5, dblExclMax=5, dblBadRangeDefault=-1"})

    def test_noreq(self):
        r = self.s.actionTest.testparms(str="xxx")
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertNotEqual(r.performance, None)

    def test_str(self):
        r = self.s.actionTest.testparms(dbl=5., str="xxx")
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='xxx', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_bool(self):
        r = self.s.actionTest.testparms(bool=False, dbl=5.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=false, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_int32(self):
        r = self.s.actionTest.testparms(i32=100001, dbl=5.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=100001, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_int32hex(self):
        r = self.s.actionTest.testparms(i32="12AB", dbl=5.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=4779, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_conversion(self):
        r = self.s.actionTest.testparms(dbl=5., i64="10000000000")
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_date(self):
        r = self.s.actionTest.testparms(date="May 19, 1963", dbl=5.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=1234, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_time(self):
        r = self.s.actionTest.testparms(time="00:00:01", dbl=5.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=1000000, datetime=9999, nodes='ALL', varz={}"})

    def test_datetime(self):
        r = self.s.actionTest.testparms(datetime="01JAN60 00:00:01", dbl=5.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=1000000, nodes='ALL', varz={}"})

    def test_strList(self):
        r = self.s.actionTest.testparms(dbl=5., strList=["aaa", "bbb", "ccc"])
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'aaa', 'bbb', 'ccc'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_strList_toolong(self):
        r = self.s.actionTest.testparms(
            dbl=5, strList=["aaa", "bbb", "ccc", "ddd", "eee", "fff"])
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertContainsMessage(
            r,
            "ERROR: Parameter 'strList' is a list of length 6, "
            "but the length must be less than or equal to 5.")

    def test_dblList(self):
        r = self.s.actionTest.testparms(
            dbl=5., dblList=[1., 2., 3., 4., 5., 6., 7., 8., 9., 10.,
                             11., 12., 13., 14., 15., 16., 17., 18., 19., 20.])
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4, 5, 6, 7, "
                "8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, secret=true, "
                "password='--------', covert='--------', color='red', date=9999, "
                "time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_numtodblList(self):
        r = self.s.actionTest.testparms(dbl=42., dblList=42.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=42, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={42}, secret=true, "
                "password='--------', covert='--------', color='red', date=9999, "
                "time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_stringtodblList(self):
        r = self.s.actionTest.testparms(str='42', dbl=42., dblList='42')
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='42', dbl=42, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={42}, secret=true, "
                "password='--------', covert='--------', color='red', date=9999, "
                "time=9999, datetime=9999, nodes='ALL', varz={}"})

    # Should test double=>string here, too, but TKNLS routine we
    # call appears to be having trouble.

    def test_parmarray(self):
        r = self.s.actionTest.testparms(
            dbl=5, varz=[{'name': 'AAA', 'type': 'AAATYPE'},
                         {'name': 'BBB', 'type': 'BBBTYPE'}])
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', "
                "varz={{name='AAA', type='AAATYPE'}, {name='BBB', type='BBBTYPE'}}"})

    def test_parmarray_badtype(self):
        # This is invalid because varz is supposed to be a list
        # of lists, not a single list.
        r = self.s.actionTest.testparms(dbl=5, varz={'AAA', 'AAATYPE'})
        self.assertEqual(r, {})
        self.assertEqual(
            r.status, "A parameter was the wrong type and could not be converted.")
        self.assertContainsMessage(
            r,
            re.compile(r"ERROR: An attempt was made to convert parameter 'varz\[\d\]' "
                       r"from string to parameter list, but the conversion failed\."))

    def test_parmarray_badsubparm(self):
        # This is invalid because 'foo' is not a valid subparm of 'varz'
        r = self.s.actionTest.testparms(
            dbl=5, varz=[{'name': 'AAA', 'type': 'AAATYPE'},
                         {'name': 'BBB', 'type': 'BBBTYPE', 'foo': 2}])
        self.assertEqual(r, {})
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertContainsMessage(
            r, re.compile(r"ERROR: Parameter 'varz\[\d\].foo' is not recognized\."))

    def test_enumfail(self):
        r = self.s.actionTest.testparms(color="orange", dbl=5.)
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertEqual(r, {})

    def test_enum(self):
        r = self.s.actionTest.testparms(color="blue", dbl=5.)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='blue', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_multi(self):
        r = self.s.actionTest.testparms(
            dbl=5, str="xxx", secret=False, strList=["A"], dblList=[3.14], i32=42)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {0: "str='xxx', dbl=5, bool=true, i64=10000000000, i32=42, "
                "strList={'A'}, dblList={3.14}, secret=false, "
                "password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})

    def test_multiCoerceFromString(self):
        # This is testing coercion to double, boolean, int32, and int64 from string
        r = self.s.actionTest.testparms(
            dbl="5", str="xxx", secret="false", strList={"A"},
            dblList={"3.14"}, i32="2A", i64="20000000000")
        self.assertEqual(
            r,
            {0: "str='xxx', dbl=5, bool=true, i64=20000000000, i32=42, "
                "strList={'A'}, dblList={3.14}, secret=false, "
                "password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})
        self.assertEqual(r.status, None)

    def test_noCoerceNumberToString(self):
        # Test to make sure we do not allow coercion from number to string
        r = self.s.actionTest.testparms(dbl=5, str=7)
        self.assertEqual(r, {})
        # Python2.7 produces "...from int32 to string" and
        # Python3.4 produces "...from int64 to string"
        self.assertContainsMessage(r, "ERROR: Cannot convert parameter 'str' ")

    def test_noCoerceBooleanToNumber(self):
        # Test to make sure we do not allow coercion from number to string
        r = self.s.actionTest.testparms(dbl=False)
        self.assertEqual(r, {})
        self.assertContainsMessage(
            r, "ERROR: Cannot convert parameter 'dbl' from boolean to double.")

        r = self.s.actionTest.testparms(dbl=5, i32=True)
        self.assertEqual(r, {})
        self.assertContainsMessage(
            r, "ERROR: Cannot convert parameter 'i32' from boolean to int32.")

        r = self.s.actionTest.testparms(dbl=5, i64=False)
        self.assertEqual(r, {})
        self.assertContainsMessage(
            r, "ERROR: Cannot convert parameter 'i64' from boolean to int64.")

    def test_noCoerceBooleanToString(self):
        # Test to make sure we do not allow coercion from boolean to string
        r = self.s.actionTest.testparms(dbl=5, str=False)
        self.assertEqual(r, {})
        self.assertContainsMessage(
            r, "ERROR: Cannot convert parameter 'str' from boolean to string.")

    def test_noCoerceNumberToBoolean(self):
        # Test to make sure we do not allow coercion from arbitrary number to boolean
        r = self.s.actionTest.testparms(dbl=5, bool=2)
        self.assertEqual(r, {})
        self.assertEqual(
            r.status, "A parameter was the wrong type and could not be converted.")
        # Python2.7 produces "...from int32 to boolean" and
        # Python3.4 produces "...from int64 to boolean"
        self.assertContainsMessage(
            r, "ERROR: An attempt was made to convert parameter 'bool' ")

    def test_noCoerceBadStringToNumber(self):
        r = self.s.actionTest.testparms(dbl="foo")
        self.assertEqual(r, {})
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertContainsMessage(
            r, "ERROR: Error converting string parameter 'dbl' to double.")

        # Coercion from string to i32 is already covered by test_wrongtype2

        r = self.s.actionTest.testparms(dbl=5, i64="bar")
        self.assertEqual(r, {})
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertContainsMessage(
            r, "ERROR: Error converting string parameter 'i64' to int64.")

    def test_noCoerceBadStringToBoolean(self):
        r = self.s.actionTest.testparms(dbl=5, bool="foo")
        self.assertEqual(r, {})
        self.assertEqual(
            r.status, "A parameter was the wrong type and could not be converted.")
        self.assertContainsMessage(
            r,
            "ERROR: An attempt was made to convert parameter 'bool' "
            "from string to boolean, but the conversion failed.")

    def test_coerce1or0ToBoolean(self):
        # Test to make sure we allow coercion from 1 or 0 to boolean
        r = self.s.actionTest.testparms(dbl=5, bool=1)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})
        self.assertEqual(r.status, None)

        r = self.s.actionTest.testparms(dbl=5, bool=0)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=false, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', color='red', "
                "date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})
        self.assertEqual(r.status, None)

    def test_badlist(self):
        r = self.s.actionTest.testparms(dbl=5, strList=[{"ABC"}])
        self.assertEqual(r, {})
        self.assertEqual(r.status, "Error parsing action parameters.")
        self.assertContainsMessage(
            r,
            re.compile(r"ERROR: Cannot convert parameter 'strList\[\d\]' "
                       r"from value_list to string\."))

    def test_altnum(self):
        # 08/20/2014: This test produces an exception on the server when
        # run with Python 2.7, but passes with 3.4.
        # Need to investigate.
        if sys.version_info[:2] == (2, 7):
            return

        r = self.s.actionTest.testparms(dbl=5, nodes=1)
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', "
                "color='red', date=9999, time=9999, datetime=9999, nodes=1, varz={}"})
        self.assertEqual(r.status, None)

    def test_altall(self):
        r = self.s.actionTest.testparms(dbl=5, nodes="ALL")
        self.assertEqual(
            r,
            {0: "str='default', dbl=5, bool=true, i64=10000000000, i32=43981, "
                "strList={'ONE', 'TWO', 'THREE'}, dblList={1, 2, 3, 4}, "
                "secret=true, password='--------', covert='--------', "
                "color='red', date=9999, time=9999, datetime=9999, nodes='ALL', varz={}"})
        self.assertEqual(r.status, None)

    def test_altbad(self):
        r = self.s.actionTest.testparms(dbl=5, nodes="FOO")
        self.assertEqual(r, {})
        self.assertEqual(
            r.status, "A parameter was the wrong type and could not be converted.")
        self.assertContainsMessage(
            r,
            "ERROR: An attempt was made to convert parameter 'nodes' from "
            "'string' to one of a set of alternative types, but the conversion failed.")

    def test_sysparm(self):
        # Because the node name doesn't match, these _fail commands never
        # cause a failure. What this test is more interested in is whether
        # the _fail commands parse successfully.
        r = self.s.about(_fail="foobar")
        self.assertNotEqual(r, None)
        self.assertEqual(r.status, None)

        if (datetime.now() > datetime(2014, 7, 26)):
            r = self.s.about(_fail={'node': 'foobar'})

        self.assertNotEqual(r, None)
        self.assertEqual(r.status, None)

    '''
    08/20/2014:
    This test is currently failing when python tries to parse the rows that come
    back from the server. Note sent to developers.

    def test_types1(self):
        r = self.s.actionTest.alltypes(casout=dict(name='ALL', caslib=self.trgLib))
        tab = r['tableName']

        # Test code to see if we can get the test case working with a
        # table other than 'ALL'.
        #self.pathname="datasources/cars_single.sashdat"
        #r = self.s.loadtable(caslib=self.trgLib, path=self.pathname)
        #tab = r['tableName']

        #r  = self.s.table.fetch(table={'caslib':self.trgLib, 'name':tab})
        #assertEqualsBench(r,'testcastab_types1_sas')

        r = self.s.table.fetch(table={'caslib': self.trgLib, 'name': tab}, sastypes=False)
        #assertEqualsBench(r,'testcastab_types1_extended')
    '''

    def test_alltypesWithZeroArguments(self):
        r = self.s.actionTest.alltypes()
        # Developer said we might get a table name back but that is about all.
        # The alltypes action is for testing only.
        self.assertIsNotNone(r)
        if not r.messages or not r.messages[0].startswith('WARNING: License for feature'):
            self.assertEqual(r.severity, 0)

        # Cleanup after ourselves
        r = self.s.table.droptable(caslib=r['caslib'], table=r['tableName'])

    def test_alltypesWithOuttableContainingOnlyASpace(self):
        r = self.s.actionTest.alltypes(casout=dict(name=' ', caslib=self.trgLib))
        self.assertEqual(r.severity, 2)
        expectedMsg = 'ERROR: A table name for Cloud Analytic Services must not be blank'
        self.assertContainsMessage(r, expectedMsg)

    def test_alltypesWithOuttableContainingOneNonSpaceChar(self):
        r = self.s.actionTest.alltypes(casout=dict(name='a', caslib=self.trgLib))
        self.assertEqual(r['tableName'], "A")
        r = self.s.table.droptable(caslib=self.trgLib, table="a")

    def test_alltypesWithOuttable(self):
        r = self.s.actionTest.alltypes(
            casout=dict(name="ALLTYPES_OUTTABLE", caslib=self.trgLib))
        r = self.s.columninfo(table={'caslib': self.trgLib,
                                     'name': 'ALLTYPES_OUTTABLE'})
        # self.s.assertEqualsBench(r, 'testcastab_alltypesWithOuttable')

        # Cleanup after ourselves
        r = self.s.table.droptable(caslib=self.trgLib, table="ALLTYPES_OUTTABLE")
        self.assertRegex(
            r.status,
            r"^Cloud Analytic Services dropped table ALLTYPES_OUTTABLE from caslib "
            + self.trgLib.upper() + r"(\([^\)]+\))?\.$")

    def test_alltypesWithOuttableContainingSpecialChars(self):
        r = self.s.actionTest.alltypes(
            casout=dict(name="MYDATA.MEM ~`!@#$%^&()_+={}[];,'", caslib=self.trgLib))
        self.assertEqual(r['tableName'], "MYDATA.MEM ~`!@#$%^&()_+={}[];,'")
        r = self.s.table.droptable(caslib=self.trgLib,
                                   table="MYDATA.MEM ~`!@#$%^&()_+={}[];,'")

    def test_alltypesWithNoExistingTableAndReplaceFalse(self):
        r = self.s.actionTest.alltypes(
            casout=dict(name='A1', caslib=self.trgLib, replace=False))
        self.assertEqual(r['tableName'], "A1")
        r = self.s.table.droptable(caslib=self.trgLib, table="A1")

    def test_alltypesWithNoExistingTableAndReplaceTrue(self):
        r = self.s.actionTest.alltypes(
            casout=dict(name='2B', caslib=self.trgLib, replace=True))
        self.assertEqual(r['tableName'], "2B")
        r = self.s.table.droptable(caslib=self.trgLib, table="2B")

    def test_alltypesWithAnExistingTableAndReplaceFalse(self):
        r = self.s.actionTest.alltypes(
            casout=dict(name='A1', caslib=self.trgLib, replace=False))
        self.assertEqual(r['tableName'], "A1")

        r = self.s.actionTest.alltypes(
            casout=dict(name='A1', caslib=self.trgLib, replace=False))
        self.assertEqual(r.status, 'The action was not successful.')
        self.assertContainsMessage(
            r, "ERROR: The table A1 already exists in the session.")
        r = self.s.table.droptable(caslib=self.trgLib, table="A1")

    def test_alltypesWithAnExistingTableAndReplaceTrue(self):
        r = self.s.actionTest.alltypes(
            casout=dict(name='A1', caslib=self.trgLib, replace=False))
        self.assertEqual(r['tableName'], "A1")

        # Call the API under test
        r = self.s.actionTest.alltypes(
            casout=dict(name='A1', caslib=self.trgLib, replace=True))
        self.assertEqual(r['tableName'], "A1")
        r = self.s.table.droptable(caslib=self.trgLib, table="A1")

    def test_alltypesWithOuttableContainingEmptyString(self):
        r = self.s.actionTest.alltypes(casout=dict(name='', caslib=self.trgLib))
        self.assertEqual('tableName' in r, True)
        r = self.s.table.droptable(caslib=r['caslib'], table=r['tableName'])

    def test_alltypesWithNumericZeroPassedForBoolReplaceArgument(self):
        # Verify that a number containing a value of zero can be
        # type coerced into a boolean.
        myTableName = "A1"
        r = self.s.actionTest.alltypes(
            casout=dict(name=myTableName, caslib=self.trgLib))

        # Table should have been added. Verify we can't add the
        # table without replacing it.
        r = self.s.actionTest.alltypes(
            casout=dict(name=myTableName, caslib=self.trgLib, replace=0))
        self.assertContainsMessage(
            r, "ERROR: The table " + myTableName + " already exists in the session.")
        r = self.s.table.droptable(caslib=self.trgLib, table=myTableName)

    def test_alltypesWithNumericOnePassedForBoolReplaceArgument(self):
        # Verify that a number containing a value of one can be
        # type coerced into a boolean.
        myTableName = "A1"
        r = self.s.actionTest.alltypes(casout=dict(name=myTableName, caslib=self.trgLib))

        # Table should have been added. Verify we can replace it.
        r = self.s.actionTest.alltypes(
            casout=dict(name=myTableName, caslib=self.trgLib, replace=1))
        self.assertEqual(r['tableName'], myTableName)
        r = self.s.table.droptable(caslib=self.trgLib, table=myTableName)

    def test_alltypesWithZeroRows(self):
        myTableName = "ZEROROWTABLE"
        r = self.s.actionTest.alltypes(
            casout=dict(name=myTableName, caslib=self.trgLib, replace=False), rows=0)
        self.assertEqual(r['tableName'], myTableName)

        # The table gets created with zero rows when zero rows specified.
        r2 = self.s.simple.numrows(table={'caslib': self.trgLib, 'name': myTableName})
        numRows = r2['numrows']
        self.assertEqual(numRows, 0)
        r = self.s.table.droptable(caslib=self.trgLib, table=myTableName)

    def test_alltypesWithOneRow(self):
        myTableName = "ONEROWTABLE"
        r = self.s.actionTest.alltypes(
            casout=dict(name=myTableName, caslib=self.trgLib, replace=False), rows=1)
        self.assertEqual(r['tableName'], myTableName)

        r2 = self.s.simple.numrows(table={'caslib': self.trgLib, 'name': myTableName})
        numRows = r2['numrows']
        self.assertEqual(numRows, 1)
        r = self.s.table.droptable(caslib=self.trgLib, table=myTableName)

    '''
    def test_alltypesWithMaxRows(self):
        # This test takes a while to run. You might want to uncomment it
        # and run it occasionally.
        # Creating 100,000 rows takes 3+ minutes in SMP mode.
        myTableName = "MAXROWTABLE"
        # http://stackoverflow.com/questions/94591/what-is-the-maximum-value-for-a-int32
        r = self.s.actionTest.alltypes(
            casout=dict(name=myTableName, caslib=self.trgLib,
                        replace=False, rows=2147483647))
        self.assertEqual(r['tableName'], myTableName)
        r  = self.s.table.droptable(caslib=self.trgLib, table=myTableName)
    '''

    def test_alltypesWithGreaterThanMaxRows(self):
        myTableName = "GREATERTHANMAXROWTABLE"
        #  http://stackoverflow.com/questions/94591/what-is-the-maximum-value-for-a-int32
        r = self.s.actionTest.alltypes(
            casout=dict(name=myTableName, caslib=self.trgLib,
                        replace=False), rows=2147483648)

        # The following produces the same status and error message as the previous.
        # r = self.s.actionTest.alltypes(
        #     casout=dict(name=myTableName, replace=False),
        #                 rows=10000000021474836490000000000)

        self.assertEqual(
            r.status, 'A parameter was the wrong type and could not be converted.')

        # According to tkcastab.prm, the rows parameter is defined as:
        # Param{'rows',   t.int32, default=1}
        # It is surprising to see the server say it tried to convert to int32.
        # Expected it would try to convert from int32.  We did get the correct
        # response. Lua sends all numbers as doubles. The server is doing the
        # coercion internally to convert to the appropriate type.
        #
        # Because Lua represents all numbers as doubles, we may have to use
        # strings to represent certain numbers that can't be exactly represented
        # in double. In addition, because either Lua or our SWAT interface can't
        # handle very large integers, we may have to use strings for the bigger
        # int64 values.
        #
        # One version of python produces '...from int64...';
        # One produces '...from double...'
        self.assertContainsMessage(
            r, "ERROR: An attempt was made to convert parameter 'rows' ")

    '''
    def test_int64(self):
        r = self.s.actionTest.testparms(i64=100001)
        self.assertEqual(r.status, None)
        self.assertEqual(
            r,
            {"string str='default'; double dbl=42; boolean bool=true; "
             "int64 i64=100001; int32 i32=100000; "
             "list strList={'ONE', 'TWO', 'THREE'}; list dblList={1, 2, 3, 4}; "
             "boolean secret=true; enum color='red'"})
    '''


if __name__ == '__main__':
    tm.runtests()
