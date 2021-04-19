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
import pprint
import time
import unittest

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()


class TestBuiltins(tm.TestCase):
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

        self.pp = pprint.PrettyPrinter(indent=4)

    def tearDown(self):
        self.s.endsession()
        del self.s
        
        # some testcases create extra connections.
        # tear them down as well
        if hasattr(self,'s2'):
            try:
                self.s2.endsession()
            except swat.SWATError:
                pass
            del self.s2

        swat.reset_option()

    def getActionList(self,conn,getall=False):
        r = conn.actionsetinfo(all=getall)
        if r.severity != 0:
            self.pp.pprint(r.messages)
            self.assertEquals(r.status, None)
        
        self.assertEqual(list(r.keys())[0], 'setinfo')
        setinfo = r['setinfo']
        # Get the actionset column
        return setinfo['actionset'].tolist()
    
    def test_echo(self):
        r = self.s.builtins.echo()
        self.assertEqual(r, {})
        self.assertEqual(r.status, None)

        r = self.s.builtins.echo(a=[1, 2, 3])
        self.assertEqual(r, {'a': [1, 2, 3]})
        self.assertEqual(r.status, None)

    def test_echo_null(self):
        r = self.s.builtins.echo()
        self.assertEqual(r, {})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_str(self):
        r = self.s.builtins.echo(x='a')
        self.assertEqual(r, {'x': 'a'})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_3(self):
        r = self.s.builtins.echo(x=3)
        self.assertEqual(r, {'x': 3})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_false(self):
        r = self.s.builtins.echo(x=False)
        self.assertEqual(r, {'x': 0})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_list(self):
        r = self.s.builtins.echo(w='a', x='b', y=3, z=False)
        self.assertEqual(r, {'w': 'a', 'x': 'b', 'y': 3, 'z': False})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_emptylist(self):
        r = self.s.builtins.echo(x=[])
        self.assertEqual(r, {'x': []})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_emptydict(self):
        r = self.s.builtins.echo(x={})
        self.assertEqual(r, {'x': []})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_emptytuple(self):
        r = self.s.builtins.echo(emptyTuple=())
        self.assertEqual(r, {'emptyTuple': []})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_singletuple(self):
        # A tuple with one item is constructed by following a value with a comma.
        # On output, tuples are always enclosed in parentheses.
        st = 7,
        r = self.s.builtins.echo(singleTuple=st)

        # Because of the way that results come back from the server,
        # there is no way to construct a list or tuple at the output.
        # There is always a possibility of mixed keys and non-keys,
        # so Python always has to use dictionaries for output objects.
        self.assertEqual(r, {'singleTuple': [7]})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_nest(self):
        mytuple = 12345, 54321, 'hello!'
        r = self.s.builtins.echo(w=3, x=4, y={5}, z=6, a=[7], t=mytuple)
        self.assertEqual(r, {'w': 3, 'x': 4, 'y': [5], 'z': 6, 'a': [7],
                             't': [12345, 54321, 'hello!']})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_echo_nest_parms(self):
        r = self.s.builtins.echo(x=3, y={0: 5, 'alpha': 'beta'}, test=4,
                                 orange=True, fred=6)
        self.assertEqual(r, {'x': 3, 'y': [5, 'beta'], 'test': 4,
                             'orange': True, 'fred': 6})
        self.assertEqual(r.status, None)
        # self.assertEqual(r.debug, None)

    def test_addnode(self):
        if not tm.get_cas_superuser_mode():
            tm.TestCase.skipTest(self, 'Testcase assumes SuperUser role.')

        info = self.s.accesscontrol.assumerole(adminrole='SuperUser')
        if info.severity != 0:
            self.skipTest("Specified user does not have admin credentials")

        try:
            r = self.s.addnode()
        except TypeError:
            tm.TestCase.skipTest(self, 'addnode action is not available')

        if r.severity == 0:
            # MPP mode
            self.assertEqual(r['Nodes'], [])
        else:
            # Could fail for a number of reasons such as we're in SMP mode,
            # we don't have proper credentials for addnode, .etc
            self.assertIn(r.status, ['Error parsing action parameters.',
                                     "Authorization",
                                     "Nodes cannot be added when the server "
                                     + "is running with in SMP mode.",
                                     "The addNode action is not supported when executing in Kubernetes. "
                                     + "To increase workers, adjust the CAS Operator workers value."])

        r = self.s.addnode(salt='controller', node=['pepper'])
        self.assertContainsMessage(r, "ERROR: The action stopped due to errors.")

        r = self.s.addnode(node=['salt', 'pepper'])
        self.assertContainsMessage(r, "ERROR: The action stopped due to errors.")

        # Can't be done in unit test because it changes state:
        # self.assertEqual(self.s.addnode{'captain','snap046'}, None)

    def test_help(self):
        r = self.s.help()
        b = r.get('builtins')
        self.assertTrue(b is not None)
        # prettyprint(b)
        # The builtin library should be loaded at least
        self.assertEqual(len(b.columns), 2)
        self.assertTrue(len(b) >= 23)
        self.assertEqual(b.columns[0], 'name')
        self.assertEqual(b.colinfo['name'].label, 'Name')
        self.assertEqual(b.colinfo['name'].name, 'name')
        self.assertIn(b.colinfo['name'].dtype, ['char', 'varchar'])
        self.assertEqual(b.colinfo['name'].width, 64)
        self.assertEqual(b.columns[1], 'description')
        self.assertEqual(b.colinfo['description'].label, 'Description')
        self.assertEqual(b.colinfo['description'].name, 'description')
        self.assertIn(b.colinfo['description'].dtype, ['char', 'varchar'])
        self.assertEqual(b.colinfo['description'].width, 256)

        data = list(list(x) for x in b.itertuples(index=False))

        self.assertIn(['addNode',
                       'Adds a machine to the server'], data)
        self.assertIn(['removeNode',
                       'Remove one or more machines from the server'], data)
        self.assertIn(['help',
                       'Shows the parameters for an action or lists '
                       + 'all available actions'], data)
        self.assertIn(['listNodes',
                       'Shows the host names used by the server'], data)
        self.assertIn(['loadActionSet',
                       'Loads an action set for use in this session'], data)
        self.assertIn(['installActionSet',
                       'Loads an action set in new sessions automatically'], data)
        self.assertIn(['log',
                       'Shows and modifies logging levels'], data)
        self.assertIn(['queryActionSet',
                       'Shows whether an action set is loaded'], data)
        self.assertIn(['queryName',
                       'Checks whether a name is an action or action set name'], data)
        self.assertIn(['reflect',
                       'Shows detailed parameter information for an '
                       + 'action or all actions in an action set'], data)
        self.assertIn(['serverStatus',
                       'Shows the status of the server'], data)
        self.assertIn(['about',
                       'Shows the status of the server'], data)
        self.assertIn(['shutdown',
                       'Shuts down the server'], data)
        self.assertIn(['userInfo',
                       'Shows the user information for your connection'], data)
        self.assertIn(['actionSetInfo',
                       'Shows the build information from loaded action sets'], data)
        self.assertIn(['history',
                       'Shows the actions that were run in this session'], data)
        self.assertIn(['casCommon',
                       'Provides parameters that are common to many actions'], data)
        self.assertIn(['ping',
                       'Sends a single request to the server to confirm '
                       + 'that the connection is working'], data)
        self.assertIn(['echo',
                       'Prints the supplied parameters to the client log'], data)
        self.assertIn(['modifyQueue',
                       'Modifies the action response queue settings'], data)
        self.assertIn(['getLicenseInfo',
                       'Shows the license information for a SAS product'], data)
        self.assertIn(['refreshLicense',
                       'Refresh SAS license information from a file'], data)
        self.assertIn(['httpAddress',
                       'Shows the HTTP address for the server monitor'], data)

    def test_listactions(self):
        # NOTE: 'listactions' is an alias to 'help'
        self.assertNotEqual(self.s.builtins.help(), {})

        # Try to list an actionset that is not loaded. Expect it to not be found.
        out = self.s.builtins.help(actionSet='neuralNet')
        self.assertEqual(self.s.builtins.help(actionSet='neuralNet'), {})

        # Try to list an action that is not loaded. Expect it to not be found.
        out = self.s.builtins.help(action='annTrain')
        self.assertEqual(self.s.builtins.help(action='annTrain'), {})

        # Load an actionSet and then list it.
        r = self.s.builtins.loadactionset(actionset='neuralNet')
        if r.severity != 0:
            self.pp.pprint(r.messages)
            self.assertEquals(r.status, None)

        # List an actionSet that is loaded.
        out = self.s.builtins.help(actionSet='neuralNet')
        # aset = out.get('neuralNet')
        aset = out['neuralNet']
        self.assertTrue(aset is not None)
        self.assertEqual(len(aset.columns), 2)
        self.assertGreaterEqual(len(aset), 3)
        self.assertEqual(aset.columns[0], 'name')
        self.assertEqual(aset.columns[1], 'description')

        info = aset.iloc[0].tolist()
        self.assertTrue(
            info == ['annTrain', 'Train an artificial neural network']
            or info == ['annTrain', 'Trains an artificial neural network']
        )

        info = aset.iloc[1].tolist()
        self.assertTrue(
            info == ['annScore',
                     'Score a table using an artificial neural network model']
            or info == ['annScore',
                        'Scores a table using an artificial neural network model']
        )

        info = aset.iloc[2].tolist()
        self.assertTrue(
            info == ['annCode',
                     'Generate DATA step scoring code from an artificial '
                     'neural network model']
            or info == ['annCode',
                        'Generates DATA step scoring code from an artificial '
                        'neural network model']
        )

        # List an action that is loaded.
        act = self.s.builtins.help(action='annTrain')
        self.assertTrue(act is not None)
        self.assertTrue(act['annTrain'])

        # We get back several hundred lines of output.
        # Verify a few at the beginning of the response.
        self.assertTrue("NOTE: Information for action 'neuralNet.annTrain':"
                        in act.messages)
        self.assertTrue(("NOTE: The following parameters are accepted.  "
                         "Default values are shown.") in act.messages)

        # Verify a line of output near the end of the response.
        self.assertTrue("NOTE:    double dropOut=0 (0 <= value < 1)," in act.messages)

    def test_listnodes(self):
        self.assertNotEqual(self.s.listnodes(), {})
        # Can we do more here?  How do we know what nodes are loaded?

    def test_loadactionset(self):
        self.assertEqual(self.s.loadactionset(), {})
        self.assertEqual(self.s.loadactionset('ohcrumbs'), {})
        self.assertEqual(self.s.loadactionset(
            actionset="You mean, we can't get out? We're trapped! Ahah, ahah! "
                      "Entombed in the blackness of night, doomed to die in the "
                      "darkness of a zipless tomb."), {})
        self.assertEqual(self.s.loadactionset('builtins'), {'actionset': 'builtins'})
        # One could argue that the following should return 'builtins', but we do
        # not believe that it is worth fixing for that one special case.
        self.assertEqual(self.s.loadactionset(actionset='tkcasablt'),
                         {'actionset': 'tkcasablt'})
        self.assertEqual(self.s.loadactionset(actionset=None), {})
        # not here self.assertEqual(self.s.invoke('loadactionset', 'actionTest'),
        #                                         (0, 'actionTest'))

    def test_queryactionset(self):
        r = self.s.queryactionset('builtins')
        self.assertEqual(r['builtins'], True)
        r = self.s.queryactionset('unknown')
        self.assertEqual(r['unknown'], False)

    def test_actionsetinfo(self):
        actionsets = self.getActionList(self.s)    
        self.assertIn('builtins', actionsets)
        self.assertNotIn('autotune', actionsets)

        allactionsets = self.getActionList(self.s, True)
        self.assertIn('builtins', allactionsets)
        self.assertIn('autotune', allactionsets)
        self.assertNotIn('unknown', allactionsets)
        self.assertTrue(len(allactionsets) > len(actionsets))

    def test_log(self):
        # see if the servers logs are immutable. 
        r = self.s.log(logger='App.cas.builtins.log', level="debug")
        # if the severity is 0, the log action can be run without 
        # assuming the superuser role.
        if r.severity != 0:
            self.assertIn(r.status,
                          ["You must be an administrator to set logging levels. "
                           + "The logger is immutable."])
            # Some of our loggers in the shipping logconfig.xml files are marked 'immutable'.
            # That means you must be an administrator to change them.
            if not tm.get_cas_superuser_mode():
                tm.TestCase.skipTest(self, 'Testcase assumes SuperUser role.')

            # Assume SuperUser
            info = self.s.accesscontrol.assumerole(adminrole='SuperUser')
            if info.severity != 0:
                self.skipTest("Specified user does not have admin credentials")

            # Expect that we are a SuperUser
            info = self.s.accesscontrol.isInRole(adminRole="SuperUser")
            self.assertEqual(info['inRole'],"TRUE")
            # re-issue the original command as a superuser
            r = self.s.log(logger='App.cas.builtins.log', level="debug")
            self.assertEqual(r.severity,0)        
        
        r = self.s.log(invalid='parameter')
        self.assertNotEqual(r.status, None)
        r = self.s.log(logger='App.cas.builtins.log', level="invalid")
        self.assertNotEqual(r.status, None)
        r = self.s.log(logger='App.cas.builtins.log')
        self.assertEqual(r.status, None)
        self.assertEqual(r.messages[0],
                         "NOTE: Logger: 'App.cas.builtins.log', level: 'debug'.")
        r = self.s.log(logger='App.cas.builtins.log', level="info")
        r = self.s.log(logger='App.cas.builtins.log')
        self.assertEqual(r.messages[0],
                         "NOTE: Logger: 'App.cas.builtins.log', level: 'info'.")
        r = self.s.log(logger='App.cas.builtins.log', level="null")
        r = self.s.log(logger='App.cas.builtins.log')
        self.assertEqual(r.messages[0],
                         "NOTE: Logger: 'App.cas.builtins.log', level: 'null'.")
        r = self.s.log(logger='App.cas.builtins.log')
        r = self.s.log()
        self.assertEqual(r.status, None)
        self.assertTrue(len(r.messages) >= 1)

    def test_reflect(self):
        r = self.s.reflect()
        self.assertTrue('label' in r[0])
        self.assertTrue('name' in r[0])
        self.assertTrue('actions' in r[0])
        self.assertTrue(len(r[0]['actions']) >= 9)

    def test_serverstatus(self):
        self.assertNotEqual([x for x in self.s.invoke('serverstatus')], [])
        # Can we do more here?  How do we know what nodes are loaded?

    def test_userinfo(self):
        r = self.s.userinfo()
        self.assertNotEqual(r, None)
        userInfo = r['userInfo']
        self.assertTrue(len(userInfo) >= 7)
        self.assertEqual(userInfo['anonymous'], 0)
        # WX6 returns an empty string for groups
        if isinstance(userInfo['groups'], list):
            self.assertTrue('users' in userInfo['groups']
                            or 'Everyone' in userInfo['groups']
                            or 'openid' in userInfo['groups']
                            or 'SASAdministrators' in userInfo['groups'])
        self.assertIn(userInfo['hostAccount'], [1, False])
        self.assertEqual(userInfo['providedName'].split('\\')[-1].split('@')[0],
                         self.s._username.split('\\')[-1].split('@')[0])
        # WX6 returns 'Windows' for providerName
        self.assertIn(userInfo['providerName'],
                      ['Active Directory', 'Windows', 'OAuth/External PAM',
                       'External PAM', 'OAuth', 'OAuth/Windows'])
        # WX6 returns the domain for uniqueId
        self.assertTrue(userInfo['uniqueId'], self.s._username.split('@')[0])
        self.assertTrue(userInfo['userId'], self.s._username.split('@')[0])

    def test_getusers(self):
        # This testcase executes a hidden action.
        # If CAS_ACTION_TEST_MODE was not enabled, skip the testcase
        if not tm.get_cas_action_test_mode():
            tm.TestCase.skipTest(self, 'CAS_ACTION_TEST_MODE not enabled.')
        
        # getusers not in optimized builds
        if 'getusers' not in self.s.builtins.actions:
            tm.TestCase.skipTest(self, 'getusers action not supported in this build')
        r = self.s.builtins.getusers()
        self.assertNotEqual(r, None)
        # Can we do more here?  How do we know what users are on the system?

    def test_getgroups(self):
        # This testcase executes a hidden action.
        # If CAS_ACTION_TEST_MODE was not enabled, skip the testcase
        if not tm.get_cas_action_test_mode():
            tm.TestCase.skipTest(self, 'CAS_ACTION_TEST_MODE not enabled.')

        # getgroups not in optimized builds
        if 'getgroups' not in self.s.builtins.actions:
            tm.TestCase.skipTest(self, 'getgroups action not supported in this build')
        r = self.s.builtins.getgroups()
        self.assertNotEqual(r, None)
        # Can we do more here?  How do we know what groups are on the system?

    # Can't be done in unit test because it changes state:
    # def test_shutdown(self):
    #     self.assertEqual(self.s.shutdown(), {})

    def test_loop(self):
        for i in range(5):
            mytuple = ["Iteration", i]
            out = self.s.builtins.echo(t=mytuple)
            d = out
            self.assertEqual(d['t'], mytuple)
            time.sleep(0.25)

    def test_http(self):
        r = self.s.builtins.httpAddress()
        self.assertNotEqual(r, None)
        self.assertTrue(r['protocol'] in ['http', 'https'])
        # This is not reliable in Viya 4.  The server port number may
        # be different than the ingress port number.
        # if self.s._protocol in ['http', 'https']:
        #     self.assertEqual(int(r['port']), self.s._port)
        # 02/20/2016: bosout: Documentation indicates the action
        # should return virtualHost.  However, that is not being returned.
        # Developers notified. Comment out until we know more.
        # self.assertNotEqual(r['virtualHost'], None)

    def test_ping(self):
        # The .prm file indicates that ping is a dummy action and does nothing.
        # It says ping is used by UIs to poll for relevant changes like new caslibs.
        r = self.s.builtins.ping()
        self.assertEqual(r, {})
        self.assertEqual(r.severity, 0)

        # Ping with an invalid argument
        try:
            r = self.s.builtins.ping(invalidArgument)
        except NameError:
            pass
        # Ping with an invalid argument
        try:
            r = self.s.builtins.ping('invalidArgument')
        except TypeError:
            pass

    def test_installactionset(self):
        if not tm.get_cas_superuser_mode():
            tm.TestCase.skipTest(self, 'Testcase assumes SuperUser role.')

        # Only a superuser can install an actionset.
        # Verify a non-superuser is prohibited from installing actionsets.
        #
        info = self.s.builtins.installactionset(actionset='ohcrumbs')
        self.assertEqual(info.severity, 2)
        self.assertEqual(info.status, 'Authorization')
        self.assertContainsMessage(info, "ERROR: The action stopped due to errors.")

        # Assume SuperUser
        info = self.s.accesscontrol.assumerole(adminrole='SuperUser')
        if info.severity != 0:
            self.skipTest("Specified user does not have admin credentials")

        # Expect that we are a SuperUser
        info = self.s.accesscontrol.isInRole(adminRole="SuperUser")
        self.assertEqual(info['inRole'],"TRUE")

        info = self.s.builtins.installactionset()
        self.assertEqual(info.severity, 2)
        self.assertContainsMessage(info, "ERROR: Parameter 'actionSet' is required but was not specified.")
        
        info = self.s.builtins.installactionset(actionset='ohcrumbs')
        self.assertEqual(info.severity, 2)
        self.assertContainsMessage(info, "ERROR: Action set 'ohcrumbs' was not loaded due to errors.")

        self.assertEqual(self.s.builtins.installactionset(actionset="Oh, 'eck! I'm on me own again! I suppose I'll have to face the fiendish foes alone! Disregarding any thought for my safety and... [DM: and stop overacting?] And stop overacting! Eh?"), {})        
        self.assertEqual(self.s.builtins.installactionset(actionset='builtins'), {'actionset': 'builtins'})
        # One could argue that the following should return 'builtins', but we do
        # not believe that it is worth fixing for that one special case.
        self.assertEqual(self.s.builtins.installactionset(actionset='tkcasablt'), {'actionset': 'tkcasablt'})
        self.assertEqual(self.s.builtins.installactionset(actionset=None), {})

        info = self.s.builtins.installactionset(actionset='actionTest')
        if info.severity == 0:
            # Must be running a debug build where actiontest is available to install
            self.assertEqual(info, {'actionset': 'actionTest'})

        # Install an actionSet in the orginal session. Create a second session.
        # Verify the installed action set is loaded automatically in the second session.
        self.assertEqual(self.s.builtins.installactionset(actionset='fedSql'), {'actionset': 'fedSql'})
        self.assertEqual(self.s.queryactionset('fedSql'), {'fedSql': True})

        self.s2 = swat.CAS(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)
        self.assertEqual(self.s2.queryactionset('fedSql'), {'fedSql': True})

        actionsets = self.getActionList(self.s2)
        self.assertIn('fedSql', actionsets)

    def test_userdefactionsetinfo(self):
        actionsets = self.getActionList(self.s, True)    
        self.assertIn('builtins', actionsets)
        self.assertNotIn('hack', actionsets)

        # add the hack actionset
        r = self.s.builtins.defineactionset(
            actions=[{'name':'hello','definition':'print "Hello World!";'}],name='hack')
        if r.severity != 0:
            self.pp.pprint(r.messages)
            self.assertEquals(r.status, None)
        
        # Defining an actionset seems to load it. Should not hurt to try loading.
        r = self.s.builtins.loadactionset(actionset='hack')
        if r.severity != 0:
            self.pp.pprint(r.messages)
            self.assertEquals(r.status, None)
        
        # hack should be in there now
        
        r = self.s.actionsetinfo(all=True)
        if r.severity != 0:
            self.pp.pprint(r.messages)
            self.assertEquals(r.status, None)
        
        self.assertEqual(list(r.keys())[0], 'setinfo')
        setinfo = r['setinfo']
        # Get the actionset column
        allactionsets = setinfo['actionset'].tolist()
        self.assertIn('hack', allactionsets)
        # take the table part, look for hack in it        
        foundhack=False
        for i, x in setinfo.iterrows():
            if (x.actionset.lower() == "hack"):
                self.assertEqual(x.user_defined.lower(), "true")
                foundhack=True
        self.assertEqual(foundhack,True)
        
    def test_modifyQueue(self):
        r = self.s.builtins.modifyQueue(maxActions=0,maxSize=1)
        self.assertContainsMessage(r, "ERROR: Value 0 was found for parameter 'maxActions', but the parameter must be greater than or equal to 1.")

        r = self.s.builtins.modifyQueue(maxActions=1,maxSize=0)
        self.assertContainsMessage(r, "ERROR: Value 0 was found for parameter 'maxSize', but the parameter must be greater than or equal to 1.")
        
        r = self.s.builtins.modifyQueue(maxActions=1, maxSize=1)
        self.assertContainsMessage(r, "NOTE: Maximum number of actions in output queue now 1")
        self.assertContainsMessage(r, "NOTE: Maximum size of results in output queue now 1")

        r = self.s.builtins.modifyQueue(maxActions=2147483647, maxSize=2147483647)
        self.assertContainsMessage(r, "NOTE: Maximum number of actions in output queue now 2147483647")
        self.assertContainsMessage(r, "NOTE: Maximum size of results in output queue now 2147483647")

        r = self.s.builtins.modifyQueue(maxActions=2147483648, maxSize=1)
        self.assertContainsMessage(r, "ERROR: An attempt was made to convert parameter 'maxActions' from int64 to int32, but the conversion failed.")

        r = self.s.builtins.modifyQueue(maxActions=1, maxSize=2147483648)
        self.assertContainsMessage(r, "NOTE: Maximum size of results in output queue now 2147483648")

        r = self.s.builtins.modifyQueue(maxActions=2147483647, maxSize=9.223372036854775807E18)
        self.assertContainsMessage(r, "ERROR: An attempt was made to convert parameter 'maxSize' from double to int64, but the conversion failed.")

    def test_getLicenseInfo(self):
        # Only administrators can see the licenseFile.
        # This testcase requires running as a SuperUser.
        # If CAS_SUPERUSER_MODE was not enabled, skip the testcase
        if not tm.get_cas_superuser_mode():
            tm.TestCase.skipTest(self, 'Testcase assumes SuperUser role.')

        # Assume SuperUser
        info = self.s.accesscontrol.assumerole(adminrole='SuperUser')
        if info.severity != 0:
            self.skipTest("Specified user does not have admin credentials")

        # Expect that we are a SuperUser
        info = self.s.accesscontrol.isInRole(adminRole="SuperUser")
        self.assertEqual(info['inRole'],"TRUE")

        # Now we should be able to see the license file in the license info
        license = self.s.builtins.getlicenseinfo()

        self.assertNotEqual(license, None)
        self.assertNotEqual(license.isExpired, None)
        self.assertNotEqual(license.isGrace, None)
        self.assertNotEqual(license.isWarning, None)
        self.assertNotEqual(license.cpuCount, None)
        self.assertNotEqual(license.cpuStart, None)
        self.assertNotEqual(license.expDate, None)
        self.assertNotEqual(license.expDateNum, None)
        self.assertNotEqual(license.gracePeriod, None)
        self.assertNotEqual(license.prodID, None)
        self.assertNotEqual(license.serverDate, None)
        self.assertNotEqual(license.serverDateNum, None)
        self.assertNotEqual(license.siteNum, None)
        self.assertNotEqual(license.warningPeriod, None)
        self.assertNotEqual(license.licenseFile, None)
        fileLen=len(license.licenseFile)
        fileSuffix=license.licenseFile[fileLen-4:fileLen]
        self.assertIn(fileSuffix, ['.sas', '.jwt', '.txt'])
        self.assertNotEqual(license.osName, None)
        self.assertNotEqual(license.productName, None)
        self.assertNotEqual(license.release, None)
        self.assertNotEqual(license.siteName, None)

    def test_getLicenseInfoByProdId(self):
        # Only administrators can see the licenseFile.
        # This testcase requires running as a SuperUser.
        # If CAS_SUPERUSER_MODE was not enabled, skip the testcase
        if not tm.get_cas_superuser_mode():
            tm.TestCase.skipTest(self, 'Testcase assumes SuperUser role.')

        # Assume SuperUser
        info = self.s.accesscontrol.assumerole(adminrole='SuperUser')
        if info.severity != 0:
            self.skipTest("Specified user does not have admin credentials")

        # Expect that we are a SuperUser
        info = self.s.accesscontrol.isInRole(adminRole="SuperUser")
        self.assertEqual(info['inRole'],"TRUE")

        # Now we should be able to see the license file in the license info
        license = self.s.builtins.getlicenseinfo(prodId=1141)

        self.assertNotEqual(license, None)
        self.assertNotEqual(license.isExpired, None)
        self.assertNotEqual(license.isGrace, None)
        self.assertNotEqual(license.isWarning, None)
        self.assertNotEqual(license.cpuCount, None)
        self.assertNotEqual(license.cpuStart, None)
        self.assertNotEqual(license.expDate, None)
        self.assertNotEqual(license.expDateNum, None)
        self.assertNotEqual(license.gracePeriod, None)
        self.assertNotEqual(license.prodID, None)
        self.assertNotEqual(license.serverDate, None)
        self.assertNotEqual(license.serverDateNum, None)
        self.assertNotEqual(license.siteNum, None)
        self.assertNotEqual(license.warningPeriod, None)
        self.assertNotEqual(license.licenseFile, None)
        fileLen=len(license.licenseFile)
        fileSuffix=license.licenseFile[fileLen-4:fileLen]
        self.assertIn(fileSuffix, ['.sas', '.jwt', '.txt'])
        self.assertNotEqual(license.osName, None)
        self.assertNotEqual(license.productName, None)
        self.assertNotEqual(license.release, None)
        self.assertNotEqual(license.siteName, None)

    def test_setLicenseInfoWithoutCredentialForValidProductId(self):
        # This testcase executes a hidden action.
        # If CAS_ACTION_TEST_MODE was not enabled, skip the testcase
        if not tm.get_cas_action_test_mode():
            tm.TestCase.skipTest(self, 'CAS_ACTION_TEST_MODE not enabled.')

        # setLicenseInfo not in optimized builds
        if 'setlicenseinfo' not in self.s.builtins.actions:
            tm.TestCase.skipTest(self, 'setlicenseinfo action not supported in this build')
    
        # Set without argument 'extra' that provides a credential to the server
        r = self.s.builtins.setLicenseInfo(prodId=1000)
        self.assertNotEqual(r.severity, 0)
        
    def test_setLicenseInfoWithoutCredentialForUnknownProdId(self):
        # This testcase executes a hidden action.
        # If CAS_ACTION_TEST_MODE was not enabled, skip the testcase
        if not tm.get_cas_action_test_mode():
            tm.TestCase.skipTest(self, 'CAS_ACTION_TEST_MODE not enabled.')

        # setLicenseInfo not in optimized builds
        if 'setlicenseinfo' not in self.s.builtins.actions:
            tm.TestCase.skipTest(self, 'setlicenseinfo action not supported in this build')

        # Set without argument 'extra' that provides a credential to the server
        # Set with an invalid product id
        r = self.s.builtins.setLicenseInfo(prodId=4242.0)
        self.assertNotEqual(r.severity, 0)
        # python seems to always return the second message, but leaving the first message
        # here in case there is some scenario that returns it.
        self.assertIn(r.status, ['The product specified is not licensed.',
                                 'Invalid attempt to set client prodid value.'])

    def test_getCacheInfo(self):
        # getCacheInfo is not support on WX6
        if self.server_type == 'windows.smp':
            tm.TestCase.skipTest(self, 'getCacheInfo is not support on WX6')

        # This testcase requires running as a SuperUser.
        # If CAS_SUPERUSER_MODE was not enabled, skip the testcase
        if not tm.get_cas_superuser_mode():
            tm.TestCase.skipTest(self, 'Testcase assumes SuperUser role.')

        # Cannot run this command if the user has not assumed the SuperUser role yet
        r = self.s.getCacheInfo()
        self.assertEqual(r.severity, 2)
        self.assertContainsMessage(r, "ERROR: The action stopped due to errors.")

        # Assume SuperUser
        info = self.s.accesscontrol.assumerole(adminrole='SuperUser')
        if info.severity != 0:
            self.skipTest("Specified user does not have admin credentials")

        # Expect that we are a SuperUser
        info = self.s.accesscontrol.isInRole(adminRole="SuperUser")
        self.assertEqual(info['inRole'],"TRUE")

        r = self.s.getCacheInfo()
        self.assertEqual(r.severity, 0)
        self.assertNotEqual(r, None)
        self.assertEqual(r.diskCacheInfo.name,"diskCacheInfo")
        self.assertEqual(r.diskCacheInfo.label,"Result table containing CAS_DISK_CACHE information")

    def test_refreshTokenWithInvalidToken(self):
        # This testcase executes a hidden action.
        # If CAS_ACTION_TEST_MODE was not enabled, skip the testcase
        if not tm.get_cas_action_test_mode():
            tm.TestCase.skipTest(self, 'CAS_ACTION_TEST_MODE not enabled.')
        # refresh token not in optimized builds
        if 'refreshtoken' not in self.s.builtins.actions:
            tm.TestCase.skipTest(self, 'refreshtoken action not supported in this build')

        r = self.s.builtins.refreshToken(token='123456789')
        self.assertTrue(r.severity > 1)

if __name__ == '__main__':
    tm.runtests()
