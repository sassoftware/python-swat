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

import copy
import swat.utils.testing as tm
import unittest
from swat.utils.compat import text_types
from swat.config import (get_option, set_option, reset_option, describe_option, options,
                         get_suboptions, SWATOptionError, get_default,
                         check_int, check_float, check_string, check_url, check_boolean)
from swat.utils.config import subscribe, _subscribers, unsubscribe


class TestConfig(tm.TestCase):

    def setUp(self):
        reset_option()

    def tearDown(self):
        reset_option()

    def test_basic(self):
        self.assertEqual(get_option('cas.print_messages'), True)
#       self.assertEqual(get_option('display.notebook.repr_html'), True)
#       self.assertEqual(get_option('display.notebook.repr_javascript'), False)

        set_option('cas.print_messages', False)

        self.assertEqual(get_option('cas.print_messages'), False)

        with self.assertRaises(SWATOptionError):
            options.cas.print_messages = 'foo'

        options.cas.print_messages = True
        self.assertEqual(options.cas.print_messages, True)

        with self.assertRaises(SWATOptionError):
            options.cas.print_messages = 10

        self.assertEqual(options.cas.print_messages, True)

        self.assertEqual(type(options.cas), type(options))

        # This key exists, but it's a level in the hierarchy, not an option
#       with self.assertRaises(SWATOptionError):
#           get_option('display.notebook.css')

        options.cas.print_messages = False

        reset_option('cas.print_messages')

        self.assertEqual(options.cas.print_messages, True)

        with self.assertRaises(SWATOptionError):
            reset_option('cas.foo')

        with self.assertRaises(SWATOptionError):
            reset_option('cas')

    def test_shortcut_options(self):
        trace_actions = get_option('cas.trace_actions')
        index_name = get_option('cas.dataset.index_name')

        self.assertEqual(get_option('trace_actions'), trace_actions)
        self.assertEqual(options.trace_actions, trace_actions)

        options.trace_actions = True

        self.assertEqual(get_option('cas.trace_actions'), True)
        self.assertEqual(options.cas.trace_actions, True)
        self.assertEqual(options.trace_actions, True)

        self.assertEqual(get_option('index_name'), index_name)
        self.assertEqual(get_option('dataset.index_name'), index_name)
        self.assertEqual(options.index_name, index_name)

        options.index_name = 'Foo'

        self.assertEqual(get_option('index_name'), 'Foo')
        self.assertEqual(get_option('dataset.index_name'), 'Foo')
        self.assertEqual(options.index_name, 'Foo')

        reset_option('index_name')

        self.assertEqual(get_option('index_name'), '_Index_')
        self.assertEqual(get_option('dataset.index_name'), '_Index_')
        self.assertEqual(options.index_name, '_Index_')

    def test_missing_options(self):
        with self.assertRaises(SWATOptionError):
            set_option('cas.foo', 10)

        with self.assertRaises(SWATOptionError):
            options.cas.foo = 10

        with self.assertRaises(SWATOptionError):
            get_option('cas.foo')

        with self.assertRaises(SWATOptionError):
            print(options.cas.foo)

        # You can not access a midpoint in the hierarchy with (s|g)et_option
        with self.assertRaises(SWATOptionError):
            set_option('cas', 10)

        with self.assertRaises(SWATOptionError):
            get_option('cas')

    def test_function_subscribers(self):
        opts = {}

        def options_subscriber(key, value, opts=opts):
            opts[key] = value

        num_subscribers = len(_subscribers)

        subscribe(options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1)

        options.cas.print_messages = True
        self.assertEqual(opts, {'cas.print_messages': True})

        options.cas.print_messages = False
        self.assertEqual(opts, {'cas.print_messages': False})

        options.cas.dataset.index_name = 'foo'
        self.assertEqual(opts, {'cas.print_messages': False,
                                'cas.dataset.index_name': 'foo'})

        options.cas.dataset.index_name = 'bar'
        self.assertEqual(opts, {'cas.print_messages': False,
                                'cas.dataset.index_name': 'bar'})

        options.cas.print_messages = True
        self.assertEqual(opts, {'cas.print_messages': True,
                                'cas.dataset.index_name': 'bar'})

        options.cas.print_messages = False
        reset_option('cas.print_messages')
        self.assertEqual(opts, {'cas.print_messages': True,
                                'cas.dataset.index_name': 'bar'})

        unsubscribe(options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers)

        subscribe(options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1)

        del options_subscriber

        self.assertEqual(len(_subscribers), num_subscribers)

        options.cas.print_messages = False

        self.assertEqual(opts, {'cas.print_messages': True,
                                'cas.dataset.index_name': 'bar'})

    def _test_method_subscribers(self):
        opts = {}

        class OptionsSubscriber(object):
            def options_subscriber(self, key, value, opts=opts):
                opts[key] = value
        os = OptionsSubscriber()

        num_subscribers = len(_subscribers)

        subscribe(os.options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1)

        options.cas.print_messages = False
        self.assertEqual(opts, {'cas.print_messages': False})

        options.cas.print_messages = True
        self.assertEqual(opts, {'cas.print_messages': True})

        options.cas.dataset.auto_index_style = 'sas'
        self.assertEqual(opts, {'cas.print_messages': True,
                                'cas.dataset.auto_index_style': 'sas'})

        options.cas.dataset.auto_index_style = 'pandas'
        self.assertEqual(opts, {'cas.print_messages': True,
                                'cas.dataset.auto_index_style': 'pandas'})

        options.cas.print_messages = False
        self.assertEqual(opts, {'cas.print_messages': False,
                                'cas.dataset.auto_index_style': 'pandas'})

        reset_option('cas.print_messages')
        self.assertEqual(opts, {'cas.print_messages': True,
                                'cas.dataset.auto_index_style': 'pandas'})

        unsubscribe(os.options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers)

        subscribe(os.options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1)

        del os.options_subscriber

        self.assertEqual(len(_subscribers), num_subscribers)

        options.cas.print_messages = False

        self.assertEqual(opts, {'cas.print_messages': True,
                                'cas.dataset.auto_index_style': 'pandas'})

    def test_errors(self):
        with self.assertRaises(SWATOptionError):
            set_option('cas.print_messages', 'hi')

    def test_doc(self):
        out = describe_option('cas.print_messages', 'encoding_errors', _print_desc=False)
        for line in out.split('\n'):
            if not line or line.startswith(' '):
                continue
            self.assertRegex(line, r'^(cas\.print_messages|encoding_errors)')

        # Displays entire option hierarchy
        out = describe_option('cas', _print_desc=False)
        for line in out.split('\n'):
            if not line or line.startswith(' '):
                continue
            self.assertRegex(line, r'^cas\.')

        with self.assertRaises(SWATOptionError):
            describe_option('cas.foo')

        out = describe_option(_print_desc=False)
        self.assertRegex(out, r'\bcas\.dataset\.format :')
        self.assertRegex(out, r'\bcas\.print_messages :')
        self.assertRegex(out, r'\btkpath :')
        self.assertRegex(out, r'\bencoding_errors :')

    def test_suboptions(self):
        self.assertEqual(list(sorted(get_suboptions('cas').keys())),
                         ['connection_retries', 'connection_retry_interval',
                          'dataset', 'debug', 'exception_on_severity',
                          'hostname', 'missing',
                          'port', 'print_messages', 'protocol',
                          'reflection_levels', 'ssl_ca_list', 'token',
                          'trace_actions', 'trace_ui_actions', 'username'])

        with self.assertRaises(SWATOptionError):
            get_suboptions('cas.foo')

        # This is an option, not a level in the hierarchy
        with self.assertRaises(SWATOptionError):
            get_suboptions('cas.print_messages')

    def test_get_default(self):
        self.assertEqual(get_default('cas.print_messages'), True)

        with self.assertRaises(SWATOptionError):
            get_default('cas.foo')

        # This is a level in the hierarchy, not an option
        with self.assertRaises(SWATOptionError):
            get_default('cas')

    def test_check_int(self):
        self.assertEqual(check_int(10), 10)
        self.assertEqual(check_int(999999999999), 999999999999)
        self.assertEqual(check_int('10'), 10)

        with self.assertRaises(SWATOptionError):
            check_int('foo')

        self.assertEqual(check_int(10, minimum=9), 10)
        self.assertEqual(check_int(10, minimum=10), 10)
        with self.assertRaises(SWATOptionError):
            check_int(10, minimum=11)

        self.assertEqual(check_int(10, minimum=9, exclusive_minimum=True), 10)
        with self.assertRaises(SWATOptionError):
            check_int(10, minimum=10, exclusive_minimum=True)
        with self.assertRaises(SWATOptionError):
            check_int(10, minimum=11, exclusive_minimum=True)

        self.assertEqual(check_int(10, maximum=11), 10)
        self.assertEqual(check_int(10, maximum=10), 10)
        with self.assertRaises(SWATOptionError):
            check_int(10, maximum=9)

        self.assertEqual(check_int(10, maximum=11, exclusive_minimum=True), 10)
        with self.assertRaises(SWATOptionError):
            check_int(10, maximum=10, exclusive_maximum=True)
        with self.assertRaises(SWATOptionError):
            check_int(10, maximum=9, exclusive_maximum=True)

        self.assertEqual(check_int(10, multiple_of=5), 10)
        with self.assertRaises(SWATOptionError):
            check_int(10, multiple_of=3)

    def test_check_float(self):
        self.assertEqual(check_float(123.567), 123.567)
        self.assertEqual(check_float(999999999999.999), 999999999999.999)
        self.assertEqual(check_float('123.567'), 123.567)

        with self.assertRaises(SWATOptionError):
            check_float('foo')

        self.assertEqual(check_float(123.567, minimum=123.566), 123.567)
        self.assertEqual(check_float(123.567, minimum=123.567), 123.567)
        with self.assertRaises(SWATOptionError):
            check_float(123.567, minimum=123.577)

        self.assertEqual(check_float(123.567, minimum=123.566,
                                     exclusive_minimum=True), 123.567)
        with self.assertRaises(SWATOptionError):
            check_float(123.567, minimum=123.567, exclusive_minimum=True)
        with self.assertRaises(SWATOptionError):
            check_float(123.567, minimum=123.568, exclusive_minimum=True)

        self.assertEqual(check_float(123.567, maximum=123.568), 123.567)
        self.assertEqual(check_float(123.567, maximum=123.567), 123.567)
        with self.assertRaises(SWATOptionError):
            check_float(123.567, maximum=123.566)

        self.assertEqual(check_float(123.567, maximum=123.567,
                                     exclusive_minimum=True), 123.567)
        with self.assertRaises(SWATOptionError):
            check_float(123.567, maximum=123.567, exclusive_maximum=True)
        with self.assertRaises(SWATOptionError):
            check_float(123.567, maximum=123.566, exclusive_maximum=True)

        with self.assertRaises(SWATOptionError):
            check_float(123.567, multiple_of=3)

    def test_check_string(self):
        self.assertEqual(check_string('hi there'), 'hi there')
        self.assertTrue(isinstance(check_string('hi there'), text_types))

        self.assertEqual(check_string('hi there', pattern=r' th'), 'hi there')
        with self.assertRaises(SWATOptionError):
            check_string('hi there', pattern=r' th$')

        self.assertEqual(check_string('hi there', max_length=20), 'hi there')
        self.assertEqual(check_string('hi there', max_length=8), 'hi there')
        with self.assertRaises(SWATOptionError):
            check_string('hi there', max_length=7)

        self.assertEqual(check_string('hi there', min_length=3), 'hi there')
        self.assertEqual(check_string('hi there', min_length=8), 'hi there')
        with self.assertRaises(SWATOptionError):
            check_string('hi there', min_length=9)

        self.assertEqual(check_string('hi there', valid_values=['hi there', 'bye now']),
                         'hi there')
        with self.assertRaises(SWATOptionError):
            check_string('foo', valid_values=['hi there', 'bye now'])

        # Invalid utf8 data
        with self.assertRaises(SWATOptionError):
            check_string(b'\xff\xfeW[')

    def test_check_url(self):
        self.assertEqual(check_url('hi there'), 'hi there')
        self.assertTrue(isinstance(check_url('hi there'), text_types))

        # Invalid utf8 data
        with self.assertRaises(SWATOptionError):
            check_url(b'\xff\xfeW[')

    def test_check_boolean(self):
        self.assertEqual(check_boolean(True), True)
        self.assertEqual(check_boolean(False), False)
        self.assertEqual(check_boolean(1), True)
        self.assertEqual(check_boolean(0), False)
        self.assertEqual(check_boolean('yes'), True)
        self.assertEqual(check_boolean('no'), False)
        self.assertEqual(check_boolean('T'), True)
        self.assertEqual(check_boolean('F'), False)
        self.assertEqual(check_boolean('true'), True)
        self.assertEqual(check_boolean('false'), False)
        self.assertEqual(check_boolean('on'), True)
        self.assertEqual(check_boolean('off'), False)
        self.assertEqual(check_boolean('enabled'), True)
        self.assertEqual(check_boolean('disabled'), False)

        with self.assertRaises(SWATOptionError):
            check_boolean(2)
        with self.assertRaises(SWATOptionError):
            check_boolean('foo')
        with self.assertRaises(SWATOptionError):
            check_boolean(1.1)


if __name__ == '__main__':
    tm.runtests()
