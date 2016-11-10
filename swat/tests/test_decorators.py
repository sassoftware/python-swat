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

import swat.utils.testing as tm
import unittest
from swat.utils.decorators import cachedproperty


class TestKeyword(tm.TestCase):

    def test_cachedproperty(self):

        class MyClass(object):

            @cachedproperty
            def cprop(self):
                return {'key': 'value'}

            @property
            def prop(self):
                return {'key': 'value'}

        mycls = MyClass()

        self.assertTrue(mycls.prop is not mycls.prop)
        self.assertEqual(mycls.prop, {'key': 'value'})

        self.assertTrue(mycls.cprop is mycls.cprop)
        self.assertEqual(mycls.prop, {'key': 'value'})


if __name__ == '__main__':
    tm.runtests()
