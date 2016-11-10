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

import swat
import swat.utils.testing as tm
import unittest


class TestTypes(tm.TestCase):

    def test_str(self):
        nil = swat.nil

        self.assertTrue(isinstance(str(nil), str))
        self.assertEqual(str(nil), 'nil')

        self.assertTrue(isinstance(repr(nil), str))
        self.assertEqual(repr(nil), 'nil')


if __name__ == '__main__':
    tm.runtests()
