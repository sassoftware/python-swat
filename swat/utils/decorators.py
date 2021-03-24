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

'''
General utility decorators

'''

from __future__ import print_function, division, absolute_import, unicode_literals


class cachedproperty(object):
    ''' Property whose value is only calculated once and cached '''

    def __init__(self, func):
        self._func = func
        self.__doc__ = func.__doc__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return getattr(obj, '@%s' % self._func.__name__)
        except AttributeError:
            result = self._func(obj)
            setattr(obj, '@%s' % self._func.__name__, result)
            return result


class getattr_safe_property(object):
    ''' Property that safely coexists with __getattr__ '''

    def __init__(self, func):
        self._func = func
        self.__doc__ = func.__doc__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return self._func(obj)
        except AttributeError as exc:
            raise RuntimeError(str(exc))

    def __set__(self, obj, value):
        raise RuntimeError("Setting the '%s' attribute is not allowed" %
                           self._func.__name__)
