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
Class for receiving values from a CAS response

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
from .table import REST_CASTable
from ..types import blob
from ...utils.compat import (a2u, int32, int64, float64, text_types,
                             binary_types, int32_types, int64_types,
                             float64_types, items_types)


def _value2python(_value, soptions, errors, connection,
                  ctb2tabular, b64decode, cas2python_datetime,
                  cas2python_date, cas2python_time):
    ''' Convert JSON generated values to Python objects '''
    if isinstance(_value, dict):
        if _value.get('_ctb'):
            return ctb2tabular(REST_CASTable(_value), soptions, connection)
        elif sorted(_value.keys()) == ['data', 'length']:
            return blob(base64.b64decode(_value['data']))

        # Short circuit reflection data
        if 'actions' in _value and _value.get('actions', [{}])[0].get('params', False):
            return _value

        out = {}
        for key, value in _value.items():
            out[key] = _value2python(value, soptions, errors, connection,
                                     ctb2tabular, b64decode, cas2python_datetime,
                                     cas2python_date, cas2python_time)
        return out

    if isinstance(_value, items_types):
        out = []
        for i, value in enumerate(_value):
            out.append(_value2python(value, soptions, errors, connection,
                                     ctb2tabular, b64decode, cas2python_datetime,
                                     cas2python_date, cas2python_time))
        return out

    if isinstance(_value, binary_types):
        return a2u(_value, 'utf-8')

    return _value

#   elif vtype == 'date':
#       return cas2python_date(_value)

#   elif vtype == 'time':
#       return cas2python_time(_value)

#   elif vtype == 'datetime':
#       return cas2python_datetime(_value)


class REST_CASValue(object):
    ''' CASValue wrapper '''

    def __init__(self, key, value):
        '''
        Create a CASValue-like object

        Parameters
        ----------
        key : string or int or None
           The key for the value
        value : any
           The value itself

        Returns
        -------
        REST_CASValue object

        '''
        self._key = key
        self._value = value

    def toPython(self, _sw_value, soptions, errors, connection, ctb2tabular,
                 b64decode, cas2python_datetime, cas2python_date, cas2python_time):
        ''' Convert a CAS value to Python '''
        return _value2python(self._value, soptions, errors, connection, ctb2tabular,
                             b64decode, cas2python_datetime, cas2python_date,
                             cas2python_time)

    def getTypeName(self):
        ''' Get the object type '''
        return 'value'

    def getSOptions(self):
        ''' Get the SOptions value '''
        return ''

    def isNULL(self):
        ''' Is this a NULL object? '''
        return False

    def hasKeys(self):
        ''' Does the value have keys? '''
        return isinstance(self._value, dict) and self._value

    def getType(self):
        ''' Get the value type '''
        _value = self._value
        if isinstance(_value, float64_types):
            return 'double'
        if isinstance(_value, text_types):
            return 'string'
        if isinstance(_value, int32_types):
            return 'int32'
        if isinstance(_value, int64_types):
            return 'int64'
        if isinstance(_value, binary_types):
            return 'string'
        if isinstance(_value, items_types):
            return 'list'
        if isinstance(_value, dict):
            if _value.get('_ctb'):
                return 'table'
            return 'list'
        if _value is None:
            return 'nil'
        raise TypeError('%s: %s' % (self._key, _value))

    def getKey(self):
        ''' Get the value's key '''
        return self._key

    def getInt32(self):
        ''' Get the value as an int32 '''
        return int32(self._value)

    def getInt64(self):
        ''' Get the value as an int64 '''
        return int64(self._value)

    def getDouble(self):
        ''' Get the value as a double '''
        return float64(self._value)

    def getString(self):
        ''' Get the value as a string '''
        return a2u(self._value, 'utf-8')

    def getBoolean(self):
        ''' Get the value as a boolean '''
        return self._value and True or False

    def getList(self):
        ''' Get the value as a list '''
        return self._value

    def getListNItems(self):
        ''' Get the number of items in the value's list '''
        return len(self._value)

    def getListItem(self, i):
        ''' Get a specific list item '''
        if isinstance(self._value, dict):
            if not hasattr(self, '_items'):
                self._items = list(sorted(self._value.items()))
            return REST_CASValue(*self._items[i])
        return REST_CASValue(None, self._value[i])

    def getTable(self):
        ''' Get the value as a table '''
        return REST_CASTable(self._value)

    def getLastErrorMessage(self):
        ''' Retrieve any queued error messages '''
        return ''
