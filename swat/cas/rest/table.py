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
Class for retrieving table values

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import decimal
import numpy as np
import pandas as pd
from ..utils.datetime import cas2python_date, cas2python_time, cas2python_datetime
from ...utils.compat import items_types, float64, int32, int64

COL_TYPE_MAP = {
    'string': 'varchar',
    'int': 'int64',
}


def _strip(value):
    ''' If `value` is a string, strip the whitespace '''
    if hasattr(value, 'strip'):
        return value.strip()
    return value


def _attr2python(attr):
    ''' Convert an attribute to a Python object '''
    atype = attr['type']
    value = attr['value']
    if atype in ['double', 'float']:
        if value is None:
            return np.nan
        return float64(value)
    elif atype in ['int32', 'int']:
        return int32(value)
    elif atype == 'int64':
        return int64(value)
    elif atype == 'date':
        return cas2python_date(value)
    elif atype == 'time':
        return cas2python_time(value)
    elif atype == 'datetime':
        return cas2python_datetime(value)
    return value


class REST_CASTable(object):
    '''
    Create a CASTable object

    Parameters
    ----------
    obj : dict
        The object returned by the CAS connection

    Returns
    -------
    REST_CASTable

    '''

    def __init__(self, obj):
        self._obj = obj

        # Map REST attributes to Python objects
        attrs = self._obj.get('attributes', {})
        for key, value in attrs.items():
            attrs[key] = _attr2python(value)
        for col in self._obj.get('schema', []):
            colattrs = col.get('attributes', {})
            for key, value in colattrs.items():
                colattrs[key] = _attr2python(value)

    def getAttributes(self):
        ''' Return full set of attributes '''
        return self._obj.get('attributes', {})

    def getColumnAttributes(self, i):
        ''' Return full set of column attributes '''
        return self._obj.get('schema')[i].get('attributes', {})

    def getTypeName(self):
        ''' Get the object type '''
        return 'table'

    def getSOptions(self):
        ''' Get the SOptions value '''
        return ''

    def isNULL(self):
        ''' Is this a NULL object? '''
        return False

    def getName(self):
        ''' Get the table name '''
        return self._obj.get('name')

    def getLabel(self):
        ''' Get the table label '''
        return self._obj.get('label') or None

    def getTitle(self):
        ''' Get the table title '''
        return self._obj.get('title') or None

    def getNColumns(self):
        ''' Get the number of columns '''
        return len(self._obj.get('schema'))

    def getNRows(self):
        ''' Get the number of rows '''
        return len(self._obj.get('rows'))

    def getColumnName(self, i):
        ''' Get the column name '''
        return self._obj.get('schema')[i].get('name')

    def getColumnLabel(self, i):
        ''' Get the column label '''
        return self._obj.get('schema')[i].get('label') or None

    def getColumnType(self, i):
        ''' Get the column type '''
        ctype = COL_TYPE_MAP.get(self._obj.get('schema')[i].get('type'),
                                 self._obj.get('schema')[i].get('type'))
        rows = self._obj.get('rows')
        if rows and rows[0] and isinstance(rows[0][i], (list, tuple)):
            return '%s-array' % ctype
        return ctype

    def getColumnWidth(self, i):
        ''' Get the column width '''
        return self._obj.get('schema')[i].get('width')

    def getColumnFormat(self, i):
        ''' Get the column format '''
        return self._obj.get('schema')[i].get('format') or None

    def getColumnArrayNItems(self, i):
        ''' Get the number of array items in a column '''
        ctype = self.getColumnType(i)
        if ctype.endswith('-array'):
            rows = self._obj.get('rows')
            return len(rows[0][i])
        return 1

    def getLastErrorMessage(self):
        ''' Get the last generated error message '''
        return ''

    def getNextAttributeKey(self):
        ''' Get the next attribute key '''
        return

    def getAttributeType(self, key):
        ''' Get the next attribute type '''
        return

    def getAttributeNItems(self, key):
        ''' Get the number of attributes '''
        return

    def getInt32Attribute(self, key):
        ''' Get an attribute as an int32 '''
        return
#       return int32(...)

    def getInt32ArrayAttributeItem(self, key, index):
        ''' Get an attribute as an int32 array '''
        return
#       return int32(...)

    def getInt64Attribute(self, key):
        ''' Get an attribute as an int64 '''
        return
#       return int64(...)

    def getInt64ArrayAttributeItem(self, key, index):
        ''' Get an attribute as an int64 array '''
        return
#       return int64(...)

    def getStringAttribute(self, key):
        ''' Get an attribute as a string '''
        return
#       return a2u(...)

    def getDoubleAttribute(self, key):
        ''' Get an attribute as a double '''
        return
#       return float64(...)

    def getDoubleArrayAttributeItem(self, key, index):
        ''' Get an attribute as a double array '''
        return
#       retun float64(...)

    def getNextColumnAttributeKey(self, col):
        ''' Get the next column attribute key '''
        return

    def getColumnAttributeType(self, col, key):
        ''' Get the column attribute type '''
        return

    def getColumnAttributeNItems(self, col, key):
        ''' Get the number of items in a column attribute '''
        return

    def getColumnInt32Attribute(self, col, key):
        ''' Get a column attribute as an int32 '''
        return
#       return int32(...)

    def getColumnInt64Attribute(self, col, key):
        ''' Get a column attribute as an int64 '''
        return
#       return int64(...)

    def getColumnInt32ArrayAttribute(self, col, key, index):
        ''' Get a column attribute as an int32 array '''
        return
#       return int32(...)

    def getColumnInt64ArrayAttribute(self, col, key, index):
        ''' Get a column attribute as an int64 array '''
        return
#       return int64(...)

    def getColumnStringAttribute(self, col, key):
        ''' Get a column attribute as a string '''
        return
#       return a2u(...)

    def getColumnDoubleAttribute(self, col, key):
        ''' Get a column attribute as a double '''
        return
#       return float64(...)

    def getColumnDoubleArrayAttribute(self, col, key, index):
        ''' Get a column attribute as a double array '''
        return
#       return float64(...)

    def toTuples(self, errors, cas2python_datetime, cas2python_date,
                 cas2python_time):
        ''' Get the table data as a list of tuples '''
        out = []
        dtypes = []
        for i in range(self.getNColumns()):
            dtypes.append(self.getColumnType(i))
        for row in self._obj.get('rows', []):
            outrow = []
            for dtype, item in zip(dtypes, row):
                # Check for arrays
                if isinstance(item, items_types):
                    for elem in item:
                        outrow.append(elem)
                # Check for binary
                elif isinstance(item, dict):
                    try:
                        outrow.append(base64.b64decode(item['data']))
                    except Exception:
                        try:
                            outrow.append(base64.b64decode(item['data'] + '='))
                        except Exception:
                            outrow.append(base64.b64decode(item['data'] + '=='))
                # Check for datetime, date, time
                elif dtype == 'datetime':
                    if item < decimal.Decimal('-9223372036854775807.5'):
                        outrow.append(pd.NaT)
                    else:
                        outrow.append(cas2python_datetime(item))
                elif dtype == 'date':
                    if item < decimal.Decimal('-2147483647.5'):
                        outrow.append(pd.NaT)
                    else:
                        outrow.append(cas2python_date(item))
                elif dtype == 'time':
                    if item < decimal.Decimal('-9223372036854775807.5'):
                        outrow.append(pd.NaT)
                    else:
                        outrow.append(cas2python_time(item))
                # Character
                elif dtype in ['char', 'varchar']:
                    outrow.append(item.rstrip())
                # Everything else
                else:
                    outrow.append(_strip(item))
            out.append(tuple(outrow))
        return out
