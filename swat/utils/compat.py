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
Smooth out some differences between Python 2 and Python 3

'''

from __future__ import print_function, division, absolute_import, unicode_literals

try:
    from collections.abc import MutableMapping
except (ImportError, AttributeError):
    from collections import MutableMapping
import sys
import numpy as np

ENCODING = sys.getdefaultencoding()
MAX_INT32 = 2**32 / 2 - 1
MIN_INT32 = -MAX_INT32
WIDE_CHARS = (sys.maxunicode > 65535)

if sys.version_info >= (3, 0):
    PY3 = True
    PY2 = False
    items_types = (list, tuple, set)
    dict_types = (dict, MutableMapping)
    int32_types = (np.int32,)
    int64_types = (np.int64, int)
    int_types = int32_types + int64_types
    float64_types = (np.float64, float)
    num_types = int_types + float64_types
    binary_types = (bytes,)
    bool_types = (bool, np.bool_)
    text_types = (str,)
    char_types = binary_types + text_types
    int32 = int
    int64 = int
    float64 = float

else:
    PY3 = False
    PY2 = True
    items_types = (list, tuple, set)
    dict_types = (dict, MutableMapping)
    int32_types = (np.int32, int)
    int64_types = (np.int64, long)    # noqa: F821
    int_types = int32_types + int64_types
    float64_types = (np.float64, float)
    num_types = int_types + float64_types
    binary_types = (str, bytes)
    bool_types = (bool, np.bool_)
    text_types = (unicode,)    # noqa: F821
    char_types = binary_types + text_types
    int32 = int
    int64 = long    # noqa: F821
    float64 = float


def patch_pandas_sort():
    ''' Add sort_values to older versions of Pandas DataFrames '''
    import pandas as pd

    if not hasattr(pd.DataFrame, 'sort_values'):
        def sort_values(self, by, axis=0, ascending=True, inplace=False,
                        kind='quicksort', na_position='last'):
            ''' `sort` wrapper for new-style sorting API '''
            return self.sort(columns=by, axis=axis, ascending=ascending, inplace=inplace,
                             kind=kind, na_position=na_position)

        pd.DataFrame.sort_values = sort_values

        def sort_values(self, axis=0, ascending=True, inplace=False,
                        kind='quicksort', na_position='last'):
            ''' `sort` wrapper for new-style sorting API '''
            return self.sort(axis=axis, ascending=ascending, inplace=inplace,
                             kind=kind, na_position=na_position)

        pd.Series.sort_values = sort_values


def a2u(arg, encoding=ENCODING):
    '''
    Convert any string type to unicode

    Parameters
    ----------
    arg : str, unicode, or bytes
       The string to convert
    encoding : string
       The encoding to use for encoding if needed

    Returns
    -------
    Unicode object

    '''
    if arg is None:
        return arg
    if isinstance(arg, text_types):
        return arg
    return arg.decode(encoding)


def a2b(arg, encoding=ENCODING):
    '''
    Convert any string type to bytes

    Parameters
    ----------
    arg : str, unicode, or bytes
       The string to convert
    encoding : string
       The encoding to use to for decoding if needed

    Returns
    -------
    Bytes object

    '''
    if arg is None:
        return arg
    if isinstance(arg, binary_types):
        if encoding.lower().replace('-', '').replace('_', '') == 'utf8':
            return arg
        arg = arg.decode(ENCODING)
    return arg.encode('utf-8')


# Use OrderedDict if possible
try:
    from collections import OrderedDict
except ImportError:
    OrderedDict = dict

# Convert any string to native string type
if PY3:
    a2n = a2u
else:
    a2n = a2b
