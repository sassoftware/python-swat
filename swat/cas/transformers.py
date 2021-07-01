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
Utilities for converting to and from Python and CAS arguments

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import datetime
import warnings
import numpy as np
import pandas as pd
import re
import six
from .utils import datetime as casdt
from .. import clib
from ..utils.compat import (a2u, a2n, int32, int64, float64, text_types,
                            binary_types, int32_types, int64_types,
                            float64_types, items_types, dict_types,
                            MAX_INT32, MIN_INT32)
from ..utils.keyword import keywordify
from ..config import get_option
from ..clib import errorcheck
from ..formatter import SASFormatter
from ..dataframe import SASDataFrame, SASColumnSpec
from .table import CASTable
from .types import nil, blob
from .utils.params import ParamManager

# pylint: disable=C0330


def casvaluelist2py(_sw_values, soptions, length=None):
    '''
    Convert a SWIG CASValueList to a Python dictionary

    Parameters
    ----------
    _sw_values : SWIG pointer
       The SWIG CASValueList object
    soptions : string
       soptions of connection object
    length : int or long, optional
       Number of items in CASValueList

    Returns
    -------
    dict
       Dictionary equivalent of CASValueList

    '''
    if length is None:
        length = errorcheck(_sw_values.getNItems(), _sw_values)
    num = 0
    output = {}
    for i in range(length):
        item = errorcheck(_sw_values.getItem(i), _sw_values)
        key = item.getKey()
        if not key:
            output[num] = cas2py(item, soptions)
            num += 1
        else:
            output[a2u(key, 'utf-8')] = cas2py(item, soptions)
    return output


def _caslist2py(_sw_value, soptions, echk, connection=None):
    '''
    Convert CAS list to Python dict/list

    Parameters
    ----------
    _sw_value : SWIG CASValue object
       CASValue object to convert to Python
    soptions : string
       String containing CAS connection soptions
    echk : function
       errorcheck function
    connection : CAS object
       The connection to associate generated CASTables with

    Returns
    -------
    list or dict
       List/dict representation of the CASValue

    '''
    length = echk(_sw_value.getListNItems(), _sw_value)
    haskeys = echk(_sw_value.hasKeys(), _sw_value)
    if haskeys:
        output = {}
        caslib = None
        tablename = None
        castable = None
        for i in range(length):
            item = echk(_sw_value.getListItem(i), _sw_value)
            key = a2u(item.getKey(), 'utf-8')
            if key.lower() == 'caslib':
                caslib = key
            elif key.lower() == 'tablename':
                tablename = key
            elif key.lower() == 'castable':
                castable = True
            output[key] = cas2py(item, soptions, connection=connection)
        if caslib and tablename and not castable:
            if connection is not None:
                output['casTable'] = connection.CASTable(output[tablename],
                                                         caslib=output[caslib])
            else:
                output['casTable'] = CASTable(output[tablename], caslib=output[caslib])
        return output
    else:
        output = []
        for i in range(length):
            item = echk(_sw_value.getListItem(i), _sw_value)
            output.append(cas2py(item, soptions, connection=connection))
        return output


def ctb2tabular(_sw_table, soptions='', connection=None):
    '''
    Convert SWIG table to a tabular structure based on cas.dataset.format option

    Parameters
    ----------
    _sw_table : SWIG table object
       The SWIG CASTable object
    soptions : string, optional
       soptions of connection object
    connection : CAS object
       The connection to associate generated CASTable objects with

    Returns
    -------
    One of the following depending on the cas.dataset.format option:

    SASDataFrame object
       SASDataFrame representation of SWIG CASTable
    DataFrame object
       Pandas DataFrame representation of SWIG CASTable
    dict or list
       Any variant of the Pandas DataFrame.to_dict() results
    tuple
       A tuple of tuples of the data values only

    '''
    tformat = get_option('cas.dataset.format')
    needattrs = (tformat == 'dataframe:sas')

    # We can short circuit right away if they just want tuples
    if tformat.startswith('tuple'):
        return _sw_table.toTuples(a2n(get_option('encoding_errors'), 'utf-8'),
                                  casdt.cas2python_datetime,
                                  casdt.cas2python_date,
                                  casdt.cas2python_time)

    kwargs = {}

    check = errorcheck
    if connection is not None:
        kwargs['formatter'] = connection.SASFormatter()
    else:
        kwargs['formatter'] = SASFormatter(soptions=soptions)
    kwargs['name'] = check(a2u(_sw_table.getName(), 'utf-8'), _sw_table)
    kwargs['label'] = check(a2u(_sw_table.getLabel(), 'utf-8'), _sw_table)
    kwargs['title'] = check(a2u(_sw_table.getTitle(), 'utf-8'), _sw_table)

    # get table attributes
    attrs = {}
    if hasattr(_sw_table, 'getAttributes'):
        attrs = _sw_table.getAttributes()
    else:
        while needattrs:
            key = check(a2n(_sw_table.getNextAttributeKey(), 'utf-8'), _sw_table)
            if key is None:
                break
            typ = check(_sw_table.getAttributeType(key), _sw_table)
            ukey = a2u(key, 'utf-8')
            if typ == 'int32':
                attrs[ukey] = check(_sw_table.getInt32Attribute(key), _sw_table)
            elif typ == 'int64':
                attrs[ukey] = check(_sw_table.getInt64Attribute(key), _sw_table)
            elif typ == 'double':
                attrs[ukey] = check(_sw_table.getDoubleAttribute(key), _sw_table)
            elif typ == 'string':
                attrs[ukey] = check(a2u(_sw_table.getStringAttribute(key), 'utf-8'),
                                    _sw_table)
            elif typ == 'date':
                attrs[ukey] = check(_sw_table.getInt32Attribute(key), _sw_table)
            elif typ == 'time':
                attrs[ukey] = check(_sw_table.getInt64Attribute(key), _sw_table)
            elif typ == 'datetime':
                attrs[ukey] = check(_sw_table.getInt64Attribute(key), _sw_table)
            elif typ == 'int32-array':
                nitems = check(_sw_table.getAttributeNItems(), _sw_table)
                attrs[ukey] = []
                for i in range(nitems):
                    attrs[key].append(check(_sw_table.getInt32ArrayAttributeItem(key, i),
                                            _sw_table))
            elif typ == 'int64-array':
                nitems = check(_sw_table.getAttributeNItems(), _sw_table)
                attrs[ukey] = []
                for i in range(nitems):
                    attrs[ukey].append(check(_sw_table.getInt64ArrayAttributeItem(key, i),
                                             _sw_table))
            elif typ == 'double-array':
                nitems = check(_sw_table.getAttributeNItems(), _sw_table)
                attrs[ukey] = []
                for i in range(nitems):
                    attrs[ukey].append(check(
                        _sw_table.getIntDoubleArrayAttributeItem(key, i),
                        _sw_table))
    kwargs['attrs'] = attrs

    # Setup date / datetime regexes

    dt_formats = get_option('cas.dataset.datetime_formats')
    if isinstance(dt_formats, six.string_types):
        dt_formats = [dt_formats]
    datetime_regex = re.compile(r'^(%s)(\d*\.\d*)?$' % '|'.join(dt_formats), flags=re.I)

    d_formats = get_option('cas.dataset.date_formats')
    if isinstance(d_formats, six.string_types):
        d_formats = [d_formats]
    date_regex = re.compile(r'^(%s)(\d*\.\d*)?$' % '|'.join(d_formats), flags=re.I)

    t_formats = get_option('cas.dataset.time_formats')
    if isinstance(t_formats, six.string_types):
        t_formats = [t_formats]
    time_regex = re.compile(r'^(%s)(\d*\.\d*)?$' % '|'.join(t_formats), flags=re.I)

    # Construct columns
    ncolumns = check(_sw_table.getNColumns(), _sw_table)
    caslib = None
    tablename = None
    castable = None
    rowscol = None
    columnscol = None
    unknownname = None
    dtypes = []
    colinfo = {}
    mimetypes = {}
    dates = []
    datetimes = []
    times = []
    intmiss = {}
    for i in range(ncolumns):
        col = SASColumnSpec.fromtable(_sw_table, i)
        if col.attrs.get('MIMEType'):
            mimetypes[col.name] = col.attrs.get('MIMEType')
        lowercolname = col.name.lower()
        if lowercolname == 'caslib':
            caslib = col.name
        elif lowercolname == 'tablename':
            tablename = col.name
        elif lowercolname == 'castable':
            castable = col.name
        elif lowercolname == 'name':
            unknownname = col.name
        elif lowercolname == 'rows':
            rowscol = col.name
        elif lowercolname == 'columns':
            columnscol = col.name
        dtype = col.dtype
        if dtype == 'double':
            dtypes.append((col.name, 'f8'))
            colinfo[col.name] = col
            if col.format:
                if datetime_regex.match(col.format):
                    datetimes.append(col.name)
                elif date_regex.match(col.format):
                    dates.append(col.name)
                elif time_regex.match(col.format):
                    times.append(col.name)
        elif dtype in set(['char', 'varchar']):
            dtypes.append((col.name, '|U%d' % (col.width or 1)))
            colinfo[col.name] = col
        elif dtype == 'int32':
            dtypes.append((col.name, 'i4'))
            colinfo[col.name] = col
            intmiss[col.name] = {-2147483648: np.nan}
        elif dtype == 'int64':
            dtypes.append((col.name, 'i8'))
            colinfo[col.name] = col
            intmiss[col.name] = {-9223372036854775808: np.nan}
        elif dtype in 'datetime':
            dtypes.append((col.name, 'O'))
            colinfo[col.name] = col
        elif dtype == 'date':
            dtypes.append((col.name, 'O'))
            colinfo[col.name] = col
        elif dtype == 'time':
            dtypes.append((col.name, 'O'))
            colinfo[col.name] = col
        elif dtype in set(['binary', 'varbinary']):
            dtypes.append((col.name, 'O'))
            colinfo[col.name] = col
        elif dtype == 'int32-array':
            for elem in range(col.size[1]):
                col = SASColumnSpec.fromtable(_sw_table, i, elem=elem)
                dtypes.append((col.name, 'i4'))
                colinfo[col.name] = col
                intmiss[col.name] = {-2147483648: np.nan}
        elif dtype == 'int64-array':
            for elem in range(col.size[1]):
                col = SASColumnSpec.fromtable(_sw_table, i, elem=elem)
                dtypes.append((col.name, 'i8'))
                colinfo[col.name] = col
                intmiss[col.name] = {-9223372036854775808: np.nan}
        elif dtype == 'double-array':
            for elem in range(col.size[1]):
                col = SASColumnSpec.fromtable(_sw_table, i, elem=elem)
                dtypes.append((col.name, 'f8'))
                colinfo[col.name] = col
    kwargs['colinfo'] = colinfo

    # Numpy doesn't like unicode column names in Python 2, so map them to utf-8
    dtypes = [(a2n(x[0], 'utf-8'), x[1]) for x in dtypes]

    # Create a np.array and fill it
    kwargs['data'] = np.array(_sw_table.toTuples(a2n(
        get_option('encoding_errors'), 'utf-8'),
        casdt.cas2python_datetime, casdt.cas2python_date,
        casdt.cas2python_time), dtype=dtypes)

    # Short circuit for numpy arrays
#   if tformat == 'numpy_array':
#       return kwargs['data']

    cdf = SASDataFrame(**kwargs)

    # Map column names back to unicode in pandas
    cdf.columns = [a2u(x[0], 'utf-8') for x in dtypes]

    # Apply int missing values
    if intmiss:
        cdf = cdf.replace(to_replace=intmiss)

    # Apply mimetype transformations
    if mimetypes:
        from io import BytesIO
        Image = True
        for key, value in mimetypes.items():
            if value.startswith('image/'):
                if Image is True:
                    Image = None
                    try:
                        from PIL import Image
                    except ImportError:
                        warnings.warn('The PIL or Pillow package is required '
                                      'to convert bytes to Image objects',
                                      RuntimeWarning)
                if Image is None:
                    continue
                cdf[key] = cdf[key].map(lambda x: Image.open(BytesIO(x)))

    # Apply date / datetime transformations
    for item in dates:
        cdf[item] = cdf[item].apply(casdt.sas2python_date)
    for item in datetimes:
        cdf[item] = cdf[item].apply(casdt.sas2python_datetime)
    for item in times:
        cdf[item] = cdf[item].apply(casdt.sas2python_time)

    # Check for By group information
    optbycol = get_option('cas.dataset.bygroup_columns')
    optbyidx = get_option('cas.dataset.bygroup_as_index')
    optbysfx = get_option('cas.dataset.bygroup_formatted_suffix')
    optbycolsfx = get_option('cas.dataset.bygroup_collision_suffix')
    cdf = cdf.reshape_bygroups(bygroup_columns=optbycol,
                               bygroup_as_index=optbyidx,
                               bygroup_formatted_suffix=optbysfx,
                               bygroup_collision_suffix=optbycolsfx)

    # Add an index as needed
    index = get_option('cas.dataset.index_name')
    if index:
        if not isinstance(index, (list, tuple, set)):
            index = [index]
        for idx in index:
            if idx in cdf.columns:
                if cdf.attrs.get('ByVar1'):
                    cdf.set_index([idx], append=True, inplace=True)
                else:
                    cdf.set_index([idx], inplace=True)
                adjust = get_option('cas.dataset.index_adjustment')
                if adjust != 0 and str(cdf.index.dtype).startswith('int'):
                    names = cdf.index.names
                    cdf.index = cdf.index.values + adjust
                    cdf.index.names = names
                if get_option('cas.dataset.drop_index_name'):
                    names = list(cdf.index.names)
                    names[-1] = None
                    cdf.index.names = names
                # Only set one index
                break

    # Detect casout tables
    if not(tablename) and unknownname and columnscol and rowscol:
        tablename = unknownname

    # if we have enough information to build CASTable objects, do it
    if caslib and tablename and not castable:
        tables = []
        for lib, tbl in zip(cdf[caslib], cdf[tablename]):
            if connection is not None:
                tbl = connection.CASTable(tbl, caslib=lib)
            else:
                tbl = CASTable(tbl, caslib=lib)
            tbl._disable_pandas()
            tables.append(tbl)
        # In newer versions of pandas, this causes the __len__ method to
        # be called, this can cause CAS results to be truncated due to
        # additional CAS actions being called.
        cdf['casTable'] = pd.Series(tables, name='casTable')
        cdf['casTable'].apply(lambda x: x._enable_pandas())
        cdf.colinfo['casTable'] = SASColumnSpec('casTable', label='Table', dtype='object')

    if tformat == 'dataframe':
        return pd.DataFrame(cdf)

    if tformat == 'dict':
        return cdf.to_dict('dict')

    if tformat.startswith('dict:'):
        return cdf.to_dict(tformat.split(':', 1)[-1])

    return cdf


CAS2PY = {
    'nil': lambda _sw_v, soptions, echk, conn: None,
    'int32': lambda _sw_v, soptions, echk, conn: echk(_sw_v.getInt32(), _sw_v),
    'int64': lambda _sw_v, soptions, echk, conn: echk(_sw_v.getInt64(), _sw_v),
    'double': lambda _sw_v, soptions, echk, conn: echk(_sw_v.getDouble(), _sw_v),
    'blob': lambda _sw_v, soptions, echk, conn:
        blob(base64.b64decode(echk(_sw_v.getBlobBase64(), _sw_v))),
    'string': lambda _sw_v, soptions, echk, conn:
        echk(a2u(_sw_v.getString(), 'utf-8'), _sw_v),
    'boolean': lambda _sw_v, soptions, echk, conn:
        echk(_sw_v.getBoolean() and True or False, _sw_v),
    'list': _caslist2py,
    'table': lambda _sw_v, soptions, echk, conn:
        ctb2tabular(echk(_sw_v.getTable(), _sw_v), soptions, connection=conn),
    'date': lambda _sw_v, soptions, echk, connection:
        casdt.cas2python_date(echk(_sw_v.getDate(), _sw_v)),
    'time': lambda _sw_v, soptions, echk, connection:
        casdt.cas2python_time(echk(_sw_v.getTime(), _sw_v)),
    'datetime': lambda _sw_v, soptions, echk, connection:
        casdt.cas2python_datetime(echk(_sw_v.getDateTime(), _sw_v)),
}


def cas2py(_sw_value, soptions, connection=None):
    '''
    Convert a CASValue object to a Python object

    Parameters
    ----------
    _sw_value : SWIG CASValue object
       Object to convert to Python
    soptions : string
       soptions of connection object

    Returns
    -------
    any
       Python representation of CASValue

    '''
    return _sw_value.toPython(_sw_value, soptions,
                              a2n(get_option('encoding_errors'), 'utf-8'),
                              connection, ctb2tabular,
                              base64.b64decode, casdt.cas2python_datetime,
                              casdt.cas2python_date, casdt.cas2python_time)
#   return CAS2PY[errorcheck(_sw_value.getType(),
#                            _sw_value)](_sw_value, soptions, errorcheck, connection)


def py2cas(soptions, _sw_error, **kwargs):
    '''
    Convert Python arguments to a CASValueList

    Parameters
    ---------
    soptions : string
       soptions of connection object
    _sw_error : SWIG CASError object
       Object to use for returned error messages
    **kwargs : any, optional
       Arbitrary keyword arguments

    Returns
    -------
    CASValueList
       CASValueList representation of Python object

    '''

    def remove_unsupported(params):
        ''' Remove any unsupported parameter types '''
        for key in list(params.keys()):
            if isinstance(params[key], dict_types):
                remove_unsupported(params[key])
            elif not isinstance(params[key], (binary_types, text_types, bool,
                                              blob, int64_types, int32_types,
                                              float64_types, items_types, ParamManager,
                                              datetime.datetime, datetime.date,
                                              datetime.time, type(nil))):
                del params[key]

    # In-place operation
    remove_unsupported(kwargs)

    _sw_values = errorcheck(clib.SW_CASValueList(len(kwargs), a2n(soptions),
                                                 _sw_error), _sw_error)

    def set_list_value(_sw_values, i, key, item):
        '''
        Set a CASValueList item i to the key/item pair

        Parameters
        ----------
        _sw_values : SWIG CASValueList object
           List to set the value in
        i : int or long
           Index in the list to set
        key : string
           Key for the list item (None of no key is desired)
        item : any
           Value to set at the list index

        Returns
        -------
        int
           The next position in the list to set

        '''
        if isinstance(key, (binary_types, text_types)):
            key = keywordify(a2n(key, 'utf-8'))

        if item is True or item is False:
            errorcheck(_sw_values.setBoolean(i, key, item and 1 or 0),
                       _sw_values)
            i = i + 1
        elif isinstance(item, blob):
            errorcheck(_sw_values.setBlob(i, key, item), _sw_values)
            i = i + 1
        elif isinstance(item, text_types):
            errorcheck(_sw_values.setString(i, key, a2n(item, 'utf-8')),
                       _sw_values)
            i = i + 1
        elif isinstance(item, binary_types):
            errorcheck(_sw_values.setString(i, key, a2n(item, 'utf-8')),
                       _sw_values)
            i = i + 1
        elif isinstance(item, int64_types):
            errorcheck(_sw_values.setInt64(i, key, int64(item)), _sw_values)
            i = i + 1
        elif isinstance(item, int32_types):
            if item > MAX_INT32 or item < MIN_INT32:
                errorcheck(_sw_values.setInt64(i, key, int64(item)), _sw_values)
            else:
                errorcheck(_sw_values.setInt32(i, key, int32(item)), _sw_values)
            i = i + 1
        elif isinstance(item, float64_types):
            errorcheck(_sw_values.setDouble(i, key, float64(item)), _sw_values)
            i = i + 1
        elif item is nil:
            errorcheck(_sw_values.setNil(i, key), _sw_values)
            i = i + 1
        elif isinstance(item, items_types):
            _sw_sublist = errorcheck(_sw_values.createListAt(
                                     i, key, len(item)), _sw_values)
            j = 0
            for v in item:
                j = set_list_value(_sw_sublist, j, None, v)
            i = i + 1
        elif isinstance(item, (dict_types, ParamManager)):
            if isinstance(item, ParamManager):
                item = item.to_params()
            _sw_sublist = errorcheck(_sw_values.createListAt(
                                     i, key, len(item)), _sw_values)
            j = 0
            for k, v in sorted(six.iteritems(item), key=lambda x: '%s' % x[0]):
                if isinstance(k, (text_types, binary_types)):
                    j = set_list_value(_sw_sublist, j, k, v)
                else:
                    j = set_list_value(_sw_sublist, j, None, v)
            i = i + 1
        elif isinstance(item, datetime.datetime):
            errorcheck(_sw_values.setDateTime(i, key, casdt.python2cas_datetime(item)),
                       _sw_values)
            i = i + 1
        elif isinstance(item, datetime.date):
            errorcheck(_sw_values.setDate(i, key, casdt.python2cas_date(item)),
                       _sw_values)
            i = i + 1
        elif isinstance(item, datetime.time):
            errorcheck(_sw_values.setTime(i, key, casdt.python2cas_time(item)),
                       _sw_values)
            i = i + 1

        return i

    i = 0
    for skey, svalue in sorted(six.iteritems(kwargs), key=lambda x: '%s' % x[0]):
        i = set_list_value(_sw_values, i, skey, svalue)

    return _sw_values
