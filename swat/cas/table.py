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
CASTable class for interfacing with data tables in CAS

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import copy
import keyword
import inspect
import numbers
import re
import sys
import uuid
import warnings
import weakref
import numpy as np
import pandas as pd
import six
from .utils.datetime import sas2python_datetime
from .utils.params import ParamManager, ActionParamManager
from .utils.misc import super_dir
from ..config import get_option
from ..exceptions import SWATError
from ..utils import dict2kwargs, getattr_safe_property, xdict
from ..utils.compat import (int_types, binary_types, text_types, items_types,
                            patch_pandas_sort, char_types, num_types)
from ..utils.datetime import is_date_format, is_datetime_format, is_time_format
from ..utils.keyword import dekeywordify

# pylint: disable=W0212, W0221, W0613, R0904, C0330

patch_pandas_sort()

pd_version = tuple([int(x) for x in re.match(r'^(\d+)\.(\d+)\.(\d+)',
                                             pd.__version__).groups()])

if pd_version >= (0, 23, 0):
    concat_sort = dict(sort=False)
else:
    concat_sort = dict()

OPERATOR_NAMES = {
    '+': 'add',
    '-': 'sub',
    '*': 'mul',
    '/': 'truediv',
    '//': 'floordiv',
    '**': 'pow',
    '%': 'mod',
    '||': 'cat',
    '>': 'gt',
    '<': 'lt',
    '>=': 'ge',
    '<=': 'le',
    '==': 'eq',
    '=': 'eq',
    '^=': 'ne',
    '!=': 'ne',
}

MAX_INT64_INDEX = 2**63 - 1 - 1  # Extra one is for 1 indexing


def _gen_table_name():
    ''' Generate a unique table name '''
    return '_PY_T_%s' % str(uuid.uuid4()).replace('-', '_').upper()


def _nlit(name, quote=False):
    ''' Return `name` as an nlit '''
    if re.match(r'^[A-Za-z_]\w*$', name):
        if quote:
            return '"%s"' % _escape_string(name)
        return name
    return '"%s"n' % _escape_string(name)


def _quote(name):
    ''' Return `name` as a quoted string '''
    return '"%s"' % _escape_string(name)


def _quote_if_string(name):
    ''' Return `name` as a quoted string if it is a string '''
    if isinstance(name, char_types):
        return '"%s"' % _escape_string(name)
    return name


def _escape_string(val):
    ''' Escape quotes in a string '''
    return val.replace('"', '""')


def _flatten(items):
    ''' Generator to yield all nested list items '''
    for item in items:
        if isinstance(item, (list, tuple)):
            for subitem in _flatten(item):
                yield subitem
        else:
            yield item


def _get_unique(seq, lowercase=False):
    '''
    Return a list with only unique items

    Parameters
    ----------
    lowercase : boolean
        If True, compare elements in a case-insensitive way

    Returns
    -------
    list

    '''
    seen = set()
    if lowercase:
        return [x for x in seq if not (x in seen or x.lower() in seen or seen.add(x))]
    return [x for x in seq if not (x in seen or seen.add(x))]


def _to_datastep_params(casout, ignore=None):
    '''
    Convert object to data step parameters

    Parameters
    ----------
    casout : string or CASTable or dict
        The object to convert to a data step table specification
    ignore : list-of-strings, optional
        List of parameters to ignore

    Returns
    -------
    string

    '''
    if casout is None:
        return _quote(_gen_table_name())

    if isinstance(casout, six.string_types):
        return _quote(casout)

    if isinstance(casout, CASTable):
        casout = dict(casout.params)
    elif isinstance(casout, dict):
        pass
    else:
        raise TypeError('Unrecognized type for casout definition: %s' % type(casout))

    if ignore is None:
        ignore = []

    outname = _quote(casout.get('name', _gen_table_name()))
    outlib = casout.get('caslib')
    outreplace = casout.get('replace')
    outpromote = casout.get('promote')
    outcopies = casout.get('copies')

    options = []
    if outlib and 'caslib' not in ignore:
        options.append('caslib=%s' % _quote(outlib))
    if outreplace is not None and 'replace' not in ignore:
        options.append('replace=%s' % (outreplace and 'yes' or 'no'))
    if outpromote is not None and 'promote' not in ignore:
        options.append('promote=%s' % (outpromote and 'yes' or 'no'))
    if outcopies is not None and 'copies' not in ignore:
        options.append('copies=%s' % outcopies)

    if options:
        return '%s(%s)' % (outname, ' '.join(options))

    return outname


def concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False, keys=None,
           levels=None, names=None, verify_integrity=False, copy=True, casout=None):
    '''
    Concatenate multiple CAS tables

    Parameters
    ----------
    objs : list-of-CASTables
        The CAS tables to concatenate
    axis : int, optional
        Not supported.
    join : string, optional
        Not supported.
    join_axes : list, optional
        Not supported.
    ignore_index : boolean, optional
        Not supported.
    keys : list, optional
        Not supported.
    levels : list-of-sequences, optional
        Not supported.
    names : list, optional
        Not supported.
    verify_integrity : boolean, optional
        Not supported.
    copy : boolean, optional
        Not supported.
    casout : string or CASTable or dict, optional
        The output CAS table specification

    Returns
    -------
    :class:`CASTable`

    '''
    for item in objs:
        if item is None:
            continue
        if not isinstance(item, CASTable):
            raise TypeError('All input objects must be CASTable instances')

    try:
        views = []
        for item in objs:
            if item is None:
                continue
            if not isinstance(item, CASTable):
                raise TypeError('All input objects must be CASTable instances')
            views.append(item.to_view())

        if not views:
            raise ValueError('There are no tables to concatenate')

        # Create data step code for concatenation
        code = []
        code.append('data %s;' % _to_datastep_params(casout))
        code.append('    set %s;' % ' '.join(x.to_input_datastep_params() for x in views))
        code.append('run;')

        out = objs[0].get_connection().retrieve('datastep.runcode', code='\n'.join(code),
                                                _apptag='UI', _messagelevel='error')
        if out.status:
            raise SWATError(out.status)

    finally:
        for item in views:
            try:
                item._retrieve('table.droptable')
            except Exception:
                pass

    return out['OutputCasTables'].iloc[0]['casTable']


def merge(left, right, how='inner', on=None, left_on=None, right_on=None,
          left_index=False, right_index=False, sort=False,
          suffixes=('_x', '_y'), copy=True, indicator=False, validate=None,
          casout=None):
    '''
    Merge CASTable objects using a database-style join on a column

    Parameters
    ----------
    right : CASTable
        The CASTable to join with
    how : string, optional
        * 'left' : use only keys from `left`
        * 'right': use only keys from `right`
        * 'outer' : all observations
        * 'inner' : use intersection of keys
        * 'left-minus-right' : `left` minus `right`
        * 'right-minus-left' : `right` minus `left`
        * 'outer-minus-inner' : opposite of 'inner'
    on : string, optional
        Column name to join on, if the same column name is in
        both tables
    left_on : string, optional
        The key from `left` to join on.  This is used if the
        column names to join on are different in each table.
    right_on : string, optional
        The key from `right` to join on.  This s used if the
        column names to join on are different in each table.
    left_index : boolean, optional
        Not supported.
    right_index : boolean, optional
        Not supported.
    sort : boolean, optional
        Not supported.
    suffixes : two-element-tuple, optional
        The suffixes to use for overlapping column names in the
        resulting tables.  The first element is used for columns
        in `left`.  The second element is used for columns in
        `right`.
    copy : boolean, optional
        Not supported.
    indicator : boolean or string, optional
        If True, a column named '_merge' will be
        created with the values: 'left_only', 'right_only', or
        'both'.  If False, no column is created.  If a string is
        specified, a column is created using that name containing
        the aforementioned values.
    validate : string, optional
        Not supported.
    casout : string or CASTable or dict, optional
        The output CAS table specification

    Returns
    -------
    :class:`CASTable`

    '''
    if not isinstance(left, CASTable):
        raise TypeError('`left` parameter must be a CASTable object')
    if not isinstance(right, CASTable):
        raise TypeError('`right` parameter must be a CASTable object')

    how = how.lower()

    # Setup join column names
    if on is None and left_on is None and right_on is None:
        raise SWATError('A column name is required for joining tables.')
    elif left_on is None and right_on is None:
        left_on = on
        right_on = on
    elif left_on is None and right_on is not None:
        left_on = right_on or on
    elif left_on is not None and right_on is None:
        right_on = left_on or on

    # Find overlapping columns
    left_rename = ''
    right_rename = ''
    left_dtypes = left.dtypes.to_dict()
    right_dtypes = right.dtypes.to_dict()
    left_columns = left.columns
    right_columns = right.columns
    left_cols = set([x for x in left_columns if x != left_on])
    right_cols = set([x for x in right_columns if x != right_on])
    same_cols = left_cols.intersection(right_cols)
    varchars = []
    varbins = []
    if same_cols:
        left_rename = ' ' + ' '.join(
            ['%s=%s' % (_nlit(x), _nlit('%s%s' % (x, suffixes[0]))) for x in same_cols])
        right_rename = ' ' + ' '.join(
            ['%s=%s' % (_nlit(x), _nlit('%s%s' % (x, suffixes[1]))) for x in same_cols])

        varchars.extend([_nlit('%s%s' % (x, suffixes[0]))
                         for x in same_cols if left_dtypes[x] == 'varchar'])
        varchars.extend([_nlit('%s%s' % (x, suffixes[1]))
                         for x in same_cols if right_dtypes[x] == 'varchar'])

        varbins.extend([_nlit('%s%s' % (x, suffixes[0]))
                        for x in same_cols if left_dtypes[x] == 'varbinary'])
        varbins.extend([_nlit('%s%s' % (x, suffixes[1]))
                        for x in same_cols if right_dtypes[x] == 'varbinary'])

    left_map = {item: item for item in left.columns}
    right_map = {item: item for item in right.columns}
    for item in same_cols:
        left_map[item] = '%s%s' % (item, suffixes[0])
        right_map[item] = '%s%s' % (item, suffixes[1])

    left_rename = ' rename=(%s=__by_var%s)' % (_nlit(left_on), left_rename)
    right_rename = ' rename=(%s=__by_var%s)' % (_nlit(right_on), right_rename)

    columns = ' '.join([_nlit(left_map[x]) for x in left_columns]
                       + [_nlit(right_map[x]) for x in right_columns])

    left_missval = '.'
    right_missval = '.'
    if left_on != right_on:
        if left_dtypes[left_on] in ['varchar', 'char', 'varbinary', 'binary']:
            left_missval = '""'
        if right_dtypes[right_on] in ['varchar', 'char', 'varbinary', 'binary']:
            right_missval = '""'

    if left_dtypes[left_on] == 'varchar':
        varchars.append(_nlit(left_on))
    elif left_dtypes[left_on] == 'varbinary':
        varbins.append(_nlit(left_on))

    if right_on != left_on:
        if right_dtypes[right_on] == 'varchar':
            varchars.append(_nlit(right_on))
        elif right_dtypes[right_on] == 'varbinary':
            varbins.append(_nlit(right_on))

    left_view = None
    right_view = None

    try:
        # Allow computed columns / where clauses to be available
        # data step code.
        left_view = left.to_view()
        right_view = right.to_view()

        left_name = ''
        right_name = ''
        left_caslib = ''
        right_caslib = ''
        left_name = left_view.params['name']
        right_name = right_view.params['name']
        if left_view.params.get('caslib'):
            left_caslib = ' caslib=%s' % _quote(left_view.params['caslib'])
        if right_view.params.get('caslib'):
            right_caslib = ' caslib=%s' % _quote(right_view.params['caslib'])

        # Create data step code for merge
        code = []
        code.append('data %s;' % _to_datastep_params(casout))
        code.append('    retain %s;' % columns)

        if varchars and varbins:
            code.append('    length %s varchar(*) %s varbinary(*);' %
                        (' '.join(varchars), ' '.join(varbins)))
        elif varbins:
            code.append('    length %s varbinary(*);' % ' '.join(varbins))
        elif varchars:
            code.append('    length %s varchar(*);' % ' '.join(varchars))

        code.append('    merge %s(in=__in_left%s%s) %s(in=__in_right%s%s);' %
                    (_quote(left_name), left_caslib, left_rename,
                     _quote(right_name), right_caslib, right_rename))

        code.append('    by __by_var;')

        if how in ['outer', 'full-outer']:
            code.append('    if __in_left or __in_right then do;'
                        '        if __in_left then do;'
                        '            %(left_on)s = __by_var;'
                        '        end;'
                        '        if __in_right then do;'
                        '            %(right_on)s = __by_var;'
                        '        end;'
                        '    end;'
                        '    else do;'
                        '        delete;'
                        '    end;')
        elif how in ['left', 'left-outer']:
            code.append('    if __in_left then do;'
                        '        if __in_right then do;'
                        '            %(right_on)s = __by_var;'
                        '        end;'
                        '        else do;'
                        '            %(right_on)s = %(right_missval)s;'
                        '        end;'
                        '        %(left_on)s = __by_var;'
                        '    end;'
                        '    else do;'
                        '        delete;'
                        '    end;')
        elif how in ['right', 'right-outer']:
            code.append('    if __in_right then do;'
                        '        if __in_left then do;'
                        '            %(left_on)s = __by_var;'
                        '        end;'
                        '        else do;'
                        '            %(left_on)s = %(left_missval)s;'
                        '        end;'
                        '        %(right_on)s = __by_var;'
                        '    end;'
                        '    else do;'
                        '        delete;'
                        '    end;')
        elif how in ['inner']:
            code.append('    if __in_left and __in_right then do;'
                        '        %(left_on)s = __by_var;'
                        '        %(right_on)s = __by_var;'
                        '    end;'
                        '    else do;'
                        '        delete;'
                        '    end;')
        elif how in ['left-minus-right']:
            code.append('    if __in_left and ^__in_right then do;'
                        '        %(left_on)s = __by_var;'
                        '        %(right_on)s = %(right_missval)s;'
                        '    end;'
                        '    else do;'
                        '        delete;'
                        '    end;')
        elif how in ['right-minus-left']:
            code.append('    if ^__in_left and __in_right then do;'
                        '        %(left_on)s = %(left_missval)s;'
                        '        %(right_on)s = __by_var;'
                        '    end;'
                        '    else do;'
                        '        delete;'
                        '    end;')
        elif how in ['outer-minus-inner']:
            code.append('    if (__in_left and ^__in_right) or '
                        '       (^__in_left and __in_right) then do;'
                        '        if __in_left then do;'
                        '            %(left_on)s = __by_var;'
                        '        end;'
                        '        else do;'
                        '            %(left_on)s = %(left_missval)s;'
                        '        end;'
                        '        if __in_right then do;'
                        '            %(right_on)s = __by_var;'
                        '        end;'
                        '        else do;'
                        '            %(right_on)s = %(right_missval)s;'
                        '        end;'
                        '    end;'
                        '    else do;'
                        '        delete;'
                        '    end;')
        else:
            raise ValueError('Unrecognized merge type: %s' % how)

        code[-1] = code[-1] % dict(left_on=_nlit(left_on), right_on=_nlit(right_on),
                                   left_missval=left_missval, right_missval=right_missval)

        if indicator:
            if indicator is True:
                indicator = '_merge'
            else:
                indicator = '%s' % indicator
            code.append('    length %s $10;' % _nlit(indicator))
            code.append('    if __in_left then')
            code.append('        if __in_right then %s = "both";' % _nlit(indicator))
            code.append('        else %s = "left_only";' % _nlit(indicator))
            code.append('    else %s = "right_only";' % _nlit(indicator))

        code.append('    drop __by_var;')

        code.append('run;')

        out = left.get_connection().retrieve('datastep.runcode', code='\n'.join(code),
                                             _apptag='UI', _messagelevel='error')
        if out.status:
            raise SWATError(out.status)

    finally:
        if left_view is not None:
            left_view._retrieve('table.droptable')
        if right_view is not None:
            right_view._retrieve('table.droptable')

    return out['OutputCasTables'].iloc[0]['casTable']


class CASTableAccessor(object):
    ''' Base class for all accessor properties '''

    def __init__(self, table):
        self._table = weakref.ref(table)


class CASTableRowScalarAccessor(CASTableAccessor):
    ''' Implemention of the `iat` property '''

    def __getitem__(self, pos):
        tbl = self._table()
        if isinstance(tbl, CASColumn):
            return tbl.iat[pos, 0]
        return tbl.iat[slice(*pos)]


class CASTableLabelScalarAccessor(CASTableAccessor):
    ''' Implemention of the `at` property '''

    def __getitem__(self, pos):
        tbl = self._table()
        if isinstance(tbl, CASColumn):
            if pos < 0 or pos >= tbl._numrows:
                raise KeyError(pos)
            return tbl.iat[pos, 0]
        if pos[0] < 0 or pos[0] >= tbl._numrows:
            raise KeyError(pos)
        return tbl.iat[slice(*pos)]


def _get_table_selection(table, args):
    '''
    Determine the inputs and computed varibles selected

    Parameters
    ----------
    table : CASTable object
        The table to index
    args : slice or tuple or int
        The argument to __getitem__

    Returns
    -------
    (dict, string)
       (Dictionary containing table parameters,
        String containing expected output type: table, row, column, scalar)

    '''
    cols = None
    computedvars = []
    computedvarsprogram = []
    outtype = 'table'

    # Rows and columns specified
    if isinstance(args, tuple):
        rows = args[0]

        # tbl.x[[0, 3, 2]] or tbl.x[[2]]
        if isinstance(rows, items_types):
            raise IndexError('Row selection is not supported.')

        # tbl.x[1:12]
        elif isinstance(rows, slice):
            if rows.start is not None or rows.stop is not None or \
                    rows.step is not None:
                raise IndexError('Row selection is not supported.')

        # tbl.x[10]
        elif isinstance(rows, int_types):
            outtype = 'row'
            raise IndexError('Row selection is not supported.')

        else:
            raise TypeError('Unknown type for row indexing: %s' % rows)

        cols = args[1]

        # tbl.x[0, ['a', 'b', 5]]
        if isinstance(cols, items_types):
            colset = set([x.lower() for x in table.columns])
            out = []
            for col in cols:
                if isinstance(col, int_types):
                    out.append(col)
                elif col.lower() in colset:
                    out.append(col)
                else:
                    out.append(col)
                    computedvars.append(col)
                    computedvarsprogram.append('%s = .; ' % _nlit(col))
            cols = out

        # tbl.x[0, 'a':10]
        elif isinstance(cols, slice):
            use_names = False
            columns = None
            lowcolumns = None
            colstart, colstep, colend = cols.start, cols.step, cols.stop
            if isinstance(colstart, (text_types, binary_types)):
                if not columns:
                    columns = list(table.columns)
                    lowcolumns = [x.lower() for x in columns]
                colstart = lowcolumns.index(colstart.lower())
                use_names = True
            if isinstance(colend, (text_types, binary_types)):
                if not columns:
                    columns = list(table.columns)
                    lowcolumns = [x.lower() for x in columns]
                colend = lowcolumns.index(colend.lower()) + 1
                use_names = True
            if use_names:
                cols = columns[colstart:colend:colstep]
            else:
                if colend is None:
                    if not columns:
                        columns = list(table.columns)
                    colend = len(columns)
                elif colend < 0:
                    if not columns:
                        columns = list(table.columns)
                    colend = len(columns) + colend
                cols = list(range(colend))[colstart:colend:colstep]

        # tbl.x[0, 10]
        elif isinstance(cols, int_types):
            cols = [cols]
            if outtype == 'row':
                outtype = 'scalar'
            elif outtype == 'table':
                outtype = 'column'

        # tbl.x[0, 'a']
        elif isinstance(cols, (text_types, binary_types)):
            cols = [cols]
            if outtype == 'row':
                outtype = 'scalar'
            elif outtype == 'table':
                outtype = 'column'

        else:
            raise TypeError('Unknown type for column indexing: %s' % cols)

    # Range of rows specified
    elif isinstance(args, slice):
        raise IndexError('Row selection is not supported.')

    # One row specified
    elif isinstance(args, int_types):
        raise IndexError('Row selection is not supported.')

    else:
        raise TypeError('Unknown type for table indexing: %s' % args)

    out = {}

    if cols:
        out['table._columns'] = cols

    if computedvars and computedvarsprogram:
        out['table.computedvars'] = computedvars
        out['table.computedvarsprogram'] = computedvarsprogram

    # TODO: Set table.where for row label indexing when
    #       a table index is supported.

    return out, outtype


class CASTableAnyLocationAccessor(CASTableAccessor):
    ''' Implementation of the ix property '''
    accessor = 'ix'

    def __getitem__(self, pos):
        is_column = isinstance(self._table(), CASColumn)
        if is_column:
            if isinstance(pos, tuple):
                raise TypeError('Too many indexers')
            pos = tuple([pos, 0])

        params, dtype = _get_table_selection(self._table(), pos)

        tbl = self._table()

        if 'table._columns' in params or 'table.computedvars' in params:
            tbl = tbl.copy()

        # Build inputs
        inputs = None
        if 'table._columns' in params:
            varlist = []
            inputs = params.pop('table._columns')
            columns = None
            for item in inputs:
                if isinstance(item, int_types):
                    if not columns:
                        columns = list(tbl.columns)
                    varlist.append(columns[item])
                else:
                    varlist.append(item)
            tbl._columns = varlist

        # Append computedvars and computedvarsprogram
        if 'table.computedvars' in params and 'table.computedvarsprogram' in params:
            tbl.append_computed_columns(params.pop('table.computedvars'),
                                        params.pop('table.computedvarsprogram'))

        if dtype == 'column':
            return tbl[tbl.columns[0]]

        return tbl


class CASTableRowLocationAccessor(CASTableAnyLocationAccessor):
    ''' Implementation of the iloc property '''
    accessor = 'iloc'


class CASTableLabelLocationAccessor(CASTableAnyLocationAccessor):
    ''' Implementation of the loc property '''
    accessor = 'loc'


class CASTablePlotter(object):
    '''
    Plotting class for CASTable

    Parameters
    ----------
    table : CASTable object
       The CASTable object to bind to

    Returns
    -------
    CASTablePlotter object

    '''

    def __init__(self, table):
        self._table = table

    def _get_fetchvars(self, x=None, y=None, by=None, c=None, C=None, s=None):
        '''
        Return a list of variables needed for the plot

        '''
        if x is None and y is None:
            return None

        x = x or []
        if isinstance(x, six.string_types):
            x = [x]

        y = y or []
        if isinstance(y, six.string_types):
            y = [y]

        if by is not None:
            if isinstance(by, six.string_types):
                by = [by]
        else:
            by = self._table.get_groupby_vars()

        c = c or []
        if isinstance(c, six.string_types):
            c = [c]

        C = C or []
        if isinstance(C, six.string_types):
            C = [C]

        s = s or []
        if isinstance(s, six.string_types):
            s = [s]

        return list(by) + list(x) + list(y) + list(c) + list(C) + list(s)

    def _get_sampling_params(self, **kwargs):
        '''
        Extract sampling parameters from keyword arguments

        '''
        samp = {}
        samp['sample'] = kwargs.pop('sample', True)
        if 'sample_pct' in kwargs:
            samp['sample_pct'] = kwargs.pop('sample_pct')
        if 'sample_seed' in kwargs:
            samp['sample_seed'] = kwargs.pop('sample_seed')
        if 'stratify_by' in kwargs:
            samp['stratify_by'] = kwargs.pop('stratify_by')
        return samp, kwargs

    def _get_plot_params(self, x=None, y=None, by=None, c=None,
                         C=None, s=None, **kwargs):
        '''
        Split parameters into fetch and plot parameter groups

        '''
        params, kwargs = self._get_sampling_params(**kwargs)
        params['grouped'] = True
        params['fetchvars'] = self._get_fetchvars(x=x, y=y, by=by, c=c, C=C, s=s)
        return params, kwargs

    def __call__(self, x=None, y=None, kind='line', **kwargs):
        '''
        Make a line plot of all columns in a table

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot` method.

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :class:`np.array` of them.

        '''
        params, kwargs = self._get_plot_params(x=x, y=y, **kwargs)
        return self._table._fetch(**params).plot(x=x, y=y, kind=kind, **kwargs)

    def area(self, x=None, y=None, **kwargs):
        '''
        Area plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.area` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.area` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.area`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(x=x, y=y, **kwargs)
        return self._table._fetch(**params).plot(x=x, y=y, kind='area', **kwargs)

    def bar(self, x=None, y=None, **kwargs):
        '''
        Bar plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.bar` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.bar` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.bar`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(x=x, y=y, **kwargs)
        return self._table._fetch(**params).plot(x=x, y=y, kind='bar', **kwargs)

    def barh(self, x=None, y=None, **kwargs):
        '''
        Horizontal bar plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.barh` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.barh` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.barh`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(x=x, y=y, **kwargs)
        return self._table._fetch(**params).plot(x=x, y=y, kind='barh', **kwargs)

    def box(self, by=None, **kwargs):
        '''
        Boxplot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.box` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.box` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.box`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(by=by, **kwargs)
        return self._table._fetch(**params).plot(by=by, kind='box', **kwargs)

    def density(self, **kwargs):
        '''
        Kernel density estimate plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.density` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.density` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.density`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(**kwargs)
        return self._table._fetch(**params).plot(kind='density', **kwargs)

    def hexbin(self, x=None, y=None, C=None, reduce_C_function=None,
               gridsize=None, **kwargs):
        '''
        Hexbin plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.hexbin` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.density` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.hexbin`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(x=x, y=y, C=C, **kwargs)
        if reduce_C_function is not None:
            kwargs['reduce_C_function'] = reduce_C_function
        if gridsize is not None:
            kwargs['gridsize'] = gridsize
        return self._table._fetch(**params)\
                   .plot(x=x, y=y, C=C, kind='hexbin', **kwargs)

    def hist(self, by=None, bins=10, **kwargs):
        '''
        Histogram

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.hist` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.hist` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.hist`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(by=by, **kwargs)
        return self._table._fetch(**params).plot(by=by, bins=bins,
                                                 kind='hist', **kwargs)

    def kde(self, **kwargs):
        '''
        Kernel density estimate plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.kde` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.kde` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.kde`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(**kwargs)
        return self._table._fetch(**params).plot(kind='kde', **kwargs)

    def line(self, x=None, y=None, **kwargs):
        '''
        Line plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.line` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.line` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.line`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(x=x, y=y, **kwargs)
        return self._table._fetch(**params).plot(x=x, y=y, kind='line', **kwargs)

    def pie(self, y=None, **kwargs):
        '''
        Pie chart

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.pie` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.pie` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.pie`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        if y is None and isinstance(self._table, CASColumn):
            y = self._table.columns[0]
        params, kwargs = self._get_plot_params(y=y, **kwargs)
        return self._table._fetch(**params).plot(y=y, kind='pie', **kwargs)

    def scatter(self, x, y, s=None, c=None, **kwargs):
        '''
        Scatter plot

        This method fetches the data from the CAS table and uses the
        :meth:`pandas.DataFrame.plot.scatter` method to plot it.  All
        arguments used in the call to this method are passed to
        the DataFrame's :meth:`plot.scatter` method.

        See Also
        --------
        :meth:`pandas.DataFrame.plot.scatter`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._get_plot_params(x=x, y=y, s=s, c=c, **kwargs)
        return self._table._fetch(**params).plot(x, y, s=s, c=c,
                                                 kind='scatter', **kwargs)


@six.python_2_unicode_compatible
class CASTable(ParamManager, ActionParamManager):
    '''
    Object for interacting with CAS tables

    :class:`CASTable` objects can be used in multiple ways.  They can be used
    as simply a container of table parameters and used as CAS action parameter
    values.  If a connection is associated with it (either by instantiating it
    from :meth:`CAS.CASTable` or using :meth:`set_connection`), it can be used
    to call CAS actions on the table.  Finally, it supports much of the
    :class:`pandas.DataFrame` API, so it can interact with CAS tables in much
    the same way you interact with local data.

    The parameters below are a superset of all of the available parameters.
    Some CAS actions may not support all parameters.  You will need to see the
    help for each CAS action on what it supports.

    Parameters
    ----------
    name : string or CASTable
        specifies the name of the table to use.
    caslib : string, optional
        specifies the caslib containing the table that you want to use
        with the action. By default, the active caslib is used. Specify a
        value only if you need to access a table from a different caslib.
    where : string, optional
        specifies an expression for subsetting the input data.
    groupby : list of dicts, optional
        specifies the names of the variables to use for grouping
        results.
    groupbyfmts : list, optional
        specifies the format to apply to each group-by variable. To
        avoid specifying a format for a group-by variable, use "" (no
        format).
        Default: []
    orderby : list of dicts, optional
        specifies the variables to use for ordering observations within
        partitions. This parameter applies to partitioned tables or it
        can be combined with groupBy variables when groupByMode is set to
        REDISTRIBUTE.
    computedvars : list of dicts, optional
        specifies the names of the computed variables to create. Specify
        an expression for each parameter in the computedvarsprogram parameter.
    computedvarsprogram : string, optional
        specifies an expression for each variable that you included in
        the computedvars parameter.
    groupbymode : string, optional
        specifies how the server creates groups.
        Default: NOSORT
        Values: NOSORT, REDISTRIBUTE
    computedondemand : boolean, optional
        when set to True, the computed variables specified in the
        compVars parameter are created when the table is loaded instead
        of when the action begins.
        Default: False
    singlepass : boolean, optional
        when set to True, the data does not create a transient table in
        the server. Setting this parameter to True can be efficient, but
        the data might not have stable ordering upon repeated runs.
        Default: False
    importoptions : dict, optional
        specifies the settings for reading a table from a data source.
    ondemand : boolean, optional
        when set to True, table access is less aggressive with virtual
        memory use.
        Default: True
    vars : list of dicts, optional
        specifies the variables to use in the action.
    timestamp : string, optional
        specifies the timestamp to apply to the table. Specify the value
        in the form that is appropriate for your session locale.
        **Used only on output table definitions.**
    compress : boolean, optional
        when set to True, data compression is applied to the table.
        **Used only on output table definitions.**
        Default: False
    replace : boolean, optional
        specifies whether to overwrite an existing table with the same
        name.
        **Used only on output table definitions.**
        Default: False
    replication : int32, optional
        specifies the number of copies of the table to make for fault
        tolerance. Larger values result in slower performance and use
        more memory, but provide high availability for data in the event
        of a node failure.
        **Used only on output table definitions.**
        Default: 1
        Note: Value range is 0 <= n < 2147483647
    threadblocksize : int64, optional
        specifies the number of bytes to use for blocks that are read by
        threads. Increase this value only if you have a large table and
        CPU utilization by threads shows thread starvation.
        **Used only on output table definitions.**
        Note: Value range is 0 <= n < 9223372036854775807
    label : string, optional
        specifies the descriptive label to associate with the table.
    maxmemsize : int64, optional
        specifies the maximum amount of physical memory, in bytes, to
        allocate for the table. After this threshold is reached, the
        server uses temporary files and operating system facilities for
        memory management.
        **Used only on output table definitions.**
        Default: 0
    promote : boolean, optional
        when set to True, the output table is added with a global scope.
        This enables other sessions to access the table, subject to
        access controls. The target caslib must also have a global scope.
        **Used only on output table definitions.**
        Default: False
    ondemand : boolean, optional
        when set to True, table access is less aggressive with virtual
        memory use.
        **Used only on output table definitions.**
        Default: True

    Examples
    --------
    Create a :class:`CASTable` registered to `conn`.

    >>> conn = swat.CAS()
    >>> iris = conn.CASTable('iris')

    Use the table as a CAS action parameter.

    >>> summ = conn.summary(table=iris)
    >>> print(summ)

    Call a CAS action directly on the :class:`CASTable`.

    >>> summ = iris.summary()
    >>> print(summ)

    Use a :class:`CASTable` as an output table definition.

    >>> summout = conn.summary(table=iris,
    ...                        casout=swat.CASTable('summout', replace=True))
    >>> print(summout)

    Use a :class:`CASTable` like a :class:`pandas.DataFrame`

    >>> print(iris.head())
    >>> print(iris[['petal_length', 'petal_width']].describe())

    Returns
    -------
    :class:`CASTable`

    '''

    table_params = set()
    outtable_params = set()
    all_params = set()

    getdoc = None

    def __init__(self, name, **table_params):
        if isinstance(name, CASTable):
            params = name.to_params()
            params.update(table_params)
            ParamManager.__init__(self, **params)
        else:
            ParamManager.__init__(self, name=name, **table_params)
        ActionParamManager.__init__(self)
        self._pandas_enabled = True
        self._connection = None
        self._contexts = []

        self._columns = []
        self._sortby = []

#       self._iat = CASTableRowScalarAccessor(self)
#       self._at = CASTableLabelScalarAccessor(self)
        self._iloc = CASTableRowLocationAccessor(self)
        self._loc = CASTableLabelLocationAccessor(self)
        self._ix = CASTableAnyLocationAccessor(self)

        self._plot = CASTablePlotter(self)

        self._dir = set([x for x in super_dir(CASTable, self)])
        self.params.set_dir_values(type(self).all_params)

        # Add doc to params
        init = self.__init__
        if hasattr(init, '__func__'):
            init = init.__func__
        if init is not None and init.__doc__ is not None:
            doc = 'Table Parameters' + init.__doc__.split('Table Parameters', 1)[-1]
            doc = doc.split('Returns')[0].rstrip()
            self.params.set_doc(doc)

    def _disable_pandas(self):
        '''
        Disable selected pandas DataFrame features

        Some versions of pandas cause lookups of attributes on CASTables
        that can cause interruptions of running actions.  These
        features can be disable temporarily to bypass the pandas
        features where needed.

        '''
        self._pandas_enabled = False

    def _enable_pandas(self):
        '''  Re-enable pandas features '''
        self._pandas_enabled = True

    def with_params(self, **kwargs):
        '''
        Create copy of table with `kwargs` inserted as parameters

        Note that any parameter names in `kwargs` that match existing
        keys in the CASTable will be overridden.

        Parameters
        ----------
        **kwargs : keyword parameters, optional
            Parameters to insert into the CASTable copy

        Returns
        -------
        :class:`CASTable`

        '''
        out = self.copy(deep=True)
        out.params.update(kwargs)
        return out

    def append_columns(self, *items, **kwargs):
        '''
        Append variable names to action inputs parameter

        Parameters
        ----------
        *items : strings or lists-of-strings
            Names to append.
        inplace : boolean, optional
            If `True`, the current action inputs are appended.
            If `False`, the new inputs is returned.

        Returns
        -------
        None
            if inplace == True
        list of strings
            if inplace == False

        '''
        varlist = self._columns
        if not varlist:
            varlist = list(self.columns)
        if not isinstance(varlist, items_types):
            varlist = [varlist]
        else:
            varlist = list(varlist)
        for item in _flatten(items):
            if item:
                varlist.append(item)
        varlist = _get_unique(varlist, lowercase=True)
        if kwargs.get('inplace', True):
            self._columns = varlist
            return
        return varlist

    def append_computedvars(self, *items, **kwargs):
        '''
        Append variable names to computedvars parameter

        Parameters
        ----------
        *items : strings or lists-of-strings
            Names to append.
        inplace : boolean, optional
            If `True` (the default), tbl.computedvars is appended.
            If `False`, a new computedvars is returned.

        Returns
        -------
        None
            if inplace == True
        list of strings
            if inplace == False

        '''
        varlist = []
        if self.has_param('computedvars'):
            varlist = self.get_param('computedvars')
        if not isinstance(varlist, items_types):
            varlist = [varlist]
        else:
            varlist = list(varlist)
        for item in _flatten(items):
            if item:
                varlist.append(item)
        varlist = _get_unique(varlist, lowercase=True)
        if kwargs.get('inplace', True):
            self.set_param('computedvars', varlist)
            return
        return varlist

    def append_groupby(self, *items, **kwargs):
        '''
        Append variable names to groupby parameter

        Parameters
        ----------
        *items : strings or lists-of-strings
            Names to append.
        inplace : boolean, optional
            If `True` (the default), tbl.groupby is appended.
            If `False`, a new groupby is returned.

        Returns
        -------
        None
            if inplace == True
        list of strings
            if inplace == False

        '''
        varlist = self.get_param('groupby', [])
        if not isinstance(varlist, items_types):
            varlist = [varlist]
        else:
            varlist = list(varlist)
        for item in _flatten(items):
            if item:
                varlist.append(item)
        varlist = _get_unique(varlist, lowercase=True)
        if kwargs.get('inplace', True):
            self.set_param('groupby', varlist)
            return
        return varlist

    def append_computedvarsprogram(self, *items, **kwargs):
        '''
        Append code to computedvarsprogram parameter

        Parameters
        ----------
        *items : strings or lists-of-strings
            Code to append.
        inplace : boolean, optional
            If `True`, tbl.computedvarsprogram is appended.
            If `False`, a new computedvarsprogram is returned.

        Returns
        -------
        None
            if inplace == True
        string
            if inplace == False

        '''
        code = []
        if self.has_param('computedvarsprogram'):
            code = self.get_param('computedvarsprogram')
        if not isinstance(code, items_types):
            code = [code]
        else:
            code = list(code)
        for item in _flatten(items):
            if item:
                code.append(item)
        for i, block in enumerate(code):
            if not re.search(r';\s*$', block):
                code[i] = '%s; ' % block.rstrip()
        # Filter out duplicate lines of code
        if code:
            newcode = []
            for item in code:
                parts = [x for x in re.split(r';[\s+|$]', item) if x.strip()]
                newcode.extend(parts)
                code = '; '.join(_get_unique(newcode)) + '; '
        else:
            code = ''
        if kwargs.get('inplace', True):
            self.set_param('computedvarsprogram', code)
            return
        return code

    def append_computed_columns(self, names, code, inplace=True):
        '''
        Append computed columns as specified

        Parameters
        ----------
        names : string or list-of-strings
            Names of computed columns.
        code : string or list-of-strings
            Code blocks for computed columns.
        inplace : boolean, True
            If `True`, the computed column specifications are appended.
            If `False`, new computed column specifications are returned.

        Returns
        -------
        None
            if inplace=True
        (computedvars, computedvarsprogram)
            if inplace=False

        '''
        out = (self.append_computedvars(names, inplace=inplace),
               self.append_computedvarsprogram(code, inplace=inplace))
        if inplace:
            return
        return out

    def append_where(self, *items, **kwargs):
        '''
        Append code to where parameter

        Parameters
        ----------
        *items : strings or lists-of-strings
            Code to append.
        inplace : boolean, optional
            If `True`, tbl.where is appended.
            If `False`, a new where is returned.

        Returns
        -------
        None
            if inplace == True
        string
            if inplace == False

        '''
        code = self.get_param('where', [])
        if not isinstance(code, items_types):
            code = [code]
        else:
            code = list(code)
        for item in _flatten(items):
            if item:
                code.append(item)
        code = ' and '.join(_get_unique(['(%s)' % x for x in code if x.strip()]))
        if kwargs.get('inplace', True):
            self.set_param('where', code)
            return
        return code

    def append_orderby(self, *items, **kwargs):
        '''
        Append orderby parameters

        Parameters
        ----------
        *items : strings or dicts or list-of-strings or list-of-dicts
            Sorting parameters.  Each item can be a name of a column,
            or a dictionary containing 'name', 'order', and 'formatted' keys.
        inplace : boolean, optional
            If `True` (the default), the tbl.orderby parameter is appended.
            If `False`, a new orderby parameter is returned.

        Returns
        -------
        None
            if inplace == True
        list of dicts
            if inplace == False

        '''
        orderby = []

        # See if there is a orderby
        if self.has_param('orderby'):
            orderby = self.get_param('orderby')

        # Force orderby to be a list
        if isinstance(orderby, dict):
            orderby = [orderby]
        elif isinstance(orderby, items_types):
            orderby = list(orderby)
        else:
            orderby = [dict(name=orderby)]

        # Append sort parameters
        for item in items:
            if not item:
                continue
            if isinstance(item, (text_types, binary_types)):
                orderby.append(dict(name=item))
            elif isinstance(item, dict):
                orderby.append(item)
            else:
                for subitem in item:
                    if not subitem:
                        continue
                    if isinstance(subitem, (text_types, binary_types)):
                        orderby.append(dict(name=subitem))
                    else:
                        orderby.append(subitem)

        # Set it as needed
        if kwargs.get('inplace', True):
            self.set_param('orderby', orderby)
            return

        return orderby

    def _to_column(self, varname=None):
        '''
        Convert CASTable to CASColumn

        Parameters
        ----------
        varname : string, optional
            The name of the column to use.  If not specified, the first
            column in the table is used.

        Returns
        -------
        CASColumn object

        '''
        column = CASColumn(**self.copy().to_params())
        column._columns = list(self._columns)
        column._sortby = list(self._sortby)

        if varname is not None:
            column._columns = [varname]

        try:
            column.set_connection(self.get_connection())
        except SWATError:
            pass

        if not column._columns:
            column._columns = [column._columninfo.iloc[0]['Column']]

        return column

    def __dir__(self):
        # Short-circuit PyCharm's introspection
        if 'get_names' in [x[3] for x in inspect.stack()]:
            return ['params']
        try:
            conn = self.get_connection()
            return list(sorted(list(self._dir) + list(conn.get_action_names())))
        except Exception:
            pass
        return list(sorted(self._dir))

    def __dir_actions__(self):
        try:
            conn = self.get_connection()
            return list(sorted(conn.get_action_names()))
        except Exception:
            pass
        return []

    def __dir_members__(self):
        return list(sorted(self._dir))

    @classmethod
    def _bootstrap(cls, connection):
        '''
        Populate documentation and method signatures

        Parameters
        ----------
        connection : CAS instance
           CAS connection to use for reflection.

        '''
        if not cls.table_params or not cls.outtable_params:
            param_names = []

            actinfo = connection._get_action_info('builtins.cascommon', levels=100)

            for item in actinfo[-1].get('params'):
                if 'parmList' in item:
                    # Populate valid fields for tables and outtables
                    if item['name'] == 'castable':
                        cls.table_params = set([x['name'] for x in item['parmList']])

                    elif item['name'] == 'casouttable':
                        cls.outtable_params = set([x['name'] for x in item['parmList']])

                    elif item['name'] == 'casouttablebasic':
                        cls.outtable_params = set([x['name'] for x in item['parmList']])

            for name in list(param_names):
                if keyword.iskeyword(name):
                    param_names.append(dekeywordify(name))

            cls.all_params = set(param_names)

            cls.param_names = cls.table_params.union(cls.outtable_params)

    def set_connection(self, connection):
        '''
        Set the connection to use for action calls

        When a connection is registered with a :class:`CASTable` object,
        CAS actions can be called on it as if they were instance methods.

        Parameters
        ----------
        connection : :class:`CAS` object
            The connection object to use.

        Notes
        -----
        This method creates a weak reference to the connection.
        If the connection is released, actions will no longer
        be able to be called from the CASTable object.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = CASTable('my-table')
        >>> tbl.set_connection(conn)
        >>> conn is tbl.get_connection()
        True

        '''
        if connection is None:
            self._connection = None
        else:
            self._connection = weakref.ref(connection)

    def get_connection(self):
        '''
        Get the registered connection object

        When a connection is registered with a :class:`CASTable` object,
        CAS actions can be called on it as if they were instance methods.

        Raises
        ------
        SWATError
            If no connection is available either because it wasn't
            set, or because the connection object was released.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = CASTable('my-table')
        >>> tbl.set_connection(conn)
        >>> conn is tbl.get_connection()
        True

        Returns
        -------
        :class:`CAS` object
            If a connection exists on the CASTable

        '''
        conn = None
        if self._connection is not None:
            try:
                conn = self._connection()
            except SWATError:
                pass
        if conn is None:
            raise SWATError('No connection is currently registered')
        return conn

    def __eq__(self, other):
        '''
        Test for equality

        CASTable objects are considered equal if they contain the exact
        same set of parameters.

        Parameters
        ----------
        other : :class:`CASTable` object
            The CASTable object to compare to.

        Examples
        --------
        >>> t1 = CASTable('one', replace=True)
        >>> t2 = CASTable('two', where='a < 1')
        >>> t3 = CASTable('two')
        >>> t3.where = 'a < 1'

        >>> t1 == t2
        False

        >>> t2 == t3
        True

        Returns
        -------
        boolean

        '''
        if isinstance(other, CASTable) and self.params == other.params:
            return True
        return False

    def __copy__(self):
        '''
        Make a shallow copy of the CASTable object

        Examples
        --------
        >>> import copy
        >>> tbl = CASTable('my-table')
        >>> tbl2 = copy.copy(tbl)
        >>> tbl == tbl2
        True

        Returns
        -------
        CASTable object
           Shallow copy of `self`

        '''
        tbl = type(self)(**self.params)
        tbl._columns = self._columns
        tbl._sortby = self._sortby
        try:
            tbl.set_connection(self.get_connection())
        except SWATError:
            pass
        return tbl

    def __deepcopy__(self, memo):
        '''
        Make a deep copy of the CASTable obect

        Parameters
        ----------
        memo : any
           Storage for deepcopy mechanism

        Examples
        --------
        >>> import copy
        >>> tbl = CASTable('my-table')
        >>> tbl2 = copy.deepcopy(tbl)
        >>> tbl == tbl2
        True

        Returns
        -------
        CASTable object
           Deep copy of `self`

        '''
        tbl = type(self)(**copy.deepcopy(self.params))
        tbl._columns = list(self._columns)
        tbl._sortby = list(self._sortby)
        try:
            tbl.set_connection(self.get_connection())
        except SWATError:
            pass
        return tbl

    def __setattr__(self, name, value):
        '''
        Set attribute or parameter value

        When an attribute is set on a CASTable object it can end up
        in one of two locations.  If the name of the attribute given
        matches the name of a CAS table parameter, the attribute value
        is stored in the :attr:`CASTable.params` dictionary and used in calls
        to CAS actions.

        If the specified name does not match a CAS table parameter, the
        attribute is set on the :class:`CASTable` object as a standard Python
        attribute.

        Parameters
        ----------
        name : string
            Name of the attribute to set.
        value : any
            The value of the attribute to set.

        Examples
        --------
        >>> tbl = CASTable('my-table')
        >>> tbl.where = 'a < 2'
        >>> tbl.noattr = True

        >>> print(tbl.where)
        'a < 2'

        >>> print(tbl.params)
        {'name': 'my-table', 'where': 'a < 2'}

        >>> print(tbl.noattr)
        True

        '''
        # Alias these names to the longer versions
        if name.lower() == 'comppgm':
            name = 'computedvarsprogram'
        if name.lower() == 'compvars':
            name = 'computedvars'
        return super(CASTable, self).__setattr__(name.lower(), value)

    def __delattr__(self, name):
        '''
        Delete an attribute

        When an attribute is deleted from a CASTable object, it can be
        deleted from one of two areas.  If the name specified matches the
        name of a CAS table parameter, the key is deleted from the
        :attr:`CASTable.params` object dictionary which holds parameters used
        when the CASTable is used in a CAS action call.

        If the attribute name is not a valid CAS table parameter, the
        attribute is deleted from the CASTable object as a standard Python
        attribute.

        Parameters
        ----------
        name : string
            Name of the attribute to delete.

        Examples
        --------
        >>> tbl = CASTable('my-table')
        >>> tbl.where = 'a < 2'
        >>> tbl.noattr = True

        >>> print(tbl.where)
        'a < 2'

        >>> print(tbl.params)
        {'name': 'my-table', 'where': 'a < 2'}

        >>> print(tbl.noattr)
        True

        >>> del tbl.where
        >>> del tbl.noattr
        >>> print(tbl.params)
        {'name': 'my-table'}

        '''
        return super(CASTable, self).__delattr__(name.lower())

    def __getattr__(self, name):
        '''
        Get named parameter, CAS action, or table column

        When attributes are accessed on a CASTable object, they lookup
        goes through several levels.  First, if the attribute is a real
        Python attribute on the CASTable object, that value will get
        returned.

        Second, if the requested attribute is the name of a CAS table
        parameter and the parameter has been set, the value of that
        parameter will be returned.

        Third, if the CASTable object has a connection registered with
        it and the requested attribute name matches the name of a CAS action
        or CAS action set, that object will be returned.

        Finally, if the attribute value matches the name of a CAS table
        column, a CASColumn is returned.

        Parameters
        ----------
        name : string
            Name of the attribute to locate.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = CASTable('my-table')
        >>> tbl.where = 'a > 2'
        >>> tbl.noattr = True

        >>> print(tbl.params)
        {'name': 'my-table', 'where': 'a > 2'}

        >>> print(tbl.noattr)
        True

        >>> print(tbl.datacol)
        CASColumn('my-table', vars=['datacol'])

        >>> print(tbl.summary)
        ?.simple.Summary()

        Returns
        -------
        any

        '''
        # Short-circuit all protected / private attributes
        if re.match(r'^__[a-z_]+__$', name):
            raise AttributeError(name)

        if name.startswith('_repr_') or name.startswith('_ipython_'):
            raise AttributeError(name)

        if name in ['_typ']:
            raise AttributeError(name)

        origname = name
        name = name.lower()

        # Short circuit any table attributes
        if '.' not in name:
            try:
                # Alias these two shorter names to the proper name
                if name == 'compvars':
                    name = 'computedvars'
                elif name == 'comppgm':
                    name = 'computedvarsprogram'
                return super(CASTable, self).__getattr__(name)
            except AttributeError:
                pass

        conn = self.get_connection()

        if '.' not in name and not(re.match(r'^[A-Z]', origname)) and \
                conn.has_actionset(name):
            asinst = conn.get_actionset(name)
            asinst.default_params = {'__table__': self.copy()}
            return asinst

        if conn.has_action(name):
            actcls = conn.get_action_class(name)
            members = {'default_params': {'__table__': self.copy()}}
            actcls = type(actcls.__name__, (actcls,), members)

            if re.match(r'^[A-Z]', origname):
                return actcls

            return actcls()

        # See if it's a column name
        if name in [x.lower() for x in self._columns]:
            return self._to_column(origname)
        elif name in [x.lower() for x in self.get_param('computedvars', [])]:
            return self._to_column(origname)
        elif name in [x.lower() for x in self.get_param('groupby', [])]:
            return self._to_column(origname)
        elif not self._columns:
            try:
                tbl = self.copy()
                tbl._columns = [origname]
                colinfo = tbl._columninfo
            except (ValueError, SWATError):
                colinfo = None
            if colinfo is not None and len(colinfo):
                return self._to_column(origname)

        raise AttributeError(origname)

    def invoke(self, _name_, **kwargs):
        '''
        Invoke an action on the registered connection

        Parameters
        ----------
        _name_ : string
            Action name.
        **kwargs : any, optional
            Keyword arguments to the CAS action.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> conn.invoke('summary', table=CASTable('my-table'))
        >>> for resp in conn:
        ...     for k, v in result:
        ...         print(k, v)

        Returns
        -------
        CAS object
            The CAS object that the action was executed on

        '''
        return getattr(self, _name_).invoke(**kwargs)

    def retrieve(self, _name_, **kwargs):
        '''
        Invoke an action on the registered connection and retrieve results

        Parameters
        ----------
        _name_ : string
            Action name.
        **kwargs : any, optional
            Keyword arguments to the CAS action.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> out = conn.retrieve('summary', table=CASTable('my-table'))
        >>> print(out.Summary)

        Returns
        -------
        CASResults object

        '''
        return getattr(self, _name_).retrieve(**kwargs)

    def _retrieve(self, _name_, **kwargs):
        '''
        Same as retrieve, but marked as a UI call

        This is used for behind-the-scenes calls to CASTable.retrieve.

        Returns
        -------
        CASResults object

        '''
        out = self.retrieve(_name_, _apptag='UI', _messagelevel='error', **kwargs)
        if out.severity > 1:
            raise SWATError(out.status)
        return out

    def __str__(self):
        ''' Return string representation of the CASTable object '''
        parts = [
            repr(self.params['name']),
            dict2kwargs(self.to_params(), ignore=['name'], fmt='%s')
        ]

        columns = ''
        if self._columns:
            columns = '[%s]' % ', '.join(repr(x) for x in self._columns)
            if not isinstance(self, CASColumn):
                columns = '[%s]' % columns

        sort = ''
        if self._sortby:
            names = [x['name'] for x in self._sortby]
            if len(names) == 1:
                names = names.pop()

            order = [x.get('order', 'ASCENDING').upper() == 'ASCENDING'
                     for x in self._sortby]
            if set(order) == set([True]):
                order = ''
            elif set(order) == set([False]):
                order = ', ascending=False'
            else:
                order = ', ascending=%s' % repr(order)

            sort = '.sort_values(%s%s)' % (repr(names), order)

        return '%s(%s)%s%s' % (type(self).__name__,
                               ', '.join([x.strip() for x in parts if x.strip()]),
                               columns, sort)

    def __repr__(self):
        return str(self)

    def get_action_names(self):
        '''
        Return a list of available CAS actions

        If a CAS connection is registered with the CASTable object,
        this method returns the list of CAS actions names available
        to the connection.

        Returns
        -------
        list of strings

        '''
        return self.get_connection().get_action_names()

    def get_actionset_names(self):
        '''
        Return a list of available actionsets

        If a CAS connection is registered with the CASTable object,
        this method returns the list of CAS action set names available
        to the connection.

        Returns
        -------
        list of strings

        '''
        return self.get_connection().get_actionset_names()

    def get_inputs_param(self):
        ''' Return the column names for the inputs= action parameter '''
        return self._columns

    def get_fetch_params(self):
        ''' Return options to be used during the table.fetch action '''
        if self._sortby:
            return dict(sortby=self._sortby, sastypes=False)
        return dict(sastypes=False)

    def to_params(self):
        '''
        Return parameters of CASTable object

        Returns
        -------
        dict

        '''
        out = {}
        for key, value in six.iteritems(super(CASTable, self).to_params()):
            if key.lower() in ['where', 'replace', 'promote'] \
                    and isinstance(self.params[key], xdict.xadict):
                continue
            out[key] = value
        return out

    def to_table_params(self):
        '''
        Create a copy of the table parameters containing only input table parameters

        Examples
        --------
        >>> tbl = CASTable('my-table', where='a < 2', replace=True)
        >>> print(tbl.to_table_params())
        {'name': 'my-table', 'where': 'a < 2'}

        Returns
        -------
        dict
           Dictionary with only input table parameters

        '''
        if type(self).table_params:
            out = {}
            for key in self.params.keys():
                if key.lower() in ['where', 'replace', 'promote'] \
                        and isinstance(self.params[key], xdict.xadict):
                    continue
                if key.lower() in type(self).table_params:
                    out[key] = copy.deepcopy(self.params[key])
            return out

        # This can only happen if the table_params class variable
        # wasn't populated when the server connection was made,
        # which should *never* happen.
        out = copy.deepcopy(self.params)
        return out

    def to_table(self):
        '''
        Create a copy of the CASTable object with only input table paramaters

        Examples
        --------
        >>> tbl = CASTable('my-table', where='a < 2', replace=True)
        >>> print(tbl.to_table())
        CASTable('my-table', where='a < 2')

        Returns
        -------
        CASTable object

        '''
        kwargs = self.to_table_params()
        name = kwargs.pop('name')
        out = type(self)(name, **kwargs)
        try:
            out.set_connection(self.get_connection())
        except SWATError:
            pass
        return out

    def to_outtable_params(self):
        '''
        Create a copy of the CASTable parameters using only the output table parameters

        Examples
        --------
        >>> tbl = CASTable('my-table', where='a < 2', replace=True)
        >>> print(tbl.to_outtable_params())
        {'name': 'my-table', replace=True}

        Returns
        -------
        dict
           Dictionary with only output table parameters

        '''
        if type(self).outtable_params:
            out = {}
            for key in self.params.keys():
                if key.lower() in ['where', 'replace', 'promote'] \
                        and isinstance(self.params[key], xdict.xadict):
                    continue
                if key.lower() in type(self).outtable_params:
                    out[key] = copy.deepcopy(self.params[key])
            return out

        # This can only happen if the table_params class variable
        # wasn't populated when the server connection was made,
        # which should *never* happen.
        out = copy.deepcopy(self.params)
        return out

    def to_outtable(self):
        '''
        Create a copy of the CASTable object with only output table paramaters

        Examples
        --------
        >>> tbl = CASTable('my-table', where='a < 2', replace=True)
        >>> print(tbl.to_table())
        CASTable('my-table', replace=True)

        Returns
        -------
        CASTable object

        '''
        kwargs = self.to_outtable_params()
        name = kwargs.pop('name')
        out = type(self)(name, **kwargs)
        try:
            out.set_connection(self.get_connection())
        except SWATError:
            pass
        return out

    def to_table_name(self):
        '''
        Return the name of the table

        Examples
        --------
        >>> tbl = CASTable('my-table', where='a < 2', replace=True)
        >>> print(tbl.to_table_name())
        my-table

        Returns
        -------
        string
           CASTable name

        '''
        return self.params['name']

    def to_datastep_params(self):
        '''
        Create a data step table specification

        Returns
        -------
        :class:`CASTable`

        '''
        return _to_datastep_params(self)

    def to_input_datastep_params(self):
        '''
        Create an input data step table specification

        Returns
        -------
        :class:`CASTable`

        '''
        return _to_datastep_params(self, ignore=['replace', 'promote', 'copies'])

    #
    # Pandas DataFrame API
    #

    # Attributes and underlying data

    def _loadactionset(self, name):
        ''' Load action set if it hasn't been loaded already '''
        if not hasattr(self, name):
            self._retrieve('builtins.loadactionset', actionset=name)

    @getattr_safe_property
    def _columninfo(self):
        ''' Return columninfo dataframe '''
        return self._retrieve('table.columninfo')['ColumnInfo']

    @getattr_safe_property
    def _numrows(self):
        ''' Return number of rows in the table '''
        return int(self.copy(exclude='groupby')._retrieve('simple.numrows')['numrows'])

    def __len__(self):
        if self._pandas_enabled:
            return self._numrows
        raise AttributeError('__len__')

    # NOTE: Workaround to keep the DataFrame text renderer from trying
    #       to fetch all the values in the table.
    def __next__(self):
        return StopIteration

    # NOTE: Workaround to keep the DataFrame text renderer from trying
    #       to fetch all the values in the table.
    def next(self):
        ''' Return next item in the iteration '''
        return StopIteration

    def exists(self):
        ''' Return True if table exists in the server '''
        return self._retrieve('table.tableexists')['exists'] > 0

    @getattr_safe_property
    def last_modified_date(self):
        ''' Return the last modified date of the table in the server '''
        modtime = self._retrieve('table.tableinfo')['TableInfo']['ModTime'][0]
        return sas2python_datetime(modtime)

    @getattr_safe_property
    def last_accessed_date(self):
        ''' Return the last access date of the table in the server '''
        acctime = self._retrieve('table.tableinfo')['TableInfo']['AccessTime'][0]
        return sas2python_datetime(acctime)

    @getattr_safe_property
    def created_date(self):
        ''' Return the created date of the table in the server '''
        cretime = self._retrieve('table.tableinfo')['TableInfo']['CreateTime'][0]
        return sas2python_datetime(cretime)

    @getattr_safe_property
    def _numcolumns(self):
        ''' Return number of visible columns '''
        # Short circuit if we can
        varlist = self._columns
        if varlist:
            return len(varlist)

        # Call tableinfo
        tblinfo = self._retrieve('table.tableinfo')['TableInfo']
        computedvars = self.get_param('computedvars', [])
        if computedvars and not isinstance(computedvars, items_types):
            computedvars = [computedvars]
        return tblinfo.iloc[0]['Columns'] + len(computedvars)

    @getattr_safe_property
    def columns(self):
        ''' The visible columns in the table '''
        varlist = self._columns
        if varlist:
            return pd.Index(varlist)
        return pd.Index(self._columninfo['Column'].tolist())

    @getattr_safe_property
    def index(self):
        ''' The table index '''
        return

    def _intersect_columns(self, columns, inplace=False):
        '''
        Return the intersection of `columns` and `inputs`

        This is used to generate a new column list that contains the
        intersection of the names in `columns` with the names of the
        current table's inputs.

        Examples
        --------
        >>> tbl = CASTable('my-table', vars=['a', 'b', 'c'])
        >>> print(tbl._intersect_columns(['a', 'c', 'd']))
        ['a', 'c']

        Returns
        -------
        list
            If inplace == False
        None
            If inplace == True

        '''
        if columns is None:
            columns = []
        elif not isinstance(columns, items_types):
            columns = [columns]

        if not columns:
            if inplace:
                return
            return self._columns

        varlist = self._columns
        if not varlist:
            varlist = columns
        else:
            varlist = list(sorted(set(varlist) & set(columns), key=varlist.index))

        if inplace:
            self._columns = varlist
            return

        return varlist

    def as_matrix(self, columns=None, n=None):
        '''
        Represent CASTable as a Numpy array

        Parameters
        ----------
        columns : list of strings, optional
            The names of the columns to add to the matrix.
        n : int or long, optional
            The maximum number of rows to fetch.  If None, then the value
            in ``swat.options.dataset.max_rows_fetched`` is used.

        See Also
        --------
        :meth:`pandas.DataFrame.as_matrix`

        Returns
        -------
        :class:`numpy.array`

        '''
        if n is None:
            n = get_option('cas.dataset.max_rows_fetched')
            if self._numrows > n:
                warnings.warn(('Data downloads are limited to %d rows.  '
                               'To change this limit, set '
                               'swat.options.cas.dataset.max_rows_fetched '
                               'to the desired limit.') % n, RuntimeWarning)
        tbl = self.copy()
        tbl._intersect_columns(columns, inplace=True)
        return tbl._fetch(to=n).values

    @getattr_safe_property
    def dtypes(self):
        ''' Series of the data types in the table '''
        out = self._columninfo.copy()
        index = out['Column']
        index.name = None
        out = out['Type']
        out.index = index
        out.name = None
        return out

    @getattr_safe_property
    def ftypes(self):
        ''' Series of the ftypes (indication of sparse/dense and dtype) in the table '''
        out = self._columninfo.copy()
        index = out['Column']
        index.name = None
        out = out['Type'] + ':dense'
        out.index = index
        out.name = None
        return out

    def get(self, key, default=None):
        '''
        Get item from object for given key (ex: DataFrame column)

        Returns default value if not found.

        Parameters
        ----------
        key : object

        Returns
        -------
        value : same type as items contained in object

        '''
        try:
            return self[key]
        except (KeyError, ValueError, IndexError):
            return default

    def get_dtype_counts(self):
        ''' Retrieve the frequency of CAS table column data types '''
        return self.dtypes.value_counts().sort_index()

    def get_ftype_counts(self):
        ''' Retrieve the frequency of CAS table column data types '''
        return self.ftypes.value_counts().sort_index()

    def _get_dtypes(self, include=None, exclude=None):
        '''
        Return a list of columns selected by `include` and `exclude`

        Parameters
        ----------
        include : list-of-strings, optional
            List of data type names to include in result
        exclude : list-of-strings, optional
            List of data type names to exclude from result

        Notes
        -----
        In addition to data type names, the names 'number' and 'numeric'
        can be used to refer to all numeric types.  The name 'character'
        can be used to refer to all character types.

        Numerics can also be referred to by the names 'integer' and
        'floating' to refer to integers and floating point types,
        respectively.

        Returns
        -------
        list-of-strings

        '''

        if include is None:
            include = set()
        elif not isinstance(include, items_types):
            include = [include]
        include = set(include)

        if exclude is None:
            exclude = set()
        elif not isinstance(exclude, items_types):
            exclude = [exclude]
        exclude = set(exclude)

        out = self._columninfo.copy()
        names = out['Column'].tolist()
        dtypes = out['Type'].tolist()

        char_dtypes = set(['char', 'varchar', 'binary', 'varbinary'])
        num_dtypes = set(dtypes).difference(char_dtypes)
        integer_types = set(['int32', 'int64', 'date', 'time', 'datetime'])
        float_types = num_dtypes.difference(integer_types)

        if 'character' in include or 'O' in include or \
                object in include or 'all' in include:
            include.update(char_dtypes)
        if 'number' in include or 'numeric' in include or \
                np.number in include or 'all' in include:
            include.update(num_dtypes)
        if 'floating' in include or 'all' in include:
            include.update(float_types)
        if 'integer' in include or 'all' in include:
            include.update(integer_types)

        if 'character' in exclude or 'O' in exclude or \
                object in exclude or 'all' in exclude:
            exclude.update(char_dtypes)
        if 'number' in exclude or 'numeric' in exclude or \
                np.number in exclude or 'all' in exclude:
            exclude.update(num_dtypes)
        if 'floating' in exclude or 'all' in exclude:
            exclude.update(float_types)
        if 'integer' in exclude or 'all' in exclude:
            exclude.update(integer_types)

        varlist = set()

        if include:
            for name, dtype in zip(names, dtypes):
                if dtype in include:
                    varlist.add(name)
        else:
            varlist = set(names)

        if exclude:
            for name, dtype in zip(names, dtypes):
                if dtype in exclude:
                    varlist.discard(name)

        return [x for x in names if x in varlist]

    def select_dtypes(self, include=None, exclude=None, inplace=False):
        '''
        Return a subset CASTable including/excluding columns based on data type

        Parameters
        ----------
        include : list-of-strings, optional
            List of data type names to include in result
        exclude : list-of-strings, optional
            List of data type names to exclude from result
        inplace : boolean, optional
            If True, the table is modified in place

        Notes
        -----
        In addition to data type names, the names 'number' and 'numeric'
        can be used to refer to all numeric types.  The name 'character'
        can be used to refer to all character types.

        Numerics can also be referred to by the names 'integer' and
        'floating' to refer to integers and floating point types,
        respectively.

        Returns
        -------
        :class:`CASTable` object
            If inplace == False
        ``self``
            If inplace == True

        '''
        varlist = self._get_dtypes(include=include, exclude=exclude)

        if inplace:
            self._columns = varlist
            return

        tblcopy = self.copy()
        tblcopy._columns = varlist
        return tblcopy

    @getattr_safe_property
    def values(self):
        ''' Numpy representation of the table '''
        return self._fetch().values

    @getattr_safe_property
    def axes(self):
        ''' List of the row axis labels and column axis labels '''
        # TODO: Create an index proxy object
        return [[], self.columns]

    @getattr_safe_property
    def ndim(self):
        ''' Number of axes dimensions '''
        return 2

    @getattr_safe_property
    def size(self):
        ''' Number of elements in the table '''
        shape = self.shape
        return shape[0] * shape[1]

    @getattr_safe_property
    def shape(self):
        ''' Return a tuple representing the dimensionality of the table '''
        return self._numrows, self._numcolumns

    # Conversion

#   def astype(self, *args, **kwargs):
#       ''' Cast the table to the specified data type '''
#       raise NotImplementedError

#   def convert_objects(self, *args, **kwargs):
#       ''' Deprecated '''
#       raise NotImplementedError

    def copy(self, deep=True, exclude=None):
        '''
        Make a copy of the CASTable object

        Parameters
        ----------
        deep : boolean, optional
            If True, all lists and dict-type objects are deep copied
        exclude : list-of-strings, optional
            Parameters that should be excluded (top-level only).

        Examples
        --------
        >>> tbl = CASTable('my-table', where='a < 2')
        >>> tbl2 = tbl.copy()
        >>> print(tbl2)
        {'name': 'my-table', 'where': 'a < 2'}
        >>> print(tbl is tbl2)
        False

        Returns
        -------
        :class:`CASTable` object

        '''
        if deep:
            out = copy.deepcopy(self)
        else:
            out = copy.copy(self)
        if exclude:
            if not isinstance(exclude, items_types):
                exclude = [exclude]
            for item in exclude:
                out.params.pop(item, None)
        return out

#   def isnull(self):
#       # TODO: Should build where clause that creates a table of booleans
#       #       where missings are True and non-missings are False.
#       raise NotImplementedError

#   def notnull(self):
#       # TODO: Same as above but reverse boolean values
#       raise NotImplementedError

    # Indexing, iteration

    def head(self, n=5, columns=None, bygroup_as_index=True, casout=None):
        '''
        Retrieve first `n` rows

        Parameters
        ----------
        n : int or long, optional
            The number of rows to return.
        columns : list-of-strings, optional
            A subset of columns to return.
        bygroup_as_index : boolean
            When by_group_index is True, By groups are converted to an index if they exist

        Notes
        -----
        Since CAS tables can be distributed across a grid of computers,
        the order is not guaranteed.  If you do not apply a sort order
        using :meth:`sort_values` the results are not predictable.

        Returns
        -------
        :class:`SASDataFrame`

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_slice(n, columns=True, ascending=True, casout=casout)
        return self.slice(start=0, stop=n, columns=columns,
                          bygroup_as_index=bygroup_as_index)

    def tail(self, n=5, columns=None, bygroup_as_index=True, casout=None):
        '''
        Retrieve last `n` rows

        Parameters
        ----------
        n : int or long, optional
            The number of rows to return.
        columns : list-of-strings, optional
            A subset of columns to return.
        bygroup_as_index : boolean
            When by_group_index is True, By groups are converted to an index if they exist

        Notes
        -----
        Since CAS tables can be distributed across a grid of computers,
        the order is not guaranteed.  If you do not apply a sort order
        using :meth:`sort_values` the results are not predictable.

        Returns
        -------
        :class:`SASDataFrame`

        '''
        if self._use_casout_for_stat(casout):
            raise NotImplementedError('tail for casout is not implemented yet')
            return self._get_casout_slice(n, columns=True, ascending=False, casout=casout)
        return self.slice(start=-n, stop=-1, columns=columns,
                          bygroup_as_index=bygroup_as_index)

    def slice(self, start=0, stop=None, columns=None, bygroup_as_index=True, casout=None):
        '''
        Retrieve the specified rows

        Parameters
        ----------
        start : int or long, optional
            The index of the first row to return.
        stop : int or long, optional
            The index of the last row to return.  If not specified, all
            rows until the end are retrieved.
        columns : list-of-strings, optional
            A subset of columns to return.
        bygroup_as_index : boolean
            When by_group_index is True, By groups are converted to an index if they exist

        Notes
        -----
        Since CAS tables can be distributed across a grid of computers,
        the order is not guaranteed.  If you do not apply a sort order
        using :meth:`sort_values` the results are not predictable.

        Returns
        -------
        :class:`SASDataFrame`

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_slice(stop - start, columns=True, ascending=True,
                                          casout=casout, start=start)

        from ..dataframe import concat

        tbl = self

        if columns is not None:
            tbl = self.copy()
            tbl._columns = list(columns)

        groupvars = self.get_groupby_vars()
        if groupvars:
            groups = tbl.groupby(groupvars)
        else:
            groups = [(None, tbl)]

        out = []
        for value, group in groups:
            fstart = start
            fstop = stop

            if stop is None:
                fstop = start + 5

            if start < 0 or stop < 0:
                numrows = group._numrows
                if start < 0:
                    fstart = numrows + start
                if stop < 0:
                    fstop = numrows + stop + 1

            grpdata = group._fetch(from_=fstart + 1, to=fstop)
            grpcols = set(grpdata.columns)
            if groupvars:
                for name, val in zip(groupvars, value):
                    if name in grpcols:
                        continue
                    grpdata[name] = val
                if bygroup_as_index:
                    grpdata = grpdata.set_index(groupvars)
            out.append(grpdata)

        return concat(out)

    def nth(self, n, dropna=False, bygroup_as_index=True, casout=None):
        '''
        Return the nth row

        Parameters
        ----------
        n : int or list-of-ints
            The rows to select.

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_slice(n, columns=True, ascending=True,
                                          casout=casout, start=n)

        from ..dataframe import concat
        if not isinstance(n, items_types):
            n = [n]
        out = []
        for item in n:
            out.append(self.slice(item, item, bygroup_as_index=True))
        return concat(out).sort_index()

#
# TODO: Until CAS tables get index support (or we fake it locally) we
#       can't implement the following methods properly.  We might be able to use
#       set_index to store a column name that is treated as the index of
#       the table and use that for the following methods.
#

    @getattr_safe_property
    def at(self):
        # ''' Label-based scalar accessor '''
        raise NotImplementedError('The `at` attribute is not implemented, '
                                  'but the attribute is reserved.')
#       return self._at

    @getattr_safe_property
    def iat(self):
        # ''' Integer location scalar accessor '''
        raise NotImplementedError('The `iat` attribute is not implemented, '
                                  'but the attribute is reserved.')
#       return self._iat

    @getattr_safe_property
    def ix(self):
        ''' Label-based indexer with integer position fallback '''
        if isinstance(self, CASColumn):
            raise NotImplementedError('The `ix` attribute is not implemented, '
                                      'but the attribute is reserved.')
        return self._ix

    @getattr_safe_property
    def loc(self):
        ''' Label-based indexer '''
        if isinstance(self, CASColumn):
            raise NotImplementedError('The `loc` attribute is not implemented, '
                                      'but the attribute is reserved.')
        return self._loc

    @getattr_safe_property
    def iloc(self):
        ''' Integer-based indexer for selecting by position '''
        if isinstance(self, CASColumn):
            raise NotImplementedError('The `iloc` attribute is not implemented, '
                                      'but the attribute is reserved.')
        return self._iloc

    def xs(self, key, axis=0, level=None, copy=None, drop_level=True):
        '''
        Return a cross-section from the CASTable

        Parameters
        ----------
        key : string or int
            Label contained in the index.
        axis : int, optional
            Axis to retrieve from (0=row, 1=column).
        level : object, optional
            Not implemented.
        copy : boolean, optional
            Not implemented.
        drop_level : boolean, optional
            Not implemented.

        See Also
        --------
        :meth:`pandas.DataFrame.xs`

        Returns
        -------
        :class:`pandas.Series`
            For axis == 0 indexing
        :class:`CASColumn`
            For axis == 1 indexing

        '''
        if axis == 0:
            return self.loc[key]
        if axis == 1:
            return self._to_column(key)
        raise SWATError('Unsupported axis: %s' % axis)

#   def insert(self, *args, **kwargs):
#       raise NotImplementedError

    def __iter__(self):
        '''
        Iterate through all visible column names in ``self``

        Yields
        ------
        string

        '''
        for col in self.columns:
            yield col

    def iteritems(self):
        '''
        Iterate over column names and CASColumn objects

        Yields
        ------
        (string, :class:`CASColumn`)
            Two-element tuple containing a column name and a :class:`CASColumn` object

        '''
        for col in self.columns:
            yield (col, self._to_column(col))

    def _generic_iter(self, name, *args, **kwargs):
        '''
        Generic iterator for various iteration implementations

        Parameters
        ----------
        name : string
            The name of the iterator type.
        *args : one or more arguments
            Positional arguments to the iterator.
        **kwargs : keyword parameters
            Keyword arguments to the iterator.

        Yields
        ------
        iterator result

        '''
        kwargs = kwargs.copy()

        has_index = 'index' in kwargs
        index = kwargs.pop('index', True)

        chunksize = kwargs.pop('chunksize', None)
        if chunksize is None:
            chunksize = 200

        # Remove index, we apply it ourselves
        if has_index:
            kwargs['index'] = False

        iterrows = name == 'iterrows' and True or False

        start = 1
        stop = chunksize

        i = 0
        while True:
            out = self._fetch(from_=start, to=stop)

            if not len(out):
                break

            for item in getattr(out, name)(*args, **kwargs):
                # iterrows
                if iterrows:
                    yield (i, item[1])

                # itertuples with index
                elif index:
                    item = list(item)
                    item.insert(0, i)
                    yield tuple(item)

                # everything else
                else:
                    yield item

                i += 1

            start = stop + 1
            stop = start + chunksize

    def iterrows(self, chunksize=None):
        '''
        Iterate over the rows of a CAS table as (index, :class:`pandas.Series`) pairs

        Parameters
        ----------
        chunksize : int or long, optional
            The number of rows to retrieve in each fetch.

        See Also
        --------
        :meth:`iteritems`
        :meth:`itertuples`

        Returns
        -------
        iterator of (index, :class:`pandas.Series`) tuples

        '''
        return self._generic_iter('iterrows', chunksize=chunksize)

    def itertuples(self, index=True, chunksize=None):
        '''
        Iterate over rows as tuples

        Parameters
        ----------
        index : boolean, optional
            If True, return the index as the first item of the tuple.
        chunksize : int or long, optional
            The number of rows to retrieve in each fetch.

        See Also
        --------
        :meth:`iterrows`
        :meth:`iteritems`

        Returns
        -------
        iterator of row tuples

        '''
        return self._generic_iter('itertuples', index=index, chunksize=chunksize)

    def get_value(self, index, col, **kwargs):
        ''' Retrieve a single scalar value '''
        if isinstance(col, int_types):
            col = self.columns[col]
        tbl = self.copy()
        tbl._columns = [col]
        numrows = self._numrows
        if abs(index) >= numrows:
            raise IndexError('index %s is out of bounds for axis 0 with size %s' %
                             (index, numrows))
        if index < 0:
            index = index + numrows
        out = self._fetch(from_=index + 1, to=index + 1)
        return out.at[out.index.values[0], col]

    def lookup(self, row_labels, col_labels):
        ''' Retrieve values indicated by `row_labels`, `col_labels` positions '''
        data = []
        for row, col in zip(row_labels, col_labels):
            data.append(self.get_value(row, col))

        types = set([type(x) for x in data])
        out = None
        if len(types) == 1:
            try:
                out = np.ndarray(shape=(len(row_labels),), dtype=types.pop())
            except ValueError:
                pass

        if out is None:
            out = np.ndarray(shape=(len(row_labels),), dtype=object)

        out[:] = data

        return out

    def pop(self, colname):
        '''
        Remove a column from the CASTable and return it

        Parameters
        ----------
        colname : string
            Name of the column to remove.

        Returns
        -------
        :class:`CASColumn` object

        '''
        out = self.copy()._to_column(colname)
        del self[colname]
        return out

    def __delitem__(self, colname):
        '''
        Remove a column from the visible columns in a CASTable

        Notes
        -----
        This method does not actually remove a column from the table on
        the CAS server.  It merely removes it from the list of visible
        columns in `self`.

        Parameters
        ----------
        colname : string
           The name of the column to remove from the visible variable list.

        Returns
        -------
        None

        '''
        varlist = self.columns.tolist()
        lcolname = colname.lower()
        newvarlist = [x for x in varlist if x.lower() != lcolname]
        if len(newvarlist) == len(varlist):
            raise KeyError(colname)
        self._columns = newvarlist

    def datastep(self, code, casout=None, *args, **kwargs):
        '''
        Execute Data step code against the table

        Parameters
        ----------
        code : string
            The Data step code to execute.
        casout : dict, optional
            The name and caslib of the output table.
        *args : any, optional
            Arbitrary positional arguments to the datastep.runcode action.
        **kwargs : any, optional
            Arbitrary keyword arguments to the datastep.runcode action.

        Returns
        -------
        :class:`CASResults` object

        '''
        view = self.to_view(name=_gen_table_name())

        if casout is None or casout is True:
            casout = {'name': _gen_table_name()}
        elif isinstance(casout, (text_types, binary_types)):
            casout = {'name': casout}

        outdata = casout['name']

        if 'caslib' in casout:
            outdata = '%s(caslib=%s)' % (_quote(outdata), _quote(casout['caslib']))
        else:
            outdata = _quote(outdata)

        if isinstance(code, (list, tuple)):
            code = ';\n'.join(list(code)) + ';'

        code = 'data %s;\n   set %s;\n %s;\nrun;' % (outdata,
                                                     _quote(view.get_param('name')),
                                                     code)
        self._loadactionset('datastep')

        kwargs = kwargs.copy()
        kwargs['code'] = code
        out = self.get_connection().retrieve('datastep.runcode', *args, **kwargs)

        view._retrieve('table.droptable')

        try:
            return out['OutputCasTables']['casTable'][0]
        except (KeyError, IndexError):
            pass

        raise SWATError(out.status)

#   def isin(self, values, casout=None):
#       raise NotImplementedError

#   def where(self, cond, other=np.nan, inplace=False, axis=None, level=None,
#             try_cast=False, raise_on_error=True):
#       raise NotImplementedError

#   def mask(self, cond, other=np.nan, inplace=False, axis=None, level=None,
#            try_cast=False, raise_on_error=True):
#       raise NotImplementedError

#   def query(self, **kwargs):
#       raise NotImplementedError

    # Binary operator functions

    # TODO: These could probably be implemented using the datastep action.
    #       The 'other' variable could be a CASTable or a DataFrame.
    #       For DataFrames, the data would be uploaded as a CAS table first.

#   def add(self, other, **kwargs):
#       raise NotImplementedError

#   def sub(self, other, **kwargs):
#       raise NotImplementedError

#   def mul(self, other, **kwargs):
#       raise NotImplementedError

#   def div(self, other, **kwargs):
#       raise NotImplementedError

#   def truediv(self, other, **kwargs):
#       raise NotImplementedError

#   def floordiv(self, other, **kwargs):
#       raise NotImplementedError

#   def mod(self, other, **kwargs):
#       raise NotImplementedError

#   def pow(self, other, **kwargs):
#       raise NotImplementedError

#   def radd(self, other, **kwargs):
#       raise NotImplementedError

#   def rsub(self, other, **kwargs):
#       raise NotImplementedError

#   def rmul(self, other, **kwargs):
#       raise NotImplementedError

#   def rdiv(self, other, **kwargs):
#       raise NotImplementedError

#   def rtruediv(self, other, **kwargs):
#       raise NotImplementedError

#   def rfloordiv(self, other, **kwargs):
#       raise NotImplementedError

#   def rmod(self, other, **kwargs):
#       raise NotImplementedError

#   def rpow(self, other, **kwargs):
#       raise NotImplementedError

    # TODO: Comparisons should return a new CASTable with a WHERE expression
    #       that implements the filtering.

#   def lt(self, other, **kwargs):
#       raise NotImplementedError

#   def gt(self, other, **kwargs):
#       raise NotImplementedError

#   def le(self, other, **kwargs):
#       raise NotImplementedError

#   def ge(self, other, **kwargs):
#       raise NotImplementedError

#   def ne(self, other, **kwargs):
#       raise NotImplementedError

#   def eq(self, other, **kwargs):
#       raise NotImplementedError

#   def combine(self, other, **kwargs):
#       raise NotImplementedError

#   def combineAdd(self, other):
#       raise NotImplementedError

#   def combine_first(self, other):
#       raise NotImplementedError

#   def combineMult(self, other):
#       raise NotImplementedError

    # Function application, GroupBy

#   def apply(self, func, **kwargs):
#       raise NotImplementedError

#   def applymap(self, func):
#       raise NotImplementedError

#   def groupby(self, *args):
#       raise NotImplementedError

    # Computations / Descriptive Stats

    # TODO: Operations that don't reduce the data down to one scalar per
    #       column, return a new CASTable object.

    def _summary(self, **kwargs):
        ''' Get summary DataFrame '''
        bygroup_columns = 'raw'
        out = self._retrieve('simple.summary', **kwargs).get_tables('Summary')
        out = [x.reshape_bygroups(bygroup_columns=bygroup_columns,
                                  bygroup_as_index=True) for x in out]
        columns = []
        if out:
            columns = list(out[0]['Column'].values)
        out = pd.concat(out)
        out = out.set_index('Column', append=self.has_groupby_vars())
        out = out.rename(columns=dict((k, k.lower()) for k in out.columns))
        out = out.rename(columns=dict(n='count'))
        out = out.stack().unstack('Column')
        out.columns.name = None
        return out[columns]

    def _materialize(self, casout=None, inplace=False, prefix=None, suffix=None):
        '''
        Materialize a table and options into an in-memory table

        Parameters
        ----------
        casout : dict, optional
            The CAS output table definition
        inplace : bool, optional
            If True, the output table is overwritten if it exists
            NOTE: If `prefix` or `suffix` are used, this option is only
                  used to determine the table's base name.
        prefix : string, optional
            A string to use as the table name prefix
        suffix : string, optional
            A string to use as the table name suffix

        Returns
        -------
        :class:`CASTable`

        '''
        if casout is None:
            casout = {}

        if casout.get('caslib'):
            caslib = casout['caslib']
        elif inplace and 'caslib' in self.params:
            caslib = self.params['caslib']
        else:
            caslib = self.getsessopt('caslib').caslib

        if casout.get('name'):
            newname = casout['name']
        elif inplace:
            newname = self.params['name']
        else:
            newname = _gen_table_name()

        newname = '%s%s%s' % ((prefix or ''), newname, (suffix or ''))

        return self._retrieve('table.partition',
                              casout=dict(name=newname, caslib=caslib))['casTable']

    def abs(self):
        '''
        Return a new CASTable with absolute values of numerics

        Returns
        -------
        :class:`CASTable`

        '''
        tbl = self._materialize(prefix='_ABS')
        code = []
        for name, dtype in tbl.dtypes.iteritems():
            if dtype not in ['char', 'varchar', 'binary', 'varbinary',
                             'date', 'time', 'datetime']:
                code.append('    %s = ABS(%s);' % (_nlit(name), _nlit(name)))
        return tbl._apply_datastep(code, inplace=True)

    def _bool(self):
        '''
        Create boolean mask of table data

        Returns
        -------
        :class:`CASTable`

        '''
        cvars = []
        ccode = []
        groups = self.get_groupby_vars()
        for name, dtype in self.dtypes.iteritems():
            if name in groups:
                continue
            boolname = _nlit('%s__bool__' % name)
            cvars.append(boolname)
            if dtype in ['char', 'varchar', 'binary', 'varbinary']:
                ccode.append('%s = LENGTHN(%s) > 0' %
                             (_nlit(boolname), _nlit(name)))
            else:
                ccode.append('%s = choosen(MISSING(%s)+1, (%s ^= 0), .)' %
                             (_nlit(boolname), _nlit(name), _nlit(name)))
        out = self.copy(deep=True)
        out.append_computed_columns(cvars, ccode, inplace=True)
        return out[cvars]

    def all(self, axis=None, bool_only=None, skipna=True, level=None, **kwargs):
        '''
        Return True for each column with only elements that evaluate to true

        Parameters
        ----------
        axis : int, optional
            Not supported.
        bool_only : bool, optional
            Not supported.
        skipna : bool, optional
            When set to True, skips missing values. When False and the entire
            column is missing, the result will also be a missing.
        level : int, optional
            Not supported.

        Notes
        -----
        Since CAS can not distiguish between a missing charater value and
        a blank value, all blanks are interpreted as False values (not missing).

        Returns
        -------
        :class:`Series`
            When no by groups are specified
        :class:`CASTable`
            When by groups are specified

        '''
        tbl = self._bool()
        out = tbl.min()

        if isinstance(out, pd.Series):
            out.name = None
            out.index = list(self.columns)
            out.index.name = None
        else:
            groups = set(self.get_groupby_vars())
            out.columns = [x for x in self.columns if x not in groups]

        if skipna:
            out = out.fillna(1.0).astype(bool)
        elif isinstance(out, pd.Series):
            out = out.apply(lambda x: pd.isnull(x) and x or bool(x))
        else:
            out = out.applymap(lambda x: pd.isnull(x) and x or bool(x))

        return out

    def any(self, axis=None, bool_only=None, skipna=True, level=None, **kwargs):
        '''
        Return True for each column with at least one true element

        Parameters
        ----------
        axis : int, optional
            Not supported.
        bool_only : bool, optional
            Not supported.
        skipna : bool, optional
            When set to True, skips missing values. When False and the entire
            column is missing, the result will also be a missing.
        level : int, optional
            Not supported.

        Notes
        -----
        Since CAS can not distiguish between a missing charater value and
        a blank value, all blanks are interpreted as False values (not missing).

        Returns
        -------
        :class:`Series`
            When no by groups are specified
        :class:`CASTable`
            When by groups are specified

        '''
        tbl = self._bool()
        out = tbl.max()

        if isinstance(out, pd.Series):
            out.name = None
            out.index = list(self.columns)
            out.index.name = None
        else:
            groups = set(self.get_groupby_vars())
            out.columns = [x for x in self.columns if x not in groups]

        if skipna:
            out = out.fillna(0.0).astype(bool)
        elif isinstance(out, pd.Series):
            out = out.apply(lambda x: pd.isnull(x) and x or bool(x))
        else:
            out = out.applymap(lambda x: pd.isnull(x) and x or bool(x))

        return out

    def clip(self, lower=None, upper=None, axis=None):
        '''
        Clip values at thresholds

        Parameters
        ----------
        lower : float, optional
            The lowest value to allow
        upper : float, optional
            The highest value to allow
        axis : int, optional
            Unsupported

        Returns
        -------
        :class:`CASTable`

        '''
        if lower is not None and upper is not None:
            fmt = '    %%s = CHOOSEN(MISSING(%%s)+1, MIN(%s, MAX(%s, %%s)), .);' \
                  % (upper, lower)
        elif lower is not None:
            fmt = '    %%s = CHOOSEN(MISSING(%%s)+1, MAX(%s, %%s), .);' % lower
        elif upper is not None:
            fmt = '    %%s = CHOOSEN(MISSING(%%s)+1, MIN(%s, %%s), .);' % upper

        tbl = self._materialize(prefix='_CLIP')
        code = []
        for name, dtype in tbl.dtypes.iteritems():
            if dtype not in ['char', 'varchar', 'binary', 'varbinary',
                             'date', 'time', 'datetime']:
                code.append(fmt % (_nlit(name), _nlit(name), _nlit(name)))

        return tbl._apply_datastep(code, inplace=True)

    def clip_lower(self, threshold, axis=None):
        '''
        Clip values at lower threshold

        Parameters
        ----------
        threshold : float, optional
            The lowest value to allow
        axis : int, optional
            Unsupported

        Returns
        -------
        :class:`CASTable`

        '''
        return self.clip(lower=threshold, axis=axis)

    def clip_upper(self, threshold, axis=None):
        '''
        Clip values at upper threshold

        Parameters
        ----------
        threshold : float, optional
            The lowest value to allow
        axis : int, optional
            Unsupported

        Returns
        -------
        :class:`CASTable`

        '''
        return self.clip(upper=threshold, axis=axis)

    def corr(self, method=None, min_periods=None):
        '''
        Compute pairwise correlation of columns

        Parameters
        ----------
        method : string, optional
            Not implemented.
        min_periods : int or long, optional
            Not implemented.

        See Also
        --------
        :meth:`pandas.DataFrame.corr`

        Returns
        -------
        :class:`SASDataFrame`

        '''
        # TODO: Need groupby support once simple.correlation adds it
        tbl = self.select_dtypes(include='numeric')
        out = tbl._retrieve('simple.correlation', simple=False)['Correlation']
        out.set_index('Variable', inplace=True)
        out.index.name = None
        # Newer versions of the CAS server return extra columns
        # containing missing value information.
        return out[tbl.columns]

#   def corrwith(self, other, axis=None, drop=None):
#       raise NotImplementedError

    def count(self, axis=0, level=None, numeric_only=False):
        '''
        Return total number of non-missing values in each column

        Parameters
        ----------
        axis : int, optional
            Not impelmented.
        level : int or level name, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.

        See Also
        --------
        :meth:`pandas.DataFrame.count`

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        self._loadactionset('aggregation')

        if numeric_only:
            inputs = self._get_dtypes(include='numeric')
        else:
            inputs = self._columns or self.columns

        groups = self.get_groupby_vars()
        if groups:
            inputs = [x for x in inputs if x not in groups]
            out = self._retrieve('aggregation.aggregate',
                                 varspecs=[dict(names=list(inputs), agg='n')])
            out.pop('ByGroupInfo', None)
            out = pd.concat(list(out.values()))
            out = out.set_index('Column', append=True)['N']
            out = out.unstack(level=-1)
            out = out.astype('int64')
            if isinstance(out, pd.DataFrame):
                out.columns.name = None
            return out[inputs]

        out = pd.concat(list(self._retrieve('aggregation.aggregate',
                                            varspecs=[
                                                dict(names=list(inputs), agg='n')
                                            ]).values()))
        out = out.set_index('Column')['N']
        out = out.loc[inputs]
        out = out.astype('int64')
        if isinstance(out, pd.DataFrame):
            out.columns.name = None
        out.name = None
        out.index.name = None
        return out

#   def cov(self, min_periods=None):
#       raise NotImplementedError

#   def cummax(self, **kwargs):
#       raise NotImplementedError

#   def cummin(self, **kwargs):
#       raise NotImplementedError

#   def cumprod(self, **kwargs):
#       raise NotImplementedError

#   def cumsum(self, **kwargs):
#       raise NotImplementedError

    def _percentiles(self, percentiles=None, format_labels=True):
        '''
        Return the requested percentile values

        Parameters
        ----------
        percentiles : list-of-ints, optional
            The percentile values (0-100) to compute

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        if percentiles is None:
            percentiles = [25, 50, 75]

        self._loadactionset('percentile')

        inputs = list(self.columns)

        if not isinstance(percentiles, items_types):
            percentiles = [percentiles]
        else:
            percentiles = list(percentiles)

        bygroup_columns = 'raw'

        out = self._retrieve('percentile.percentile', inputs=inputs,
                             multitable=True, values=percentiles)
        out = [x.reshape_bygroups(bygroup_columns=bygroup_columns, bygroup_as_index=True)
               for x in out.get_tables('Percentile')]
        out = pd.concat(out)

        if format_labels:
            out['Pctl'] = out['Pctl'].apply('{:,.0f}%'.format)
        else:
            out['Pctl'] = out['Pctl'].div(100)

        out = out.set_index(['Pctl', 'Variable'], append=self.has_groupby_vars())['Value']
        out = out.unstack()

        if len(out.index.names) > 1:
            if pd_version >= (1, 0, 0):
                out = out.set_index(pd.MultiIndex(levels=out.index.levels,
                                                  codes=out.index.codes,
                                                  names=out.index.names[:-1] + [None]))
            else:
                out = out.set_index(pd.MultiIndex(levels=out.index.levels,
                                                  labels=out.index.labels,
                                                  names=out.index.names[:-1] + [None]))
        else:
            out.index.name = None

        out.columns.name = None

        return out

    def _topk_frequency(self, maxtie=0, skipna=True):
        '''
        Return the top value by frequency

        Parameters
        ----------
        maxtie : int or long, optional
            Maximum number of tied values to include.  Zero means no limit.

        Returns
        -------
        :class:`DataFrame`

        '''
        bygroup_columns = 'raw'

        inputs = list(self.columns)

        out = self._retrieve('simple.topk', topk=1, bottomk=0,
                             inputs=inputs, includemissing=not skipna, raw=True,
                             maxtie=maxtie, order='freq').get_tables('Topk')
        out = [x.reshape_bygroups(bygroup_columns=bygroup_columns,
                                  bygroup_as_index=True) for x in out]
        out = pd.concat(out)
        out = out.set_index('Column', append=self.has_groupby_vars())
        out = out.drop('Rank', axis=1)

        if 'NumVar' in out.columns and 'CharVar' in out.columns:
            out['NumVar'].fillna(out['CharVar'], inplace=True)
            out.drop('CharVar', axis=1, inplace=True)
            out.rename(columns=dict(NumVar='top'), inplace=True)

        if 'NumVar' in out.columns:
            out.rename(columns=dict(NumVar='top'), inplace=True)
        elif 'CharVar' in out.columns:
            out.rename(columns=dict(CharVar='top'), inplace=True)

        out.rename(columns=dict(Score='freq'), inplace=True)
        out = out.stack().unstack('Column')
        out.columns.name = None

        return out

    def _get_all_stats(self):
        '''
        Determine all possible statistics for summary action

        Returns
        -------
        list-of-strings
            The names of the supported statistics

        '''
        categories = ['count', 'unique', 'top', 'freq', 'mean', 'std', 'min'] + \
                     ['%d%%' % x for x in range(101)] + \
                     ['max', 'nmiss', 'sum', 'stderr', 'var', 'uss'] + \
                     ['cv', 'tvalue', 'probt', 'css']

        labels = ['count', 'unique', 'mean', 'std', 'min', 'pct'] + \
                 ['max', 'nmiss', 'sum', 'stderr', 'var', 'uss'] + \
                 ['cv', 'tvalue', 'probt', 'css']

        for param in self._retrieve('builtins.reflect',
                                    action='simple.summary')[0]['actions'][0]['params']:
            if param['name'].lower() == 'subset':
                allowed_values = [x.lower() for x in param['allowedValues']
                                  if x.lower() not in ['n', 't', 'tstat']]

        for item in allowed_values:
            if item not in labels:
                labels.append(item)
            if item not in categories:
                categories.append(item)

        return categories, labels

    def describe(self, percentiles=None, include=None, exclude=None, stats=None):
        '''
        Get descriptive statistics

        Parameters
        ----------
        percentiles : list-of-floats, optional
            The percentiles to include in the output.  The values should be
            in the interval [0, 1].  By default, ``percentiles`` is [0.25, 0.5, 0.75],
            returning the 25th, 50th, and 75th percentiles.

        include, exclude : list or 'all' or None (default), optional
            Specify the data types to include in the result.
                * If both are None, The result will include only numeric columns, or
                  if no numeric columns are present, all columns are included.
                * A list of dtypes or strings.  To select all numerics, use the
                  value 'number' or 'numeric'.  For all character columns, use
                  the value 'character'.
                * If the value is 'all', all columns will be used.

        stats : list-of-strings or 'all' or None (default), optional
            The statistics to include in the output.  By default, numeric
            columns return `count`, `std`, `min`, `pct`, `max`,
            where `pct` is the collection of percentiles specified in the
            ``percentiles=`` argument.  Character statistics include `count`,
            `unique`, `top`, and `freq`.  In addition, the following can be
            specified, `nmiss`, `sum`, `stderr`, `var`, `uss`, `cv`, `tvalue`,
            `probt`, `css`, `kurtosis`, and `skewness`.  If `all` is
            specified, all relevant statistics will be returned.

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        categories = None
        numrows = self._numrows

        # Auto-specify all numeric or all character
        if include is None and exclude is None:
            include = ['number']
            tbl = self.select_dtypes(include=include)
            if not tbl._columns:
                tbl = self.select_dtypes(include=['character'])

        # include/exclude was specified by the user
        else:
            tbl = self.select_dtypes(include=include, exclude=exclude)

        # Short circuit if there are no rows
        if not numrows:
            return pd.DataFrame([[0] * len(tbl.columns),
                                 [0] * len(tbl.columns)],
                                index=['count', 'unique'],
                                columns=tbl.columns)

        # Get percentiles
        if percentiles is not None:
            if not isinstance(percentiles, items_types):
                percentiles = [percentiles]
            else:
                percentiles = list(percentiles)
            for i, pct in enumerate(percentiles):
                percentiles[i] = max(min(pct * 100, 100), 0)

        if not percentiles:
            percentiles = [25, 50, 75]

        if 50 not in percentiles:
            percentiles.append(50)

        percentiles = _get_unique(sorted(percentiles))

        columns = tbl.columns
        dtypes = tbl.dtypes
        char_dtypes = set(['char', 'varchar', 'binary', 'varbinary'])

        # See if we need to do numeric summarization
        has_numeric = set(dtypes).difference(char_dtypes) and True or False
        has_character = set(dtypes).intersection(char_dtypes) and True or False

        # Get top value and frequency
        topk_freq = None
        if stats is None or stats == 'all' or 'freq' in stats or 'top' in stats:
            topk_freq = tbl._topk_frequency(skipna=True)

        # Get unique value counts
        topk_val = None
        if stats is None or stats == 'all' or 'unique' in stats \
                or 'max' in stats or 'min' in stats:
            topk_val = tbl._topk_values(leave_index=True)

        def _expand_items(into, key, items):
            ''' Expand a single element with a collection '''
            if not isinstance(items, items_types):
                items = [items]
            out = []
            for elem in into:
                if elem == key:
                    for item in items:
                        out.append(item)
                else:
                    out.append(elem)
            return out

        if has_numeric:
            pct_labels = ['%d%%' % x for x in percentiles]

            if stats is None:
                labels = ['count', 'mean', 'std', 'min', 'pct', 'max']

                if has_character:
                    labels = _expand_items(labels, 'count',
                                           ['count', 'unique', 'top', 'freq'])

            elif stats == 'all':
                categories, labels = self._get_all_stats()
                if has_character:
                    labels = _expand_items(labels, 'unique',
                                           ['unique', 'top', 'freq'])

            else:
                labels = stats

            if 'pct' in labels:
                labels = _expand_items(labels, 'pct', pct_labels)

            # Create table with only numeric columns
            numtbl = tbl.select_dtypes(include=['numeric'])

            # Get percentiles
            pct = numtbl._percentiles(percentiles=percentiles)

            # Get remaining summary values
            summ = numtbl._summary()
            if len(summ.index.names) > 1:
                summ.drop(['min', 'max'], level=-1, inplace=True)
            else:
                summ.drop(['min', 'max'], inplace=True)

            out = pd.concat((x for x in [topk_val, pct, summ, topk_freq]
                             if x is not None), **concat_sort)

        else:
            if stats is None:
                labels = ['count', 'unique', 'top', 'freq']
            elif stats == 'all':
                labels = ['count', 'unique', 'top', 'freq', 'min', 'max']
            else:
                labels = stats
            out = pd.concat((x for x in [topk_freq, topk_val] if x is not None),
                            **concat_sort)

        groups = self.get_groupby_vars()
        idx = tuple([slice(None) for x in groups] + [labels])
        columns = [x for x in columns if x not in groups]

        out = out[columns]

        # Fill in counts using `count` / `nmiss` method if possible
        if has_character:
            nmiss = count = None
            if 'nmiss' in labels:
                nmiss = tbl.nmiss()
                if isinstance(nmiss, pd.Series):
                    nmiss = nmiss.to_frame().T
                elif not isinstance(count, pd.DataFrame):
                    nmiss = pd.DataFrame([[nmiss]], columns=columns)
                nmiss['__stat_label__'] = 'nmiss'
                nmiss = nmiss.set_index('__stat_label__', append=bool(groups))
                if pd_version >= (0, 16, 1):
                    out = out.drop(['nmiss'], level=groups and -1 or None,
                                   errors='ignore')
                elif 'nmiss' in out.index:
                    out = out.drop(['nmiss'], level=groups and -1 or None)
            if 'count' in labels:
                count = tbl.count()
                if isinstance(count, pd.Series):
                    count = count.to_frame().T
                elif not isinstance(count, pd.DataFrame):
                    count = pd.DataFrame([[count]], columns=columns)
                count['__stat_label__'] = 'count'
                count = count.set_index('__stat_label__', append=bool(groups))
                if pd_version >= (0, 16, 1):
                    out = out.drop(['count'], level=groups and -1 or None,
                                   errors='ignore')
                elif 'count' in out.index:
                    out = out.drop(['count'], level=groups and -1 or None)
            out = pd.concat([out] + [x for x in [nmiss, count] if x is not None])

        if not groups:
            return out.loc[idx[0], columns]

        out.sort_index(inplace=True)

        out = out.loc[idx, columns]

        if not categories:
            categories, labels = self._get_all_stats()

        # This is done so that the row labels will come out in category-sorted order.
        tmpname = str(uuid.uuid4())
        out.index.names = groups + [tmpname]
        out.reset_index(inplace=True)
        if pd_version >= (0, 21, 0):
            from pandas.api.types import CategoricalDtype
            out[tmpname] = out[tmpname].astype(CategoricalDtype(
                categories=categories, ordered=True))
        else:
            out[tmpname] = out[tmpname].astype('category',
                                               categories=categories,
                                               ordered=True)
        out.sort_values(groups + [tmpname], inplace=True)
        out.set_index(groups + [tmpname], inplace=True)
        out.index.names = groups + [None]

        return out

#   def diff(self, periods=1, axis=0):
#       raise NotImplementedError

    def eval(self, expr, inplace=True, kwargs=None):
        '''
        Evaluate a CAS table expression

        Parameters
        ----------
        expr : string
            The expression to evaluate
        inplace : bool, optional
            If the expression contains an assignment and inplace=True,
            add the column to the existing table.
        kwargs : dict, optional
            Not supported

        Returns
        -------
        :class:`CASColumn`
            If `expr` does not contain an assignment
        None
            If inplace=True and `expr` contains an assignment
        :class:`CASTable`
            If inplace=False and `expr` contains an assignment

        '''
        col = self.copy()

        # Check for assignment
        if re.match(r'^\s*\w+\s*=', expr):
            colname = re.match(r'^\s*(\w+)\s*', expr).group(1)
            expr = re.sub(r'^\s*\w+\s*=s*', r'', expr)
            insert = True
        else:
            colname = '_eval_%s_' % self.get_connection()._gen_id()
            insert = False

        col.append_computed_columns(colname, '%s = %s; ' % (_nlit(colname),
                                                            _escape_string(expr)))

        col = col._to_column(colname)

        # Insert or return
        if insert:
            if inplace:
                self[colname] = col
            else:
                newtbl = self.copy()
                newtbl[colname] = col
                return newtbl
        else:
            return col

#   def mad(self, *args, **kwargs):
#       raise NotImplementedError

    def _get_summary_stat(self, name):
        '''
        Run simple.summary and get the given statistic

        Parameters
        ----------
        name : string
            The name of the simple.summary column.

        Returns
        -------
        :class:`pandas.DataFrame`
            for multi-index output on CASTable
        :class:`pandas.Series`
            for single index output on CASTable, or multi-index output on CASColumn
        scalar
            for single index output on CASColumn

        '''
        name = name.lower()
        out = self._summary()
        if len(out.index.names) > 1:
            return out.xs(name, level=-1)
        out = out.xs(name)
        out.index.name = None
        out.name = None
        return out

    def _topk_values(self, stats=None, axis=None, skipna=True, level=None,
                     numeric_only=False, leave_index=False, **kwargs):
        '''
        Compute min / max / unique value(s)

        Parameters
        ----------
        stats : string or list-of-strings, optional
            'unique', 'min', 'max' or a list of any combination.

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        if stats is None:
            stats = ['unique', 'min', 'max']

        if numeric_only:
            inputs = self._get_dtypes(include='numeric')
        else:
            inputs = list(self.columns)

        if not isinstance(stats, items_types):
            stats = [stats]
        else:
            stats = list(stats)

        out = self._retrieve('simple.topk', order='value', includemissing=not skipna,
                             inputs=inputs, raw=True, topk=1, bottomk=1, **kwargs)

        bygroup_columns = 'raw'

        groups = self.get_groupby_vars()
        groupset = set(groups)
        columns = [x for x in inputs if x not in groupset]

        # Minimum / Maximum
        minmax = None
        if 'min' in stats or 'max' in stats:
            minmax = [x.reshape_bygroups(bygroup_columns=bygroup_columns,
                                         bygroup_as_index=False)
                      for x in out.get_tables('Topk')]
            minmax = pd.concat(minmax)
            minmax.loc[:, 'stat'] = ['max', 'min'] * int(len(minmax) / 2)
            if 'NumVar' in minmax.columns and 'CharVar' in minmax.columns:
                minmax['NumVar'].fillna(minmax['CharVar'], inplace=True)
                minmax.rename(columns=dict(NumVar='value', Column='column'),
                              inplace=True)
            elif 'NumVar' in minmax.columns:
                minmax.rename(columns=dict(NumVar='value', Column='column'),
                              inplace=True)
            else:
                minmax.rename(columns=dict(CharVar='value', Column='column'),
                              inplace=True)
            minmax = minmax.reindex(columns=groups + ['stat', 'column', 'value'])
            if skipna:
                minmax.dropna(inplace=True)
            if 'min' not in stats:
                minmax = minmax.set_index('stat').drop('min').reset_index()
            if 'max' not in stats:
                minmax = minmax.set_index('stat').drop('max').reset_index()
            minmax.set_index(groups + ['stat', 'column'], inplace=True)
            if groups:
                minmax.drop(groups, level=-1, inplace=True, errors='ignore')
            minmax = minmax.unstack()
            minmax.index.name = None
            minmax.columns.names = [None] * len(minmax.columns.names)
            minmax.columns = minmax.columns.droplevel()
            minmax = minmax.reindex(columns=columns)

        # Unique
        unique = None
        if 'unique' in stats:
            unique = [x.reshape_bygroups(bygroup_columns=bygroup_columns,
                                         bygroup_as_index=False)
                      for x in out.get_tables('TopkMisc')]
            unique = pd.concat(unique)
            unique.loc[:, 'unique'] = 'unique'
            unique.rename(columns=dict(N='value', Column='column'), inplace=True)
            unique = unique.reindex(columns=groups + ['unique', 'column', 'value'])
            if skipna:
                unique.dropna(inplace=True)
            unique.set_index(groups + ['unique', 'column'], inplace=True)
            if groups:
                unique.drop(groups, level=-1, inplace=True, errors='ignore')
            unique = unique.unstack()
            unique.index.name = None
            unique.columns.names = [None] * len(unique.columns.names)
            unique.columns = unique.columns.droplevel()
            unique = unique.reindex(columns=columns)

        out = pd.concat(x for x in [unique, minmax] if x is not None)
        out = out.sort_index(ascending=([True] * len(groups)) + [False])

        if len(stats) > 1 or leave_index:
            return out

        if len(out.index.names) > 1:
            return out.xs(stats[0], level=-1)

        return out.loc[stats[0]]

    def _get_casout_stat(self, stat, axis=None, skipna=True, level=None,
                         numeric_only=False, percentile_values=None,
                         casout=None, **kwargs):
        '''
        Get the requested statistic in a pandas-like CAS table

        Parameters
        ----------
        stat : string
            The name of the statistic to compute
        axis : int, optional
            Unsupported
        skipna : bool, optional
            If True, missing values are dropped
        level : int, optional
            Unsupported
        numeric_only : bool, optional
            If True, just the numeric variables will be used
        percentile_values : list-of-floats, optional
            The list of percentiles to compute

        Returns
        -------
        :class:`CASTable`

        '''
        if casout:
            if isinstance(casout, CASTable):
                casout = casout.to_outtable_params()
            elif isinstance(casout, dict):
                casout = CASTable(**casout).to_outtable_params()
        else:
            casout = _gen_table_name()

        groups = set(self.get_groupby_vars())

        if numeric_only:
            inputs = self._get_dtypes(include='numeric')
        else:
            inputs = self.columns

        inputs = [x for x in inputs if x not in groups]

        if stat == 'min':
            out = self._retrieve('simple.topk', order='value', includemissing=not skipna,
                                 inputs=inputs, raw=True, topk=0, bottomk=1,
                                 casout=casout, **kwargs)
            return self._normalize_topk_casout(out['OutputCasTables']['casTable'][0])

        elif stat == 'max':
            out = self._retrieve('simple.topk', order='value', includemissing=not skipna,
                                 inputs=inputs, raw=True, topk=1, bottomk=0,
                                 casout=casout, **kwargs)
            return self._normalize_topk_casout(out['OutputCasTables']['casTable'][0])

        # NOTE: Only works with a single column
        elif stat == 'unique':
            out = self._retrieve('simple.freq', includemissing=not skipna,
                                 inputs=[inputs[0]], casout=casout, **kwargs)
            return self._normalize_freq_casout(out['OutputCasTables']['casTable'][0],
                                               column=inputs[0], stat='value')

        # NOTE: Only works with a single column
        elif stat == 'nunique':
            out = self._retrieve('simple.distinct', includemissing=not skipna,
                                 inputs=[inputs[0]], casout=casout, **kwargs)
            return self._normalize_distinct_casout(out['OutputCasTables']['casTable'][0],
                                                   skipna=skipna)

        # NOTE: Only works with a single column
        elif stat == 'n':
            out = self._retrieve('simple.freq', includemissing=not skipna,
                                 inputs=[inputs[0]], casout=casout, **kwargs)
            return self._normalize_freq_casout(out['OutputCasTables']['casTable'][0],
                                               column=inputs[0], skipna=skipna)

        elif stat == 'nmiss':
            out = self._retrieve('simple.distinct',
                                 inputs=[inputs[0]], casout=casout, **kwargs)
            return self._normalize_distinct_casout(out['OutputCasTables']['casTable'][0],
                                                   column='_NMiss_', skipna=False)

        elif stat in ['median', 'percentile']:
            num_cols = self._get_dtypes(include='numeric')
            inputs = [x for x in inputs if x in num_cols]

            if stat == 'median':
                percentile_values = [50]
            if not isinstance(percentile_values, (list, tuple, set)):
                percentile_values = [percentile_values]

            out = self._retrieve('percentile.percentile',  # includemissing=not skipna,
                                 inputs=inputs, values=percentile_values,
                                 casout=casout, **kwargs)

            return self._normalize_percentile_casout(
                out['OutputCasTables']['casTable'][0],
                single=(stat == 'median' or len(percentile_values) == 1))

        else:
            summ_stats = ['css', 'cv', 'kurtosis', 'mean',
                          'probt', 'skewness', 'std', 'stderr', 'sum',
                          't', 'tstat', 'uss', 'var']

            if stat not in summ_stats:
                raise ValueError('%s is not a valid statistic' % stat)

            num_cols = self._get_dtypes(include='numeric')
            inputs = [x for x in inputs if x in num_cols]
            out = self._retrieve('simple.summary',  # includemissing=not skipna,
                                 inputs=inputs, casout=casout, **kwargs)

            return self._normalize_summary_casout(
                out['OutputCasTables']['casTable'][0], stat)

    def _normalize_bygroups(self, drop=None, rename=None):
        '''
        Return bygroups names as well as drop / rename statements for bygroup options

        Parameters
        ----------
        drop : list, optional
            List of additional variables to drop
        rename : list, optional
            List of additional variables to rename

        Returns
        -------
        (column-list, bygroups-list, raw-bygroups-list, fmt-groups-list,
         keep-stmt, drop-stmt, rename-stmt)

        '''
        drop = list(drop or [])
        rename = list(rename or [])
        keep = []
        cols = []
        retain = []

        groups = []
        raw_groups = []
        fmt_groups = []
        for item in self.get_groupby_vars():
            raw_groups.append(item)
            groups.append(item)
            fmt_groups.append('%s_f' % item)
            groups.append('%s_f' % item)
            retain.append(item)
            retain.append('%s_f' % item)

        bygroup_columns = get_option('cas.dataset.bygroup_columns')
        bygroup_formatted_suffix = get_option('cas.dataset.bygroup_formatted_suffix')

        if bygroup_columns == 'none':
            drop.extend(groups)

        elif bygroup_columns == 'raw':
            drop.extend(fmt_groups)
            keep.extend(raw_groups)
            cols.extend(raw_groups)

        elif bygroup_columns == 'formatted':
            drop.extend(raw_groups)
            for i, item in enumerate(fmt_groups):
                newname = _nlit(re.sub(r'_f$', r'', fmt_groups[i]))
                rename.append('%s=%s' % (_nlit(fmt_groups[i]), newname))
                keep.append(newname)
                cols.append(newname)

        elif bygroup_columns == 'both':
            if bygroup_formatted_suffix != '_f':
                for i, item in enumerate(fmt_groups):
                    newname = _nlit(re.sub(r'_f$', bygroup_formatted_suffix,
                                           fmt_groups[i]))
                    rename.append('%s=%s' % (_nlit(fmt_groups[i]), newname))
                    keep.append(raw_groups[i])
                    keep.append(newname)
                    cols.append(newname)
            else:
                keep.extend(groups)
                cols.append(groups)

        for col in list(self.columns):
            if col not in groups:
                keep.append(col)
                cols.append(col)
            if col not in retain:
                retain.append(col)

        keep = 'keep %s;' % ' '.join(_nlit(x) for x in keep)
        drop = 'drop %s;' % ' '.join(_nlit(x) for x in drop)
        retain = 'retain %s;' % ' '.join(_nlit(x) for x in retain)
        rename = rename and ('rename %s;' % ' '.join(rename)) or ''

        return cols, groups, raw_groups, fmt_groups, retain, keep, drop, rename

    def _normalize_percentile_casout(self, table, single=False):
        '''
        Normalize percentile output table to pandas-like structure

        Parameters
        ----------
        table : CASTable
            Percentile output table
        single : bool, optional
            If True, this is a single quantile computation and the quantile column
            is dropped

        Returns
        -------
        :class:'CASTable'

        '''
        self._loadactionset('transpose')

        if single:
            out = self._normalize_bygroups(drop=['_NAME_', '_Pctl_'])
        else:
            out = self._normalize_bygroups(drop=['_NAME_'], rename=['_Pctl_=quantile'])

        cols, groups, raw_groups, fmt_groups, retain, keep, drop, rename = out

        dstbl = table.to_datastep_params()

        table.params['replace'] = True

        table.groupby(groups + ['_Pctl_'])._retrieve('transpose.transpose', id='_Column_',
                                                     transpose=['_Value_'],
                                                     casout=table)

        dsout = self._retrieve('datastep.runcode', code=r'''
            data %s;
                %s
                set %s;
                %s
                %s
            run;''' % (dstbl, retain, dstbl, drop, rename))

        tbl = dsout['OutputCasTables']['casTable'][0]

        return tbl

    def _normalize_summary_casout(self, table, stat):
        '''
        Normalize summary output table to pandas-like structure

        Parameters
        ----------
        table : CASTable
            Summary output table
        stat : string
            The name of the output statistic

        Returns
        -------
        :class:`CASTable`

        '''
        self._loadactionset('transpose')

        stat = stat.lower()
        if stat in ['t', 'tstat']:
            stat = 'T'
        elif stat == 'probt':
            stat = 'prt'
        else:
            stat = stat.title()

        out = self._normalize_bygroups(drop=['_NAME_'])
        cols, groups, raw_groups, fmt_groups, retain, keep, drop, rename = out

        tbl = _gen_table_name()

        table.groupby(groups)._retrieve('transpose.transpose', id='_Column_',
                                        transpose=['_%s_' % stat],
                                        casout=dict(name=tbl))

        dsout = self._retrieve('datastep.runcode', code=r'''
            data %s;
                %s
                set %s;
                %s
                %s
            run;''' % (_quote(tbl), retain, _quote(tbl), drop, rename))

        tbl = dsout['OutputCasTables']['casTable'][0]

        return tbl

    def _normalize_freq_casout(self, table, column, stat='freq', skipna=True):
        '''
        Normalize freq output table to pandas-like structure

        Parameters
        ----------
        table : CASTable
            Distinct output table
        column : string
            The name of the column
        stat : string, optional
            Specifies the statistic: 'value' or 'freq'

        Returns
        -------
        :class:`CASTable`

        '''
        typecol = '_Numvar_'
        if '_Charvar_' in table.columns:
            typecol = '_Charvar_'

        if stat == 'value':
            out = self._normalize_bygroups(drop=['_Column_', '_Level_',
                                                 '_Frequency_', '_Fmtvar_'],
                                           rename=['%s=%s' % (typecol, column)])
        else:
            out = self._normalize_bygroups(drop=['_Column_', '_Level_',
                                                 '_Fmtvar_'],
                                           rename=['%s=%s' % (typecol, column)])

        cols, groups, raw_groups, fmt_groups, retain, keep, drop, rename = out

        dsout = self._retrieve('datastep.runcode', code=r'''
            data %s;
                %s
                set %s;
                %s
                %s
            run;''' % (table.to_datastep_params(), retain,
                       table.to_input_datastep_params(), drop, rename))

        dsout = dsout['OutputCasTables']['casTable'][0]

        if skipna:
            dsout = self._retrieve('datastep.runcode', code=r'''
                data %s;
                    set %s;
                    if cmiss(of _all_) then delete;
                run;''' % (dsout.to_datastep_params(),
                           dsout.to_input_datastep_params()))

            dsout = dsout['OutputCasTables']['casTable'][0]

        return dsout

    def _normalize_distinct_casout(self, table, column='_NDis_', skipna=True):
        '''
        Normalize distinct output table to pandas-like structure

        Parameters
        ----------
        table : CASTable
            Distinct output table

        Returns
        -------
        :class:`CASTable`

        '''
        self._loadactionset('transpose')

        out = self._normalize_bygroups(drop=['_NAME_'])
        cols, groups, raw_groups, fmt_groups, retain, keep, drop, rename = out

        tbl = _gen_table_name()

        table.groupby(groups)._retrieve('transpose.transpose', id='_Column_',
                                        transpose=[column],
                                        casout=dict(name=tbl))

        dsout = self._retrieve('datastep.runcode', code=r'''
            data %s;
                set %s;
                %s
                %s
            run;''' % (_quote(tbl), _quote(tbl), drop, rename))

        dsout = dsout['OutputCasTables']['casTable'][0]

        if skipna:
            dsout.params['replace'] = True

            dsout = self.retrieve('datastep.runcode', code=r'''
                data %s;
                    set %s;
                    if cmiss(of _all_) then delete;
                run;''' % (dsout.to_datastep_params(),
                           dsout.to_input_datastep_params()))

            dsout = dsout['OutputCasTables']['casTable'][0]

        return dsout

    def _normalize_topk_casout(self, table):
        '''
        Normalize topk output table to pandas-like structure

        Parameters
        ----------
        table : CASTable
            Topk output table

        Returns
        -------
        :class:`CASTable`

        '''
        self._loadactionset('transpose')

        out = self._normalize_bygroups(drop=['_NAME_'])
        cols, groups, raw_groups, fmt_groups, retain, keep, drop, rename = out

        char_tbl = None
        num_tbl = None
        num_cols = self._get_dtypes(include='numeric')
        nums = ' '.join(_nlit(x) for x in num_cols)
        char_cols = self._get_dtypes(include='character')
        chars = ' '.join(_nlit(x) for x in char_cols)

        if '_Charvar_' in table.columns:
            char_tbl = _gen_table_name()
            table.groupby(groups)._retrieve('transpose.transpose', id='_Column_',
                                            transpose=['_Charvar_'],
                                            casout=dict(name=char_tbl))

        if '_Numvar_' in table.columns:
            num_tbl = _gen_table_name()
            table.groupby(groups)._retrieve('transpose.transpose', id='_Column_',
                                            transpose=['_Numvar_'],
                                            casout=dict(name=num_tbl))

        ds_groups = ' '.join([_nlit(x) for x in groups])

        out_tbl = _gen_table_name()

        if char_tbl and num_tbl:
            dsout = self._retrieve('datastep.runcode', code=r'''
                data %s;
                    merge %s(keep=%s %s in=__numeric)
                          %s(keep=%s %s in=__character);
                    by %s;
                    if __numeric and __character;
                    %s
                    %s
                run;''' % (_quote(out_tbl), _quote(num_tbl), ds_groups, nums,
                           _quote(char_tbl), ds_groups, chars, ds_groups,
                           rename, drop))

            out_tbl = dsout['OutputCasTables']['casTable'][0]

            out_tbl.params['replace'] = True
            out_tbl.vars = cols

            out_tbl._retrieve('table.partition', casout=out_tbl)

        elif char_tbl:
            dsout = self._retrieve('datastep.runcode', code=r'''
                data %s;
                    set %s;
                    %s
                    %s
                run;''' % (_quote(out_tbl), _quote(char_tbl), drop, rename))
            out_tbl = dsout['OutputCasTables']['casTable'][0]

        elif num_tbl:
            dsout = self._retrieve('datastep.runcode', code=r'''
                data %s;
                    set %s;
                    %s
                    %s
                run;''' % (_quote(out_tbl), _quote(num_tbl), drop, rename))
            out_tbl = dsout['OutputCasTables']['casTable'][0]

        if char_tbl:
            self.get_connection().CASTable(char_tbl)._retrieve('table.droptable')

        if num_tbl:
            self.get_connection().CASTable(num_tbl)._retrieve('table.droptable')

        table._retrieve('table.droptable')

        return out_tbl

    def _use_casout_for_stat(self, casout):
        ''' Determine if an casout table should be used for action output '''
        bygroups = self.get_groupby_vars()

        if bygroups:

            if casout is not None:
                return True

            self._loadactionset('datapreprocess')

            num_groups_tbl = self._retrieve('simple.groupbyinfo',
                                            novars=True)['OutputCasTables']['casTable'][0]
            num_groups = len(num_groups_tbl)
            num_groups_tbl._retrieve('table.droptable')

#           tbl = self.copy()
#           tbl.params.pop('groupby')
#           out = tbl._retrieve('datapreprocess.highcardinality', inputs=bygroups)
#           num_groups = out['HighCardinalityDetails']['CardinalityEstimate'].product()

            if num_groups > get_option('cas.dataset.bygroup_casout_threshold'):
                warnings.warn('The number of potential by groupings is greater than '
                              'cas.dataset.bygroup_casout_threshold.  The results will '
                              'be written to a CAS table.', RuntimeWarning)
                return True

        return False

    def max(self, axis=None, skipna=True, level=None, numeric_only=False,
            casout=None, **kwargs):
        '''
        Return the maximum value of each column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        skipna : boolean, optional
            Exclude missing values from the computation.
        level : int or string, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.max`

        Returns
        -------
        :class:`pandas.Series`
            If no by groups are specified.
        :class:`pandas.DataFrame`
            If by groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('max', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout,
                                         **kwargs)
        return self._topk_values('max', axis=axis, skipna=skipna, level=level,
                                 numeric_only=numeric_only, **kwargs)

    def mean(self, axis=None, skipna=True, level=None, numeric_only=False, casout=None,
             **kwargs):
        '''
        Return the mean value of each column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        skipna : boolean, optional
            Not implemented.
        level : int or string, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.mean`

        Returns
        -------
        :class:`pandas.Series`
            If no by groups are specified.
        :class:`pandas.DataFrame`
            If by groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('mean', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout,
                                         **kwargs)
        return self._get_summary_stat('mean')

    def median(self, axis=None, skipna=None, level=None, numeric_only=None,
               casout=None, **kwargs):
        '''
        Return the median value of each numeric column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        skipna : boolean, optional
            Exclude missing values from the computation.
        level : int or string, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.median`

        Returns
        -------
        :class:`pandas.Series`
            If no by groups are specified.
        :class:`pandas.Dataframe`
            If by groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('median', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout,
                                         **kwargs)
        return self.quantile(0.5, axis=axis, interpolation='nearest')

    def min(self, axis=None, skipna=True, level=None, numeric_only=False,
            casout=None, **kwargs):
        '''
        Return the minimum value of each column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        skipna : boolean, optional
            Exclude missing values from the computation.
        level : int or string, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.min`

        Returns
        -------
        :class:`pandas.Series`
            If no by groups are specified.
        :class:`pandas.Dataframe`
            If by groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('min', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout,
                                         **kwargs)
        return self._topk_values('min', axis=axis, skipna=skipna, level=level,
                                 numeric_only=numeric_only, **kwargs)

    def _get_casout_slice(self, n, columns=None, ascending=True,
                          casout=None, start=None):
        '''
        Get a slice of a table (with by groups) and output to a CAS table

        Parameters
        ----------
        n : int
            The number of rows to return per by group
        columns : string or list-of-strings, optional
            Names of the columns to sort by
        ascending : bool, optional
            If True, the sort order is ascending
        casout : bool or string or CASTable or dict, optional
            The CAS output table specification
        single : bool, optional
            If True, `n` is interpretted as a single value

        Returns
        -------
        :class:'CASTable'

        '''
        if not self.has_groupby_vars():
            raise ValueError('This method requires by groupings')

        groups = self.get_groupby_vars()
        sorts = [x['name'] for x in self._sortby]

        if columns is None:
            columns = sorts
        elif columns is True:
            columns = sorts = [x for x in self.columns if x not in sorts]

        if not isinstance(columns, items_types):
            columns = [columns]

        out = self._normalize_bygroups()
        cols, groups, raw_groups, fmt_groups, retain, keep, drop, rename = out

        groups = [_nlit(x) for x in self.get_groupby_vars()]

        group_str = ' '.join(groups)
        sortby_str = ' '.join(columns)
        cond_str = ' or '.join(['first.%s' % x for x in groups])

        if ascending:
            sort_order = ''
        else:
            sort_order = 'descending '

        if isinstance(casout, CASTable):
            pass
        elif isinstance(casout, dict):
            casout = self.get_connection().CASTable(**casout)
        elif isinstance(casout, six.string_types):
            casout = self.get_connection().CASTable(casout)
        else:
            casout = self.get_connection().CASTable(_gen_table_name())

        if start is None:
            comp = '__count le %s' % int(n)
        elif isinstance(n, items_types):
            comp = '__count in (%s)' % ','.join('%s' % (int(x) + 1) for x in n)
        elif start == n:
            comp = '__count eq %s' % int(n + 1)
        else:
            comp = '__count ge %s and __count le %s' % (int(start + 1), int(start + n))

        casin = None
        out = None

        try:
            casin = self.to_view()

            out = self._retrieve('datastep.runcode', single='yes', code=r'''
                 data %s;
                     %s
                     set %s;
                       by %s%s %s;
                     if %s then __count = 0;
                     __count + 1;
                       if %s then output;
                     drop __count;
                   run;
             ''' % (casout.to_datastep_params(), retain,
                    casin.to_input_datastep_params(), sort_order, group_str,
                    sortby_str, cond_str, comp))

        finally:
            if casin is not None:
                casin._retrieve('table.droptable')

        return out['OutputCasTables']['casTable'][0]

    def nlargest(self, n, columns, keep='first', casout=None):
        '''
        Return the `n` largest values ordered by `columns`

        Parameters
        ----------
        n : int
            Return this many descending sorted values.
        columns : list-of-strings or string
            Column name or names to order by.
        keep : string, optional
            Not implemented.

        See Also
        --------
        :meth:`pandas.DataFrame.nlargest`
        :meth:`nsmallest`

        Returns
        -------
        :class:`pandas.Series`

        '''
        if self._use_casout_for_stat(casout):
            raise NotImplementedError('nlargest is not implemented for casout yet')
            return self._get_casout_slice(n, columns=columns,
                                          ascending=False, casout=casout)
        return self.sort_values(columns, ascending=False).slice(0, n)

    def nsmallest(self, n, columns, keep='first', casout=None):
        '''
        Return the `n` smallest values ordered by `columns`

        Parameters
        ----------
        n : int
            Return this many ascending sorted values.
        columns : list-of-strings or string
            Column name or names to order by.
        keep : string, optional
            Not implemented.

        See Also
        --------
        :meth:`pandas.DataFrame.nsmallest`
        :meth:`nlargest`

        Returns
        -------
        :class:`pandas.Series`

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_slice(n, columns=columns,
                                          ascending=True, casout=casout)
        return self.sort_values(columns, ascending=True).slice(0, n)

    def mode(self, axis=0, numeric_only=False, max_tie=100, skipna=True):
        '''
        Return the mode of each column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        max_tie : int, optional
            The maximum number of tied values to return.

        See Also
        --------
        :meth:`pandas.DataFrame.mode`

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.Dataframe`
            If By groups are specified.

        '''
        # TODO: If a column has all unique values, it should just be set to NaN.

        if numeric_only:
            inputs = self._get_dtypes(include='numeric')
        else:
            inputs = list(self.columns)

        bygroup_columns = 'raw'

        out = self._retrieve('simple.topk', order='freq', topk=1, raw=True,
                             includemissing=not skipna, inputs=inputs,
                             bottomk=0, maxtie=max_tie)

        groups = self.get_groupby_vars()
        if groups:
            out = [x.reshape_bygroups(bygroup_columns=bygroup_columns,
                                      bygroup_as_index=False)
                   for x in out.get_tables('Topk')]
            out = pd.concat(out)
            out['Rank'] = out['Rank'] - 1
            items = []
            for key, item in out.groupby(groups):
                if not isinstance(key, items_types):
                    key = [key]
                charout = numout = None
                if 'CharVar' in out.columns:
                    charout = item.pivot(columns='Column', values='CharVar',
                                         index='Rank').replace([None, ''], np.nan)
                if 'NumVar' in out.columns:
                    numout = item.pivot(columns='Column', values='NumVar', index='Rank')
                if numout is not None and charout is not None:
                    item = numout.fillna(charout)
                elif numout is not None:
                    item = numout
                elif charout is not None:
                    item = charout
                for name, value in zip(groups, key):
                    item[name] = value

                item = pd.concat([item[col].sort_values(ascending=True,
                                  inplace=False).reset_index(drop=True)
                                  for col in inputs], axis=1)

                # Add By group values in for the index as needed
                for byname, byval in zip(groups, key):
                    if byname not in item.columns:
                        item[byname] = byval

                item = item.set_index(groups, append=True)
                item = item.reorder_levels(groups + [None])
                item.index.names = list(item.index.names[:-1]) + [None]

                items.append(item)

            out = pd.concat(items)

        else:
            out = out['Topk']
            out['Rank'] = out['Rank'] - 1
            charout = numout = None
            if 'CharVar' in out.columns:
                charout = out.pivot(columns='Column', values='CharVar',
                                    index='Rank').replace([None, ''], np.nan)
            if 'NumVar' in out.columns:
                numout = out.pivot(columns='Column', values='NumVar', index='Rank')
            if numout is not None and charout is not None:
                out = numout.fillna(charout)
            elif numout is not None:
                out = numout
            elif charout is not None:
                out = charout

            out = pd.concat([out[col].sort_values(ascending=True).reset_index(drop=True)
                             for col in inputs], axis=1)

        out.columns.name = None
        out.index.name = None

        return out

#   def pct_change(self, *args, **kwargs):
#       raise NotImplementedError

#   def prod(self, *args, **kwargs):
#       raise NotImplementedError

    def quantile(self, q=0.5, axis=0, numeric_only=True, interpolation='nearest',
                 casout=None, **kwargs):
        '''
        Return values at the given quantile

        Parameters
        ----------
        q : float, optional
            The quantiles to compute (0 <= q <= 1).
        axis : int, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        interpolation : string, optional
            Only 'nearest' is supported.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.quantile`

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified, or only a single quantile is requested.
        :class:`pandas.Dataframe`
            If By groups are specified.

        '''
        single_quantile = False
        if not isinstance(q, items_types):
            q = [q]
            single_quantile = True

        q = [x * 100 for x in q]

        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('percentile', axis=axis,
                                         numeric_only=numeric_only, casout=casout,
                                         percentile_values=q,
                                         **kwargs)

        tbl = self

        if numeric_only:
            tbl = tbl.select_dtypes(include='numeric')

        groups = tbl.get_groupby_vars()

        columns = [x for x in tbl.columns if x not in groups]

        out = tbl._percentiles(percentiles=q, format_labels=False)[columns]

        if single_quantile:
            out = out.reset_index(level=-1, drop=True)
            if not groups:
                out = out.stack().reset_index(level=0, drop=True)

        return out

#   def rank(self, *args, **kwargs):
#       raise NotImplementedError

#   def sem(self, *args, **kwargs):
#       raise NotImplementedError

    def sum(self, axis=None, skipna=None, level=None, numeric_only=True,
            casout=None):
        '''
        Return the sum of the values of each column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        skipna : boolean, optional
            Not implemented.
        level : int, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.sum`

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        # TODO: Need character variables (??)
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('sum', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout)
        return self._get_summary_stat('sum')

    def std(self, axis=None, skipna=None, level=None, ddof=1, numeric_only=True,
            casout=None):
        '''
        Return the standard deviation of the values of each column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        skipna : boolean, optional
            Not implemented.
        level : int, optional
            Not implemented.
        ddof : int, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.std`

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('std', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout)
        return self._get_summary_stat('std')

    def var(self, axis=None, skipna=None, level=None, ddof=1, numeric_only=True,
            casout=None):
        '''
        Return the variance of the values of each column

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        skipna : boolean, optional
            Not implemented.
        level : int, optional
            Not implemented.
        ddof : int, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`pandas.DataFrame.var`

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('var', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout)
        return self._get_summary_stat('var')

    # Not DataFrame methods, but they are available statistics.

    def nmiss(self, axis=0, level=None, numeric_only=False, casout=None):
        '''
        Return total number of missing values in each column

        Parameters
        ----------
        axis : int, optional
            Not impelmented.
        level : int or level name, optional
            Not implemented.
        numeric_only : boolean, optional
            Include only numeric columns.
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        See Also
        --------
        :meth:`count`
        :meth:`pandas.DataFrame.count`

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('nmiss', axis=axis, level=level,
                                         numeric_only=numeric_only, casout=casout)

        self._loadactionset('aggregation')

        if numeric_only:
            inputs = self._get_dtypes(include='numeric')
        else:
            inputs = self._columns or self.columns

        groups = self.get_groupby_vars()
        if groups:
            inputs = [x for x in inputs if x not in groups]
            out = self._retrieve('aggregation.aggregate',
                                 varspecs=[dict(names=list(inputs), agg='nmiss')])
            out.pop('ByGroupInfo', None)
            out = pd.concat(list(out.values()))
            out = out.set_index('Column', append=True)['NMiss']
            out = out.unstack(level=-1)
            out = out.astype('int64')
            if isinstance(out, pd.DataFrame):
                out.columns.name = None
            return out[inputs]

        out = pd.concat(list(self._retrieve('aggregation.aggregate',
                                            varspecs=[
                                                dict(names=list(inputs), agg='nmiss')
                                            ]).values()))
        out = out.set_index('Column')['NMiss']
        out = out.loc[inputs]
        out = out.astype('int64')
        if isinstance(out, pd.DataFrame):
            out.columns.name = None
        out.name = None
        out.index.name = None
        return out

    def stderr(self, casout=None):
        '''
        Return the standard error of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('stderr', casout=casout)
        return self._get_summary_stat('stderr')

    def uss(self, casout=None):
        '''
        Return the uncorrected sum of squares of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('uss', casout)
        return self._get_summary_stat('uss')

    def css(self, casout=None):
        '''
        Return the corrected sum of squares of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('css', casout=casout)
        return self._get_summary_stat('css')

    def cv(self, casout=None):
        '''
        Return the coefficient of variation of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('cv', casout=casout)
        return self._get_summary_stat('cv')

    def tvalue(self, casout=None):
        '''
        Return the T-statistics for hypothesis testing of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('tstat', casout=casout)
        return self._get_summary_stat('tvalue')

    def probt(self, casout=None):
        '''
        Return the p-value of the T-statistics of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('probt', casout=casout)
        return self._get_summary_stat('probt')

    def skewness(self, axis=None, skipna=True, level=None, numeric_only=None,
                 casout=None):
        '''
        Return the skewness of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('skew', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout)

        tbl = self
        if numeric_only:
            tbl = self.select_dtypes(include='numeric')

        return tbl._get_summary_stat('skewness')

    skew = skewness

    def kurtosis(self, axis=None, skipna=True, level=None, numeric_only=None,
                 casout=None):
        '''
        Return the kurtosis of the values of each column

        Parameters
        ----------
        casout : bool or string or dict or CASTable, optional
            Indicates the CAS output table to use for output.
            NOTE: This is only use if by groups are used.

        Returns
        -------
        :class:`pandas.Series`
            If no By groups are specified.
        :class:`pandas.DataFrame`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('kurt', axis=axis, skipna=skipna, level=level,
                                         numeric_only=numeric_only, casout=casout)

        tbl = self
        if numeric_only:
            tbl = self.select_dtypes(include='numeric')

        return tbl._get_summary_stat('kurtosis')

    kurt = kurtosis

    # Reindexing / Selection / Label manipulation

#   def add_prefix(self, prefix):
#       raise NotImplementedError

#   def add_suffix(self, suffix):
#       raise NotImplementedError

#   def align(self, *args, **kwargs):
#       raise NotImplementedError

    def drop(self, labels, axis=0, level=None, inplace=False, errors='raise'):
        '''
        Return a new CASTable object with the specified columns removed

        Parameters
        ----------
        labels : string or list-of-strings
            The items to remove.
        axis : int, optional
            Only axis=1 is supported.
        level : int or string, optional
            Not implemented.
        inplace : boolean, optional
            If True, apply the operation in place and return None.
        errors : string, optional
            If 'raise', then an exception is raised if the requested labels
            do not exist.  If 'ignore', any errors are ignored.

        Examples
        --------
        >>> tbl = CASTable('my-table')
        >>> print(tbl.columns)
        Index(['A', 'B', 'C'], dtype='object')

        >>> tbl = tbl.drop(['B', 'C'], axis=1)
        >>> print(tbl.columns)
        Index(['A'], dtype='object')

        See Also
        --------
        :meth:`pandas.DataFrame.drop`

        Returns
        -------
        ``None``
            If inplace == True
        :class:`CASTable`
            If inplace == False

        '''
        if axis != 1:
            raise NotImplementedError('Only axis=1 is supported.')

        if not isinstance(labels, items_types):
            labels = [labels]

        labels = set(labels)
        columns = list(self.columns)
        if errors == 'raise':
            diff = labels.difference(set(columns))
            if diff:
                raise IndexError('Requested name(s) do not exist in the '
                                 'column list: %s.' % ', '.join(list(diff)))

        columns = [x for x in columns if x not in labels]

        if inplace:
            self._columns = columns
            return

        out = self.copy()
        out._columns = columns
        out._sortby = list(self._sortby)
        return out

#   def drop_duplicates(self, *args, **kwargs):
#       raise NotImplementedError

#   def duplicated(self, *args, **kwargs):
#       raise NotImplementedError

#   def equals(self, *args, **kwargs):
#       raise NotImplementedError

#   def filter(self, *args, **kwargs):
#       raise NotImplementedError

#   def first(self, *args, **kwargs):
#       raise NotImplementedError

#   def idxmax(self, *args, **kwargs):
#       raise NotImplementedError

#   def idxmin(self, *args, **kwargs):
#       raise NotImplementedError

#   def last(self, *args, **kwargs):
#       raise NotImplementedError

#   def reindex(self, *args, **kwargs):
#       raise NotImplementedError

#   def reindex_axis(self, *args, **kwargs):
#       raise NotImplementedError

#   def reindex_like(self, *args, **kwargs):
#       raise NotImplementedError

#   def rename(self, *args, **kwargs):
#       raise NotImplementedError

    def reset_index(self, level=None, drop=False, inplace=False,
                    col_level=0, col_fill='', **kwargs):
        '''
        Reset the CASTable index

        NOTE: CAS tables do not support indexing, so this method
              just returns self (if inplace=True), or a copy of
              self (if inplace=False) simply for DataFrame
              compatibility.

        Returns
        -------
        :class:`CASTable`

        '''
        if inplace:
            return self
        return copy.deepcopy(self)

    def sample(self, n=None, frac=None, replace=False, weights=None,
               random_state=None, axis=None, stratify_by=None, **kwargs):
        '''
        Returns a random sample of the table rows

        Parameters
        ----------
        n : int, optional
            The number of samples to return.  The default is 1 if frac=None.
        frac : float, optional
            The percentage of rows to return.
        replace : bool, optional
            Not supported.
        weights : str or ndarray-like, optional
            Not supported.
        random_state : int, optional
            Seed for the random number generator.
        axis : int or string, optional
            Not supported.
        stratify_by : string, optional
            The variable to stratify by.

        Returns
        -------
        :class:`CASTable`

        '''
        if n is not None and frac is not None:
            raise ValueError('Both `n` and `frac` can not be specified at the same time')

        if n is None and frac is None:
            n = 1

        if n is not None:
            frac = float(n) / self._numrows
            if frac <= 0:
                raise RuntimeError('Sample percentage will return no samples.')
            if frac >= 1:
                return self._retrieve('table.partition')['casTable']

        return self._sample(sample_pct=frac, sample_seed=random_state,
                            stratify_by=stratify_by)

#   def select(self, *args, **kwargs):
#       raise NotImplementedError

#   def take(self, *args, **kwargs):
#       raise NotImplementedError

#   def truncate(self, *args, **kwargs):
#       raise NotImplementedError

    # Missing data handling

    def dropna(self, axis=0, how='any', thresh=None, subset=None,
               inplace=False, **kwargs):
        '''
        Drop rows that contain missing values

        Parameters
        ----------
        axis : int or string, optional
            Not supported.  Only dropping of rows is supported.
        how : string, optional
            If any, the row is dropped if any value is missing.
            If all, the row is dropped only if all values are missing.
        thresh : int, optional
            Not supported
        subset : list-of-strings, optional
            Not supported
        inplace : boolean, optional
            If True, the table modified in place. If False, a new
            table is created.

        Returns
        -------
        :class:`CASTable` object

        '''
        dtypes = self.dtypes
        all_dtypes_len = len(dtypes)
        dtypes = dtypes[dtypes.isin(['double', 'char', 'varchar'])].to_dict()
        miss_dtypes_len = len(dtypes)

        code = []

        if how == 'any':
            else_str = ''
            for name, dtype in dtypes.items():
                code.append('    %sif ( missing(%s) ) then delete;' %
                            (else_str, _nlit(name)))
                else_str = 'else '

        elif how == 'all':
            if all_dtypes_len == miss_dtypes_len:
                for name, dtype in dtypes.items():
                    code.append('missing(%s)' % _nlit(name))
                code = '    if ( %s ) then delete;' % ' and \n'.join(code)

        else:
            raise ValueError('Unknown value for parameter "how": %s' % how)

        return self._apply_datastep(code, inplace=inplace)

    def fillna(self, value=None, method=None, axis=None, inplace=False,
               limit=None, downcast=None, **kwargs):
        '''
        Fill missing values using the specified method

        Parameters
        ----------
        value : scalar or dict or Series or DataFrame or CASColumn or CASTable
            The value used to fill missing values.  If a dict, Series,
            or DataFrame is specified, the keys / index values
            will be used as the column names.
        method : string, optional
            Not supported
        axis : int or string, optional
            Not supported.   The axis is always 'columns'.
        inplace : boolean, optional
            If True, the data is modified in place. If False, a new table
            be created.
        limit : int, optional
            Not supported
        downcast : dict, optional
            Not supported

        Returns
        -------
        :class:`CASTable` object

        '''
        is_scalar = False
        if isinstance(value, dict):
            pass
        elif isinstance(value, pd.Series):
            value = value.to_dict()
        elif isinstance(value, pd.DataFrame):
            value = value.iloc[0, :].to_dict()
        else:
            is_scalar = True

        dtypes = self.dtypes
        dtypes = dtypes[dtypes.isin(['double', 'char', 'varchar'])].to_dict()

        code = []
        if is_scalar:
            for name, dtype in dtypes.items():
                code.append('    if ( missing(%s) ) then %s = %s;' %
                            (_nlit(name), _nlit(name), float(value)))
        else:
            for name, repl in value.items():
                if name in dtypes:
                    code.append('     if ( missing(%s) ) then %s = %s;' %
                                (_nlit(name), _nlit(name), float(repl)))

        out = self._apply_datastep(code, inplace=inplace)

        if inplace:
            return

        return out

    def replace(self, to_replace=None, value=None, inplace=False, limit=None,
                regex=False, method='pad', **kwargs):
        '''
        Replace values in the data set

        Parameters
        ----------
        to_replace : str or regex or list or dict or Series or Numeric or None, optional
            * str or regex
                - str : string matching this value will be replaced with `value`
                - regex : string matching this pattern will be replaced with `value`
            * list-of-strings or list-of-regexes or list-of-numerics
                - This list **must** be the same length as `value`.
                - If `regex=True`, both this list and `value` are regexes.
            * dict
                - Dictionaries are of the form {'col': {'value': rep-value}}.
                  The top-level contains the column names to match, the inner
                  dictionary specifies the values to match and the replacement
                  values.
            * None
                - This means that the `regex=` parameter contains the patterns
                  to match.
        value : scalar or dict or list or string or regex or None
            Values to use as replacements.  If a dict is specified, it takes
            the same form as a dictionary in the `to_replace=` parameter.
        inplace : boolean, optional
            If True, the table is modified in-place. If False, a new table is created.
        limit : int, optional
            Not supported
        regex : boolean or same types as `to_replace`, optional
            If True, the `to_replace=` and/or `value=` values are interpreted
            as regular expressions.
        method : string, optional
            Not supported

        Raises
        ------
        AssertionError
            If regex is not a boolean and `to_replace` is not None.
        TypeError
            If `to_replace` is None and `value` is not a list, dict, or Series.
            If `to_replace` is None and `regex` is a list, dict, or Series.
        ValueError
            If `to_replace` and `value` are lists, but are not the same length.

        Returns
        -------
        :class:`CASTable` object

        '''
        if regex is not True and regex is not False:
            assert(to_replace is None)

        regex_type = type(re.compile(''))

        def is_regex(val):
            ''' See if value should be considered a regex '''
            if regex and isinstance(val, char_types):
                return True
            elif type(val) is regex_type:
                return True
            return False

        def dict_to_repl(to_replace, value):
            ''' Convert dict to replacements '''
            out = {}
            for k, v in value.items():
                out[k] = {(to_replace, is_regex(to_replace)): (v, is_regex(v))}
            return out

        def scalar_to_repl(to_replace, val):
            ''' Setup scalar replacement '''
            return {None: {(to_replace, is_regex(to_replace)): (val, is_regex(val))}}

        def list_to_repl(to_replace, vals):
            ''' Convert list replacements to dict '''
            if len(to_replace) != len(vals):
                raise ValueError('replacements value lists are not the same length')
            out = {}
            for before, after in zip(to_replace, vals):
                if isinstance(after, dict):
                    out.update(dict_to_repl(before, after))
                else:
                    if None not in out:
                        out[None] = {}
                    out[None].update({(before, is_regex(before)):
                                      (after, is_regex(after))})
            return out

        def repl_to_repl(to_replace):
            ''' Normalize dictionary to replacement dictionary '''
            out = {}
            for k, v in to_replace.items():
                if k not in out:
                    out[k] = {}
                for pat, repl in v.items():
                    out[k].update({(pat, is_regex(pat)): (repl, is_regex(repl))})
            return out

        repl = {}

        # Use regex rather than to_replace
        if to_replace is None:
            to_replace = regex
            regex = True

        # to_replace is a string
        if isinstance(to_replace, (char_types, regex_type)):
            if isinstance(value, (char_types, num_types, regex_type)):
                repl.update(scalar_to_repl(to_replace, value))
            elif isinstance(value, dict):
                repl.update(dict_to_repl(to_replace, value))
            else:
                raise TypeError('value=%s is not compatible with to_replace=string'
                                % type(value))

        # to_replace is numeric
        elif isinstance(to_replace, num_types):
            if isinstance(value, (num_types, char_types, regex_type)):
                repl.update(scalar_to_repl(to_replace, value))
            elif isinstance(value, dict):
                repl.update(dict_to_repl(to_replace, value))
            else:
                raise TypeError('value=%s is not compatible with to_replace=numeric'
                                % type(value))

        # to_replace is a list
        elif isinstance(to_replace, (items_types, pd.Series)):
            to_replace = list(to_replace)
            if isinstance(value, (items_types, pd.Series)):
                repl.update(list_to_repl(to_replace, list(value)))
            else:
                repl.update(list_to_repl(to_replace, [value] * len(to_replace)))

        # to_replace is a dictionary
        elif isinstance(to_replace, dict):
            if value is not None:
                raise TypeError('Replacement values are not allowed when '
                                'to_replace is a dictionary')
            repl.update(repl_to_repl(to_replace))

        # Construct data step code for the replacements
        code = []
        columns = None
        dtypes = None
        col_char_types = set(['char', 'varchar', 'binary', 'varbinary'])

        def re_flags_to_str(flags):
            ''' Convert regex flags to string representation '''
            out = []
            if hasattr(re, 'A') and (flags & re.A):
                out.append('a')
            if flags & re.I:
                out.append('i')
            if flags & re.L:
                out.append('L')
            if flags & re.M:
                out.append('m')
            if flags & re.S:
                out.append('s')
#           if flags & re.U:
#               out.append('u')
            if flags & re.X:
                out.append('x')
            return ''.join(out)

        def to_re_match(patt):
            ''' Convert object to regex pattern match syntax '''
            flags = ''
            if type(patt) is regex_type:
                flags = re_flags_to_str(patt.flags)
                patt = patt.pattern
            if not isinstance(patt, char_types):
                raise TypeError('Regular expression pattern is not a string: %s' % patt)
            return _quote('/%s/%s' % (patt, flags))

        def to_re_sub(patt, to):
            ''' Convert object to regex pattern substitution syntax '''
            flags = ''
            if type(patt) is regex_type:
                flags = re_flags_to_str(patt.flags)
                patt = patt.pattern
            if not isinstance(patt, char_types):
                raise TypeError('Regular expression pattern is not a string: %s' % patt)
            if not isinstance(to, char_types):
                raise TypeError('Regular expression substitution is not a string: %s'
                                % to)
            to = re.sub(r'\\(\d)', r'$\1', to)
            return _quote('s/%s/%s/%s' % (patt, to, flags))

        # Generate data step code
        for col, repl_dict in repl.items():

            # Cache column list
            if col is None and columns is None:
                dtypes = self.dtypes
                columns = [x[0] for x in dtypes.iteritems()]
                dtypes = [x[1] for x in dtypes.iteritems()]

            # Apply replacements for each column
            for from_, to in repl_dict.items():
                from_, from_is_regex = from_
                to, to_is_regex = to

                # If col is None, it applies to all columns
                if col is None:
                    for colname, dtype in zip(columns, dtypes):
                        if from_is_regex or to_is_regex:
                            if dtype not in col_char_types:
                                continue
                            code.append(('if ( prxmatch(%s, %s) ) '
                                         'then %s = prxchange(%s, -1, %s);') %
                                        (to_re_match(from_), _nlit(colname),
                                         _nlit(colname),
                                         to_re_sub(from_, to), _nlit(colname)))
                        else:
                            code.append('if ( %s = %s ) then %s = %s;' %
                                        (_nlit(colname), _quote_if_string(from_),
                                         _nlit(colname), _quote_if_string(to)))
                else:
                    if from_is_regex or to_is_regex:
                        if dtype not in col_char_types:
                            continue
                        code.append(('if ( prxmatch(%s, %s) ) '
                                     'then %s = prxchange(%s, -1, %s);') %
                                    (to_re_match(from_), _nlit(col),
                                     _nlit(col),
                                     to_re_sub(from_, to), _nlit(col)))
                    else:
                        code.append('if ( %s = %s ) then %s = %s;' %
                                    (_nlit(col), _quote_if_string(from_),
                                     _nlit(col), _quote_if_string(to)))

        return self._apply_datastep(code, inplace=inplace)

    def _apply_datastep(self, code, inplace=False, casout=None,
                        prefix=None, suffix=None):
        '''
        Apply the given data step code to the table

        If a CASLib is specified in the `casout=` parameter, that CASLib
        will be used for the resulting table.  If no CASLib is specified,
        the new table will be created in the CASLib of the source table.

        If no table name is specified in the `casout=` parameter, a name
        is generated.  The exception being that if `inplace=True` is
        specified, then the same table name as the source is used.

        In all cases, the `casout=` parameter takes highest priority.

        Parameters
        ----------
        code : string or list-of-strings
            The date step code to apply
        inplace : boolean, optional
            If True, the table is modified in place
        casout : dict, optional
            The output table specification
        prefix : string, optional
            String to use as table name prefix
        suffix : string, optional
            String to use as table name suffix

        Returns
        -------
        :class:`CASTable` object

        '''
        if casout is None:
            casout = {}

        default_caslib = self.getsessopt('caslib').caslib

        if casout.get('caslib'):
            caslib = casout['caslib']
        elif inplace and 'caslib' in self.params:
            caslib = self.params['caslib']
        else:
            caslib = default_caslib

        if casout.get('name'):
            newname = casout['name']
        elif inplace:
            newname = self.params['name']
        else:
            newname = _gen_table_name()

        newname = '%s%s%s' % ((prefix or ''), newname, (suffix or ''))

        dscode = []
        dscode.append('data %s(caslib=%s);' % (_quote(newname), _quote(caslib)))
        dscode.append('    set %s(caslib=%s);' % (_quote(self.params.name),
                      _quote(self.params.get('caslib', default_caslib))))
        if isinstance(code, items_types):
            dscode.extend(code)
        else:
            dscode.append(code)
        dscode.append('run;')
        dscode = '\n'.join(dscode)

        out = self.get_connection().retrieve('datastep.runcode', code=dscode,
                                             _apptag='UI', _messagelevel='error')
        if out.status:
            raise SWATError(out.status)

        if inplace:
            return self

        tbl = out['OutputCasTables'].iloc[0]['casTable']

        out = copy.deepcopy(self)

        out.params['name'] = tbl.params['name']
        out.params['caslib'] = tbl.params['caslib']

        return out

    # Reshaping, sorting, transposing

#   def pivot(self, *args, **kwargs):
#       raise NotImplementedError

#   def reorder_levels(self, *args, **kwargs):
#       raise NotImplementedError

    def sort_values(self, by, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        '''
        Specify sort parameters for data in a CASTable

        Parameters
        ----------
        by : string or list-of-strings
            The name or names of columns to sort by.
        axis : int, optional
            Not implemented.
        ascending : boolean or list-of-booleans, optional
            Sort ascending or descending.  Specify a list of booleans
            if sort orders are not all one type.
        inplace : boolean, optional
            If True, the sort order is embedded into the CASTable
            instance.  If False, a new CASTable is returned with the
            sort parameters embedded.
        kind : string, optional
            Not implemented.
        na_position : string, optional
            Not implemented.

        Notes
        -----
        Since CAS tables can be distributed across a grid of machines
        the order that they are stored in is not guaranteed.  A sort order
        is required to retrieve data in a predictable order.

        Returns
        -------
        ``None``
            If inplace == True.
        :class:`CASTable`
            If inplace == False.

        '''
        if inplace:
            out = self
        else:
            out = copy.deepcopy(self)
        if not isinstance(by, items_types):
            by = [by]
        if not isinstance(ascending, items_types):
            ascending = [ascending] * len(by)
        out._sortby = []
        for col, asc in zip(by, ascending):
            out._sortby.append(
                dict(name=col, order=asc and 'ASCENDING' or 'DESCENDING',
                     formatted='RAW'))
        if inplace:
            return
        return out

    sort = sort_values

#   def sort_index(self, *args, **kwargs):
#       raise NotImplementedError

#   def sortlevel(self, *args, **kwargs):
#       raise NotImplementedError

#   def swaplevel(self, *args, **kwargs):
#       raise NotImplementedError

#   def stack(self, *args, **kwargs):
#       raise NotImplementedError

#   def unstack(self, *args, **kwargs):
#       raise NotImplementedError

#   @getattr_safe_property
#   def T(self):
#       return self.transpose()

    def to_view(self, *args, **kwargs):
        '''
        Create a view using the current CASTable parameters

        The parameters to this method are the same as the `table.view`
        action.  `self` will automatically be added as the `tables=`
        parameter.

        Returns
        -------
        :class:`CASTable` object

        '''
        kwargs = kwargs.copy()
        kwargs['tables'] = [self.to_table_params()]
        if not args and 'name' not in kwargs:
            kwargs['name'] = _gen_table_name()
        if 'groupby' in kwargs['tables'][0]:
            kwargs['tables'][0].pop('groupby', None)
        groups = self.get_groupby_vars()
        kwargs['tables'][0]['vars'] = groups + [x for x in self.columns
                                                if x not in groups]
        out = self._retrieve('table.view', *args, **kwargs)
        if 'caslib' in out and 'viewName' in out:
            conn = self.get_connection()
            out = conn.CASTable(out['viewName'], caslib=out['caslib'])
            out._sortby = list(self._sortby)
            return out
        raise SWATError('No output table was returned')

#   def to_panel(self, *args, **kwargs):
#       raise NotImplementedError

#   def transpose(self, *args, **kwargs):
#       raise NotImplementedError

    # Combining / joining / merging

    def append(self, other, ignore_index=False, verify_integrity=False,
               casout=None):
        '''
        Append rows of `other` to `self`

        Parameters
        ----------
        other : CASTable
            The CAS table containing the rows to append
        ignore_index : boolean, optional
            Not supported.
        verify_integrity : boolean, optional
            Not supported.

        Returns
        -------
        :class:`CASTable`

        '''
        return concat([self, other], ignore_index=ignore_index,
                      verify_integrity=verify_integrity, casout=casout)

#   def assign(self, **kwargs):
#       raise NotImplementedError

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False, sort=False,
              suffixes=('_x', '_y'), copy=True, indicator=False, casout=None):
        '''
        Merge CASTable objects using a database-style join on a column

        Parameters
        ----------
        right : CASTable
            The CASTable to join with
        how : string, optional
            * 'left' : use only keys from `self`
            * 'right': use only keys from `right`
            * 'outer' : all observations
            * 'inner' : use intersection of keys
            * 'left-minus-right' : `self` minus `right`
            * 'right-minus-left' : `right` minus `self`
            * 'outer-minus-inner' : opposite of 'inner'
        on : string, optional
            Column name to join on, if the same column name is in
            both tables
        left_on : string, optional
            The key from `self` to join on.  This is used if the
            column names to join on are different in each table.
        right_on : string, optional
            The key from `right` to join on.  This s used if the
            column names to join on are different in each table.
        left_index : boolean, optional
            Not supported.
        right_index : boolean, optional
            Not supported.
        sort : boolean, optional
            Not supported.
        suffixes : two-element-tuple, optional
            The suffixes to use for overlapping column names in the
            resulting tables.  The first element is used for columns
            in `self`.  The second element is used for columns in
            `right`.
        copy : boolean, optional
            Not supported.
        indicator : boolean or string, optional
            If True, a column named '_merge' will be
            created with the values: 'left_only', 'right_only', or
            'both'.  If False, no column is created.  If a string is
            specified, a column is created using that name containing
            the aforementioned values.
        casout : string or CASTable or dict, optional
            The CAS output table specification

        Returns
        -------
        :class:`CASTable`

        '''
        return merge(self, right, how=how, on=on, left_on=left_on,
                     right_on=right_on, left_index=left_index,
                     right_index=right_index, sort=sort, suffixes=suffixes,
                     copy=copy, indicator=indicator, casout=casout)

#   def join(self, other, on=None, how='left', lsuffix='', rsuffix='', sort=False):
#       raise NotImplementedError

#   def update(self, other, **kwargs):
#       raise NotImplementedError

    # Time series-related

#   def asfreq(self, freq, **kwargs):
#       raise NotImplementedError

#   def shift(self, **kwargs):
#       raise NotImplementedError

#   def first_valid_index(self):
#       raise NotImplementedError

#   def last_valid_index(self):
#       raise NotImplementedError

#   def resample(self, rule, **kwargs):
#       raise NotImplementedError

#   def to_period(self, **kwargs):
#       raise NotImplementedError

#   def to_timestamp(self, **kwargs):
#       raise NotImplementedError

#   def tz_convert(self, tz, **kwargs):
#       raise NotImplementedError

#   def tz_localize(self, *args, **kwargs):
#       raise NotImplementedError

    def _fetch(self, grouped=False, sample_pct=None, sample_seed=None,
               stratify_by=None, sample=False, **kwargs):
        '''
        Return the fetched DataFrame given the fetch parameters

        Parameters
        ----------
        grouped : bool, optional
            If True, the output DataFrame is returned as By groups.
        sample_pct : int, optional
            Percentage of original data set to return as samples
        sample_seed : int, optional
            Random number seed
        stratify_by : string, optional
            Variable name to stratify samples by
        sample : bool, optional
            Flag to indicate that the result set should be sampled.
            This can be used instead of sample_pct to indicate that the
            values returned in the range of to= / from=, or the rows
            returned limited by swat.options.cas.dataset.max_rows_fetched
            should be sampled.

        Returns
        -------
        :class:`SASDataFrame`

        '''
        from .. import dataframe as df

        kwargs = kwargs.copy()
        groups = self.get_groupby_vars()

        for key, value in six.iteritems(self.get_fetch_params()):
            if key in kwargs:
                continue
            if key == 'sortby' and ('orderby' in kwargs or 'orderBy' in kwargs):
                continue
            kwargs[key] = value

        from_ = 0
        if 'from' in kwargs:
            from_ = kwargs['from']
        elif 'from_' in kwargs:
            from_ = kwargs['from_']

        if 'to' not in kwargs:
            max_rows_fetched = get_option('cas.dataset.max_rows_fetched')
            kwargs['to'] = min(from_ + max_rows_fetched, MAX_INT64_INDEX)
            if self._numrows > max_rows_fetched:
                warnings.warn(('Data downloads are limited to %d rows.  '
                               'To change this limit, set '
                               'swat.options.cas.dataset.max_rows_fetched '
                               'to the desired limit.') %
                              max_rows_fetched, RuntimeWarning)

        # Compute sample percentage as needed
        if sample_pct is None and sample:
            ntblrows = self._numrows
            nrows = kwargs['to'] - from_ + 1
            if ntblrows > nrows:
                sample_pct = float(nrows) / ntblrows

        if 'index' not in kwargs:
            kwargs['index'] = True

        # Add grouping columns if they aren't in the list
        columns = None
        if groups and 'fetchvars' in kwargs:
            kwargs['fetchvars'] = list(kwargs['fetchvars'])
            for group in reversed(groups):
                if group not in kwargs['fetchvars']:
                    kwargs['fetchvars'].insert(0, group)
            columns = kwargs['fetchvars']
        elif 'fetchvars' in kwargs:
            columns = kwargs['fetchvars']

        tbl = self._sample(sample_pct=sample_pct, sample_seed=sample_seed,
                           stratify_by=stratify_by, columns=columns)

        # Sort based on 'Fetch#' key.  This will be out of order in REST.
        values = [x[1] for x in sorted(tbl._retrieve('table.fetch', **kwargs).items(),
                  key=lambda x: int(x[0].replace('Fetch', '') or '0'))]
        out = df.concat(values)

        if tbl is not self:
            tbl._retrieve('table.droptable')

        if len(out.columns) and out.columns[0] == '_Index_':
            out['_Index_'] = out['_Index_'] - 1
            out = out.set_index('_Index_')
            out.index.name = None

        if grouped and groups:
            return out.groupby(groups)

        return out

    def _sample(self, sample_pct=None, sample_seed=None, stratify_by=None, columns=None):
        ''' Return a CASTable containing a sample of the rows '''
        if sample_pct is None:
            return self

        if sample_pct <= 0 or sample_pct >= 1:
            raise ValueError('Sample percentage should be a floating point '
                             'value between 0 and 1')

        action_name = 'sampling.srs'

        self._loadactionset('sampling')

        if columns is None:
            columns = list(self.columns)

        samptbl = self.copy()
        groupby = samptbl.get_groupby_vars()

        if groupby:
            samptbl.params.pop('groupby', None)
            samptbl.params.pop('groupBy', None)

        if stratify_by:
            action_name = 'sampling.stratified'
            samptbl.params['groupby'] = stratify_by

        params = dict(samppct=sample_pct * 100)
        if sample_seed is not None:
            params['seed'] = sample_seed

        out = samptbl._retrieve(action_name,
                                output=dict(casout=dict(name=_gen_table_name(),
                                                        replace=True),
                                            copyvars=columns),
                                **params)['OutputCasTables'].iloc[0]['casTable']

        if stratify_by:
            del samptbl.params['groupby']

        if groupby:
            out.params['groupby'] = groupby

        return out

    def _fetchall(self, grouped=False, sample_pct=None, sample_seed=None,
                  sample=False, stratify_by=None, **kwargs):
        ''' Fetch all rows '''
        kwargs = kwargs.copy()
        if 'to' not in kwargs:
            kwargs['to'] = MAX_INT64_INDEX
        return self._fetch(grouped=grouped, sample_pct=sample_pct,
                           sample_seed=sample_seed, sample=sample,
                           stratify_by=stratify_by, **kwargs)

    # Plotting

    def boxplot(self, column=None, by=None, **kwargs):
        '''
        Make a boxplot from the table data

        This method fetches the data from the CAS table and
        calls the :meth:`pandas.DataFrame.boxplot` method to do the
        rendering.  All arguments passed to this method are passed
        to the DataFrame's :meth:`boxplot` method.

        See Also
        --------
        :meth:`pandas.DataFrame.boxplot`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._plot._get_plot_params(**kwargs)
        return self._fetch(**params).boxplot(column=column, by=by, **kwargs)

    def hist(self, column=None, by=None, **kwargs):
        '''
        Make a histogram from the table data

        This method fetches the data from the CAS table and
        calls the :meth:`pandas.DataFrame.hist` method to do the
        rendering.  All arguments passed to this method are passed
        to the DataFrame's :meth:`hist` method.

        See Also
        --------
        :meth:`pandas.DataFrame.hist`

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them.

        '''
        params, kwargs = self._plot._get_plot_params(**kwargs)
        return self._fetch(**params).hist(column=column, by=by, **kwargs)

    @getattr_safe_property
    def plot(self):
        '''
        Plot the data in the table

        This method requires all of the data in the CAS table to be
        fetched to the **client side**.  The data is then plotted using
        :meth:`pandas.DataFrame.plot`.

        The ``plot`` attribute can be used as both a method and an
        object.  When called as a method, the parameters are the same
        as :meth:`pandas.DataFrame.plot`.  When used as an attribute
        each of the plot types are available as methods.  For example,
        ``tbl.plot(kind='bar')`` is equivalent to ``tbl.plot.bar()``.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.plot`.
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.plot`.

        Returns
        -------
        :class:`matplotlib.AxesSubplot` or :func:`numpy.array` of them

        '''
        return self._plot

    # Serialization / IO / Conversion

    @classmethod
    def from_csv(cls, connection, path, casout=None, **kwargs):
        '''
        Create a CASTable from a CSV file

        Parameters
        ----------
        connection : :class:`CAS`
            The CAS connection to read the data into.
        path : string or file-like object
            The path, URL, or file-like object to get the data from.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : keyword arguments
            Keyword arguments to pass to :func:`pandas.read_csv`.

        See Also
        --------
        :meth:`CAS.read_csv`
        :func:`pandas.read_csv`

        Returns
        -------
        :class:`CASTable`

        '''
        kwargs = kwargs.copy()
        kwargs.setdefault('index_col', 0)
        kwargs.setdefault('parse_dates', True)
        return connection.read_csv(path, casout=casout, **kwargs)

    @classmethod
    def _from_any(cls, name, connection, data, **kwargs):
        '''
        Upload data from various sources

        Parameters
        ----------
        name : string
            The data reader method to call.
        connection : :class:`CAS`
            The CAS connection to read the data into.
        data : :class:`pandas.DataFrame`
            The :class:`pandas.DataFrame` to upload.
        **kwargs : keyword parameters
            Keyword parameters sent to data reader method.

        Returns
        -------
        :class:`CASTable`

        '''
        use_addtable = kwargs.pop('use_addtable', False)
        table, kwargs = connection._get_table_args(**kwargs)
        dframe = getattr(pd.DataFrame, 'from_' + name)(data, **kwargs)
        if not use_addtable or connection._protocol.startswith('http'):
            if 'table' in table:
                table['name'] = table.pop('table')
            return connection.upload_frame(dframe, casout=table and table or None)
#                      importoptions=connection._importoptions_from_dframe(dframe)
        from swat.cas.datamsghandlers import PandasDataFrame
        dmh = PandasDataFrame(dframe)
        table.update(dmh.args.addtable)
        return connection.retrieve('table.addtable', **table)['casTable']

    @classmethod
    def from_dict(cls, connection, data, casout=None, **kwargs):
        '''
        Create a CASTable from a dictionary

        Parameters
        ----------
        connection : :class:`CAS`
            The :class:`CAS` connection to read the data into.
        data : dict
            The dictionary containing the data.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : keyword arguments
            Keyword arguments sent to :meth:`pandas.DataFrame.from_dict`.

        See Also
        --------
        :meth:`pandas.DataFrame.from_dict`

        Returns
        -------
        :class:`CASTable`

        '''
        return cls._from_any('dict', connection, data, casout=casout, **kwargs)

    @classmethod
    def from_items(cls, connection, items, casout=None, **kwargs):
        '''
        Create a CASTable from a (key, value) pairs

        Parameters
        ----------
        connection : :class:`CAS`
            The :class:`CAS` connection to read the data into.
        items : tuples
            The tuples containing the data.  The values should be arrays
            or :class:`pandas.Series`.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : keyword arguments
            Keyword arguments sent to :meth:`pandas.DataFrame.from_items`.

        See Also
        --------
        :meth:`pandas.DataFrame.from_items`

        Returns
        -------
        :class:`CASTable`

        '''
        return cls._from_any('items', connection, items, casout=casout, **kwargs)

    @classmethod
    def from_records(cls, connection, data, casout=None, **kwargs):
        '''
        Create a CASTable from records

        Parameters
        ----------
        connection : :class:`CAS`
            The :class:`CAS` connection to read the data into.
        data : :func:`numpy.ndarray`, list-of-tuples, dict, or :class:`pandas.DataFrame`
            The data to upload.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : keyword arguments
            Keyword arguments sent to :meth:`pandas.DataFrame.from_records`.

        See Also
        --------
        :meth:`pandas.DataFrame.from_records`

        Returns
        -------
        :class:`CASTable`

        '''
        return cls._from_any('records', connection, data, casout=casout, **kwargs)

    def info(self, verbose=None, buf=None, max_cols=None,
             memory_usage=None, null_counts=None):
        '''
        Print summary of CASTable information

        Parameters
        ----------
        verbose : boolean, optional
            If True, the full summary is printed
        buf : writeable file-like object
            Where the summary is printed to.
        max_cols : int, optional
            The maximum number of columns to include in the summary.
        memory_usage : boolean, optional
            If True, the memory usage is displayed
        null_counts : boolean, optional
            If True, missing values will be displayed

        See Also
        --------
        :meth:`pandas.DataFrame.info`

        '''
        if buf is None:
            buf = sys.stdout

        buf.write(u'%s\n' % self)

        nrows, ncols = self.shape

        counts = self._retrieve('simple.distinct')['Distinct']
        counts.set_index('Column', inplace=True)
        counts['N'] = (nrows - counts['NMiss']).astype('i8')
        counts['Miss'] = counts['NMiss'] > 0
        counts = counts[['N', 'Miss']]

        colinfo = self._columninfo
        colinfo.set_index('Column', inplace=True)
        colinfo = colinfo[['Type']]
        if null_counts is None or null_counts is True:
            colinfo = colinfo.join(counts)[['N', 'Miss', 'Type']]
        else:
            colinfo = colinfo.join(counts)[['N', 'Type']]
        colinfo.index.name = None

        if verbose is None or verbose is True:
            buf.write(u'Data columns (total %s columns):\n' % ncols)
            if max_cols is not None:
                buf.write(u'%s\n' % colinfo.iloc[:max_cols])
                if max_cols < ncols:
                    buf.write(u'...\n')
            else:
                buf.write(u'%s\n' % colinfo)
        else:
            buf.write(u'Columns: %s entries, %s to %s\n' %
                      (ncols, colinfo.index[0], colinfo.index[-1]))

        buf.write(u'dtypes: %s\n' % ', '.join([u'%s(%s)' % (name, dtype)
                  for name, dtype in sorted(
                      colinfo['Type'].value_counts().to_dict().items())]))

        if memory_usage is None or memory_usage is True:
            details = self._retrieve('table.tabledetails')['TableDetails']
            details = details[['DataSize', 'VardataSize', 'AllocatedMemory']].iloc[0]
            buf.write(u'data size: %s\n' % details['DataSize'])
            buf.write(u'vardata size: %s\n' % details['VardataSize'])
            buf.write(u'memory usage: %s\n' % details['AllocatedMemory'])

    def to_frame(self, sample_pct=None, sample_seed=None, sample=False,
                 stratify_by=None, **kwargs):
        '''
        Retrieve entire table as a SASDataFrame

        Parameters
        ----------
        sample_pct : float, optional
            Specifies the percentage of samples to return rather than the
            entire data set.  The value should be a float between 0 and 1.
        sample_seed : int, optional
            The seed to use for sampling.  This is used when deterministic
            results are required.
        **kwargs : keyword arguments, optional
            Additional keyword parameters to the ``table.fetch`` CAS action.

        Returns
        -------
        :class:`SASDataFrame`

        '''
        return self._fetchall(sample_pct=sample_pct, sample_seed=sample_seed,
                              sample=sample, stratify_by=stratify_by, **kwargs)

    def _to_any(self, method, *args, **kwargs):
        '''
        Generic converter to various output types

        Parameters
        ----------
        method : string
            The name of the export method.
        *args : positional arguments
            Positional arguments to the export method.
        **kwargs : keyword arguments
            Keyword arguments to the export method.

        '''
        kwargs = kwargs.copy()
        params = {}
        params['sample_pct'] = kwargs.pop('sample_pct', None)
        params['sample_seed'] = kwargs.pop('sample_seed', None)
        params['sample'] = kwargs.pop('sample', None)
        params['stratify_by'] = kwargs.pop('stratify_by', None)
        params['to'] = kwargs.pop('to', None)
        params['from'] = kwargs.pop('from', kwargs.pop('from_', None))
        params = {k: v for k, v in params.items() if v is not None}
        standard_dataframe = kwargs.pop('standard_dataframe', False)
        dframe = self._fetch(**params)
        if standard_dataframe:
            dframe = pd.DataFrame(dframe)
        return getattr(dframe, 'to_' + method)(*args, **kwargs)

    def to_xarray(self, *args, **kwargs):
        '''
        Represent table data as a numpy.xarray

        This method creates an object on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_xarray`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_xarray`

        See Also
        --------
        :meth:`pandas.DataFrame.to_xarray`

        '''
        return self._to_any('xarray', standard_dataframe=True, *args, **kwargs)

    def to_pickle(self, *args, **kwargs):
        '''
        Pickle (serialize) the table data

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_pickle`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_pickle`

        See Also
        --------
        :meth:`pandas.DataFrame.to_pickle`

        '''
        return self._to_any('pickle', standard_dataframe=True, *args, **kwargs)

    def to_csv(self, *args, **kwargs):
        '''
        Write table data to comma-separated values (CSV)

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_csv`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_csv`

        See Also
        --------
        :meth:`pandas.DataFrame.to_csv`

        '''
        return self._to_any('csv', *args, **kwargs)

    def to_hdf(self, *args, **kwargs):
        '''
        Write table data to HDF

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_hdf`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_hdf`

        See Also
        --------
        :class:`pandas.DataFrame.to_hdf`

        '''
        return self._to_any('hdf', standard_dataframe=True, *args, **kwargs)

    def to_sql(self, *args, **kwargs):
        '''
        Write table records to SQL database

        This method depends on data on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_sql`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_sql`

        See Also
        --------
        :class:`pandas.DataFrame.to_sql`

        '''
        return self._to_any('sql', *args, **kwargs)

    def to_dict(self, *args, **kwargs):
        '''
        Convert table data to a Python dictionary

        This method writes an object on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_dict`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_dict`

        See Also
        --------
        :meth:`pandas.DataFrame.to_dict`

        Returns
        -------
        dict

        '''
        return self._to_any('dict', *args, **kwargs)

    def to_excel(self, *args, **kwargs):
        '''
        Write table data to an Excel spreadsheet

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_excel`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_excel`

        See Also
        --------
        :meth:`pandas.DataFrame.to_excel`

        '''
        return self._to_any('excel', *args, **kwargs)

    def to_json(self, *args, **kwargs):
        '''
        Convert the table data to a JSON string

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_json`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_json`

        See Also
        --------
        :meth:`pandas.DataFrame.to_json`

        '''
        return self._to_any('json', *args, **kwargs)

    def to_html(self, *args, **kwargs):
        '''
        Render the table data to an HTML table

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_html`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_html`

        See Also
        --------
        :meth:`pandas.DataFrame.to_html`

        '''
        return self._to_any('html', *args, **kwargs)

    def to_latex(self, *args, **kwargs):
        '''
        Render the table data to a LaTeX tabular environment

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_latex`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_latex`

        See Also
        --------
        :meth:`pandas.DataFrame.to_latex`

        '''
        return self._to_any('latex', *args, **kwargs)

    def to_stata(self, *args, **kwargs):
        '''
        Write table data to Stata file

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_stata`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_stata`

        See Also
        --------
        :meth:`pandas.DataFrame.to_stata`

        '''
        return self._to_any('stata', *args, **kwargs)

    def to_msgpack(self, *args, **kwargs):
        '''
        Write table data to msgpack object

        This method writes a file on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_msgpack`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_msgpack`

        See Also
        --------
        :meth:`pandas.DataFrame.to_msgpack`

        '''
        return self._to_any('msgpack', standard_dataframe=True, *args, **kwargs)

    def to_gbq(self, *args, **kwargs):
        '''
        Write table data to a Google BigQuery table

        This method depends on data on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_gbq`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_gbq`

        See Also
        --------
        :meth:`pandas.DataFrame.to_gbq`

        '''
        return self._to_any('gbq', *args, **kwargs)

    def to_records(self, *args, **kwargs):
        '''
        Convert table data to record array

        This method writes objects on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_records`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_records`

        See Also
        --------
        :meth:`pandas.DataFrame.to_records`

        Returns
        -------
        :func:`numpy.recarray`

        '''
        return self._to_any('records', *args, **kwargs)

    def to_sparse(self, *args, **kwargs):
        '''
        Convert table data to SparseDataFrame

        This method writes an object on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_sparse`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_sparse`

        See Also
        --------
        :meth:`pandas.DataFrame.to_sparse`

        Returns
        -------
        :class:`pandas.SparseDataFrame`

        '''
        return self._to_any('sparse', *args, **kwargs)

    def to_dense(self, *args, **kwargs):
        '''
        Return dense representation of table data

        This method writes an object on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_dense`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_dense`

        See Also
        --------
        :meth:`pandas.DataFrame.to_dense`

        Returns
        -------
        :class:`pandas.SparseDataFrame`

        '''
        return self._to_any('dense', *args, **kwargs)

    def to_string(self, *args, **kwargs):
        '''
        Render the table to a console-friendly tabular output

        This method writes a string on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_string`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_string`

        See Also
        --------
        :meth:`pandas.DataFrame.to_string`

        Returns
        -------
        string

        '''
        return self._to_any('string', *args, **kwargs)

    def to_clipboard(self, *args, **kwargs):
        '''
        Write the table data to the clipboard

        This method writes the clipboard on the **client side**.  This means
        that **all of the data in the table must all be fetched**.
        If you want to save a file on the server side, use the
        ``table.save`` CAS action.

        Parameters
        ----------
        *args : positional arguments
            Positional arguments to :meth:`pandas.DataFrame.to_clipboard`
        **kwargs : keyword arguments
            Keyword arguments to :meth:`pandas.DataFrame.to_clipboard`

        See Also
        --------
        :meth:`pandas.DataFrame.to_clipboard`

        '''
        return self._to_any('clipboard', *args, **kwargs)

    # Fancy indexing

    def __setitem__(self, key, value):
        '''
        Create a new computed column

        Parameters
        ----------
        key : string
            The name of the column.
        value : :class:`CASColumn` or string or numeric or bool
            The value of the column.

        '''
        computedvars = [key]
        computedvarsprogram = []

        if value is True:
            computedvarsprogram.append('%s = 1; ' % key)

        elif value is False:
            computedvarsprogram.append('%s = 0; ' % key)

        elif value is None:
            computedvarsprogram.append('%s = .; ' % key)

        elif isinstance(value, CASColumn):
            cexpr, cvars, cpgm = value._to_expression()
            computedvarsprogram.append(cpgm)
            computedvarsprogram.append('%s = %s; ' % (key, cexpr))

        elif isinstance(value, (text_types, binary_types)):
            computedvarsprogram.append('%s = "%s"; ' % (key, _escape_string(value)))

        elif isinstance(value, numbers.Number):
            if pd.isnull(value):
                computedvarsprogram.append('%s = .; ' % key)
            else:
                computedvarsprogram.append('%s = %s; ' % (key, value))

        else:
            raise TypeError('Unrecognized type for column: %s' % type(value))

        self.append_computed_columns(computedvars, computedvarsprogram)
        self.append_columns(key)

    def __getitem__(self, key):
        '''
        Retrieve a slice of a :class:`CASTable` / :class:`CASColumn`

        Returns
        -------
        :class:`CASTable` for the following:
            tbl[collist]
            tbl[rowslice|int]
            tbl[rowslice|int|rowlist, colslice|int|colname|collist]

        :class:`CASColumn` for the following:
            tbl[colname] => CASColumn

        Scalar for the following:
            col[int]

        '''
        is_column = isinstance(self, CASColumn)

        # tbl[colname]
        if not(is_column) and isinstance(key, (text_types, binary_types)):
            columns = set([x.lower() for x in list(self.columns)])
            if key.lower() not in columns:
                raise KeyError(key)
            return self._to_column(key)

        # tbl[[colnames|colindexes]]
        if not(is_column) and isinstance(key, (list, pd.Index)):
            out = self.copy()
            columns = list(out.columns)
            colset = set([x.lower() for x in columns])
            computedvars = []
            computedvarsprogram = []
            varlist = []
            for k in key:
                if isinstance(k, int_types):
                    k = columns[k]
                if k.lower() not in colset:
                    computedvars.append(k)
                    computedvarsprogram.append('%s = .; ' % _nlit(k))
                varlist.append(k)
            out._columns = varlist
            if computedvars:
                out.append_computed_columns(computedvars, computedvarsprogram)
            return out

        # tbl[CASColumn]
        if isinstance(key, CASColumn):
            out = self.copy()
            expr, ecomputedvars, ecomputedvarsprogram = key._to_expression()

            out.append_where(expr)

            if ecomputedvars:
                out.append_computedvars(ecomputedvars)

            if ecomputedvarsprogram:
                out.append_computedvarsprogram(ecomputedvarsprogram)

            if not out._columns:
                out._columns = list(self.columns)

            return out

        # tbl[rowslice]
        if isinstance(key, slice):
            return self.iloc[key]

        # col[row]
        if is_column and isinstance(key, int_types):
            return self.iloc[key]

        # Everything else
        raise KeyError(key)

    def groupby(self, by, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False, **kwargs):
        '''
        Specify grouping variables for the table

        Parameters
        ----------
        by : string or list-of-strings
            The column names that specify the grouping variables.
        axis : int, optional
            Not implemented.
        level : int, optional
            Not implemented.
        as_index : boolean, optional
            If True, the grouping variables are set as index levels
        sort : boolean, optional
            If True, output will be sorted by group keys.
        squeeze : boolean, optional
            Not implemented.

        See Also
        --------
        :meth:`pandas.DataFrame.groupby`

        Returns
        -------
        :class:`CASTableGroupBy`

        '''
        return CASTableGroupBy(self, by, axis=axis, level=level, as_index=as_index,
                               sort=sort, group_keys=group_keys, squeeze=squeeze,
                               **kwargs)

    def query(self, expr, inplace=False, engine='cas', **kwargs):
        '''
        Query the table with a boolean expression

        Parameters
        ----------
        expr : string
            The query string to evaluate.  The expression must be a valid
            CAS expression.
        inplace : boolean, optional
            Whether the :class:`CASTable` should be modified in place, or a copy
            should be returned.
        engine : string, optional
            The type of expression used in the expression.
        **kwargs : dict, optional
            Not implemented.

        Returns
        -------
        ``None``
            If inplace == True
        :class:`CASTable` object
            If inplace == False

        '''
        if engine != 'cas':
            raise SWATError('Only CAS queries are supported at this time.')
        tbl = self
        if not inplace:
            tbl = tbl.copy()
        tbl.append_where(expr)
        return tbl

#   def where(self, cond, other=None, inplace=False, axis=None, level=None,
#             try_cast=False, raise_on_error=True):
#       if not isinstance(cond, CASColumn):
#           raise TypeError('Only CASTable conditions are supported')

#       out = self
#       if not inplace:
#           out = copy.deepcopy(out)

#       expr, ecomputedvars, ecomputedvarsprogram = cond._to_expression()

#       out.append_where(expr)

#       if ecomputedvars:
#           out.append_computedvars(ecomputedvars)

#       if ecomputedvarsprogram:
#           out.append_computedvarsprogram(ecomputedvarsprogram)

#       if not out._columns:
#           out._columns = list(self.columns)

#       if not inplace:
#           return out

    def get_groupby_vars(self):
        ''' Return a list of By group variable names '''
        out = []
        if self.has_groupby_vars():
            groups = self.get_param('groupby')
            if not isinstance(groups, items_types):
                groups = [groups]
            for grp in groups:
                if not grp:
                    continue
                if isinstance(grp, dict):
                    out.append(grp['name'])
                else:
                    out.append(grp)
        return out

    def has_groupby_vars(self):
        ''' Return True if the table has By group variables configured '''
        return self.has_param('groupby') and self.get_param('groupby')


class CharacterColumnMethods(object):
    ''' CASColumn string methods '''

    def __init__(self, column):
        self._column = column
        if column._is_numeric():
            raise TypeError('string methods are not usable on numeric columns')

    def _compute(self, *args, **kwargs):
        ''' Call the CASColumn's _compute method '''
        return self._column._compute(*args, **kwargs)

    def _get_re_flags(self, flags, case=True):
        ''' Convert regex flags to strings '''
        re_flags = ''
        if flags & re.IGNORECASE or not case:
            re_flags += 'i'
        if flags & re.LOCALE:
            re_flags += 'l'
        if flags & re.MULTILINE:
            re_flags += 'm'
        if flags & re.DOTALL:
            re_flags += 's'
        if flags & re.UNICODE:
            re_flags += 'u'
        if flags & re.VERBOSE:
            re_flags += 'x'
        return re_flags

    def capitalize(self):
        '''
        Capitalize first letter, lowercase the rest

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('capitalize',
                             'upcase(substr({value}, 1, 1)) || '
                             'lowcase(substr({value}, 2))',
                             add_length=True)

#   def cat(self, others=None, sep=None, na_rep=None, **kwargs):
#       ''' Concatenate values with given separator '''
#       return self._column._fetch().iloc[:, 0].str.cat(others=others, sep=sep,
#                                                       na_rep=na_rep, **kwargs)
#   def center(self, width, fillchar=' '):
#       return self._compute('center', 'put({value}, ${width}., -c)', width=width)

    def contains(self, pat, case=True, flags=0, na=np.nan, regex=True):
        '''
        Indicates whether the value contains the specified pattern

        Parameters
        ----------
        pat : string or :class:`CASColumn`
            The pattern to search for.
        case : boolean, optional
            If True, the pattern matching is case-sensitive.
        flags : int, optional
            Regular expression matching flags.
        na : string, optional
            Not implemented.
        regex : boolean, optional
            If True, the pattern is treated as a regular expression

        See Also
        --------
        :meth:`pandas.Series.str.contains`

        Returns
        -------
        :class:`CASColumn`

        '''
        if regex:
            if isinstance(pat, CASColumn):
                return self._compute('regex',
                                     r"prxmatch('/' || {pat} || '/%s', {value}) > 0" %
                                     self._get_re_flags(flags, case=case),
                                     pat=pat)
            return self._compute('regex',
                                 r"prxmatch('/{pat}/{flags}', {value}) > 0",
                                 pat=pat, flags=self._get_re_flags(flags, case=case),
                                 use_quotes=False)
        if case:
            return self._compute('contains', 'index({value}, {pat}) > 0',
                                 pat=pat)
        return self._compute('icontains',
                             'index(lowcase({value}), lowcase({pat})) > 0',
                             pat=pat)

    def count(self, pat, flags=0, **kwargs):
        '''
        Count occurrences of pattern in each value

        Parameters
        ----------
        pat : string or :class:`CASColumn`
            The pattern to match.
        flags : int, optional
            Not implemented.

        See Also
        --------
        :meth:`pandas.Series.str.count`

        Returns
        -------
        :class:`CASColumn`

        '''
        if flags & re.IGNORECASE:
            return self._compute('count', 'count({value}, {pat})', pat=pat)
        return self._compute('count', 'count(lowcase({value}), lowcase({pat}))', pat=pat)

    def endswith(self, pat, case=True, flags=0, na=np.nan, regex=True):
        '''
        Indicates whether the table column ends with the given pattern

        Parameters
        ----------
        pat : string or :class:`CASColumn`
            The string to search for.
        case : boolean, optional
            If True, the pattern matching is case-sensitive.
        flags : int, optional
            Regular expression flags.
        na : string, optional
            Not implemented.
        regex : boolean, optional
            If True, the pattern is considered a regular expression.

        See Also
        --------
        :meth:`pandas.Series.str.endswith`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('endswith',
                             r"prxmatch('/{pat}\s*$/{flags}', {value}) > 0",
                             pat=pat, flags=self._get_re_flags(flags, case=case),
                             use_quotes=False)

    def find(self, sub, start=0, end=None):
        '''
        Return lowest index of pattern in each value, or -1 on failure

        Parameters
        ----------
        sub : string or :class:`CASColumn`
            The pattern to locate.
        start : int, optional
            The position in the source string to start looking.
        end : int, optional
            The position in the source string to stop looking.

        See Also
        --------
        :meth:`pandas.Series.str.find`

        Returns
        -------
        :class:`CASColumn`

        '''
        if end is None:
            return self._compute('find',
                                 'find({value}, {sub}, {start}) - 1',
                                 sub=sub, start=start + 1)
        return self._compute('find',
                             'find(substr({value}, 1, {end}), {sub}, {start}) - 1',
                             sub=sub, start=start + 1, end=end - start + 1)

    def index(self, sub, start=0, end=None):
        '''
        Return lowest index of pattern in each value

        This method works the same way as :meth:`find` except that
        an exception is raised when the pattern is not found.

        Parameters
        ----------
        sub : string or :class:`CASColumn`
            The substring to search for.
        start : int, optional
            The position in the source string to start looking.
        end : int, optional
            The position in the source string to stop looking.

        See Also
        --------
        :meth:`find`
        :meth:`pandas.Series.str.index`

        Returns
        -------
        :class:`CASColumn`

        Raises
        ------
        ValueError
            If the substring is not found in a data element

        '''
        col = self.find(sub, start=start, end=end)
        if col[col < 0]._numrows:
            raise ValueError('substring not found')
        return col

    def len(self):
        '''
        Compute the length of each value

        See Also
        --------
        :meth:`pandas.Series.str.len`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('len', 'lengthn({value})')

    def lower(self):
        '''
        Lowercase the value

        See Also
        --------
        :meth:`pandas.Series.str.lower`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('lower', 'lowcase({value})', add_length=True)

    def lstrip(self, to_strip=None):
        '''
        Strip leading spaces

        Parameters
        ----------
        to_strip
            Not implemented.

        See Also
        --------
        :meth:`pandas.Series.str.lstrip`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('lstrip', 'strip({value})', add_length=True)

    def repeat(self, repeats):
        '''
        Duplicate value the specified number of times

        Parameters
        ----------
        repeats : int
            Duplicate value the specified number of times.

        See Also
        --------
        :meth:`pandas.Series.str.repeat`

        Returns
        -------
        :class:`CASColumn`

        '''
        trim = ''
        if not re.match(r'^_\w+_[A-Za-z0-9]+_$', self._column.name):
            trim = 'trim'
        return self._compute('repeat', 'repeat(%s({value}), {repeats}-1)' % trim,
                             repeats=repeats, add_length=True)

    def replace(self, pat, repl, n=-1, case=True, flags=0):
        '''
        Replace a pattern in the data

        Parameters
        ----------
        pat : string or :class:`CASColumn`
            The pattern to search for.
        repl : string or :class:`CASColumn`
            The replacement string.
        n : int, optional
            The maximum number of replacements.
        case : boolean, optional
            If True, the pattern matching is case-sensitive.
        flags : int, optional
            Regular expression flags.

        See Also
        --------
        :meth:`pandas.Series.str.replace`

        Returns
        -------
        :class:`CASColumn`

        '''
        if isinstance(pat, CASColumn) and isinstance(repl, CASColumn):
            rgx = "prxchange('s/'|| trim({pat}) ||'/' trim({repl}) || '/%s',{n},{value})"
        elif isinstance(pat, CASColumn):
            rgx = "prxchange('s/' || trim({pat}) || '/{repl}/%s',{n},{value})"
        elif isinstance(repl, CASColumn):
            rgx = "prxchange('s/{pat}/' || trim({repl}) || '/%s',{n},{value})"
        else:
            rgx = "prxchange('s/{pat}/{repl}/%s',{n},{value})"
        return self._compute('replace', rgx % self._get_re_flags(flags, case=case),
                             pat=pat, repl=repl, n=n, use_quotes=False, add_length=True)

    def rfind(self, sub, start=0, end=None):
        '''
        Return highest index of the pattern

        Parameters
        ----------
        sub : string or :class:`CASColumn`
            Substring to search for.
        start : int, optional
            Not implemented.
        end : int, optional
            Not implemented.

        See Also
        --------
        :meth:`find`
        :meth:`pandas.Series.str.rfind`

        Returns
        -------
        :class:`CASColumn`

        '''
        # TODO: start / end
        return self._compute('find',
                             'find({value}, {sub}, -lengthn({value})-1) - 1',
                             sub=sub, start=start + 1)

    def rindex(self, sub, start=0, end=None):
        '''
        Return highest index of the pattern

        Parameters
        ----------
        sub : string or :class:`CASColumn`
            Substring to search for.
        start : int, optional
            Position in the string to start the search.
        end : int, optional
            Position in the string to stop the search.

        See Also
        --------
        :meth:`index`
        :meth:`pandas.Series.str.rindex`

        Returns
        -------
        :class:`CASColumn`

        '''
        # TODO: start / end
        col = self.rfind(sub, start=start, end=end)
        if col[col < 0]._numrows:
            raise ValueError('substring not found')
        return col

    def rstrip(self, to_strip=None):
        '''
        Strip trailing whitespace

        See Also
        --------
        :meth:`strip`
        :meth:`pandas.Series.str.rstrip`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('rstrip', 'trimn({value})', add_length=True)

    def slice(self, start=0, stop=None, step=None):
        '''
        Slice a substring from the value

        Parameters
        ----------
        start : int, optional
            Starting position of the slice.
        stop : int, optional
            Ending position of the slice.
        step : int, optional
            Not implemented.

        See Also
        --------
        :meth:`pandas.Series.str.slice`

        Returns
        -------
        :class:`CAScolumn`

        '''
        # TODO: step
        if stop is None:
            stop = 'lengthn({value})+1'
        else:
            stop = stop + 1
        return self._compute('slice', 'substr({value}, {start}, {stop}-{start})',
                             start=start + 1, stop=stop, add_length=True)

    def startswith(self, pat, case=True, flags=0, na=np.nan, regex=True):
        '''
        Indicates whether the table column start with the given pattern

        Parameters
        ----------
        pat : string or :class:`CASColumn`
            The pattern to search for.
        case : boolean, optional
            If True, the matching is case-sensitive.
        flags : int, optional
            Regular expression flags.
        na : string, optional
            Not implemented.
        regex : boolean, optional
            If True, the pattern is considered a regular expression.

        See Also
        --------
        :meth:`endswith`
        :meth:`pandas.Series.str.endswith`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('startswith',
                             "prxmatch('/^{pat}/{flags}', {value}) > 0",
                             pat=pat, flags=self._get_re_flags(flags, case=case),
                             use_quotes=False)

    def strip(self, to_strip=None):
        '''
        Strip leading and trailing whitespace

        See Also
        --------
        :meth:`pandas.Series.str.strip`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('strip', 'strip({value})', add_length=True)

    def title(self):
        '''
        Capitalize each word in the value

        See Also
        --------
        :meth:`pandas.Series.str.title`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('title', 'propcase({value})', add_length=True)

    def upper(self):
        '''
        Uppercase the value

        See Also
        --------
        :meth:`pandas.Series.str.upper`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('upper', 'upcase({value})', add_length=True)

    def isalnum(self):
        '''
        Indicates whether the value contains only alphanumeric characters

        See Also
        --------
        :meth:`pandas.Series.str.isalnum`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('isalnum', 'notalnum({value}) < 1')

    def isalpha(self):
        '''
        Indicates whether the value contains only alpha characters

        See Also
        --------
        :meth:`pandas.Series.str.isalpha`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('isalpha', 'notalpha({value}) < 1')

    def isdigit(self):
        '''
        Indicates whether the value contains only digits

        See Also
        --------
        :meth:`pandas.Series.str.isdigit`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('isdigit', 'notdigit({value}) < 1')

    def isspace(self):
        '''
        Indicates whether the value contains only whitespace

        See Also
        --------
        :meth:`pandas.Series.str.isspace`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('isspace', 'notspace({value}) < 1')

    def islower(self):
        '''
        Indicates whether the value contain only lowercase characters

        See Also
        --------
        :meth:`pandas.Series.str.islower`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('islower', '(lowcase({value}) = {value})')

    def isupper(self):
        '''
        Indicates whether the value contains only uppercase characters

        See Also
        --------
        :meth:`pandas.Series.str.isupper`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('isupper', '(upcase({value}) = {value})')

    def istitle(self):
        '''
        Indicates whether the value is equivalent to the title representation

        See Also
        --------
        :meth:`pandas.Series.str.istitle`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('istitle', '(propcase({value}) = {value})')

    def isnumeric(self):
        '''
        Indicates whether the value contains a numeric representation

        See Also
        --------
        :meth:`pandas.Series.str.isnumeric`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('isnumeric', r"prxmatch('/^\s*\d+\s*$/', {value}) > 0")

    def isdecimal(self):
        '''
        Indicates whether the value contains a decimal representation

        See Also
        --------
        :meth:`pandas.Series.str.isdecimal`

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('isnumeric',
                             r"prxmatch('/^\s*(0?\.\d+|\d+(\.\d*)?)\s*$/', "
                             r'{value}) > 0')

#   def soundslike(self, arg):
#       '''
#       Does the table column sound like `arg`?

#       Parameters
#       ----------
#       arg : CASColumn or string
#           The string to compare to

#       Returns
#       -------
#       CASColumn

#       '''
#       return self._compute('soundslike', '({value} =* {arg})', arg=arg)


class SASColumnMethods(object):
    ''' CASColumn SAS methods '''

    def __init__(self, column):
        self._column = column
        self._dtype = column.dtype

    def _compute(self, *args, **kwargs):
        ''' Call the _compute method on the table column '''
        return self._column._compute(*args, **kwargs)

    def abs(self):
        '''
        Computes the absolute value

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('abs', 'abs({value})')

    def airy(self):
        '''
        Computes the value of the Airy function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('airy', 'airy({value})')

    def beta(self, param):
        '''
        Computes the value of the beta function

        Parameters
        ----------
        param : int
            Second shape parameter.

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('beta', 'beta({value}, {param})', param=param)

    def cnonct(self, df, prob):
        '''
        Computes the noncentrality parameter from a chi-square distribution

        Parameters
        ----------
        df : int
            Degrees of freedom.
        prob : float
            Probability.

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('cnonct', 'cnonct({value}, {df}, {prob})',
                             df=df, prob=prob)

#   def coalesce(self, *args):
#       '''
#       Returns the first non-missing value from a list of numeric arguments

#       Returns
#       -------
#       :class:`CASColumn`

#       '''
#       return self._compute('coalesce', 'coalesce({value}, {other})', other=other)

    def constant(self, name, parameter=None):
        '''
        Computes machine and mathematical constants

        Parameters
        ----------
        name : string
            Name of the constant value to return.  The possible names are:
                * e : the natural base
                * euler : Euler constant
                * pi : Pi
                * exactint : exact integer
                * big : largest double-precision number
                * logbig : log with respect to to base of `big`
                * sqrtbig : square root of `big`
                * small : smallest double-precision number
                * logsmall : log with respect to base of `small`
                * sqrtsmall : square root of `small`
                * maceps : machine precision constant
                * logmaceps : log with respect to base of `maceps`
                * sqrtmaceps : square root of `maceps`
        parameter : any
            Optional parameter for certain constant values.

        Returns
        -------
        :class:`CASColumn`

        '''
        if parameter is None:
            return self._compute('constant', 'constant({name})', name=name)
        return self._compute('constant', 'constant({name}, {parameter})',
                             name=name, parameter=parameter)

    def dairy(self):
        '''
        Computes the derivative of the AIRY function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('dairy', 'dairy({value})')

#   def deviance(self, distribution, *parameters):
#       '''
#       Computes the deviance based on a probability distribution

#       Parameters
#       ----------
#       distribution : string
#           Name of the distribution: bernoulli, binomial, gamma, igauss, wald,
#           normal, gaussian, or poisson.

#       Returns
#       -------
#       :class:`CASColumn`

#       '''
#       return self._compute('deviance',
#                            'deviance({distribution}, {value}, {parameters})',
#                            distribution=distribution, parameters=parameters)

    def digamma(self):
        '''
        Computes the value of the digamma function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('digamma', 'digamma({value})')

    def erf(self):
        '''
        Computes the value of the (normal) error function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('erf', 'erf({value})')

    def erfc(self):
        '''
        Computes the value of the complementary (normal) error function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('erfc', 'erfc({value})')

    def exp(self):
        '''
        Computes the value of the exponential function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('exp', 'exp({value})')

    def fact(self):
        '''
        Computes a factorial

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('fact', 'fact({value})')

    def fnonct(self, ndf, ddf, prob):
        '''
        Computes the value of the noncentrality parameter of an F distribution

        Parameters
        ----------
        ndf : int
            Numerator degree of freedom parameter.
        ddf : int
            Denominator degree of freedom parameter.
        prob : float
            Probability.

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('fnonct', 'fnonct({value}, {ndf}, {ddf}, {prob})',
                             ndf=ndf, ddf=ddf, prob=prob)

    def gamma(self):
        '''
        Computes the value of the gamma function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('gamma', 'gamma({value})')

#   def gcd(self, *args):
#       '''
#       Computes the greatest common divisor for one or more integers

#       Returns
#       -------
#       :class:`CASColumn`

#       '''
#       return self._compute('gcd', 'gcd({value})')

#   def ibessel(self, nu, kode):
#       '''
#       Computes the value of the modified Bessel function

#       Returns
#       -------
#       :class:`CASColumn`

#       '''
#       return self._compute('ibessel', 'ibessel({nu}, {value}, {kode})',
#                            nu=nu, kode=kode)

#   def jbessel(self, nu):
#       '''
#       Computes the value of the Bessel function

#       Returns
#       -------
#       :class:`CASColumn`

#       '''
#       return self._compute('jbessel', 'jbessel({nu}, {value}', nu=nu)

#   def lcm(self, *args):
#       '''
#       Computes the least common multiple

#       Returns
#       -------
#       :class:`CASColumn`

#       '''
#       return self._compute('lcm', 'lcm({value})')

    def lgamma(self):
        '''
        Computes the natural logarithm of the Gamma function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('lgamma', 'lgamma({value})')

    def log(self):
        '''
        Computes the natural (base e) logarithm

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('log', 'log({value})')

    def log1px(self):
        '''
        Computes the log of 1 plus the argument

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('log1px', 'log1px({value})')

    def log10(self):
        '''
        Computes the logarithm to the base 10

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('log10', 'log10({value})')

    def log2(self):
        '''
        Computes the logarithm to the base 2

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('log2', 'log2({value})')

    def logbeta(self, param):
        '''
        Computes the logarithm of the beta function

        Parameters
        ----------
        param : int
            Second shape parameter.

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('logbeta', 'logbeta({value}, {param})', param=param)

    def mod(self, divisor):
        '''
        Computes the remainder from the division with fuzzing

        Parameters
        ----------
        divisor : int
           Divisor.

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('mod', 'mod({value}, {divisor})', divisor=divisor)

    def modz(self, divisor):
        '''
        Computes the remainder from the division without fuzzing

        Parameters
        ----------
        divisor : int
           Divisor.

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('modz', 'modz({value}, {divisor})', divisor=divisor)

#   def msplint(self, n, *args):
#       '''
#       Returns the ordinate of a monotonicity-preserving interpolating spline

#       Returns
#       -------
#       :class:`CASColumn`

#       '''
#       return self._compute('mpsplint', 'mpsplint({value}, {n})', n=n)

    def sign(self):
        '''
        Returns the sign of a value

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('sign', 'sign({value})')

    def sqrt(self):
        '''
        Computes the square root of a value

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('sqrt', 'sqrt({value})')

    def tnonct(self, df, prob):
        '''
        Computes the noncentrality parameter from the Student's t distribution

        Parameters
        ----------
        df : int
            Degrees of freedom.
        prob : float
            Probability.

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('tnonct', 'tnonct({value}, {df}, {prob})',
                             df=df, prob=prob)

    def trigamma(self):
        '''
        Returns the value of the trigamma function

        Returns
        -------
        :class:`CASColumn`

        '''
        return self._compute('trigamma', 'trigamma({value})')


class DatetimeColumnMethods(object):
    ''' CASColumn datetime methods '''

    def __init__(self, column):
        self._column = column

        columninfo = column._columninfo

        self._dtype = columninfo['Type'][0]
        if self._dtype not in ['date', 'datetime', 'time', 'double']:
            raise TypeError('datetime methods are only usable on CAS dates, '
                            'times, datetimes, and doubles')

        fmt = columninfo['Format'][0]
        if self._dtype == 'double':
            if is_date_format(fmt):
                self._dtype = 'sas-date'
            elif is_datetime_format(fmt):
                self._dtype = 'sas-datetime'
            elif is_time_format(fmt):
                self._dtype = 'sas-time'
            else:
                raise TypeError('double columns must have a date, time, or '
                                'datetime format')

    def _compute(self, *args, **kwargs):
        ''' Call the _compute method on the table column '''
        return self._column._compute(*args, **kwargs)

    def _get_part(self, func):
        ''' Get the specified part of the datetime '''
        if self._dtype in ['date', 'sas-date']:
            if func in ['hour', 'minute']:
                return self._compute(func, '0')
            return self._compute(func, '%s({value})' % func)
        if self._dtype in ['time', 'sas-time']:
            if func in ['hour', 'minute']:
                return self._compute(func, '%s({value})' % func)
            return self._compute(func, '%s(today())' % func)
        if func in ['month', 'day', 'year', 'week', 'qtr']:
            return self._compute(func, '%s(datepart({value}))' % func)
        return self._compute(func, '%s({value})' % func)

    @property
    def year(self):
        ''' The year of the datetime '''
        return self._get_part('year')

    @property
    def month(self):
        ''' The month of the datetime January=1, December=12 '''
        return self._get_part('month')

    @property
    def day(self):
        ''' The day of the datetime '''
        return self._get_part('day')

    @property
    def hour(self):
        ''' The hour of the datetime '''
        return self._get_part('hour')

    @property
    def minute(self):
        ''' The minute of the datetime '''
        return self._get_part('minute')

    @property
    def second(self):
        ''' The second of the datetime '''
        if self._dtype in ['date', 'sas-date']:
            return self._compute('second', '0')
        return self._compute('second', 'int(second({value}))')

    @property
    def microsecond(self):
        ''' The microsecond of the datetime '''
        if self._dtype in ['date', 'sas-date']:
            return self._compute('microsecond', '0')
        return self._compute('microsecond', 'int(mod(second({value}), 1) * 1000000)')

    @property
    def nanosecond(self):
        ''' The nanosecond of the datetime (always zero) '''
        return self._compute('nanosecond', '0')

    def _get_date(self):
        ''' Return an expression that will return the date only '''
        if self._dtype in ['date', 'sas-date']:
            return '{value}'
        if self._dtype in ['time', 'sas-time']:
            return 'today()'
        return 'datepart({value})'

    @property
    def week(self):
        ''' The week ordinal of the year '''
        return self._compute('week', 'week(%s, "v")' % self._get_date())

    @property
    def weekofyear(self):
        ''' The week ordinal of the year '''
        return self.week

    @property
    def dayofweek(self):
        ''' The day of the week (Monday=0, Sunday=6) '''
        return self._compute('weekday', 'mod(weekday(%s) + 5, 7)' % self._get_date())

    @property
    def weekday(self):
        ''' The day of the week (Monday=0, Sunday=6) '''
        return self.dayofweek

    @property
    def dayofyear(self):
        ''' The ordinal day of the year '''
        return self._compute('dayofyear', 'mod(juldate(%s), 1000.)' % self._get_date())

    @property
    def quarter(self):
        ''' The quarter of the date '''
        return self._get_part('qtr')

    @property
    def is_month_start(self):
        ''' Logical indicating if first day of the month '''
        return self._compute('is_month_start', '(day(%s) = 1)' % self._get_date())

    @property
    def is_month_end(self):
        ''' Logical indicating if last day of the month '''
        return self._compute('is_month_end', '(intnx("month", %s, 0, "e") = %s)' %
                             (self._get_date(), self._get_date()))

    @property
    def is_quarter_start(self):
        ''' Logical indicating if first day of quarter '''
        return self._compute('is_quarter_start', '(intnx("qtr", %s, 0, "b") = %s)' %
                             (self._get_date(), self._get_date()))

    @property
    def is_quarter_end(self):
        ''' Logical indicating if last day of the quarter '''
        return self._compute('is_quarter_end', '(intnx("qtr", %s, 0, "e") = %s)' %
                             (self._get_date(), self._get_date()))

    @property
    def is_year_start(self):
        ''' Logical indicating if first day of the year '''
        return self._compute('is_year_start', '(intnx("year", %s, 0, "b") = %s)' %
                             (self._get_date(), self._get_date()))

    @property
    def is_year_end(self):
        ''' Logical indicating if the last day of the year '''
        return self._compute('is_year_end', '(intnx("year", %s, 0, "e") = %s)' %
                             (self._get_date(), self._get_date()))

    @property
    def daysinmonth(self):
        ''' The number of days in the month '''
        return self._compute('daysinmonth', 'day(intnx("month", %s, 0, "e"))' %
                             self._get_date())

    @property
    def days_in_month(self):
        ''' The number of days in the month '''
        return self.daysinmonth


class CASColumn(CASTable):
    '''
    Special subclass of CASTable for holding single columns

    '''

    @getattr_safe_property
    def str(self):
        ''' Accessor for string methods '''
        return CharacterColumnMethods(self)

    @getattr_safe_property
    def dt(self):
        ''' Accessor for the datetime methods '''
        return DatetimeColumnMethods(self)

    @getattr_safe_property
    def sas(self):
        ''' Accessor for the sas methods '''
        return SASColumnMethods(self)

    @getattr_safe_property
    def name(self):
        ''' Return the column name '''
        name = self._columns
        if name:
            if isinstance(name, items_types):
                name = name[0]
            return name
        raise SWATError('There is no name associated with this column.')

    @getattr_safe_property
    def dtype(self):
        ''' The data type of the underlying data '''
        return self._columninfo['Type'][0]

    @getattr_safe_property
    def ftype(self):
        ''' The data type and whether it is sparse or dense '''
        return self._columninfo['Type'][0] + ':dense'

    def xs(self, *args, **kwargs):
        ''' Only exists for CASTable '''
        raise AttributeError('xs')

    @getattr_safe_property
    def values(self):
        ''' Return column data as :func:`numpy.ndarray` '''
        return self._fetch().iloc[:, 0].values

    @getattr_safe_property
    def shape(self):
        ''' Return a tuple of the shape of the underlying data '''
        return (self._numrows,)

    @getattr_safe_property
    def ndim(self):
        ''' Return the number of dimensions of the underlying data '''
        return 1

    @getattr_safe_property
    def axes(self):
        ''' Return the row axis labels and column axis labels '''
        # TODO: Create an index proxy object
        return [[]]

    @getattr_safe_property
    def size(self):
        ''' Return the number of elements in the underlying data '''
        return self._numrows

    @getattr_safe_property
    def itemsize(self):
        ''' Return the size of the data type of the underlying data '''
        return self._columninfo['RawLength'][0]

    def isnull(self):
        ''' Return a boolean :class:`CASColumn` indicating if the values are null '''
        return self._compute('isnull', 'missing({value})')

    def notnull(self):
        ''' Return a boolean :class:`CASColumn` indicating if the values are not null '''
        return self._compute('notnull', '(missing({value}) = 0)')

    def get(self, key, default=None):
        '''
        Get item from CASColumn for the given key

        Parameters
        ----------
        key : int
            The index of the item to return.
        default : any, optional
            The value to return if the index doesn't exist.

        See Also
        --------
        :meth:`pandas.Series.get`

        Returns
        -------
        any

        '''
        out = self._fetch(from_=key + 1, to=key + 1)
        try:
            return out.at[out.index.values[0], self._columns[0]]
        except (KeyError, IndexError):
            pass
        return default

    def sort_values(self, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        '''
        Apply sort order parameters to fetches of the data in this column

        CAS tables do not have a predictable order due to the fact that the
        data may be distributed across machines in a grid.  By using the
        :meth:`sort_values` method, you are simply applying a ``sortby=``
        parameter to any ``table.fetch`` actions executed on the
        :class:`CASColumn`.  This gives the appearance of sorted data when
        it is being retrieved.

        Parameters
        ----------
        axis : int, optional
            Not implemented.
        ascending : boolean, optional
            If True, the order sort is ascending. If False, order sort is descending.
        inplace : boolean, optional
            If True, the :class:`CASColumn` is modified in place.
        kind : string, optional
            Not implemented.
        na_position : string, optional
            Not implemented.

        See Also
        --------
        :class:`CASTable.sort_values`
        :class:`pandas.Series.sort_values`

        Returns
        -------
        ``None``
            If inplace == True
        :class:`CASColumn`
            If inplace == False

        '''
        return CASTable.sort_values(self, self.name, axis=axis, ascending=ascending,
                                    inplace=inplace, kind=kind, na_position=na_position)

    sort = sort_values

    def __iter__(self):
        for item in self._generic_iter('itertuples', index=False):
            yield item[0]

    def iteritems(self, chunksize=None):
        ''' Lazily iterate over (index, value) tuples '''
        return self._generic_iter('itertuples', index=True, chunksize=chunksize)

    def _is_numeric(self):
        ''' Return boolean indicating if the data type is numeric '''
        return self.dtype not in set(['char', 'varchar', 'binary', 'varbinary'])

    def _is_character(self):
        ''' Return boolean indicating if the data type is character '''
        return self.dtype in set(['char', 'varchar', 'binary', 'varbinary'])

    def tolist(self):
        ''' Return a list of the column values '''
        return self._fetch().iloc[:, 0].tolist()

    def head(self, n=5, bygroup_as_index=True, casout=None):
        ''' Return first `n` rows of the column in a Series '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_slice(n, columns=True, ascending=True, casout=casout)
        return self.slice(start=0, stop=n, bygroup_as_index=bygroup_as_index)

    def tail(self, n=5, bygroup_as_index=True, casout=None):
        ''' Return last `n` rows of the column in a Series '''
        if self._use_casout_for_stat(casout):
            raise NotImplementedError('tail is not implement for casout yet')
            return self._get_casout_slice(n, columns=True, ascending=True, casout=casout)
        return self.slice(start=-n, stop=-1, bygroup_as_index=True)

    def slice(self, start=0, stop=None, bygroup_as_index=True, casout=None):
        ''' Return from rows from `start` to `stop` in a Series '''
        if self._use_casout_for_stat(casout):
            if stop is None:
                stop = len(self)
            return self._get_casout_slice(stop - start, columns=True, ascending=True,
                                          casout=casout, start=start)
        return CASTable.slice(self, start=start, stop=stop,
                              bygroup_as_index=bygroup_as_index)[self.name]

    def nth(self, n, dropna=False, bygroup_as_index=True, casout=None):
        ''' Return the `n`th row '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_slice(n, columns=True, ascending=True,
                                          casout=casout, start=n)
        return CASTable.nth(self, n=n, bygroup_as_index=True)

    def add(self, other, level=None, fill_value=None, axis=0):
        ''' Addition of CASColumn with other, element-wise '''
        if self._is_character():
            trim_value = ''
            trim_other = ''
            if not re.match(r'^_\w+_[A-Za-z0-9]+_$', self.name):
                trim_value = 'trim'
            if isinstance(other, CASColumn) and \
                    not re.match(r'^_\w+_[A-Za-z0-9]+_$', other.name):
                trim_other = 'trim'
            return self._compute('add', '%s({value}) || %s({other})' %
                                 (trim_value, trim_other),
                                 other=other, add_length=True)
        return self._compute('add', '({value}) + ({other})', other=other)

    def __add__(self, other):
        return self.add(other)

    def sub(self, other, level=None, fill_value=None, axis=0):
        ''' Subtraction of CASColumn with other, element-wise '''
        if self._is_character():
            raise AttributeError('sub')
        return self._compute('sub', '({value}) - ({other})', other=other)

    def __sub__(self, other):
        return self.sub(other)

    def mul(self, other, level=None, fill_value=None, axis=0):
        ''' Multiplication of CASColumn with other, element-wise '''
        if self._is_character():
            return self.str.repeat(other)
        return self._compute('mul', '({value}) * ({other})', other=other)

    def __mul__(self, other):
        return self.mul(other)

    def div(self, other, level=None, fill_value=None, axis=0):
        ''' Floating division of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('div')
        return self._compute('div', '({value}) / ({other})', other=other)

    def __div__(self, other):
        return self.div(other)

    def truediv(self, other, level=None, fill_value=None, axis=0):
        ''' Floating division of CASColumn and other, element-wise '''
        return self.div(other, level=level, fill_value=fill_value, axis=axis)

    def __truediv__(self, other):
        return self.div(other)

    def floordiv(self, other, level=None, fill_value=None, axis=0):
        ''' Integer division of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('floordiv')
        return self._compute('div', 'floor(({value}) / ({other}))', other=other)

    def __floordiv__(self, other):
        return self.floordiv(other)

    def __floor__(self, other):
        if self._is_character():
            raise AttributeError('floor')
        return self._compute('floor', 'floor({value})', other=other)

    def __ceil__(self, other):
        if self._is_character():
            raise AttributeError('ceil')
        return self._compute('ceil', 'ceil({value})', other=other)

    def __trunc__(self, other):
        if self._is_character():
            raise AttributeError('trunc')
        return self._compute('trunc', 'int({value})', other=other)

    def mod(self, other, level=None, fill_value=None, axis=0):
        ''' Modulo of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('mod')
        return self._compute('mod', 'mod({value}, {other})', other=other)

    def __mod__(self, other):
        return self.mod(other)

    def pow(self, other, level=None, fill_value=None, axis=0):
        ''' Exponential power of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('pow')
        return self._compute('pow', '({value})**({other})', other=other)

    def __pow__(self, other):
        return self.pow(other)

    def radd(self, other, level=None, fill_value=None, axis=0):
        ''' Addition of CASColumn and other, element-wise '''
        if self._is_character():
            return self._compute('radd', '{other} || {value}', other=other,
                                 add_length=True)
        return self._compute('radd', '({other}) + ({value})', other=other)

    def __radd__(self, other):
        return self.radd(other)

    def rsub(self, other, level=None, fill_value=None, axis=0):
        ''' Subtraction of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('rsub')
        return self._compute('rsub', '({other}) - ({value})', other=other)

    def __rsub__(self, other):
        return self.rsub(other)

    def rmul(self, other, level=None, fill_value=None, axis=0):
        ''' Multiplication of CASColumn and other, element-wise '''
        if self._is_character():
            return self.str.repeat(other)
        return self._compute('rmul', '({other}) * ({value})', other=other)

    def __rmul__(self, other):
        return self.rmul(other)

    def rdiv(self, other, level=None, fill_value=None, axis=0):
        ''' Floating division of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('rdiv')
        return self._compute('rdiv', '({other}) / ({value})', other=other)

    def __rdiv__(self, other):
        return self.rdiv(other)

    def rtruediv(self, other, level=None, fill_value=None, axis=0):
        ''' Floating division of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('rtruediv')
        return self._compute('rtruediv', '({other}) / ({value})', other=other)

    def __rtruediv__(self, other):
        return self.rtruediv(other)

    def rfloordiv(self, other, level=None, fill_value=None, axis=0):
        ''' Integer division of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('floordiv')
        return self._compute('div', 'floor(({other}) / ({value}))', other=other)

    def __rfloordiv__(self, other):
        return self.rfloordiv(other)

    def rmod(self, other, level=None, fill_value=None, axis=0):
        ''' Modulo of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('rmod')
        return self._compute('rmod', 'mod({other}, {value})', other=other)

    def __rmod__(self, other):
        return self.rmod(other)

    def rpow(self, other, level=None, fill_value=None, axis=0):
        ''' Exponential power of CASColumn and other, element-wise '''
        if self._is_character():
            raise AttributeError('rpow')
        return self._compute('rpow', '({other})**({value})', other=other)

    def __rpow__(self, other):
        return self.rpow(other)

    def round(self, decimals=0, out=None):
        ''' Round each value of the CASColumn to the given number of decimals '''
        if self._is_character():
            raise AttributeError('round')
        if decimals <= 0:
            decimals = 1
        else:
            decimals = 1 / (decimals * 10.)
        return self._compute('round', 'rounde({value}, {decimals})', decimals=decimals)

    def __neg__(self):
        if self._is_character():
            raise AttributeError('__neg__')
        return self._compute('negate', '(-({value}))')

    def __pos__(self):
        if self._is_character():
            raise AttributeError('__pos__')
        return self._compute('pos', '(+({value}))')

    def lt(self, other, axis=0):
        ''' Less-than comparison of CASColumn and other, element-wise '''
        return self._compare('<', other)

    def __lt__(self, other):
        return self.lt(other)

    def gt(self, other, axis=0):
        ''' Greater-than comparison of CASColumn and other, element-wise '''
        return self._compare('>', other)

    def __gt__(self, other):
        return self.gt(other)

    def le(self, other, axis=0):
        ''' Less-than-or-equal-to comparison of CASColumn and other, element-wise '''
        return self._compare('<=', other)

    def __le__(self, other):
        return self.le(other)

    def ge(self, other, axis=0):
        ''' Greater-than-or-equal-to comparison of CASColumn and other, element-wise '''
        return self._compare('>=', other)

    def __ge__(self, other):
        return self.ge(other)

    def ne(self, other, axis=0):
        ''' Not-equal-to comparison of CASColumn and other, element-wise '''
        return self._compare('^=', other)

    def __ne__(self, other):
        return self.ne(other)

    def eq(self, other, axis=0):
        ''' Equal-to comparison of CASColumn and other, element-wise '''
        return self._compare('=', other)

    def __eq__(self, other):
        return self.eq(other)

    def isin(self, values):
        ''' Return a boolean CASColumn indicating if the value is in the given values '''
        if not isinstance(values, items_types):
            values = [values]
        return self._compute('isin', '({value} in {values})',
                             values=values, eval_values=True)

    def __invert__(self):
        return self._compute('invert', '(^({value}))')

    def _compute(self, funcname, code, use_quotes=True, extra_computedvars=None,
                 extra_computedvarsprogram=None, add_length=False, dtype=None,
                 eval_values=False, **kwargs):
        '''
        Create a computed column from given expression

        Parameters
        ----------
        funcname : string
            Name for the function
        code : string
            Python string template containing computed column
            expression.  {value} will be replaced by the value
            of the current column.  All string template variables
            will be populated by `kwargs`.
        use_quotes : boolean, optional
            Use quotes around string literals
        extra_computedvars : list, optional
            Additional computed variables
        extra_computedvarsprogram : string, optional
            Additional computed program
        add_length : boolean, optional
            Add a 'length varname varchar(*)' for the output variable
        dtype : string, optional
            The output data type for the computed value
        eval_values : boolean, optional
            If True, the values of CASColumn / Series will be evaluated
            before being substituted

        Returns
        -------
        :class:`CASColumn`

        '''
        out = self.copy()

        outname = '_%s_%s_' % (funcname, self.get_connection()._gen_id())

        out._columns = [outname]
        if outname in self.get_param('computedvars', []):
            return out

        kwargs = kwargs.copy()

        computedvars = [outname]
        computedvarsprogram = []

        if dtype:
            computedvarsprogram.append('length %s %s' % (_nlit(outname), dtype))
        elif add_length:
            computedvarsprogram.append('length %s varchar(*)' % _nlit(outname))

        if extra_computedvars:
            computedvars.append(extra_computedvars)
        if extra_computedvarsprogram:
            computedvarsprogram.append(extra_computedvarsprogram)

        for key, value in six.iteritems(kwargs):
            if eval_values and isinstance(value, (CASColumn, pd.Series)):
                value = value.unique().tolist()
            if isinstance(value, CASColumn):
                aexpr, acomputedvars, acomputedvarsprogram = value._to_expression()
                computedvars.append(acomputedvars)
                computedvarsprogram.append(acomputedvarsprogram)
                kwargs[key] = aexpr
            elif use_quotes and isinstance(value, (text_types, binary_types)):
                kwargs[key] = '"%s"' % _escape_string(value)
            elif isinstance(value, items_types):
                items = []
                for item in value:
                    if eval_values and isinstance(item, (CASColumn, pd.Series)):
                        for subitem in item.unique().tolist():
                            if isinstance(subitem, (text_types, binary_types)):
                                items.append('"%s"' % _escape_string(subitem))
                            else:
                                items.append(str(subitem))
                    elif isinstance(item, CASColumn):
                        aexpr, acomputedvars, acomputedvarsprogram = item._to_expression()
                        computedvars.append(acomputedvars)
                        computedvarsprogram.append(acomputedvarsprogram)
                        items.append(aexpr)
                    elif isinstance(item, (text_types, binary_types)):
                        items.append('"%s"' % _escape_string(item))
                    else:
                        items.append(str(item))
                if items:
                    kwargs[key] = '(%s)' % ', '.join(items)
            else:
                kwargs[key] = str(value)

        kwargs['value'] = _nlit(self.name)
        kwargs['out'] = _nlit(outname)

        if '{out}' not in code:
            code = '{out} = %s' % code

        if not re.search(r';\s*$', code):
            code = '%s; ' % code

        computedvarsprogram.append(code.format(**kwargs))

        out.append_computed_columns(computedvars, computedvarsprogram)

        return out

    def _to_expression(self):
        ''' Convert CASColumn to an expression '''
        return (_nlit(self.name),
                self.get_param('computedvars', []),
                self.get_param('computedvarsprogram', ''))

    def __and__(self, arg):
        return self._compare('and', arg)

    def __or__(self, arg):
        return self._compare('or', arg)

    def _compare(self, operator, other):
        ''' Compare CASColumn to other using given operator '''
        left = self
        right = other

        # Left side
        left, lcomputedvars, lcomputedvarsprogram = left._to_expression()

        computedvars = []
        computedvarsprogram = []

        # Right side
        if isinstance(right, CASColumn):
            right, rcomputedvars, rcomputedvarsprogram = right._to_expression()
            computedvars.append(rcomputedvars)
            computedvarsprogram.append(rcomputedvarsprogram)
        elif isinstance(right, (text_types, binary_types)):
            right = repr(right)

        opname = OPERATOR_NAMES.get(operator, operator)
        col = self._compute(opname, '(%s %s %s)' % (str(left), operator, str(right)),
                            extra_computedvars=computedvars,
                            extra_computedvarsprogram=computedvarsprogram)
        return col

    def abs(self):
        ''' Return absolute values element-wise '''
        if self._is_character():
            raise TypeError("bad operand type for abs(): 'str'")
        return self._compute('abs', 'abs({value})')

    def all(self, axis=None, bool_only=None, skipna=None, level=None, **kwargs):
        ''' Return whether all elements are True '''
        numrows = self._numrows
        col = self.copy()
        if self._is_character():
            col.append_where('lengthn(%s) ^= 0' % _nlit(col.name))
        else:
            col.append_where('(%s) ^= 0' % _nlit(col.name))
        return col._numrows == numrows

    def any(self, axis=None, bool_only=None, skipna=None, level=None, **kwargs):
        ''' Return True for each column with one or more element treated as true '''
        col = self.copy()
        if self._is_character():
            col.append_where('lengthn(%s) ^= 0' % _nlit(col.name))
        else:
            col.append_where('(%s) ^= 0' % _nlit(col.name))
        return col._numrows > 0

    def between(self, left, right, inclusive=True):
        ''' Return boolean CASColumn equivalent to left <= value <= right '''
        if inclusive:
            return self._compute('between',
                                 '({left} <= {value}) and ({value} <= {right})',
                                 left=left, right=right)
        return self._compute('between', '({left} < {value}) and ({value} < {right})',
                             left=left, right=right)

    def clip(self, lower=None, upper=None, out=None, axis=0):
        ''' Trim values at input threshold(s) '''
        if lower is not None and upper is not None:
            return self._compute('clip', 'min({upper}, max({lower}, {value}))',
                                 upper=upper, lower=lower)
        elif lower is not None:
            return self._compute('clip_lower', 'max({lower}, {value})', lower=lower)
        elif upper is not None:
            return self._compute('clip_upper', 'min({upper}, {value})', upper=upper)
        return self.copy()

    def clip_lower(self, threshold, axis=0):
        ''' Trim values below given threshold '''
        return self.clip(lower=threshold)

    def clip_upper(self, threshold, axis=0):
        ''' Trim values above given threshold '''
        return self.clip(upper=threshold)

    def _to_table(self):
        ''' Convert CASColumn object to a CASTable object '''
        column = self.copy()

        table = CASTable(**column.to_params())

        try:
            table.set_connection(column.get_connection())
        except SWATError:
            pass

        return table

    def _combine(self, *others):
        ''' Combine CASColumn objects into a CASTable object '''
        tbl = self._to_table()
        for item in others:
            tbl.append_columns(item._columns)
            tbl.append_computedvars(item.get_param('computedvars', []))
            tbl.append_computedvarsprogram(item.get_param('computedvarsprogram', ''))
            tbl.append_where(item.get_param('where', ''))
        if not tbl.get_param('computedvars', None):
            tbl.del_param('computedvars')
        if not tbl.get_param('computedvarsprogram', None):
            tbl.del_param('computedvarsprogram')
        if not tbl.get_param('where', None):
            tbl.del_param('where')
        return tbl

    def corr(self, other, method='pearson', min_periods=None):
        '''
        Compute correlation with other column

        See Also
        --------
        :meth:`CASTable.corr`
        :meth:`pandas.Series.corr`

        Returns
        -------
        :class:`pandas.Series`

        '''
        return self._combine(other).corr().iloc[0, 1]

    def count(self, level=None):
        '''
        Return the number of non-NA/null observations in the CASColumn

        See Also
        --------
        :meth:`pandas.Series.count`

        Returns
        -------
        int
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        out = CASTable.count(self, level=level)
        if isinstance(out, pd.DataFrame):
            return out[self.name].astype(np.int64)
        return out.iat[0]

    def describe(self, percentiles=None, include=None, exclude=None, stats=None):
        '''
        Generate various summary statistics

        See Also
        --------
        :meth:`CASTable.describe`
        :meth:`pandas.Series.describe`

        Returns
        -------
        :class:`pandas.Series`

        '''
        return CASTable.describe(self, percentiles=percentiles, include=include,
                                 exclude=exclude, stats=stats).iloc[:, 0]

    def _get_summary_stat(self, name):
        '''
        Run simple.summary and get the given statistic

        Parameters
        ----------
        name : string
            The name of the simple.summary column

        Returns
        -------
        :class:`pandas.Series`
            for single index output
        :class:`pandas.DataFrame`
            for multi-index output

        '''
        return CASTable._get_summary_stat(self, name)[self.name]

    def max(self, axis=None, skipna=True, level=None, casout=None, **kwargs):
        '''
        Return the maximum value

        See Also
        --------
        :meth:`CASTable.max`
        :meth:`pandas.Series.max`

        Returns
        -------
        any scalar
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('max', axis=axis, skipna=skipna, level=level,
                                         casout=casout, **kwargs)

        out = self._topk_values('max', axis=axis, skipna=skipna, level=level,
                                **kwargs)

        if self.get_groupby_vars():
            return out[self.name]

        return out.at[self.name]

    def mean(self, axis=None, skipna=True, level=None, casout=None, **kwargs):
        '''
        Return the mean value

        See Also
        --------
        :meth:`CASTable.mean`
        :meth:`pandas.Series.mean`

        Returns
        -------
        numeric
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('mean', axis=axis, skipna=skipna, level=level,
                                         casout=casout, **kwargs)

        return self._get_summary_stat('mean')

    def median(self, q=0.5, axis=0, interpolation='nearest', casout=None):
        '''
        Return the median value

        See Also
        --------
        :meth:`CASTable.median`
        :meth:`pandas.Series.median`

        Returns
        -------
        any scalar
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('median', axis=axis, casout=casout)

        return self.quantile(0.5, axis=axis, interpolation='nearest')

    def min(self, axis=None, skipna=True, level=None, casout=None, **kwargs):
        '''
        Return the minimum value

        See Also
        --------
        :meth:`CASTable.min`
        :meth:`pandas.Series.min`

        Returns
        -------
        any scalar
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('min', axis=axis, skipna=skipna, level=level,
                                         casout=casout, **kwargs)

        out = self._topk_values('min', axis=axis, skipna=skipna, level=level, **kwargs)

        if self.get_groupby_vars():
            return out[self.name]

        return out.at[self.name]

    def mode(self, axis=0, max_tie=100):
        '''
        Return the mode values

        See Also
        --------
        :meth:`CASTable.mode`
        :meth:`pandas.Series.mode`

        Returns
        -------
        :class:`pandas.Series`

        '''
        return CASTable.mode(self, axis=axis, max_tie=max_tie)[self.name]

    def quantile(self, q=0.5, axis=0, interpolation='nearest', casout=None):
        '''
        Return the value at the given quantile

        See Also
        --------
        :meth:`CASTable.quantile`
        :meth:`pandas.Series.quantile`

        Returns
        -------
        any scalar
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            if not isinstance(q, items_types):
                q = [q]
            q = [x * 100 for x in q]
            return self._get_casout_stat('percentile', axis=axis, casout=casout,
                                         percentile_values=q)

        return CASTable.quantile(self, q=q, axis=axis, numeric_only=False,
                                 interpolation=interpolation)[self.name]

    def sum(self, axis=None, skipna=None, level=None, casout=None):
        '''
        Return the sum of the values

        See Also
        --------
        :meth:`CASTable.sum`
        :meth:`pandas.Series.sum`

        Returns
        -------
        any scalar
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('sum', axis=axis, skipna=skipna, level=level,
                                         casout=casout)

        return self._get_summary_stat('sum')

    def nlargest(self, n=5, keep='first', casout=None):
        '''
        Return the n largest values

        See Also
        --------
        :meth:`CASTable.nlargest`
        :meth:`pandas.Series.nlargest`

        Returns
        -------
        :class:`pandas.Series`

        '''
        if self._use_casout_for_stat(casout):
            raise NotImplementedError('nlargest is not implemented for casout yet')
            return self._get_casout_slice(n, columns=[self.name],
                                          ascending=False, casout=casout)
        return self.sort_values([self.name], ascending=False).slice(0, n)

    def nsmallest(self, n=5, keep='first', casout=None):
        '''
        Return the n smallest values

        See Also
        --------
        :meth:`CASTable.nsmallest`
        :meth:`pandas.Series.nsmallest`

        Returns
        -------
        :class:`pandas.Series`

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_slice(n, columns=[self.name],
                                          ascending=True, casout=casout)
        return self.sort_values([self.name], ascending=True).slice(0, n)

    def std(self, axis=None, skipna=None, level=None, ddof=1, casout=None):
        '''
        Return the standard deviation of the values

        See Also
        --------
        :meth:`CASTable.std`
        :meth:`pandas.Series.std`

        Returns
        -------
        any scalar
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('std', axis=axis, skipna=skipna, level=level,
                                         casout=casout)

        return self._get_summary_stat('std')

    def var(self, axis=None, skipna=None, level=None, ddof=1, casout=None):
        '''
        Return the unbiased variance of the values

        See Also
        --------
        :meth:`CASTable.var`
        :meth:`pandas.Series.var`

        Returns
        -------
        any scalar
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('var', axis=axis, skipna=skipna, level=level,
                                         casout=casout)

        return self._get_summary_stat('var')

    def unique(self, casout=None):
        '''
        Return array of unique values in the CASColumn

        See Also
        --------
        :meth:`CASTable.unique`
        :meth:`pandas.Series.unique`

        Returns
        -------
        :func:`numpy.ndarray`
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('unique', casout=casout)

        tmpname = str(uuid.uuid4())
        out = self._frequencies(includemissing=True)

        if len(out.index.names) > 1:
            names = list(out.index.names)
            out.name = tmpname
            var = names.pop()
            out = out.reset_index()
            del out[tmpname]
            return out.groupby(names)[var].unique()

        return pd.Series(out.index, name=self.name).values

    def nunique(self, dropna=True, casout=None):
        '''
        Return number of unique elements in the CASColumn

        See Also
        --------
        :meth:`CASTable.nunique`
        :meth:`pandas.Series.nunique`

        Returns
        -------
        int
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('nunique', skipna=dropna, casout=casout)
        return self._topk_values('unique', skipna=dropna)[self.name]

    @getattr_safe_property
    def is_unique(self):
        ''' Return boolean indicating if the values in the CASColumn are unique '''
        is_unique = self.value_counts(dropna=False) == 1
        if self.get_groupby_vars():
            return is_unique
        return is_unique.iat[0]

    def _frequencies(self, includemissing=False):
        '''
        Compute frequencies taking groupby into account

        Parameters
        ----------
        includemissing: boolean, optional
            Should missing values be included in the frequency counts?

        Returns
        -------
        Series

        '''
        bygroup_columns = 'raw'
        out = self._retrieve('simple.freq', inputs=self._columns,
                             includemissing=includemissing).get_tables('Frequency')
        out = [x.reshape_bygroups(bygroup_columns=bygroup_columns,
                                  bygroup_as_index=True) for x in out]
        out = pd.concat(out)

        if 'CharVar' in out.columns:
            out.rename(columns=dict(CharVar=self.name), inplace=True)
        else:
            out.rename(columns=dict(NumVar=self.name), inplace=True)

        out.set_index(self.name, append=self.has_groupby_vars(), inplace=True)

        out = out['Frequency'].astype(np.int64)
        out.name = self.name
        return out

    def value_counts(self, normalize=False, sort=True, ascending=False,
                     bins=None, dropna=True, casout=None):
        '''
        Return object containing counts of unique values

        Parameters
        ----------
        normalize : boolean, optional
            If True, the relative frequencies are normalized to 1.
        sort : boolean, optional
            Sort by values.
        ascending : boolean, optional
            If True, sort in ascending order.
        bins : int, optional
            Not implemented.
        dropna : boolean, optional
            If True, do not include missing values.

        See Also
        --------
        :meth:`pandas.Series.value_counts`

        Returns
        -------
        :class:`pandas.Series`

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('n', skipna=dropna, casout=casout)

        tmpname = str(uuid.uuid4())
        out = self._frequencies(includemissing=not dropna)

        # Drop NaN indexes / data
        if dropna:
            indexes = list(out.index.names)
            out.name = tmpname
            out = out.reset_index()
            out.dropna(inplace=True)
            out.set_index(indexes, inplace=True)
            out = out[tmpname]

        # Normalize data / groups to 1
        if normalize:
            groups = self.get_groupby_vars()
            if groups:
                out.name = tmpname
                sum = out.sum(level=list(range(len(out.index.names) - 1))).to_frame()
                out = out.reset_index(level=-1)
                out = pd.merge(out, sum, left_index=True, right_index=True, how='inner')
                out[tmpname] = out[tmpname + '_x'] / out[tmpname + '_y']
                out.set_index(self.name, append=True, inplace=True)
                out = out[tmpname]
            else:
                out = out / out.sum()

        # Prep for sorting
        out.name = tmpname
        indexes = list(out.index.names)
        columns = [out.name]
        out = out.to_frame()
        out.reset_index(inplace=True)

        # Sort (at least by the index)
        if sort:
            out.sort_values(indexes[:-1] + columns, inplace=True,
                            ascending=([True] * len(indexes[:-1])) + [ascending])
        else:
            out.sort_values(indexes, inplace=True,
                            ascending=([True] * len(indexes)))

        # Set indexes and names
        out.set_index(indexes, inplace=True)
        out = out[out.columns[0]]
        out.index.name = None
        out.name = None

        return out

    # Not DataFrame methods, but they are available statistics.

    def nmiss(self, casout=None):
        '''
        Return number of missing values

        See Also
        --------
        :meth:`CASTable.nmiss`

        Returns
        -------
        int
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('nmiss', casout=casout)

        if self.get_groupby_vars():
            return CASTable.nmiss(self)[self.name]
        return CASTable.nmiss(self).iloc[0]

    def stderr(self, casout=None):
        '''
        Return standard error of the values

        See Also
        --------
        :meth:`CASTable.stderr`

        Returns
        -------
        float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('stderr', casout=casout)

        return self._get_summary_stat('stderr')

    def uss(self, casout=None):
        '''
        Return uncorrected sum of squares of the values

        See Also
        --------
        :meth:`CASTable.uss`

        Returns
        -------
        int or float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('uss', casout=casout)

        return self._get_summary_stat('uss')

    def css(self, casout=None):
        '''
        Return corrected sum of squares of the values

        See Also
        --------
        :meth:`CASTable.css`

        Returns
        -------
        int or float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('css', casout=casout)

        return self._get_summary_stat('css')

    def cv(self, casout=None):
        '''
        Return coefficient of variation of the values

        See Also
        --------
        :meth:`CASTable.cv`

        Returns
        -------
        float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('cv', casout=casout)

        return self._get_summary_stat('cv')

    def tvalue(self, casout=None):
        '''
        Return value of T-statistic for hypothetical testing

        See Also
        --------
        :meth:`CASTable.tvalue`

        Returns
        -------
        float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('tstat', casout=casout)

        return self._get_summary_stat('tvalue')

    def probt(self, casout=None):
        '''
        Return p-value of the T-statistic

        See Also
        --------
        :meth:`CASTable.probt`

        Returns
        -------
        float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('probt', casout=casout)

        return self._get_summary_stat('probt')

    def skewness(self, casout=None):
        '''
        Return skewness

        See Also
        --------
        :meth:`CASTable.skewness`

        Returns
        -------
        float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('skew', casout=casout)

        return self._get_summary_stat('skewness')

    skew = skewness

    def kurtosis(self, casout=None):
        '''
        Return kurtosis

        See Also
        --------
        :meth:`CASTable.kurtosis`

        Returns
        -------
        float
            If no By groups are specified.
        :class:`pandas.Series`
            If By groups are specified.

        '''
        if self._use_casout_for_stat(casout):
            return self._get_casout_stat('kurt', casout=casout)

        return self._get_summary_stat('kurtosis')

    kurt = kurtosis

    # Serialization / IO / Conversion

    @classmethod
    def from_csv(cls, connection, path, header=0, sep=',', index_col=0, parse_dates=True,
                 tupleize_cols=False, infer_datetime_format=False, casout=None, **kwargs):
        ''' Create a CASColumn from a CSV file '''
        return connection.read_csv(path, header=header, sep=sep, index_col=index_col,
                                   parse_dates=parse_dates, tupleize_cols=tupleize_cols,
                                   infer_datetime_format=infer_datetime_format,
                                   casout=casout,
                                   **kwargs)._to_column()

    def to_series(self, *args, **kwargs):
        '''
        Retrieve all elements into a Series

        Parameters
        ----------
        **kwargs : any
            Keyword parameters sent to the ``fetch`` action.

        Returns
        -------
        :class:`pandas.Series`

        '''
        return self._fetchall(**kwargs)[self.name]

    def _to_any(self, method, *args, **kwargs):
        ''' Generic converter to various forms '''
        kwargs = kwargs.copy()
        sort = kwargs.pop('sort', False)
        sample_pct = kwargs.pop('sample_pct', None)
        sample_seed = kwargs.pop('sample_seed', None)
        sample = kwargs.pop('sample', None)
        stratify_by = kwargs.pop('stratify_by', None)
        grouped = kwargs.pop('grouped', None)
        sortby = None
        if sort:
            if sort is True:
                sortby = [dict(name=self.name)]
            elif isinstance(sort, (text_types, binary_types)):
                sortby = [dict(name=self.name, order=sort)]
            else:
                sortby = sort
        out = self._fetch(sample_pct=sample_pct, sample_seed=sample_seed,
                          sample=sample, stratify_by=stratify_by,
                          sortby=sortby, grouped=grouped)[self.name]
        return getattr(out, 'to_' + method)(*args, **kwargs)

    def to_frame(self, *args, **kwargs):
        '''
        Convert :class:`CASColumn` to a :class:`pandas.DataFrame`

        Parameters
        ----------
        *args : one or more objects
            Positional parameters.
        **kwargs : any
            Keyword parameters passed to :meth:`pandas.Series.to_frame`.

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        return self._to_any('frame', *args, **kwargs)

    def to_xarray(self, *args, **kwargs):
        '''
        Return an xarray object from the CASColumn

        Parameters
        ----------
        *args : one or more objects
            Positional parameters.
        **kwargs : any
            Keyword parameters passed to :meth:`pandas.Series.to_xarray`.

        Returns
        -------
        ``xarray``

        '''
        return self._to_any('xarray', *args, **kwargs)


class CASTableGroupBy(object):
    '''
    Group CASTable / CASColumn objects by specified values

    Parameters
    ----------
    table : CASTable or CASColumn
        The CASTable / CASColumn to group.
    by : string or list-of-strings
        The column name(s) that specify the group values.
    axis : int, optional
        Unsupported.
    level : int or level-name, optional
        Unsupported.
    as_index : boolean, optional
        If True, the group labels become the index in the output.
    sort : boolean, optional
        If True, the output is sorted by the group keys.
    group_keys : boolean, optional
        Unsupported.
    squeeze : boolean, optional
        Unsupported.

    Returns
    -------
    :class:`CASTableGroupBy`

    '''

    def __init__(self, table, by, axis=0, level=None, as_index=True, sort=True,
                 group_keys=True, squeeze=False, **kwargs):
        if isinstance(by, items_types):
            self._by = list(by)
        else:
            self._by = [by]

        new_by = []
        for item in self._by:
            if isinstance(item, CASColumn):
                item = item.name
            new_by.append(item)
        by = new_by

        self._table = table.copy()
        self._table.append_groupby(by)
        self._sort = sort
        self._plot = CASTablePlotter(self._table)
        self._as_index = as_index

    def __iter__(self):
        tbl = self._table.copy(exclude='groupby')
        groupby = tbl._retrieve('simple.groupby', inputs=self._by)['Groupby']
        groupby = groupby[self._by].to_records(index=False)
        for group in groupby:
            yield tuple(group), self.get_group(group)

    def __getitem__(self, name):
        out = self._table[name]
        if isinstance(out, (CASTable, CASColumn)):
            out = type(self)(out.copy(), list(self._by),
                             as_index=self._as_index, sort=self._sort)
        return out

    def __getattr__(self, name):
        out = getattr(self._table, name)
        if isinstance(out, (CASTable, CASColumn)):
            out = type(self)(out.copy(), list(self._by),
                             as_index=self._as_index, sort=self._sort)
        return out

    def get_group(self, name, obj=None):
        '''
        Construct a CASTable / CASColumn with the given group key

        Parameters
        ----------
        name : any or tuple-of-anys
            The groupby value (or tuple of values for multi-level groups)
        obj : CASTable or CASColumn, optional
            The CASTable / CASColumn to use instead of self

        Returns
        -------
        :class:`CASTable` or :class:`CASColumn`

        '''
        if obj is None:
            obj = self

        grptbl = obj._table.copy()

        for key, value in zip(self._by, name):

            if pd.isnull(value):
                grptbl.append_where('%s = .' % _nlit(key))
            else:
                if isinstance(value, (text_types, binary_types)):
                    value = '"%s"' % _escape_string(value)
                else:
                    value = str(value)
                grptbl.append_where('%s = %s' % (_nlit(key), value))

        return grptbl

    def get_groupby_vars(self):
        '''
        Get groupby variables from table

        Returns
        -------
        list of strings

        '''
        return self._table.get_groupby_vars()

    @getattr_safe_property
    def plot(self):
        ''' Plot using groups '''
        return self._plot

    def head(self, *args, **kwargs):
        '''
        Retrieve first values of each group

        See Also
        --------
        :class:`CASTable.head`
        :class:`CASColumn.head`

        '''
        kwargs = kwargs.copy()
        kwargs.setdefault('bygroup_as_index', isinstance(self._table, CASColumn))
        return self._table.head(*args, **kwargs)

    def tail(self, *args, **kwargs):
        '''
        Retrieve last values of each group

        See Also
        --------
        :class:`CASTable.tail`
        :class:`CASColumn.tail`

        '''
        kwargs = kwargs.copy()
        kwargs.setdefault('bygroup_as_index', isinstance(self._table, CASColumn))
        return self._table.tail(*args, **kwargs)

    def slice(self, *args, **kwargs):
        '''
        Retrieve requested values of each group

        See Also
        --------
        :class:`CASTable.slice`
        :class:`CASColumn.slice`

        '''
        kwargs = kwargs.copy()
        kwargs.setdefault('bygroup_as_index', isinstance(self._table, CASColumn))
        return self._table.slice(*args, **kwargs)

    def to_frame(self, **kwargs):
        '''
        Retrieve all values into a DataFrame

        See Also
        --------
        :class:`CASTable.to_frame`
        :class:`CASColumn.to_frame`

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        if isinstance(self._table, CASColumn):
            tbl = self._table._to_table()
        else:
            tbl = self._table.copy()
        if self._as_index:
            from ..dataframe import concat
            groups = tbl.get_groupby_vars()
            tbl.append_columns(*groups, inplace=True)
            out = tbl.to_frame(grouped=True, **kwargs)
            return concat([x[1].set_index(groups) for x in out])
        return tbl.to_frame(grouped=False, **kwargs)

    def to_series(self, name=None, **kwargs):
        '''
        Retrieve all values into a Series

        See Also
        --------
        :class:`CASColumn.to_series`

        Returns
        -------
        :class:`pandas.Series`

        '''
        columns = list(self._table.columns)
        if len(columns) > 1:
            raise ValueError('Too many columns to convert to a Series')
        out = self.to_frame(**kwargs)[columns[0]]
        if name is not None:
            out.name = name
        return out

    def nth(self, n, dropna=None, **kwargs):
        '''
        Return the nth row from each group

        Parameters
        ----------
        n : int or list-of-ints
            The rows to select.

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        kwargs = kwargs.copy()
        kwargs.setdefault('bygroup_as_index', False)
        return self._table.nth(n=n, dropna=dropna, **kwargs)

    def unique(self, *args, **kwargs):
        '''
        Get unique values using groups

        See Also
        --------
        :class:`CASTable.unique`
        :class:`CASColumn.unique`

        '''
        if self._as_index:
            return self._table.unique(*args, **kwargs)
        return self._table.unique(*args, **kwargs).reset_index(self.get_groupby_vars())

    def nunique(self, *args, **kwargs):
        '''
        Get number of unique values using groups

        See Also
        --------
        :class:`CASTable.nunique`
        :class:`CASColumn.nunique`

        '''
        if self._as_index:
            return self._table.nunique(*args, **kwargs)
        return self._table.nunique(*args, **kwargs).reset_index(self.get_groupby_vars())

    def value_counts(self, *args, **kwargs):
        '''
        Get value counts using groups

        See Also
        --------
        :class:`CASTable.value_counts`
        :class:`CASColumn.value_counts`

        '''
        if self._as_index:
            return self._table.value_counts(*args, **kwargs)
        return self._table.value_counts(*args,
                                        **kwargs).reset_index(self.get_groupby_vars())

    def max(self, *args, **kwargs):
        '''
        Get maximum values using groups

        See Also
        --------
        :class:`CASTable.max`
        :class:`CASColumn.max`

        '''
        if self._as_index:
            return self._table.max(*args, **kwargs)
        return self._table.max(*args, **kwargs).reset_index(self.get_groupby_vars())

    def mean(self, *args, **kwargs):
        '''
        Get mean values using groups

        See Also
        --------
        :class:`CASTable.mean`
        :class:`CASColumn.mean`

        '''
        if self._as_index:
            return self._table.mean(*args, **kwargs)
        return self._table.mean(*args, **kwargs).reset_index(self.get_groupby_vars())

    def min(self, *args, **kwargs):
        '''
        Get minimum values using groups

        See Also
        --------
        :class:`CASTable.min`
        :class:`CASColumn.min`

        '''
        if self._as_index:
            return self._table.min(*args, **kwargs)
        return self._table.min(*args, **kwargs).reset_index(self.get_groupby_vars())

    def median(self, *args, **kwargs):
        '''
        Get median values using groups

        See Also
        --------
        :class:`CASTable.median`
        :class:`CASColumn.median`

        '''
        if self._as_index:
            return self._table.median(*args, **kwargs)
        return self._table.median(*args, **kwargs).reset_index(self.get_groupby_vars())

    def mode(self, *args, **kwargs):
        '''
        Get mode values using groups

        See Also
        --------
        :class:`CASTable.mode`
        :class:`CASColumn.mode`

        '''
        if self._as_index:
            return self._table.mode(*args, **kwargs)
        return self._table.mode(*args, **kwargs).reset_index(self.get_groupby_vars())

    def quantile(self, *args, **kwargs):
        '''
        Get quantiles using groups

        See Also
        --------
        :class:`CASTable.quantile`
        :class:`CASColumn.quantile`

        '''
        if self._as_index:
            return self._table.quantile(*args, **kwargs)
        return self._table.quantile(*args, **kwargs).reset_index(self.get_groupby_vars())

    def sum(self, *args, **kwargs):
        '''
        Get sum using groups

        See Also
        --------
        :class:`CASTable.sum`
        :class:`CASColumn.sum`

        '''
        if self._as_index:
            return self._table.sum(*args, **kwargs)
        return self._table.sum(*args, **kwargs).reset_index(self.get_groupby_vars())

    def std(self, *args, **kwargs):
        '''
        Get std using groups

        See Also
        --------
        :class:`CASTable.std`
        :class:`CASColumn.std`

        '''
        if self._as_index:
            return self._table.std(*args, **kwargs)
        return self._table.std(*args, **kwargs).reset_index(self.get_groupby_vars())

    def var(self, *args, **kwargs):
        '''
        Get var using groups

        See Also
        --------
        :class:`CASTable.var`
        :class:`CASColumn.var`

        '''
        if self._as_index:
            return self._table.var(*args, **kwargs)
        return self._table.var(*args, **kwargs).reset_index(self.get_groupby_vars())

    def nmiss(self, *args, **kwargs):
        '''
        Get nmiss using groups

        See Also
        --------
        :class:`CASTable.nmiss`
        :class:`CASColumn.nmiss`

        '''
        if self._as_index:
            return self._table.nmiss(*args, **kwargs)
        return self._table.nmiss(*args, **kwargs).reset_index(self.get_groupby_vars())

    def stderr(self, *args, **kwargs):
        '''
        Get stderr using groups

        See Also
        --------
        :class:`CASTable.stderr`
        :class:`CASColumn.stderr`

        '''
        if self._as_index:
            return self._table.stderr(*args, **kwargs)
        return self._table.stderr(*args, **kwargs).reset_index(self.get_groupby_vars())

    def uss(self, *args, **kwargs):
        '''
        Get uss using groups

        See Also
        --------
        :class:`CASTable.uss`
        :class:`CASColumn.uss`

        '''
        if self._as_index:
            return self._table.uss(*args, **kwargs)
        return self._table.uss(*args, **kwargs).reset_index(self.get_groupby_vars())

    def css(self, *args, **kwargs):
        '''
        Get css using groups

        See Also
        --------
        :class:`CASTable.css`
        :class:`CASColumn.css`

        '''
        if self._as_index:
            return self._table.css(*args, **kwargs)
        return self._table.css(*args, **kwargs).reset_index(self.get_groupby_vars())

    def cv(self, *args, **kwargs):
        '''
        Get cv using groups

        See Also
        --------
        :class:`CASTable.cv`
        :class:`CASColumn.cv`

        '''
        if self._as_index:
            return self._table.cv(*args, **kwargs)
        return self._table.cv(*args, **kwargs).reset_index(self.get_groupby_vars())

    def tvalue(self, *args, **kwargs):
        '''
        Get tvalue using groups

        See Also
        --------
        :class:`CASTable.tvalue`
        :class:`CASColumn.tvalue`

        '''
        if self._as_index:
            return self._table.tvalue(*args, **kwargs)
        return self._table.tvalue(*args, **kwargs).reset_index(self.get_groupby_vars())

    def probt(self, *args, **kwargs):
        '''
        Get probt using groups

        See Also
        --------
        :class:`CASTable.probt`
        :class:`CASColumn.probt`

        '''
        if self._as_index:
            return self._table.probt(*args, **kwargs)
        return self._table.probt(*args, **kwargs).reset_index(self.get_groupby_vars())

    def skewness(self, *args, **kwargs):
        '''
        Get skewness using groups

        See Also
        --------
        :class:`CASTable.skewness`
        :class:`CASColumn.skewness`

        '''
        if self._as_index:
            return self._table.skewness(*args, **kwargs)
        return self._table.skewness(*args, **kwargs).reset_index(self.get_groupby_vars())

    skew = skewness

    def kurtosis(self, *args, **kwargs):
        '''
        Get kurtosis using groups

        See Also
        --------
        :class:`CASTable.kurtosis`
        :class:`CASColumn.kurtosis`

        '''
        if self._as_index:
            return self._table.kurtosis(*args, **kwargs)
        return self._table.kurtosis(*args, **kwargs).reset_index(self.get_groupby_vars())

    kurt = kurtosis

    def describe(self, *args, **kwargs):
        '''
        Get basic statistics using groups

        See Also
        --------
        :class:`CASTable.describe`
        :class:`CASColumn.describe`

        '''
        out = self._table.describe(*args, **kwargs)

        if self._as_index:
            if pd_version >= (0, 20, 0):
                out = out.unstack(level=-1)
            return out

        if isinstance(out, pd.Series) or pd_version >= (0, 20, 0):
            out = out.unstack(level=-1)
            # Prevent CategoricalIndex from causing problems
            out.columns = list(out.columns)

        return out.reset_index(self.get_groupby_vars())

    def nlargest(self, *args, **kwargs):
        '''
        Return the `n` largest values ordered by `columns`

        See Also
        --------
        :meth:`CASTable.nlargest`
        :meth:`CASColumn.nlargest`

        '''
        if self._as_index:
            return self._table.nlargest(*args, **kwargs)
        return self._table.nlargest(*args, **kwargs).reset_index(self.get_groupby_vars())

    def nsmallest(self, *args, **kwargs):
        '''
        Return the `n` smallest values ordered by `columns`

        See Also
        --------
        :meth:`CASTable.nsmallest`
        :meth:`CASColumn.nsmallest`

        '''
        if self._as_index:
            return self._table.nsmallest(*args, **kwargs)
        return self._table.nsmallest(*args, **kwargs).reset_index(self.get_groupby_vars())

    def query(self, *args, **kwargs):
        '''
        Query the table with a boolean expression

        See Also
        --------
        :meth:`CASTable.query`
        :meth:`CASColumn.query`

        '''
        out = self._table.query(*args, **kwargs)
        if out is not None:
            self._table = out
            return self
