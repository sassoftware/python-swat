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
DataFrame that includes SAS metadata (formats, labels, titles)

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import datetime
import functools
import json
import re
import pandas as pd
import six
try:
    from pandas.io.formats import console as pdconsole
    get_console_size = pdconsole.get_console_size
except ImportError:
    try:
        from pandas.formats.format import get_console_size
    except ImportError:
        from pandas.core.format import get_console_size
try:
    from pandas.io.formats import format as pdfmt
    notebook_opts = {'notebook': True}
except ImportError:
    notebook_opts = {}
    try:
        from pandas.formats import format as pdfmt
    except ImportError:
        from pandas.core import format as pdfmt
from .cas.table import CASTable
from .config import get_option
from .utils.compat import (a2u, a2n, int32, int64, float64, int32_types,
                           int64_types, float64_types, bool_types, text_types,
                           binary_types, dict_types)
from .utils import dict2kwargs
from .clib import errorcheck
from .formatter import SASFormatter


def dtype_from_var(value):
    ''' Guess the CAS data type from the value '''
    if isinstance(value, int64_types):
        return 'int64'
    if isinstance(value, int32_types):
        return 'int32'
    if isinstance(value, float64_types):
        return 'double'
    if isinstance(value, text_types):
        return 'varchar'
    if isinstance(value, binary_types):
        return 'varbinary'
    if isinstance(value, datetime.datetime):
        return 'datetime'
    if isinstance(value, datetime.date):
        return 'date'
    if isinstance(value, datetime.time):
        return 'time'
    raise TypeError('Unrecognized type for value: %s' % value)


def split_format(fmt):
    ''' Split a SAS format name into components '''
    if not fmt:
        sasfmt = collections.namedtuple('SASFormat', ['ischar', 'name', 'width', 'ndec'])
        return sasfmt(False, '', 0, 0)
    parts = list(re.match(r'(\$)?(\w*?)(\d*)\.(\d*)', fmt).groups())
    parts[0] = parts[0] and True or False
    parts[2] = parts[2] and int(parts[2]) or 0
    parts[3] = parts[3] and int(parts[3]) or 0
    sasfmt = collections.namedtuple('SASFormat', ['ischar', 'name', 'width', 'ndec'])
    return sasfmt(*parts)


def concat(objs, **kwargs):
    '''
    Concatenate :class:`SASDataFrames` while preserving table and column metadata

    This function is equivalent to :func:`pandas.concat` except that it also
    preserves metadata in :class:`SASDataFrames`.  It can be used on standard
    :class:`pandas.DataFrames` as well.

    Parameters
    ----------
    objs : a sequence of mapping of Series, (SAS)DataFrame, or Panel objects
        The DataFrames to concatenate.
    **kwargs : any, optional
        Additional arguments to pass to :func:`pandas.concat`.

    Examples
    --------
    >>> conn = swat.CAS()
    >>> tbl = conn.read_csv('data/cars.csv')
    >>> out = tbl.groupby('Origin').summary()
    >>> print(concat([out['ByGroup1.Summary'], out['ByGroup2.Summary'],
    ...               out['ByGroup3.Summary']]))

    Returns
    -------
    :class:`SASDataFrame`

    '''
    proto = objs[0]

    if not isinstance(proto, SASDataFrame):
        return pd.concat(objs, **kwargs)

    title = proto.title
    label = proto.label
    name = proto.name
    formatter = proto.formatter

    attrs = {}
    colinfo = {}
    columns = collections.OrderedDict()
    for item in objs:
        attrs.update(item.attrs)
        colinfo.update(item.colinfo)
        for col in item.columns:
            columns[col] = True

    return SASDataFrame(pd.concat(objs, **kwargs), title=title, label=label,
                        name=name, attrs=attrs, colinfo=colinfo,
                        formatter=formatter)[list(columns.keys())]


def reshape_bygroups(items, bygroup_columns='formatted',
                     bygroup_as_index=True, bygroup_formatted_suffix='_f',
                     bygroup_collision_suffix='_by'):
    '''
    Convert current By group representation to the specified representation

    Parameters
    ----------
    items : :class:`SASDataFrame` or list of :class:`SASDataFrames`
        The DataFrames to process.
    bygroup_columns : string, optional
        The way By group columns should be represented in the output table.  The
        options are 'none' (only use metadata), 'formatted', 'raw', or 'both'.
    bygroup_as_index : boolean, optional
        Specifies whether the By group columns should be converted to indices.
    bygroup_formatted_suffix : string, optional
        The suffix to use on formatted columns if the names collide with existing
        columns.
    bygroup_collision_suffix : string, optional
        The suffix to use on By group columns if there is also a data column
        with the same name.

    See Also
    --------
    :meth:`SASDataFrame.reshape_bygroups`

    Returns
    -------
    :class:`SASDataFrame` or list of :class:`SASDataFrame` objects

    '''
    if hasattr(items, 'reshape_bygroups'):
        return items.reshape_bygroups(bygroup_columns=bygroup_columns,
                                      bygroup_as_index=bygroup_as_index,
                                      bygroup_formatted_suffix=bygroup_formatted_suffix,
                                      bygroup_collision_suffix=bygroup_collision_suffix)

    out = []
    for item in items:
        if hasattr(item, 'reshape_bygroups'):
            out.append(
                item.reshape_bygroups(bygroup_columns=bygroup_columns,
                                      bygroup_as_index=bygroup_as_index,
                                      bygroup_formatted_suffix=bygroup_formatted_suffix,
                                      bygroup_collision_suffix=bygroup_collision_suffix))
        else:
            out.append(item)
    return out


@six.python_2_unicode_compatible
class SASColumnSpec(object):
    '''
    Create a :class:`SASDataFrame` column information object

    Parameters
    ----------
    name : string
       Name of the column.
    label : string
       Label for the column.
    type : string
       SAS/CAS data type of the column.
    width : int or long
       Width of the formatted column.
    format : string
       SAS format.
    size : two-element tuple
       Dimensions of the data.
    attrs : dict
       Extended attributes of the column.

    Returns
    -------
    :class:`SASColumnSpec` object

    '''

    def __init__(self, name, label=None, dtype=None, width=0, format='',
                 size=(1, 1), attrs=None):
        self.name = a2u(name)
        self.label = a2u(label)
        self.dtype = a2u(dtype)
        self.width = width
        self.format = a2u(format)
        self.size = size
        self.attrs = attrs
        if self.attrs is None:
            self.attrs = {}

    @classmethod
    def fromtable(cls, _sw_table, col, elem=None):
        '''
        Create instance from SWIG table

        Parameters
        ----------
        _sw_table : SWIG table object
           The table object to get column information from
        col : int or long
           The index of the column
        elem : int or long, optional
           Optional array index element; None for non-array columns

        Returns
        -------
        :class:`SASColumnSpec` object

        '''
        name = errorcheck(a2u(_sw_table.getColumnName(col), 'utf-8'), _sw_table)

        if elem is not None:
            name = name + str(elem + 1)

        label = errorcheck(a2u(_sw_table.getColumnLabel(col), 'utf-8'), _sw_table)
        dtype = errorcheck(a2u(_sw_table.getColumnType(col), 'utf-8'), _sw_table)
        width = errorcheck(_sw_table.getColumnWidth(col), _sw_table)
        format = errorcheck(a2u(_sw_table.getColumnFormat(col), 'utf-8'), _sw_table)
        size = (1, errorcheck(_sw_table.getColumnArrayNItems(col), _sw_table))

        # Get table attributes
        attrs = {}
        if hasattr(_sw_table, 'getColumnAttributes'):
            attrs = _sw_table.getColumnAttributes(col)
        else:
            while True:
                key = errorcheck(_sw_table.getNextColumnAttributeKey(col), _sw_table)
                if key is None:
                    break
                typ = errorcheck(_sw_table.getColumnAttributeType(col, a2n(key, 'utf-8')),
                                 _sw_table)
                key = a2u(key, 'utf-8')
                if typ == 'double':
                    attrs[key] = errorcheck(
                        _sw_table.getColumnDoubleAttribute(col, a2n(key, 'utf-8')),
                        _sw_table)
                elif typ == 'int32':
                    attrs[key] = errorcheck(
                        _sw_table.getColumnInt32Attribute(col, a2n(key, 'utf-8')),
                        _sw_table)
                elif typ == 'int64':
                    attrs[key] = errorcheck(
                        _sw_table.getColumnInt64Attribute(col, a2n(key, 'utf-8')),
                        _sw_table)
                elif typ == 'string':
                    attrs[key] = errorcheck(
                        a2u(_sw_table.getColumnStringAttribute(col, a2n(key, 'utf-8')),
                            'utf-8'), _sw_table)
                elif typ == 'int32-array':
                    nitems = errorcheck(_sw_table.getColumnAttributeNItems(), _sw_table)
                    attrs[key] = []
                    for i in range(nitems):
                        attrs[key].append(errorcheck(
                            _sw_table.getColumnInt32ArrayAttributeItem(col,
                                                                       a2n(key, 'utf-8'),
                                                                       i),
                            _sw_table))
                elif typ == 'int64-array':
                    nitems = errorcheck(_sw_table.getColumnAttributeNItems(), _sw_table)
                    attrs[key] = []
                    for i in range(nitems):
                        attrs[key].append(errorcheck(
                            _sw_table.getColumnInt64ArrayAttributeItem(col,
                                                                       a2n(key, 'utf-8'),
                                                                       i),
                            _sw_table))
                elif typ == 'double-array':
                    nitems = errorcheck(_sw_table.getColumnAttributeNItems(), _sw_table)
                    attrs[key] = []
                    for i in range(nitems):
                        attrs[key].append(errorcheck(
                            _sw_table.getColumnDoubleArrayAttributeItem(col,
                                                                        a2n(key, 'utf-8'),
                                                                        i),
                            _sw_table))

        return cls(name=name, label=label, dtype=dtype, width=width, format=format,
                   size=size, attrs=attrs)

    def __str__(self):
        return 'SASColumnSpec(%s)' % \
            dict2kwargs({k: v for k, v in six.iteritems(vars(self))
                         if v is not None}, fmt='%s')

    def __repr__(self):
        return str(self)


@six.python_2_unicode_compatible
class SASDataFrame(pd.DataFrame):
    '''
    Two-dimensional tabular data structure with SAS metadata added

    Attributes
    ----------
    name : string
        The name given to the table.
    label : string
        The SAS label for the table.
    title : string
        Displayed title for the table.
    attr : dict
        Table extended attributes.
    formatter : :class:`SASFormatter`
        A :class:`SASFormatter` object for applying SAS data formats.
    colinfo : dict
        Metadata for the columns in the :class:`SASDataFrame`.

    Parameters
    ----------
    data : :func:`numpy.ndarray` or dict or :class:`pandas.DataFrame`
       Dict can contain :class:`pandas.Series`, arrays, constants, or list-like objects.
    index : :class:`pandas.Index` or list, optional
       Index to use for resulting frame.
    columns : :class:`pandas.Index` or list, optional
       Column labels to use for resulting frame.
    dtype : data-type, optional
       Data type to force, otherwise infer.
    copy : boolean, optional
       Copy data from inputs.  Default is False.
    colinfo : dict, optional
       Dictionary of SASColumnSpec objects containing column metadata.
    name : string, optional
       Name of the table.
    label : string, optional
       Label on the table.
    title : string, optional
       Title of the table.
    formatter : :class:`SASFormatter` object, optional
       :class:`SASFormatter` to use for all formatting operations.
    attrs : dict, optional
       Table extended attributes.

    See Also
    --------
    :class:`pandas.DataFrame`

    Returns
    -------
    :class:`SASDataFrame` object

    '''

    class SASDataFrameEncoder(json.JSONEncoder):
        '''
        Custom JSON encoder for SASDataFrame

        '''

        def default(self, obj):
            '''
            Convert objects unrecognized by the default encoder

            Parameters
            ----------
            obj : any
               Arbitrary object to convert

            Returns
            -------
            any
               Python object that JSON encoder will recognize

            '''
            if isinstance(obj, float64_types):
                return float64(obj)
            if isinstance(obj, int64_types):
                return int64(obj)
            if isinstance(obj, (int32_types, bool_types)):
                return int32(obj)
            if isinstance(obj, CASTable):
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    _metadata = ['colinfo', 'name', 'label', 'title', 'attrs', 'formatter']

    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False,
                 name=None, label=None, title=None, formatter=None, attrs=None,
                 colinfo=None):
        super(SASDataFrame, self).__init__(data=data, index=index,
                                           columns=columns, dtype=dtype, copy=copy)
        # Only copy column info for columns that exist
        self.colinfo = {}
        if colinfo:
            for col in self.columns:
                if col in colinfo:
                    self.colinfo[col] = colinfo[col]
        self.name = a2u(name)
        self.label = a2u(label)
        self.title = a2u(title)
        # TODO: Should attrs be walked and converted to unicode?
        self.attrs = attrs or {}
        self.formatter = formatter
        if self.formatter is None:
            self.formatter = SASFormatter()

        # Count used for keeping unique data frame IDs in IPython notebook.
        # If a table is rendered more than once, we need to make sure it gets a
        # unique ID each time.
        self._idcount = 0

    @property
    def _constructor(self):
        '''
        Constructor used by DataFrame when returning a new DataFrame from an operation

        '''
        return SASDataFrame

#   @property
#   def _constructor_sliced(self):
#       return pd.Series

#   def __getattr__(self, name):
#       if name == '_repr_html_' and get_option('display.notebook.repr_html'):
#           return self._my_repr_html_
#       if name == '_repr_javascript_' and get_option('display.notebook.repr_javascript'):
#           return self._my_repr_javascript_
#       return super(SASDataFrame, self).__getattr__(name)

    #
    # Dictionary methods
    #

    def pop(self, k, *args):
        '''
        Pop item from a :class:`SASDataFrame`

        Parameters
        ----------
        k : string
            The key to remove.

        See Also
        --------
        :meth:`pandas.DataFrame.pop`

        Returns
        -------
        any
            The value stored in `k`.

        '''
        self.colinfo.pop(k, None)
        return super(SASDataFrame, self).pop(k, *args)

    def __setitem__(self, *args, **kwargs):
        '''
        Set an item in a SASDataFrame

        See Also
        --------
        :meth:`pandas.DataFrame.__setitem__`

        '''
        result = super(SASDataFrame, self).__setitem__(*args, **kwargs)
        for col in self.columns:
            if col not in self.colinfo:
                self.colinfo[col] = SASColumnSpec(col)
        return result

    def __getitem__(self, *args, **kwargs):
        '''
        Retrieve items from a SASDataFrame

        See Also
        --------
        :meth:`pandas.DataFrame.__getitem__`

        '''
        result = super(SASDataFrame, self).__getitem__(*args, **kwargs)

        if isinstance(result, SASDataFrame):

            # Copy metadata fields
            for name in self._metadata:
                selfattr = getattr(self, name, None)
                if isinstance(selfattr, dict):
                    selfattr = selfattr.copy()
                object.__setattr__(result, name, selfattr)

        return result

    def insert(self, *args, **kwargs):
        '''
        Insert an item at a particular position in a SASDataFrame

        See Also
        --------
        :meth:`pandas.DataFrame.insert`

        '''
        result = super(SASDataFrame, self).insert(*args, **kwargs)
        for col in self.columns:
            if isinstance(col, (tuple, list)) and col:
                col = col[0]
            if col not in self.colinfo:
                self.colinfo[col] = SASColumnSpec(col)
        return result

    #
    # End dictionary methods
    #

    def _get_formatters(self, formatters=None):
        ''' Retrieve formatter functions for DataFrame formatters= '''
        format = self.formatter.format

        out = {}

        formatters = formatters or {}

        if isinstance(formatters, dict_types):
            for col in self.columns:
                if col in formatters:
                    out[col] = formatters[col]
                elif col in self.colinfo:
                    info = self.colinfo[col]
                    if info.format:
                        out[col] = functools.partial(format,
                                                     sasfmt=info.format,
                                                     width=info.width)
        else:
            for col, fmter in zip(self.columns, formatters):
                if fmter is not None:
                    out[col] = fmter
                elif col in self.colinfo:
                    info = self.colinfo[col]
                    if info.format:
                        out[col] = functools.partial(format,
                                                     sasfmt=info.format,
                                                     width=info.width)

        return out

    def __repr__(self):
        ''' Return a string representation for a particular DataFrame '''
        buf = six.StringIO('')

        # Calling a private DataFrame method here, so protect it.
        if getattr(self, '_info_repr', lambda: False)():
            self.info(buf=buf)
            return buf.getvalue()

        if self.label:
            buf.write('%s\n\n' % self.label)

        kwargs = {}
        try:
            kwargs['min_rows'] = pd.get_option('display.min_rows')
        except Exception:  # noqa: E722
            pass
        max_rows = pd.get_option('display.max_rows')
        max_cols = pd.get_option('display.max_columns')
        show_dimensions = pd.get_option('display.show_dimensions')
        if pd.get_option('display.expand_frame_repr'):
            width, _ = get_console_size()
        else:
            width = None
        if not hasattr(pdfmt, 'DataFrameRenderer'):
            kwargs['line_width'] = width

        if get_option('display.apply_formats'):
            kwargs['formatters'] = self._get_formatters()
            if 'na_rep' not in kwargs:
                kwargs['na_rep'] = '.'

        formatter = pdfmt.DataFrameFormatter(
            self,
            max_rows=max_rows,
            max_cols=max_cols,
            show_dimensions=show_dimensions,
            **kwargs
        )
        # NOTE: Patch for bug in pandas DataFrameFormatter when using
        #       formatters on a DataFrame that is truncated in the console.
        formatter.columns = formatter.tr_frame.columns

        # pandas 1.2.0 uses a renderer instead of just a formatter
        if hasattr(pdfmt, 'DataFrameRenderer'):
            formatter = pdfmt.DataFrameRenderer(formatter)
            txt = formatter.to_string(line_width=width)
        else:
            txt = formatter.to_string()

        if txt is None:
            if getattr(formatter, 'buf', None) is not None:
                buf.write(formatter.buf.getvalue())
        else:
            buf.write(txt)

        return buf.getvalue()

    def __str__(self):
        ''' Return a string representation for a particular DataFrame '''
        return repr(self)

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('...')
            return
        p.text(self.to_string())

    def to_string(self, apply_formats=None, **kwargs):
        '''
        Return a string representation of a DataFrame

        Parameters
        ----------
        apply_formats : bool or None, optional
            Should SAS formats be applied to the data values in the
            rendered output?  If None, the `display.apply_formats`
            option value will be used.
        **kwargs : keyword-parameters, optional
            All keyword parameters for the `pandas.DataFrame.to_string`
            method are accepted here as well.

        See Also
        --------
        :meth:`pandas.DataFrame.to_string`

        Returns
        -------
        string

        '''
        buf = six.StringIO('')

        if self.label:
            buf.write('%s\n\n' % self.label)

        formatters = kwargs.get('formatters', None)
        if apply_formats or (apply_formats is None
                             and get_option('display.apply_formats')):
            kwargs['formatters'] = self._get_formatters(formatters)
            if 'na_rep' not in kwargs:
                kwargs['na_rep'] = '.'

        formatter = pdfmt.DataFrameFormatter(self, **kwargs)
        # NOTE: Patch for bug in pandas DataFrameFormatter when using
        #       formatters on a DataFrame that is truncated in the console.
        formatter.columns = formatter.tr_frame.columns

        # pandas 1.2.0 uses a renderer instead of just a formatter
        if hasattr(pdfmt, 'DataFrameRenderer'):
            formatter = pdfmt.DataFrameRenderer(formatter)

        txt = formatter.to_string()
        if txt is None:
            if getattr(formatter, 'buf', None) is not None:
                buf.write(formatter.buf.getvalue())
        else:
            buf.write(txt)

        return buf.getvalue()

    def _repr_html_(self):
        ''' Return a html representation for a particular DataFrame  '''

        # Calling a private DataFrame method here, so protect it.
        if getattr(self, '_info_repr', lambda: False)():
            buf = six.StringIO('')
            self.info(buf=buf)
            # need to escape the <class>, should be the first line.
            val = buf.getvalue().replace('<', r'&lt;', 1)
            val = val.replace('>', r'&gt;', 1)
            return '<pre>' + val + '</pre>'

        kwargs = {}
        if get_option('display.apply_formats'):
            kwargs['formatters'] = self._get_formatters()
            kwargs['na_rep'] = '.'

        if pd.get_option('display.notebook_repr_html'):
            try:
                kwargs['min_rows'] = pd.get_option('display.min_rows')
            except:  # noqa: E722
                pass
            max_rows = pd.get_option('display.max_rows')
            max_cols = pd.get_option('display.max_columns')
            show_dimensions = pd.get_option('display.show_dimensions')

            formatter = pdfmt.DataFrameFormatter(
                self,
                max_rows=max_rows,
                max_cols=max_cols,
                show_dimensions=show_dimensions,
                **kwargs
            )
            # NOTE: Patch for bug in pandas DataFrameFormatter when using
            #       formatters on a DataFrame that is truncated in the console.
            formatter.columns = formatter.tr_frame.columns

            # pandas 1.2.0 uses a renderer instead of just a formatter
            if hasattr(pdfmt, 'DataFrameRenderer'):
                formatter = pdfmt.DataFrameRenderer(formatter)

            html = formatter.to_html(**notebook_opts)
            if html is None:
                if getattr(formatter, 'buf', None) is not None:
                    html = formatter.buf.getvalue()
                else:
                    return None
            return self._post_process_html(html)

        return None

    def to_html(self, apply_formats=None, **kwargs):
        '''
        Return a html representation of a DataFrame

        Parameters
        ----------
        apply_formats : bool or None, optional
            Should SAS formats be applied to the data values in the
            rendered output?  If None, the `display.apply_formats`
            option value will be used.
        **kwargs : keyword-parameters, optional
            All keyword parameters for the `pandas.DataFrame.to_html`
            method are accepted here as well.

        See Also
        --------
        :meth:`pandas.DataFrame.to_html`

        Returns
        -------
        string or None

        '''
        formatters = kwargs.get('formatters', None)
        if apply_formats or (apply_formats is None
                             and get_option('display.apply_formats')):
            kwargs['formatters'] = self._get_formatters(formatters)
            if 'na_rep' not in kwargs:
                kwargs['na_rep'] = '.'

        html = pd.DataFrame.to_html(self, **kwargs)
        if html is None:
            return None

        return self._post_process_html(html)

    def _post_process_html(self, html):
        ''' Add SAS-isms to generated HTML table '''
        try:
            from html import escape
        except ImportError:
            from cgi import escape

        # Add table label
        if self.label:
            html = re.sub(r'(<table[^>]*>)',
                          r'\1<caption>%s</caption>' % self.label, html, count=1)

        # Add column labels as titles
        thead = re.search(r'<thead[^>]*>.*?</thead>', html, flags=re.S).group(0)

        labels = {k: v.label for k, v in self.colinfo.items()}

        def add_title(match):
            ''' Add title attribute '''
            if labels.get(match.group(2)):
                return '%s title="%s">%s%s' % (match.group(1),
                                               escape(labels[match.group(2)]),
                                               match.group(2), match.group(3))
            return '%s title="%s">%s%s' % (match.group(1), match.group(2),
                                           match.group(2), match.group(3))

        thead = re.sub(r'(<th\b[^>]*)>(.*?)(</th>)', add_title, thead, flags=re.S)

        html = re.sub(r'<thead.*?</thead>', thead, html, count=1, flags=re.S)

        return html

#       if not get_option('display.notebook.repr_html'):
#           return None

#       colinfo = self.colinfo.copy()
#       columns = self.columns
#       formatter = self.formatter

#       for col in columns:
#           if col not in colinfo:
#               colinfo[col] = SASColumnSpec(col)

#       out = []
#       out.append('<div style="max-height:1000px; max-width:1500px; overflow:auto">')
#       out.append('<table class="cas-dataframe">')
#       out.append('<thead>')

#       # Add the table label if needed
#       label = self.title or self.label or self.name
#       if label:
#           out.append('<tr><th colspan="%d">%s</th></tr>' % (len(columns) +
#                      len(self.index.names), label))

#       #
#       # Add column headers
#       #

#       out.append('<tr>')

#       # Blank cell for index
#       out.append('<th colspan="%d"></th>' % len(self.index.names))
#       for col in columns:
#           col = colinfo[col]
#           out.append('<th>%s</th>' % (col.label or col.name))
#       out.append('</tr>')

#       # Add index row
#       if [x for x in self.index.names if x]:
#           out.append('<tr>')
#           for name in self.index.names:
#               out.append('<th>%s</th>' % (name or ''))
#           out.append('<th></th>' * len(columns))
#           out.append('</tr>')

#       out.append('</thead>')
#       out.append('<tbody>')

#       truncate = False
#       numrows = len(self)
#       maxrows = get_option('display.max_rows')
#       if numrows <= maxrows:
#           rows = range(numrows)
#       else:
#           truncate = True
#           rows = list(range(int(maxrows / 2)))
#           if maxrows % 2:
#               rows.append(rows[-1] + 1)
#           rows.append(None)
#           rows.extend(list(range(numrows - int(maxrows / 2), numrows)))

#       values = self.values
#       last_index = None
#       for rownum in rows:
#           out.append('<tr>')
#           if rownum is None:
#               out.append('<th colspan="%d">...</th>' % len(self.index.names))
#           else:
#               if isinstance(self.index[rownum], (list, tuple)):
#                   for i, name in enumerate(self.index[rownum]):
#                       out.append('<th>%s</th>' % name)
#               else:
#                   out.append('<th>%s</th>' % self.index[rownum])
#           for colnum, colname in enumerate(columns):
#               if rownum is None:
#                   out.append('<td>...</td>')
#               else:
#                   col = colinfo[colname]
#                   value = values[rownum, colnum]
#                   out.append('<td>%s</td>' % formatter.format(value, col.format,
#                                                               col.width))
#           out.append('</tr>')

#       out.append('</tbody>')
#       out.append('</table>')

#       if str(get_option('display.show_dimensions')) == 'True' or \
#               (truncate and str(get_option('display.show_dimensions')) == 'truncate'):
#           out.append('<p>%s rows x %s columns</p>' % (len(self), len(self.columns)))

#       out.append('</div>')

#       return '\n'.join(out)

#   def _my_repr_json_(self):
#       '''
#       Return a JSON representation of the SASDataFrame

#       The structure used to hold the data of the table is of the following form:
#          {
#             label = 'table label',
#             nrows = #,       // number of rows in full dataframe, not displayed
#             ncolumns = #,
#             columns = [
#                { title = 'column label' },
#                ...                           ,
#                { title = 'column label' }
#             ],
#             data = [
#                [ [display-data, sort-key, search-terms],
#                  [display-data, sort-key, search-terms], ... ],
#                [ [display-data, sort-key, search-terms],
#                  [display-data, sort-key, search-terms], ... ],
#                ...                                            ,
#                [ [display-data, sort-key, search-terms],
#                  [display-data, sort-key, search-terms], ... ]
#             ]
#          }

#       Returns
#       -------
#       string
#          JSON representation of the SASDataFrame

#       '''
#       columns = self.columns
#       formatter = self.formatter
#       colinfo = self.colinfo.copy()
#       for col in columns:
#           if col not in colinfo:
#               colinfo[col] = SASColumnSpec(col)

#       output = {}

#       # convert data to JSON
#       dataout = []
#       for rownum in range(min(len(self), get_option('display.max_rows'))):
#           row = []
#           row.append([self.index[rownum]])
#           for colnum, colname in enumerate(columns):
#               col = colinfo[colname]
#               value = self.iloc[rownum, colnum]
#               cell = []
#               # formatted value
#               cell.append(formatter.format(value, col.format, col.width))
#               # if formatted value is different than raw value, add the raw value for
#               # sorting
#               if value != cell[0]:
#                   # don't put missing values in, they aren't allowed in JSON
#                   if (isinstance(value, np.float64) and np.isnan(value)) or \
#                           value is None or value is nil:
#                       pass
#                   else:
#                       cell.append(value)  # orderData
#               # cell.append(...) # search terms
#               row.append(cell)
#           dataout.append(row)
#       output['data'] = dataout

#       # add meta-data
#       output['label'] = self.title or self.label or self.name

#       jscolumns = []
#       jscolumns.append({'title': self.index.name or ''})
#       for colnum, colname in enumerate(columns):
#           col = colinfo[colname]
#           jscolumns.append({
#               'title': col.label or col.name,
#               'type': col.dtype
#           })
#       output['columns'] = jscolumns

#       truncate = (len(dataout) < len(self))
#       if str(get_option('display.show_dimensions')) == 'True' or \
#               (truncate and str(get_option('display.show_dimensions')) == 'truncate'):
#           output['nrows'] = len(self)
#           output['ncolumns'] = len(jscolumns) - 1  # Don't count the index

#       return escapejson(json.dumps(output, cls=self.SASDataFrameEncoder,
#                                    allow_nan=False))

#   def _repr_javascript_(self):
#       '''
#       Render the SASDataFrame to Javascript for IPython

#       Returns
#       -------
#       string
#          Javascript representation of SASDataFrame

#       '''
#       if not get_option('display.notebook.repr_javascript'):
#           return None

#       self._idcount = self._idcount + 1
#       currentid = 'cdf-%s-%s' % (id(self), self._idcount)
#       return notebook.bootstrap(r'''
#           element.append($.elem('div#%s'));
#           require(['swat'], function (swat) {
#              new swat.SASDataFrame($('#%s'), JSON.parse('%s'));
#           }, function () { console.log('Could not load swat.js') } );''' %
#                                 (currentid, currentid,
#                                  self._my_repr_json_().replace("'", "\\'")))

    def reshape_bygroups(self, bygroup_columns='formatted',
                         bygroup_as_index=True, bygroup_formatted_suffix='_f',
                         bygroup_collision_suffix='_by'):
        '''
        Convert current By group representation to the specified representation

        Parameters
        ----------
        self : :class:`SASDataFrame`
            The :class:`DataFrame` to process.
        bygroup_columns : string, optional
            The way By group columns should be represented in the output table.  The
            options are 'none' (only use metadata), 'formatted', 'raw', or 'both'.
        bygroup_as_index : boolean, optional
            Specifies whether the By group columns should be converted to indices.
        bygroup_formatted_suffix : string, optional
            The suffix to use on formatted columns if the names collide with existing
            columns.
        bygroup_collision_suffix : string, optional
            The suffix to use when a By group column name has the same name as
            a data column.

        Returns
        -------
        :class:`SASDataFrame`

        '''
        # Make a copy of the DataFrame
        dframe = self[self.columns]
        dframe.colinfo = dframe.colinfo.copy()
        dframe.attrs = dframe.attrs.copy()

        if not self.attrs.get('ByVar1'):
            return dframe

        # 'attributes', 'index', or 'columns'
        dframe.attrs.setdefault('ByGroupMode', 'attributes')

        # 'none', 'raw', 'formatted', or 'both'
        dframe.attrs.setdefault('ByGroupColumns', 'none')

        # Short circuit if possible
        if bygroup_columns == dframe.attrs['ByGroupColumns']:
            if dframe.attrs['ByGroupMode'] == 'attributes':
                return dframe
            if bygroup_as_index and dframe.attrs['ByGroupMode'] == 'index':
                return dframe
            if not bygroup_as_index and dframe.attrs['ByGroupMode'] == 'columns':
                return dframe

        # Get the names of all of the By variables
        byvars = []
        byvals = []
        byvalsfmt = []
        numbycols = 0
        i = 1
        while True:
            byvar = 'ByVar%d' % i

            if byvar not in dframe.attrs:
                break

            byvars.append(dframe.attrs[byvar])
            byvals.append(dframe.attrs[byvar + 'Value'])
            byvalsfmt.append(dframe.attrs[byvar + 'ValueFormatted'])

            dframe.attrs.pop(byvar + 'Formatted', None)

            numbycols = numbycols + 1
            if dframe.attrs['ByGroupColumns'] == 'both':
                numbycols = numbycols + 1

            i = i + 1

        # Drop existing indexes
        if dframe.attrs['ByGroupMode'] == 'index':
            dframe = dframe.reset_index(level=list(range(numbycols)), drop=True)

        # Drop existing columns
        elif dframe.attrs['ByGroupMode'] == 'columns':
            dframe = dframe.iloc[:, :numbycols]

        # Bail out if we are doing attributes
        if bygroup_columns == 'none':
            dframe.attrs['ByGroupMode'] = 'attributes'
            dframe.attrs['ByGroupColumns'] = 'none'
            return dframe

        # Construct By group columns
        dframe.attrs['ByGroupColumns'] = bygroup_columns

        if bygroup_as_index:
            dframe.attrs['ByGroupMode'] = 'index'
            nlevels = len([x for x in dframe.index.names if x])
            appendlevels = nlevels > 0
            bylevels = 0

            i = 1
            for byname, byval, byvalfmt in zip(byvars, byvals, byvalsfmt):
                bykey = 'ByVar%d' % i
                bylabel = dframe.attrs.get(bykey + 'Label')
                sasfmt = dframe.attrs.get(bykey + 'Format')
                sasfmtwidth = split_format(sasfmt).width
                if bygroup_columns in ['both', 'raw']:
                    dframe = dframe.set_index(pd.Series(data=[byval] * len(dframe),
                                                        name=byname),
                                              append=appendlevels)
                    dframe.colinfo[byname] = SASColumnSpec(byname, label=bylabel,
                                                           dtype=dtype_from_var(byval),
                                                           format=sasfmt,
                                                           width=sasfmtwidth)
                    bylevels += 1
                    appendlevels = True
                if bygroup_columns in ['both', 'formatted']:
                    if bygroup_columns == 'both':
                        byname = byname + bygroup_formatted_suffix
                    dframe = dframe.set_index(pd.Series(data=[byvalfmt] * len(dframe),
                                                        name=byname),
                                              append=appendlevels)
                    dframe.colinfo[byname] = SASColumnSpec(byname, label=bylabel,
                                                           dtype='varchar',
                                                           format=sasfmt,
                                                           width=sasfmtwidth)
                    bylevels += 1
                    appendlevels = True
                i = i + 1

            # Set the index level order
            if nlevels:
                dframe = dframe.reorder_levels(list(range(nlevels, nlevels + bylevels))
                                               + list(range(nlevels)))

        else:
            dframe.attrs['ByGroupMode'] = 'columns'
            allcolnames = list(dframe.columns)
            bycols = []

            i = 1
            for byname, byval, byvalfmt in zip(byvars, byvals, byvalsfmt):
                bykey = 'ByVar%d' % i
                bylabel = dframe.attrs.get(bykey + 'Label')
                sasfmt = dframe.attrs.get(bykey + 'Format')
                sasfmtwidth = split_format(sasfmt).width
                if bygroup_columns in ['both', 'raw']:
                    if byname in allcolnames:
                        byname = byname + bygroup_collision_suffix
                    dframe[byname] = byval
                    bycols.append(byname)
                    dframe.colinfo[byname] = SASColumnSpec(byname, label=bylabel,
                                                           dtype=dtype_from_var(byval),
                                                           format=sasfmt,
                                                           width=sasfmtwidth)
                if bygroup_columns in ['both', 'formatted']:
                    if bygroup_columns == 'both':
                        byname = byname + bygroup_formatted_suffix
                    elif bygroup_columns == 'formatted' and byname in allcolnames:
                        byname = byname + bygroup_collision_suffix
                    dframe[byname] = byvalfmt
                    bycols.append(byname)
                    dframe.colinfo[byname] = SASColumnSpec(byname, label=bylabel,
                                                           dtype='varchar',
                                                           format=sasfmt,
                                                           width=sasfmtwidth)
                i = i + 1

            # Put the By group columns at the beginning
            dframe = dframe[bycols + allcolnames]

        return dframe

    def apply_labels(self, **kwargs):
        '''
        Rename columns using the label value

        Notes
        -----
        Keyword parameters are simply passed to the underlying
        :meth:`pandas.DataFrame.rename` call.

        Returns
        -------
        :class:`SASDataFrame`

        '''
        return self.rename(columns={k: v.label for k, v in self.colinfo.items()
                                    if v.label}, **kwargs)

    def _get_byvars(self):
        '''
        Get the list of By variables

        Returns
        -------
        list of strings

        '''
        out = []
        if 'ByVar1' in self.attrs:
            i = 1
            while True:
                byvar = 'ByVar%d' % i
                if byvar in self.attrs:
                    col = self.attrs[byvar]
                    if col in self.columns:
                        out.append(col)
                    if byvar + 'Formatted' in self.attrs:
                        col = self.attrs[byvar + 'Formatted']
                        if col in self.columns:
                            out.append(col)
                else:
                    break
                i = i + 1
        return out

    def _render_html_(self):
        '''
        Create an ODS-like HTML rendering of the DataFrame

        '''
        output = []

        tbl = self
        byvars = tbl._get_byvars()
        if byvars:
            tbl = tbl.drop(byvars, axis=1)

        title = tbl.title or tbl.label
        colinfo = tbl.colinfo
        col_labels = [colinfo[x].label or colinfo[x].name for x in tbl.columns]
        col_formats = [colinfo[x].format or '' for x in tbl.columns]
        col_widths = [colinfo[x].width or 0 for x in tbl.columns]
        col_dtypes = [colinfo[x].dtype or '' for x in tbl.columns]
        col_heads = [colinfo[x].attrs.get('Index', False) for x in tbl.columns]
        values = tbl.values
        format = tbl.formatter.format

        output.append('<table class="sas-dataframe">')

        # Build colspec
        colspec = []
        colspec.append('<colgroup>')
        prev_is_head = False
        for i, is_head in enumerate(col_heads):
            dtype = col_dtypes[i].lower()
            if is_head and (not(prev_is_head) or i != 0):
                colspec.append('</colgroup>')
                colspec.append('<colgroup>')
            colspec.append('<col class="%s" />' % dtype)
        colspec.append('</colgroup>')
        output.append(''.join(colspec))

        output.append('<thead>')

        if title:
            output.append('<tr><th colspan="%d">%s</th></tr>' % (len(tbl.columns), title))

        for label, dtype in zip(col_labels, col_dtypes):
            output.append('<th class="%s">%s</th>' % (dtype, label))

        output.append('</thead>')
        output.append('<tbody>')

        for row in values:
            outrow = []
            for fmt, width, cell, dtype, is_head in zip(col_formats, col_widths,
                                                        row, col_dtypes, col_heads):
                if is_head:
                    outrow.append('<th class="%s">%s</th>' %
                                  (dtype, format(cell, sasfmt=fmt, width=width)))
                else:
                    outrow.append('<td class="%s">%s</td>' %
                                  (dtype, format(cell, sasfmt=fmt, width=width)))
            output.append('<tr>%s</tr>' % ''.join(outrow))

        output.append('</tbody>')
        output.append('</table>')

        return '\n'.join(output)
