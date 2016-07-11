#!/usr/bin/env python
# encoding: utf-8

'''
DataFrame that includes SAS metadata (formats, labels, titles)

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import datetime
import json
import re
import pandas as pd
import six
import textwrap
from .cas.table import CASTable
from .utils.compat import (a2u, a2n, int32, int64, float64, int32_types,
                           int64_types, float64_types, bool_types, text_types,
                           binary_types)
from .utils import dict2kwargs
from .clib import errorcheck
from .formatter import SASFormatter


def dtype_from_var(value):
    ''' Guess the CAS data type from the value '''
    if isinstance(value, int64):
        return 'int64'
    if isinstance(value, int32):
        return 'int32'
    if isinstance(value, float64):
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
    Concatenate SASDataFrames while preserving table and column metadata

    Parameters
    ----------
    objs : a sequence of mapping of Series, (SAS)DataFrame, or Panel objects
        The DataFrames to concatenate.
    **kwargs : any, optional
        Additional arguments to pass to pd.concat.

    Returns
    -------
    SASDataFrame

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
                     bygroup_as_index=True, bygroup_suffix='_f'):
    '''
    Convert current By group representation to the specified representation

    Parameters
    ----------
    items : SASDataFrame or list of SASDataFrames
        The DataFrames to process.

    Returns
    -------
    SASDataFrame or list of SASDataFrames

    '''
    if hasattr(items, 'reshape_bygroups'):
        return items.reshape_bygroups(bygroup_columns=bygroup_columns,
                                      bygroup_as_index=bygroup_as_index,
                                      bygroup_suffix=bygroup_suffix)

    out = []
    for item in items:
        if hasattr(item, 'reshape_bygroups'):
            out.append(item.reshape_bygroups(bygroup_columns=bygroup_columns,
                                             bygroup_as_index=bygroup_as_index,
                                             bygroup_suffix=bygroup_suffix))
        else:
            out.append(item)
    return out


@six.python_2_unicode_compatible
class SASColumnSpec(object):
    '''
    Create a SASDataFrame column information object

    Parameters
    ----------
    name : string
       Name of the column
    label : string
       Label for the column
    type : string
       SAS/CAS data type of the column
    width : int or long
       Width of the formatted column
    format : string
       SAS format
    size : two-element tuple
       Dimensions of the data
    attrs : dict
       Extended attributes of the column

    Returns
    -------
    SASColumnSpec object

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
        SASColumnSpec object

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
    Create a SASDataFrame object

    Parameters
    ----------
    data : numpy.ndarray or dict or DataFrame
       Dict can contain Series, arrays, constants, or list-like objects
    index : Index or list, optional
       Index to use for resulting frame
    columns : Index or list, optional
       Column labels to use for resulting frame
    dtype : data-type, optional
       Data type to force, otherwise infer
    copy : boolean, optional
       Copy data from inputs.  Default is False.
    colinfo : dict, optional
       Dictionary of SASColumnSpec objects containing column metadata
    name : string, optional
       Name of the table
    label : string, optional
       Label on the table
    title : string, optional
       Title of the table
    formatter : SASFormatter object, optional
       SASFormatter to use for all formatting operations
    attrs : dict, optional
       Table attributes

    See Also
    --------
    pandas.DataFrame

    Returns
    -------
    SASDataFrame object

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
            if isinstance(obj, int32_types) or isinstance(obj, bool_types):
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
        self.attrs = attrs
        if self.attrs is None:
            self.attrs = {}
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

    @property
    def _constructor_sliced(self):
        return pd.Series

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
        Pop item from a SASDataFrame

        See
        ---
        pandas.DataFrame.pop

        '''
        self.colinfo.pop(k, None)
        return super(SASDataFrame, self).pop(k, *args)

    def __setitem__(self, *args, **kwargs):
        '''
        Set an item in a SASDataFrame

        See
        ---
        pandas.DataFrame.__setitem__

        '''
        result = super(SASDataFrame, self).__setitem__(*args, **kwargs)
        for col in self.columns:
            if col not in self.colinfo:
                self.colinfo[col] = SASColumnSpec(col)
        return result

    def __getitem__(self, *args, **kwargs):
        '''
        Retrieve items from a SASDataFrame

        See
        ---
        pandas.DataFrame.__getitem__

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

        See
        ---
        pandas.DataFrame.insert

        '''
        result = super(SASDataFrame, self).insert(*args, **kwargs)
        for col in self.columns:
            if col not in self.colinfo:
                self.colinfo[col] = SASColumnSpec(col)
        return result

    #
    # End dictionary methods
    #

    def __str__(self):
        try:
            from IPython.lib.pretty import pretty
            return pretty(self)
        except ImportError:
            if self.label:
                return '%s\n\n%s' % (self.label, pd.DataFrame.to_string(self))
            return pd.DataFrame.to_string(self)

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('...')
            return

        if self.label:
            p.text(self.label)
            p.break_()
            p.break_()

        p.pretty(pd.DataFrame(self))

    def _repr_html_(self):
        '''
        Render the SASDataFrame to HTML for IPython

        Returns
        -------
        string
           HTML representation of SASDataFrame

        '''
        html = pd.DataFrame._repr_html_(self)
        if html is None:
            return

        import re
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
                         bygroup_as_index=True, bygroup_suffix='_f'):
        '''
        Convert current By group representation to the specified representation

        Parameters
        ----------
        self : SASDataFrame
            The DataFrame to process.
        bygroup_columns : string, optional
            The way By group columns should be represented in the output table.  The
            options are 'none' (only use metadata), 'formatted', 'raw', or 'both'.
        bygroup_as_index : boolean, optional
            Specifies whether the By group columns should be converted to indices.
        bygroup_suffix : string, optional
            The suffix to use on formatted columns if the names collide with existing
            columns.

        Returns
        -------
        SASDataFrame

        '''
        if not self.attrs.get('ByVar1'):
            return self

        # Make a copy of the DataFrame
        dframe = self[self.columns]
        dframe.colinfo = dframe.colinfo.copy()
        dframe.attrs = dframe.attrs.copy()

        attrs = dframe.attrs

        # Get the names of all of the By variables
        byvars = []
        byvarsfmt = []
        byvals = []
        byvalsfmt = []
        i = 1
        while True:
            byvar = 'ByVar%d' % i

            if byvar not in attrs:
                break

            byvars.append(attrs[byvar])
            byvarsfmt.append(attrs.get(byvar + 'Formatted', attrs[byvar] + bygroup_suffix))
            byvals.append(attrs[byvar + 'Value'])
            byvalsfmt.append(attrs[byvar + 'ValueFormatted'])

            attrs.pop(byvar + 'Formatted', None)

            i = i + 1

        # Drop all By columns and indexes
        for name in byvars:
            if name in dframe.columns:
                dframe = dframe.drop(name, axis=1)
            if name in dframe.index.names:
                dframe = dframe.reset_index(level=dframe.index.names.index(name), drop=True)

        for name in byvarsfmt:
            if name in dframe.columns:
                dframe = dframe.drop(name, axis=1)
            if name in dframe.index.names:
                dframe = dframe.reset_index(level=dframe.index.names.index(name), drop=True)

        # Construct By group columns
        i = 1
        bycols = []
        allcols = list(dframe.columns)
        for byname, byval, bynamefmt, byvalfmt in zip(byvars, byvals, byvarsfmt, byvalsfmt):
            bykey = 'ByVar%d' % i
            bylabel = attrs.get(bykey + 'Label')
            sasfmt = attrs.get(bykey + 'Format')
            sasfmtwidth = split_format(sasfmt).width
            if bygroup_columns == 'both' or bygroup_columns == 'raw':
                dframe[byname] = byval
                bycols.append(byname)
                dframe.colinfo[byname] = SASColumnSpec(byname, label=bylabel,
                                                       dtype=dtype_from_var(byval),
                                                       format=sasfmt, width=sasfmtwidth)
            if bygroup_columns == 'both' or bygroup_columns == 'formatted':
                if bygroup_columns == 'both':
                    byname = byname + bygroup_suffix
                dframe[byname] = byvalfmt
                bycols.append(byname)
                attrs[bykey + 'Formatted'] = byname
                dframe.colinfo[byname] = SASColumnSpec(byname, label=bylabel, dtype='varchar',
                                                       format=sasfmt, width=sasfmtwidth)
            i = i + 1

        # Put the By group columns at the beginning
        dframe = dframe[bycols + allcols]

        # Convert the By group columns to index columns as needed
        if bycols and bygroup_as_index:
            dframe = dframe.set_index(bycols)
            dframe.index.names = bycols

        return dframe

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
                    if (byvar + 'Formatted') in self.attrs:
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
