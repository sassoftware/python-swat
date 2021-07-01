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
CAS data message handlers

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import copy
import pytz
import re
import datetime
import warnings
import numpy as np
import pandas as pd
from .utils.datetime import (str2cas_timestamp, str2cas_datetime, str2cas_date,
                             str2cas_time, str2sas_timestamp, str2sas_datetime,
                             str2sas_date, str2sas_time, cas2python_timestamp,
                             cas2python_datetime, cas2python_date, cas2python_time,
                             sas2python_timestamp, sas2python_datetime,
                             sas2python_date, sas2python_time, cas2sas_timestamp,
                             cas2sas_datetime, cas2sas_date, cas2sas_time,
                             sas2cas_timestamp, sas2cas_datetime, sas2cas_date,
                             sas2cas_time, python2sas_timestamp, python2sas_datetime,
                             python2sas_date, python2sas_time, python2cas_timestamp,
                             python2cas_datetime, python2cas_date, python2cas_time)
from .. import clib
from ..config import get_option
from ..clib import errorcheck
from ..exceptions import SWATError
from ..utils.compat import a2b, a2n, text_types, binary_types, int32, int64, float64
from ..dataframe import SASDataFrame
from ..utils import getsoptions
from .connection import getone, CASRequest, CASResponse


_SIZES = {
    'char': 1,
    'varchar': 16,
    'binary': 1,
    'varbinary': 16,
    'int32': 4,
    'int64': 8,
    'double': 8,
    'date': 4,
    'time': 8,
    'datetime': 8,
    'sas': 8,
}


class CASDataMsgHandler(object):
    '''
    Base class for all CAS data message handlers

    All CAS data message handlers should inherit from this class.
    The communication between the client and CAS server requires
    several steps and error handling that is implemented in this
    class.

    When subclassing :class:`CASDataMsgHandler`, you only need to
    implement two pieces: ``__init__`` (the constructor) and ``getrow``.
    The constructor must create the ``vars=`` parameter for the
    ``table.addtable`` CAS action and store it in the ``vars`` instance
    attribute.  The ``getrow`` method, must return a single row of data
    values to be added to the data buffer.

    Parameters
    ----------
    vars : list-of-dicts
        The list of variables to upload.  This has the same format as
        the ``vars=`` argument to the ``table.addtable`` action.  Each dict should
        at least have the keys: name, rtype, and length.
    nrecs : int, optional
        The number of records in the buffer.
    reclen : int, optional
        The length of each record in the buffer.
    locale : string, optional
        The locale to use for messages.
    transformers : dict-of-functions
        Transformers to use for variables.  Keys are the column names.
        Values are the function that does the transformation.

    Examples
    --------
    The example below creates a custom data message handler with hard-coded
    data and variable definitions.  The ``getrow`` method is defined to simply
    return the requested row in the data array.

    >>> conn = swat.CAS()
    >>> import swat.cas.datamsghandlers as dmh
    >>> class MyDMH(dmh.CASDataMsgHandler):
    ...
    ...     def __init__(self):
    ...         self.data = [
    ...             ('A', 1, 100.2),
    ...             ('B', 2, 234.5),
    ...             ('C', 3, 999.0)
    ...         ]
    ...
    ...         vars = [
    ...             dict(name='name', label='Name', length=16,
    ...                  type='varchar', rtype='char', offset=0),
    ...             dict(name='index', label='Index', length=4,
    ...                  type='int32', rtype='numeric', offset=16),
    ...             dict(name='value', label='Value', length=8,
    ...                  type='sas', rtype='numeric', offset=20),
    ...         ]
    ...
    ...         super(MyDMH, self).__init__(vars)
    ...
    ...     def getrow(self, row):
    ...         try:
    ...             return self.data[row]
    ...         except IndexError:
    ...             return
    ...
    >>> mydmh = MyDMH()
    >>> out = conn.addtable(table='mytable', **mydmh.args.addtable)
    >>> tbl = out.casTable
    >>> print(tbl.head())

    Returns
    -------
    :class:`CASDataMsgHandler` object

    '''

    class CASDataMsgHandlerArgs(object):
        ''' Generic object to hold data message handler arguments '''
        pass

    def __init__(self, vars, nrecs=1000, reclen=None, locale=None, transformers=None):
        for item in vars:
            if item.get('type', '').upper() == 'SAS' and \
                    item.get('rtype', '').upper() == 'CHAR':
                raise SWATError('All character data must be varchars: %s'
                                % item.get('name'))
        soptions = getsoptions(locale=locale)
        self._finished = False
        self.nrecs = nrecs
        self.vars = copy.deepcopy(vars)

        if transformers is None:
            transformers = {}
        self.transformers = transformers

        # Fill in lengths
        for v in self.vars:
            if v.get('length', 0) <= 0:
                v['length'] = _SIZES[v['type'].lower()]
            # Map CAS names to SAS names
            if v.get('type', '').upper() == 'DOUBLE':
                v['type'] = 'sas'
                v['rtype'] = 'numeric'
            elif v.get('type', '').upper() == 'CHAR':
                v['type'] = 'sas'
                v['rtype'] = 'char'
            # Fill in missing rtypes
            if 'rtype' not in v:
                if v['type'].upper() in ['VARCHAR', 'CHAR', 'BINARY', 'VARBINARY']:
                    v['rtype'] = 'CHAR'
                else:
                    v['rtype'] = 'NUMERIC'

        # Compute reclen
        if reclen is None:
            reclen = sum([v['length'] for v in self.vars])
        self.reclen = reclen

        # Compute offsets
        next_offset = 0
        for v in self.vars:
            if v.get('offset', None) is None:
                v['offset'] = next_offset
            next_offset = v['offset'] + (v['length'] * v.get('nvalues', 1))

        _sw_error = clib.SW_CASError(a2n(soptions))
        self._sw_databuffer = errorcheck(clib.SW_CASDataBuffer(int64(self.reclen),
                                                               int64(self.nrecs),
                                                               a2n(soptions), _sw_error),
                                         _sw_error)

    @property
    def args(self):
        '''
        Property that generates the CAS action parameters
        for CAS actions that use a data message handler.  To use it,
        you specify the CAS action name as an attribute name.  The
        resulting dictionary can be expanded into keyword parameters
        as follows: `conn.addtable(**mydmh.args.addtable)`.

        '''
        args = type(self).CASDataMsgHandlerArgs()
        args.addtable = {'datamsghandler': self, 'vars': self.vars, 'reclen': self.reclen}
        return args

    def __call__(self, request, connection):
        '''
        Send data to the server

        Parameters
        ----------
        request : :class:`CASRequest` object
            Request object that came from the server.
        connection : :class:`CAS` object
            Connection where the request came from.

        Returns
        -------
        :class:`CASResponse` object
            The response object retrieved after sending a batch of data

        '''
        if self._finished:
            raise SWATError('The data message handler has already been used.')

        nbuffrows = self.nrecs
        inputrow = -1
        row = 0

        # Loop until we're out of data (i.e., values = None)
        while True:
            written = False

            # populate buffer
            for row in range(nbuffrows):
                inputrow = inputrow + 1
                try:
                    values = self.getrow(inputrow)
                except:  # noqa: E722
                    import traceback
                    traceback.print_exc()
                    break
                if values is None:
                    row = row - 1
                    break
                try:
                    self.write(row, values)
                except:  # noqa: E722
                    import traceback
                    traceback.print_exc()
                    break
                written = True

            # send it
            if written:
                self.send(connection, row + 1)
                res, conn = self.getone(connection)
                if isinstance(res, CASRequest):
                    continue
                elif isinstance(res, CASResponse):
                    if res.disposition.severity <= 1:
                        messages = list(res.messages)
                        while isinstance(res, CASResponse):
                            res, conn = self.getone(connection)
                            messages += res.messages
                            if res.disposition.severity > 1:
                                res.messages = messages
                                break
                        if isinstance(res, CASRequest):
                            continue
            else:
                break

            # If we failed for some reason, return the last response
            if isinstance(res, CASResponse) and res.disposition.severity > 1:
                return (res, conn)

        # End it
        self.finish(connection)
        return self.getone(connection)

    def write(self, row, values):
        '''
        Write the value to the row and column specified in the buffer

        Parameters
        ----------
        row : int
            The row (or record) number to write to.
        values : list of int, long, string, or float
            The values to write.

        Raises
        ------
        :exc:`SWATError`
            If any error occurs in writing the data

        '''
        row = int64(row)

        def identity(val):
            ''' Return `val` '''
            return val

        def get(arr, idx, default=0):
            ''' Return index value or default '''
            try:
                return arr[idx]
            except IndexError:
                return default

        for col in range(len(self.vars)):
            v = self.vars[col]
            offset = int64(v['offset'])
            length = int64(v['length'])
            value = values[col]
            transformer = self.transformers.get(v['name'], identity)
            vtype = v.get('type', '').upper()
            vrtype = v.get('rtype', '').upper()
            if transformer is identity:
                if vtype == 'DATE' and isinstance(value, (datetime.datetime,
                                                          datetime.date)):
                    value = python2cas_date(value)
                elif vtype == 'TIME' and isinstance(value, (datetime.datetime,
                                                            datetime.time)):
                    value = python2cas_time(value)
                elif vtype == 'DATETIME' and isinstance(value, (datetime.date,
                                                                datetime.time,
                                                                datetime.datetime)):
                    value = python2cas_datetime(value, tz='UTC')
            if vrtype == 'CHAR' or vtype in ['VARCHAR', 'CHAR', 'BINARY', 'VARBINARY']:
                if vtype in ['BINARY', 'VARBINARY'] \
                        and hasattr(self._sw_databuffer, 'setBinaryFromBase64'):
                    if isinstance(value, (binary_types, text_types)):
                        errorcheck(self._sw_databuffer.setBinaryFromBase64(row, offset,
                            a2n(base64.b64encode(a2b(transformer(value))))),  # noqa: E128
                            self._sw_databuffer)
                    else:
                        errorcheck(self._sw_databuffer.setBinaryFromBase64(row,
                                                                           offset,
                                                                           a2n('')),
                                   self._sw_databuffer)
                else:
                    if isinstance(value, (text_types, binary_types)):
                        errorcheck(self._sw_databuffer.setString(row, offset,
                                                                 a2n(transformer(value))),
                                   self._sw_databuffer)
                    else:
                        errorcheck(self._sw_databuffer.setString(row, offset, a2n('')),
                                   self._sw_databuffer)
            elif vrtype == 'NUMERIC' and vtype in ['INT32', 'DATE']:
                if pd.isnull(value):
                    value = get_option('cas.missing.%s' % vtype.lower())
                    warnings.warn(('Missing value found in 32-bit '
                                   + 'integer-based column \'%s\'.\n' % v['name'])
                                  + ('Substituting cas.missing.%s option value (%s).' %
                                     (vtype.lower(), value)),
                                  RuntimeWarning)
                if length > 4:
                    for i in range(int64(length / 4)):
                        errorcheck(self._sw_databuffer.setInt32(row, offset + (i * 4),
                                   int32(transformer(get(value, i, 0)))),
                                   self._sw_databuffer)
                else:
                    errorcheck(self._sw_databuffer.setInt32(row, offset,
                                                            int32(transformer(value))),
                               self._sw_databuffer)
            elif vrtype == 'NUMERIC' and vtype in ['INT64', 'DATETIME', 'TIME']:
                if pd.isnull(value):
                    value = get_option('cas.missing.%s' % vtype.lower())
                    warnings.warn(('Missing value found in 64-bit '
                                   + 'integer-based column \'%s\'.\n' % v['name'])
                                  + ('Substituting cas.missing.%s option value (%s).' %
                                     (vtype.lower(), value)),
                                  RuntimeWarning)
                if length > 8:
                    for i in range(int64(length / 8)):
                        errorcheck(self._sw_databuffer.setInt64(row, offset + (i * 8),
                                   int64(transformer(get(value, i, 0)))),
                                   self._sw_databuffer)
                else:
                    errorcheck(self._sw_databuffer.setInt64(row, offset,
                                                            int64(transformer(value))),
                               self._sw_databuffer)
            else:
                if length > 8:
                    for i in range(int64(length / 8)):
                        errorcheck(self._sw_databuffer.setDouble(row, offset + (i * 8),
                                   float64(transformer(get(value, i, np.nan)))),
                                   self._sw_databuffer)
                else:
                    errorcheck(self._sw_databuffer.setDouble(row, offset,
                               float64(transformer(value))),
                               self._sw_databuffer)

    def getone(self, connection, **kwargs):
        '''
        Get a single response from the server

        Parameters
        ----------
        connection : :class:`CAS` object
            The connection to get the response from.
        **kwargs : keyword arguments, optional
             Keyword arguments to pass to :func:`getone`

        Returns
        -------
        :class:`CASResponse` object
            The next response from the connection

        '''
        return getone(connection, **kwargs)

    def send(self, connection, nrecs):
        '''
        Send the records to the connection

        Parameters
        ----------
        connection : :class:`CAS` object
            The connection that will receive the data.
        nrecs : int
            The number of records to send.

        '''
        errorcheck(self._sw_databuffer.send(
            connection._sw_connection, nrecs), self._sw_databuffer)

    def finish(self, connection):
        '''
        Finish the data sending operation

        Parameters
        ----------
        connection : :class:`CAS` object
            The connection that has been receiving the data.

        '''
        self._finished = True
        self.send(connection, 0)

    def getrow(self, row):
        '''
        Return the list of values for the requested row

        This method must be overridden by the subclass.

        Parameters
        ----------
        row : int
            The row number for the values to retrieve.

        Returns
        -------
        list-of-any
            One row of data values

        '''
        raise NotImplementedError


class PandasDataFrame(CASDataMsgHandler):
    '''
    CAS data message handler for :class:`pandas.DataFrame` objects

    Parameters
    ----------
    data : :class:`pandas.DataFrame` object
       The data to be uploaded.
    nrecs : int, optional
       The number of rows to allocate in the buffer.  This can be
       smaller than the number of totals rows since they are uploaded
       in batches `nrecs` long.

    See Also
    --------
    :class:`pandas.DataFrame`
    :class:`CASDataMsgHandler`

    Returns
    -------
    :class:`PandasDataFrame` object

    '''

    def __init__(self, data, nrecs=1000, dtype=None, labels=None,
                 formats=None, transformers=None):
        if transformers is None:
            transformers = {}

        def typemap(name, typ, dtype=dtype):
            '''
            Map DataFrame type to CAS type

            Parameters
            ----------
            name : string
                Name of the column.
            typ : Numpy data type

            Returns
            -------
            tuple
                ( width, SAS data type string, CAS data type string )

            Raises
            ------
            :exc:`TypeError`
                If an unrecognized type in encountered

            '''
            # pylint: disable=unused-variable
            if dtype and name in dtype:
                typ = dtype[name].upper()
                if typ in ['CHAR', 'VARCHAR']:
                    return (16, 'CHAR', 'VARCHAR')
                elif typ in ['BINARY', 'VARBINARY']:
                    return (16, 'CHAR', 'VARBINARY')
                elif typ == 'DOUBLE':
                    return (8, 'NUMERIC', 'SAS')
                elif typ == 'INT64':
                    return (8, 'NUMERIC', 'INT64')
                elif typ == 'INT32':
                    return (4, 'NUMERIC', 'INT32')
                elif typ == 'DATE':
                    return (4, 'NUMERIC', 'DATE')
                elif typ == 'DATETIME':
                    return (8, 'NUMERIC', 'DATETIME')
                elif typ == 'TIME':
                    return (8, 'NUMERIC', 'TIME')
            else:
                match = re.match(r'^\W?([A-Za-z])(\d*)', typ.str)
                if match:
                    out = None
                    dtype = match.group(1)
                    try:
                        width = int(match.group(2))
                    except ValueError:
                        width = 0
                    if dtype in ['S', 'a', 'U', 'O', 'V']:
                        out = (16, 'CHAR', 'VARCHAR')
                    elif dtype in ['f', 'd', 'e', 'g']:
                        out = (8, 'NUMERIC', 'SAS')
                    elif dtype in ['i', 'b', 'h', 'l', 'q', 'p', 'u',
                                   'I', 'B', 'H', 'L', 'Q', 'P']:
                        if width <= 4:
                            out = (4, 'NUMERIC', 'INT32')
                        else:
                            out = (8, 'NUMERIC', 'INT64')
                    elif dtype in ['M', 'm']:
                        out = (8, 'NUMERIC', 'DATETIME')
                    if out is not None:
                        return out
            raise TypeError('%s is an unrecognized data type' % typ.str)

        # Empty reader iterator
        self.reader = iter([])

        # Add support for SASDataFrame metadata
        colinfo = {}
        if isinstance(data, SASDataFrame):
            # label = data.label
            colinfo = data.colinfo

        elif isinstance(data, pd.DataFrame):
            pass

        # Add support for chunked dataframes
        else:
            self.reader = iter(data)
            data = next(self.reader)

        if data.index.name is None:
            data = data.reset_index(drop=True)
        else:
            data = data.reset_index()

        reclen = 0
        variables = []
        for name, nptype in zip(data.columns, data.dtypes):
            length, rtype, subtype = typemap(name, nptype)
            if subtype == 'DATETIME' and name not in transformers:
                transformers[name] = str2cas_timestamp
            elif subtype == 'DATE' and name not in transformers:
                transformers[name] = str2cas_date
            elif subtype == 'TIME' and name not in transformers:
                transformers[name] = str2cas_time

            variables.append({'name': name, 'rtype': rtype, 'type': subtype,
                              'offset': reclen, 'length': length})

            # Set default formats
            if subtype == 'DATETIME':
                variables[-1]['format'] = 'DATETIME'
                variables[-1]['formattedlength'] = 20
            elif subtype == 'DATE':
                variables[-1]['format'] = 'DATE'
                variables[-1]['formattedlength'] = 9
            elif subtype == 'TIME':
                variables[-1]['format'] = 'TIME'
                variables[-1]['formattedlength'] = 8

            # Add column metadata as needed
            if name in colinfo:
                lbl = getattr(colinfo[name], 'label', None)
                if lbl:
                    variables[-1]['label'] = lbl
                fmt = getattr(colinfo[name], 'format', None)
                if fmt:
                    variables[-1]['format'] = fmt
                wid = getattr(colinfo[name], 'width', None)
                if fmt and wid:
                    variables[-1]['formattedlength'] = wid

            if formats and name in formats:
                variables[-1]['format'] = formats[name]

            if labels and name in labels:
                variables[-1]['label'] = labels[name]

            reclen = reclen + length

        self.data = data

        self.chunksize = len(self.data)

        super(PandasDataFrame, self).__init__(
            variables, nrecs=nrecs, reclen=reclen, transformers=transformers)

    def getrow(self, row):
        '''
        Get a row of values from the data source

        Parameters
        ----------
        row : int
            The row index to return.

        Returns
        -------
        list-of-any
            One row of data values

        '''
        if self.data is None:
            return

        # Get row number in the batch
        batchrow = 0
        if row > 0:
            batchrow = row % self.chunksize

        # See if we need another batch
        if row > 0 and batchrow == 0:
            self.data = None
            try:
                self.data = next(self.reader)
                if self.data.index.name is None:
                    self.data = self.data.reset_index(drop=True)
                else:
                    self.data = self.data.reset_index()
            except StopIteration:
                return
            return self.getrow(0)

        # Return a row of data
        if batchrow < len(self.data):
            return self.data.iloc[batchrow].tolist()

        return


class SAS7BDAT(PandasDataFrame):
    '''
    Create a SAS7BDAT data message handler

    Parameters
    ----------
    path : string
       Path to SAS7BDAT file.
    nrecs : int, optional
       Number of records sent at a time.
    **kwargs : keyword arguments, optional
       Arguments sent to the :class:`sas7bdat.SAS7BDAT` constructor.

    See Also
    --------
    :class:`sas7bdat.SAS7BDAT`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`SAS7BDAT` data message handler object

    '''

    def __init__(self, path, nrecs=1000, transformers=None, **kwargs):
        import sas7bdat
        super(SAS7BDAT, self).__init__(
            sas7bdat.SAS7BDAT(path, **kwargs).to_data_frame(), nrecs=nrecs,
            transformers=transformers)


class CSV(PandasDataFrame):
    '''
    Create a CSV data messsage handler

    Parameters
    ----------
    path : string
        Path to CSV file.
    nrecs : int, optional
        Number of records to send at a time.
    **kwargs : keyword arguments, optional
        Arguments sent to :func:`pandas.read_csv`.

    See Also
    --------
    :func:`pandas.read_csv`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`CSV` data message handler object

    '''

    def __init__(self, path, nrecs=1000, transformers=None, **kwargs):
        kwargs.setdefault('chunksize', nrecs)
        try:
            super(CSV, self).__init__(pd.io.parsers.read_csv(path, **kwargs),
                                      nrecs=nrecs, transformers=transformers)
        except StopIteration:
            del kwargs['chunksize']
            super(CSV, self).__init__(pd.io.parsers.read_csv(path, **kwargs),
                                      nrecs=nrecs, transformers=transformers)


class Text(PandasDataFrame):
    '''
    Create a Text data message handler

    Parameters
    ----------
    path : string
        Path to text file.
    nrecs : int, optional
        Number of records to send at a time.
    **kwargs : keyword arguments, optional
        Arguments sent to :func:`pandas.io.parsers.read_table`.

    See Also
    --------
    :func:`pandas.io.parsers.read_table`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`Text` data message handler object

    '''

    def __init__(self, path, nrecs=1000, transformers=None, **kwargs):
        kwargs.setdefault('chunksize', nrecs)
        try:
            super(Text, self).__init__(pd.io.parsers.read_table(path, **kwargs),
                                       nrecs=nrecs, transformers=transformers)
        except StopIteration:
            del kwargs['chunksize']
            super(Text, self).__init__(pd.io.parsers.read_table(path, **kwargs),
                                       nrecs=nrecs, transformers=transformers)


class FWF(PandasDataFrame):
    '''
    Create an FWF data message handler

    Parameters
    ----------
    path : string
       Path to text file.
    nrecs : int, optional
       Number of records to send at a time.
    **kwargs : keyword arguments, optional
       Arguments sent to :func:`pandas.io.parsers.read_table`.

    See Also
    --------
    :func:`pandas.io.parsers.read_table`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`FWF` data message handler object

    '''

    def __init__(self, path, nrecs=1000, transformers=None, **kwargs):
        kwargs.setdefault('chunksize', nrecs)
        try:
            super(FWF, self).__init__(pd.io.parsers.read_fwf(path, **kwargs),
                                      nrecs=nrecs, transformers=transformers)
        except StopIteration:
            del kwargs['chunksize']
            super(FWF, self).__init__(pd.io.parsers.read_fwf(path, **kwargs),
                                      nrecs=nrecs, transformers=transformers)


class JSON(PandasDataFrame):
    '''
    Create a JSON data message handler

    Parameters
    ----------
    path : string
        Path to JSON file.
    nrecs : int, optional
        Number of records to send at a time
    **kwargs : keyword arguments, optional
        Arguments sent to :func:`pandas.read_json`.

    See Also
    --------
    :func:`pandas.read_json`
    :class:`PandasDataFrame`

    Returns
    -------
    :func:`JSON` data message handler object

    '''

    def __init__(self, path, nrecs=1000, transformers=None, **kwargs):
        super(JSON, self).__init__(pd.read_json(path, **kwargs),
                                   nrecs=nrecs, transformers=transformers)


class HTML(PandasDataFrame):
    '''
    Create an HTML data message handler

    Parameters
    ----------
    path : string
        Path or URL to HTML file.
    index : int, optional
        Index of table in the file.
    nrecs : int, optional
        Number of records to send at a time.
    **kwargs : keyword arguments, optional
        Arguments sent to :func:`pandas.read_html`.

    See Also
    --------
    :func:`pandas.read_html`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`HTML` data message handler object

    '''

    def __init__(self, path, index=0, nrecs=1000, transformers=None, **kwargs):
        super(HTML, self).__init__(pd.read_html(path, **kwargs)[index],
                                   nrecs=nrecs, transformers=transformers)


class SQLTable(PandasDataFrame):
    '''
    Create an SQLTable data message handler

    Parameters
    ----------
    table : string
        Name of table in database to fetch.
    engine : sqlalchemy engine
        sqlalchemy engine.
    nrecs : int, optional
        Number of records to send at a time.
    **kwargs : keyword arguments, optional
        Arguments sent to :class:`pandas.io.read_sql_table`.

    Returns
    -------
    :class:`SQLTable` data message handler object
    :class:`PandasDataFrame`

    '''

    def __init__(self, table, engine, nrecs=1000, transformers=None, **kwargs):
        super(SQLTable, self).__init__(
            pd.io.sql.read_sql_table(table, engine, **kwargs),
            nrecs=nrecs, transformers=transformers)

    @classmethod
    def create_engine(cls, *args, **kwargs):
        '''
        Return engine from :func:`sqlalchemy.create_engine`

        Parameters
        ----------
        *args : any
            Positional arguments to :func:`sqlalchemy.create_engine`.
        **kwargs : any
            Keyword arguments to :func:`sqlalchemy.create_engine`.

        See Also
        --------
        :func:`sqlalchemy.create_engine`

        Returns
        -------
        :class:`SQLAlchemy` engine

        '''
        from sqlalchemy import create_engine
        return create_engine(*args, **kwargs)


class SQLQuery(PandasDataFrame):
    '''
    Create an SQLQuery data message handler

    Parameters
    ----------
    query : string
        SQL query.
    engine : sqlalchemy engine
        sqlalchemy engine.
    nrecs : int or long, optional
        Number of records to send at a time.
    **kwargs : any, optional
        Arguments sent to :func:`pandas.io.sql.read_sql_query`.

    See Also
    --------
    :func:`pandas.io.sql.read_sql_query`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`SQLQuery` data message handler object

    '''

    def __init__(self, query, engine, nrecs=1000, transformers=None, **kwargs):
        super(SQLQuery, self).__init__(
            pd.io.sql.read_sql_query(query, engine, **kwargs),
            nrecs=nrecs, transformers=transformers)

    @classmethod
    def create_engine(cls, *args, **kwargs):
        '''
        Return engine from :func:`sqlalchemy.create_engine`

        Parameters
        ----------
        *args : any
            Positional arguments to :func:`sqlalchemy.create_engine`.
        **kwargs : any
            Keyword arguments to :func:`sqlalchemy.create_engine`.

        See Also
        --------
        :func:`sqlalchemy.create_engine`

        Returns
        -------
        :class:`SQLAlchemy` engine

        '''
        from sqlalchemy import create_engine
        return create_engine(*args, **kwargs)


class Excel(PandasDataFrame):
    '''
    Create an Excel data message handler

    Parameters
    ----------
    path : string
        Path to Excel file.
    sheet : string or int, optional
        Sheet name or index to import.
    nrecs : int, optional
        Number of records to send at a time.
    **kwargs : keyword arguments, optional
        Arguments sent to :func:`pandas.read_excel`.

    See Also
    --------
    :func:`pandas.read_excel`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`Excel` data message handler object

    '''

    def __init__(self, path, sheet=0, nrecs=1000, transformers=None, **kwargs):
        super(Excel, self).__init__(pd.read_excel(path, sheet, **kwargs),
                                    nrecs=nrecs, transformers=transformers)


class Clipboard(PandasDataFrame):
    '''
    Create a Clipboard data message handler

    Parameters
    ----------
    nrecs : int, optional
        Number of recods to send at a time.
    **kwargs : keyword arguments, optional
        Arguments sent to :func:`pandas.read_clipboard`.

    See Also
    --------
    :func:`pandas.read_clipboard`
    :class:`PandasDataFrame`

    Returns
    -------
    :class:`Clipboard` data message handler object

    '''

    def __init__(self, nrecs=1000, transformers=None, **kwargs):
        super(Clipboard, self).__init__(pd.read_clipboard(**kwargs),
                                        nrecs=nrecs, transformers=transformers)


class DBAPITypeObject(object):
    ''' Object for comparing against multiple values '''

    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values


class DBAPI(CASDataMsgHandler):
    '''
    Create a Python DB-API 2.0 compliant data message handler

    Parameters
    ----------
    module : database module
        The database module used to create the cursor.  This is used
        for the data type constants for determining column types.
    cursor : Cursor object
        The cursor where the results should be fetched from.
    nrecs : int, optional
        The number of records to fetch and upload at a time.

    See Also
    --------
    :class:`CASDataMsgHandler`

    Returns
    -------
    :class:`DBAPI` data message handler object

    '''

    def __init__(self, module, cursor, nrecs=1000, transformers=None):
        self.cursor = cursor
        self.cursor.arraysize = nrecs

        # array of functions to transform data types that don't match SAS types
        if transformers is None:
            transformers = {}

        DATETIME = getattr(module, 'DATETIME', datetime.datetime)
        STRING = getattr(module, 'STRING', DBAPITypeObject(*text_types))
        BINARY = getattr(module, 'BINARY', DBAPITypeObject(*binary_types))

        def typemap(typ):
            '''
            Map database type to CAS type

            Parameters
            ----------
            typ : DBAPI data type

            Returns
            -------
            list
               [ column name, SAS data type string, CAS data type string, width ]

            '''
            output = [typ[0], 'NUMERIC', 'SAS', 8]
            if typ[1] == DATETIME:
                output[1] = 'NUMERIC'
                output[2] = 'DATETIME'
            elif typ[1] == STRING:
                output[1] = 'CHAR'
                output[2] = 'VARCHAR'
                output[3] = 16
            elif typ[1] == BINARY:
                output[1] = 'CHAR'
                output[2] = 'VARBINARY'
                output[3] = 16
            return output

        reclen = 0
        variables = []
        for item in self._get_description(module):
            name, rtype, dtype, length = typemap(item)
            if dtype == 'DATETIME' and name not in transformers:
                transformers[name] = lambda x: str2cas_timestamp(x)

            variables.append({'name': name, 'rtype': rtype, 'type': dtype,
                              'offset': reclen, 'length': length})

            # Set default formats
            if dtype == 'DATETIME':
                variables[-1]['format'] = 'DATETIME'
                variables[-1]['formattedlength'] = 20
            elif dtype == 'DATE':
                variables[-1]['format'] = 'DATE'
                variables[-1]['formattedlength'] = 9
            elif dtype == 'TIME':
                variables[-1]['format'] = 'TIME'
                variables[-1]['formattedlength'] = 8

            reclen = reclen + length

        super(DBAPI, self).__init__(variables, nrecs=nrecs, reclen=reclen,
                                    transformers=transformers)

    def _get_description(self, module):
        ''' Make SQLite's description behave properly '''
        if getattr(module, 'sqlite_version', None):
            desc = [list(x) for x in self.cursor.description]
            self._firstrow = self.cursor.fetchone()
            for data, col in zip(self._firstrow, desc):
                col[1] = type(data)
            return desc
        return self.cursor.description

    def getrow(self, row):
        '''
        Return a row of values from the data source

        Parameters
        ----------
        row : int
            Index of row to return.

        Returns
        -------
        list of any
            One row of data values

        '''
        if hasattr(self, '_firstrow'):
            row = self._firstrow
            del self._firstrow
            return row
        return self.cursor.fetchone()
