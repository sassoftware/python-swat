#!/usr/bin/env python
# encoding: utf-8

'''
CAS data message handlers

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import copy
import re
import datetime
import numpy as np
import pandas as pd
from .utils.datetime import (str2cas_timestamp, str2cas_datetime, str2cas_date, str2cas_time,
                             str2sas_timestamp, str2sas_datetime, str2sas_date, str2sas_time,
                             cas2python_timestamp, cas2python_datetime, cas2python_date,
                             cas2python_time,
                             sas2python_timestamp, sas2python_datetime, sas2python_date,
                             sas2python_time,
                             cas2sas_timestamp, cas2sas_datetime, cas2sas_date, cas2sas_time,
                             sas2cas_timestamp, sas2cas_datetime, sas2cas_date, sas2cas_time,
                             python2sas_timestamp, python2sas_datetime, python2sas_date,
                             python2sas_time,
                             python2cas_timestamp, python2cas_datetime, python2cas_date,
                             python2cas_time)
from .. import clib
from ..clib import errorcheck
from ..exceptions import SWATError
from ..utils.compat import a2n, text_types, binary_types, int32, int64, float64
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

    '''

    class CASDataMsgHandlerArgs(object):
        '''
        Container to hang data message handler arguments from

        '''
        pass

    def __init__(self, vars, nrecs=1000, reclen=None, locale=None, transformers=None):
        '''
        Create a data handler object

        Parameters
        ----------
        vars : list of dicts
           The list of variables to upload.  This has the same format as
           the vars= argument to the addtable action.  Each dict should
           at least have the keys: name, rtype, and length.

        nrecs : int or long, optional
           The number of records in the buffer.
        reclen : int or long, optional
           The length of each record in the buffer.
        locale : string, optional
           The locale to use for messages.
        transformers : dict of functions
           Transformers to use for variables.  Keys are the column names.
           Values are the function that does the transformation.

        Returns
        -------
        CASDataMsgHandler object

        '''
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
        Property that contains prepopulated keyword arguments for common data actions

        '''
        args = type(self).CASDataMsgHandlerArgs()
        args.addtable = {'datamsghandler': self, 'vars': self.vars, 'reclen': self.reclen}
        return args

    def __call__(self, request, connection):
        '''
        Send data to the server

        Parameters
        ----------
        request : CASRequest object
           Request object that came from the server.
        connection : CAS object
           Connection where the request came from.

        Returns
        -------
        CASResponse object
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
                values = self.getrow(inputrow)
                if values is None:
                    row = row - 1
                    break
                self.write(row, values)
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
        row : int or long
           The row (or record) number to write to.
        values : list of int, long, string, float
           The values to write.

        Returns
        -------
        None

        Raises
        ------
        SWATError
           If any error occurs in writing the data

        '''
        row = int64(row)

        def identity(val):
            ''' Return val '''
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
            if vrtype == 'CHAR' or vtype in ['VARCHAR', 'CHAR']:
                if isinstance(value, binary_types) or isinstance(value, text_types):
                    errorcheck(self._sw_databuffer.setString(row, offset,
                                                             a2n(transformer(value))),
                               self._sw_databuffer)
                else:
                    errorcheck(self._sw_databuffer.setString(row, offset, a2n('')),
                               self._sw_databuffer)
            elif vrtype == 'NUMERIC' and vtype in ['INT32', 'DATE']:
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
        connection : CAS object
           The connection to get the response from.

        **kwargs : dict, optional
           Arbitrary keyword arguments.

        Returns
        -------
        CASResponse object
           The next response from the connection

        '''
        return getone(connection, **kwargs)

    def send(self, connection, nrecs):
        '''
        Send the records to the connection

        Parameters
        ----------
        connection : CASConnection object
           The connection that will receive the data.
        nrecs : int or long
           The number of records to send.

        Returns
        -------
        None

        '''
        errorcheck(self._sw_databuffer.send(
            connection._sw_connection, nrecs), self._sw_databuffer)

    def finish(self, connection):
        '''
        Finish the data sending operation

        Parameters
        ----------
        connection : CASConnection object
           The connection that has been receiving the data.

        Returns
        -------
        None

        '''
        self._finished = True
        self.send(connection, 0)

    def getrow(self, row):
        '''
        Return the list of values for the requested row

        This method must be overridden by the subclass

        Parameters
        ----------
        row : int or long
           The row number for the values to retrieve

        Returns
        -------
        list of any
           One row of data values

        '''
        raise NotImplementedError


class PandasDataFrame(CASDataMsgHandler):
    '''
    Pandas DataFrame data message handler

    '''

    def __init__(self, data, nrecs=1000, dtype=None, labels=None, formats=None):
        '''
        Create a Pandas data message handler

        Parameters
        ----------
        data : pandas.DataFrame object
           The data to be uploaded

        nrecs : int or long, optional
           The number of rows to allocate in the buffer.  This can be
           smaller than the number of totals rows since they are uploaded
           in batches nrecs long.

        Returns
        -------
        PandasDataFrame object

        '''
        transformers = {}

        def typemap(name, typ, dtype=dtype):
            '''
            Map DataFrame type to CAS type

            Parameters
            ----------
            name : string
                Name of the column
            typ : Numpy data type

            Returns
            -------
            tuple
               ( width, SAS data type string, CAS data type string )

            Raises
            ------
            TypeError
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
            label = data.label
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
            if subtype == 'DATETIME':
                transformers[name] = lambda x: str2cas_timestamp(x)
            elif subtype == 'DATE':
                transformers[name] = lambda x: str2cas_date(x)
            elif subtype == 'TIME':
                transformers[name] = lambda x: str2cas_time(x)

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
        Get a row of values

        Parameters
        ----------
        row : int or long
           The row index to return

        Returns
        -------
        list of any
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
    SAS7BDAT data message handler

    '''

    def __init__(self, f, nrecs=1000, **kwargs):
        '''
        Create a SAS7BDAT data message handler

        Parameters
        ----------
        f : string
           Path to SAS7BDAT file

        nrecs : int, long (optional)
           Number of records sent at a time
        **kwargs : any (optional)
           Arguments sent to the sas7bdat.SAS7BDAT constructor

        Returns
        -------
        SAS7BDAT data message handler object

        '''
        import sas7bdat
        super(SAS7BDAT, self).__init__(
            sas7bdat.SAS7BDAT(f, **kwargs).to_data_frame(), nrecs)


class CSV(PandasDataFrame):
    '''
    CSV data message handler

    '''

    def __init__(self, f, nrecs=1000, **kwargs):
        '''
        Create a CSV data messsage handler

        Parameters
        ----------
        f : string
           Path to CSV file

        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.io.parsers.read_csv

        Returns
        -------
        CSV data message handler object

        '''
        kwargs.setdefault('chunksize', nrecs)
        try:
            super(CSV, self).__init__(pd.io.parsers.read_csv(f, **kwargs), nrecs)
        except StopIteration:
            del kwargs['chunksize']
            super(CSV, self).__init__(pd.io.parsers.read_csv(f, **kwargs), nrecs)


class Text(PandasDataFrame):
    '''
    Text data message handler

    '''

    def __init__(self, f, nrecs=1000, **kwargs):
        '''
        Create a Text data message handler

        Parameters
        ----------
        f : string
           Path to text file

        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.io.parsers.read_table

        Returns
        -------
        Text data message handler object

        '''
        kwargs.setdefault('chunksize', nrecs)
        try:
            super(Text, self).__init__(pd.io.parsers.read_table(f, **kwargs), nrecs)
        except StopIteration:
            del kwargs['chunksize']
            super(Text, self).__init__(pd.io.parsers.read_table(f, **kwargs), nrecs)


class FWF(PandasDataFrame):
    '''
    Fixed-width formatted data message handler

    '''

    def __init__(self, f, nrecs=1000, **kwargs):
        '''
        Create an FWF data message handler

        Parameters
        ----------
        f : string
           Path to text file

        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.io.parsers.read_table

        Returns
        -------
        FWF data message handler object

        '''
        kwargs.setdefault('chunksize', nrecs)
        try:
            super(FWF, self).__init__(pd.io.parsers.read_fwf(f, **kwargs), nrecs)
        except StopIteration:
            del kwargs['chunksize']
            super(FWF, self).__init__(pd.io.parsers.read_fwf(f, **kwargs), nrecs)


class JSON(PandasDataFrame):
    '''
    JSON data message handler

    '''

    def __init__(self, f, nrecs=1000, **kwargs):
        '''
        Create a JSON data message handler

        Parameters
        ----------
        f : string
           Path to JSON file

        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.read_json

        Returns
        -------
        JSON data message handler object

        '''
        super(JSON, self).__init__(pd.read_json(f, **kwargs), nrecs)


class HTML(PandasDataFrame):
    '''
    HTML data message handler

    '''

    def __init__(self, f, index=0, nrecs=1000, **kwargs):
        '''
        Create an HTML data message handler

        Parameters
        ----------
        f : string
           Path/URL to HTML file

        index : int or long, optional
           Index of table in the file
        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.read_html

        Returns
        -------
        HTML data message handler object

        '''
        super(HTML, self).__init__(pd.read_html(f, **kwargs)[index], nrecs)


class SQLTable(PandasDataFrame):
    '''
    SQL Alchemy table data message handler

    '''

    def __init__(self, table, engine, nrecs=1000, **kwargs):
        '''
        Create an SQLTable data message handler

        Parameters
        ----------
        table : string
           Name of table in database to fetch
        engine : sqlalchemy engine
           sqlalchemy engine

        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.io.read_sql_table

        Returns
        -------
        SQLTable data message handler object

        '''
        super(SQLTable, self).__init__(
            pd.io.sql.read_sql_table(table, engine, **kwargs), nrecs)

    @classmethod
    def create_engine(cls, *args, **kwargs):
        '''
        Return engine from sqlalchemy.create_engine

        Parameters
        ----------
        *args : any
           Positional arguments to sqlalchemy.create_engine
        **kwargs : any
           Keyword arguments to sqlalchemy.create_engine

        Returns
        -------
        SQLAlchemy engine

        '''
        from sqlalchemy import create_engine
        return create_engine(*args, **kwargs)


class SQLQuery(PandasDataFrame):
    '''
    SQL Alchemy query data message handler

    '''

    def __init__(self, query, engine, nrecs=1000, **kwargs):
        '''
        Create an SQLQuery data message handler

        Parameters
        ----------
        query : string
           SQL query
        engine : sqlalchemy engine
           sqlalchemy engine

        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.io.sql.read_sql_query

        Returns
        -------
        SQLQuery data message handler object

        '''
        super(SQLQuery, self).__init__(
            pd.io.sql.read_sql_query(query, engine, **kwargs), nrecs)

    @classmethod
    def create_engine(cls, *args, **kwargs):
        '''
        Return engine from sqlalchemy.create_engine

        Parameters
        ----------
        *args : any
           Positional arguments to sqlalchemy.create_engine
        **kwargs : any
           Keyword arguments to sqlalchemy.create_engine

        Returns
        -------
        SQLAlchemy engine

        '''
        from sqlalchemy import create_engine
        return create_engine(*args, **kwargs)


class Excel(PandasDataFrame):
    '''
    Excel data message handler

    '''

    def __init__(self, f, sheet=0, nrecs=1000, **kwargs):
        '''
        Create an Excel data message handler

        Arguments
        ---------
        f : string
           Path to Excel file
        sheet : string or int or long
           Sheet name or index to import

        nrecs : int or long, optional
           Number of records to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.read_excel

        Returns
        -------
        Excel data message handler object

        '''
        super(Excel, self).__init__(pd.read_excel(f, sheet, **kwargs), nrecs)


class Clipboard(PandasDataFrame):
    '''
    Clipboard data message handler

    '''

    def __init__(self, nrecs=1000, **kwargs):
        '''
        Create a Clipboard data message handler

        Parameters
        ----------
        nrecs : int or long, optional
           Number of recods to send at a time
        **kwargs : any, optional
           Arguments sent to pandas.read_clipboard

        Returns
        -------
        Clipboard data message handler object

        '''
        super(Clipboard, self).__init__(pd.read_clipboard(**kwargs), nrecs)


class DBAPITypeObject(object):
    ''' Object for comparing against multiple values '''

    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values


class DBAPI(CASDataMsgHandler):
    ''' DBAPI data message handler '''

    def __init__(self, module, cursor, nrecs=1000):
        '''
        Create a Python DB-API 2.0 compliant data message handler

        Parameters
        ----------
        module : Python module
           The database module used to create the cursor.  This is used
           for the data type constants for determining column types.
        cursor : Cursor object
           The cursor where the results should be fetched from.

        nrecs : int or long, optional
           The number of records to fetch and upload at a time.

        Returns
        -------
        DBAPI data message handler object

        '''
        self.cursor = cursor
        self.cursor.arraysize = nrecs

        # array of functions to transform data types that don't match SAS types
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
            if dtype == 'DATETIME':
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
        Return a row of values

        Parameters
        ----------
        row : int or long
           Index of row to return

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
