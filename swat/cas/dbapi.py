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
Module that implements the DB-API 2.0 for CAS

NOTE: This module is considered pre-produciton.

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import datetime
try:
    from exceptions import StandardError
except ImportError:
    StandardError = Exception
import time
import uuid
from collections import namedtuple
import six
import pandas as pd
from .connection import CAS
from ..compat import items_types, binary_types, text_types
from ..exceptions import SWATError
from ..utils.compat import (int32, int64, float64, int32_types,
                            int64_types, float64_types)


# Globals
apilevel = '2.0'
threadsafety = 1
paramstyle = 'pyformat'  # Also supports 'format'


# Exceptions
class Warning(StandardError):
    ''' Important warnings such as data truncation while inserting, etc. '''
    pass


class Error(StandardError):
    ''' Base class for all other error exceptions '''
    pass


class InterfaceError(Error):
    ''' Errors related to the database interface rather than the database itself '''
    pass


class DatabaseError(Error):
    ''' Errors related to the database '''
    pass


class DataError(DatabaseError):
    ''' Errors due to problems such as division by zero, out of range, etc. '''
    pass


class OperationalError(DatabaseError):
    ''' Errors related to database's operation '''
    pass


class IntegrityError(DatabaseError):
    ''' Errors for database integrity such as foreign key checks '''
    pass


class InternalError(DatabaseError):
    ''' Errors for internal errors in the database such as out of sync transactions '''
    pass


class ProgrammingError(DatabaseError):
    ''' Programming errors such as table not found or syntax errors in the SQL '''
    pass


class NotSupportedError(DatabaseError):
    ''' Methods not supported by the database API '''
    pass


# Connection Objects

def connect(dsn=None, user=None, password=None, host=None, port=0, database=None):
    '''
    Return a new connection

    Parameters
    ----------
    dsn : string, optional
        Data source name as a string.
    user : string, optional
        User name.
    password : string, optional
        Password.
    host : string, optional
        Hostname of the database.  This can also contain the form
        'host:port' if the port number is to be specified.
    port : int, optional
        The port number of the server.
    database : string, optional
        The name of the CASLib to set as the default.

    Returns
    -------
    Connection object

    '''
    params = dict(port=port, user=user, password=password, host=host,
                  database=database, options=None)

    if dsn is not None:
        parts = dsn.split(':', 4)
        params['host'] = parts.pop(0)
        port = parts.pop(0)
        try:
            params['port'] = int(port)
            if parts:
                params['database'] = parts.pop(0)
        except TypeError:
            params['database'] = port
        if parts:
            params['user'] = parts.pop(0)
        if parts:
            params['password'] = parts.pop(0)
        if parts:
            params['options'] = parts.pop(0)

    if host and ':' in host:
        host, port = host.split(':', 1)
        params['port'] = int(port)

    if host:
        params['host'] = host

    if user:
        params['user'] = user

    if password:
        params['password'] = password

    if database:
        params['database'] = database

    try:
        out = Connection(hostname=params['host'], port=params['port'],
                         username=params['user'], password=params['password'])

        # Set default caslib according to database option
        if params['database'] is not None:
            out._connection.retrieve('sessionprop.setsessopt', caslib=params['database'],
                                     _messagelevel='error')
    except SWATError as exc:
        OperationalError(str(exc))

    return out


@six.python_2_unicode_compatible
class Connection(object):
    ''' DB-API 2.0 Adapter for CAS '''

    # Exceptions
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, **kwargs):
        self._connection = CAS(**kwargs)
        self.cursor_type = Cursor

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def connection(self):
        return self._connection

    def commit(self):
        ''' Database commit - not supported '''
        pass

    def rollback(self):
        ''' Database rollback - not supported '''
        pass

    def cursor(self):
        ''' Create a new cursor object '''
        return self.cursor_type(self)

    def close(self):
        ''' Close the connection '''
        self._connection.close()
        self._connection = None

    @property
    def closed(self):
        return self._connection is None


# Cursor Objects

@six.python_2_unicode_compatible
class Cursor(object):
    ''' DB-API 2.0 Cursor '''

    def __init__(self, connection):
        self._connection = connection
        self._casout = None
        self._messages = []
        self._description = None
        self._colnames = []
        self._coltypes = []
        self._row_factory = None
        self.arraysize = 1
        self.errorhandler = None
        self._rowid = 1
        self._retrieve('builtins.loadactionset', actionset='fedsql')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _check_connection(self):
        ''' Verify that the connection still exists '''
        if self._connection is None:
            self._raise_error(Error, 'Connection has been closed')

    def _retrieve(self, *args, **kwargs):
        ''' Retrieve the output of the action call '''
        self._check_connection()
        kwargs = kwargs.copy()
        kwargs['_messagelevel'] = 'error'
        try:
            out = self._connection._connection.retrieve(*args, **kwargs)
            if out.severity > 1:
                self._raise_error(Error, out.status)
            return out
        except SWATError as exc:
            self._raise_error(InterfaceError, str(exc))

    def _reset_output(self):
        ''' Reset all variables between DB calls '''
        del self.messages[:]
        self._description = None
        self._colnames = []
        self._coltypes = []
        self._row_factory = None
        self._rowid = 1
        _casout = self._casout
        self._casout = None
        if _casout is not None:
            self._retrieve('table.droptable', table=_casout)

    @property
    def description(self):
        ''' Return the results description '''
        return self._description

    @property
    def colnames(self):
        ''' Return a tuple of column names '''
        return self._colnames

    @property
    def coltypes(self):
        ''' Return a tuple of column types '''
        return self._coltypes

    def _set_description(self):
        ''' Retrieve the results description and store it '''
        if self._casout is None:
            self._description = None
            return
        colinfo = self._retrieve('table.columninfo', table=self._casout)['ColumnInfo']
        colinfo = colinfo[['Column', 'Type', 'FormattedLength',
                           'RawLength', 'NFL', 'NFD']]
        colinfo['NullOK'] = colinfo.Type.isin(['double'])
        self._colnames = tuple(colinfo['Column'].tolist())
        self._coltypes = tuple(colinfo['Type'].tolist())
        self._row_factory = self.build_row_factory()
        self._description = tuple(tuple(x) for x in colinfo.values)

    def build_row_factory(self):
        ''' Construct the storage class for rows '''
        return namedtuple('Row', self.colnames, rename=True)._make

    def row_factory(self, row):
        ''' Construct a row instance '''
        return self._row_factory(row)

    @property
    def rowcount(self):
        ''' Return the row count of the results '''
        if self._casout is not None:
            return self._retrieve('simple.numrows', table=self._casout)['numrows']
        return -1

    def callproc(self, procname, parameters):
        ''' Call a stored procedure '''
        self._reset_output()
        return self._raise_error(NotSupportedError, 'callproc')

    def callaction(self, actionname, **parameters):
        ''' Call a CAS action '''
        self._reset_output()
        return self._retrieve(actionname, **parameters)

    def close(self):
        ''' Close the connection '''
        self._reset_output()
        self._connection = None
        self._result = None
        self.errorhandler = None

    @property
    def closed(self):
        ''' Is the connection closed? '''
        return self._connection is None

    def _get_table(self):
        ''' Drop the previous result and return a new table name '''
        if self._casout is not None:
            self._retrieve('table.droptable', table=self._casout)
        self._casout = '_PYDB_%s_' % str(uuid.uuid4()).replace('-', '_')
        return self._casout

    def _format_params(self, parameters):
        ''' Format parameters for use in a query '''
        if not parameters:
            return []
        keys = []
        if isinstance(parameters, dict):
            keys = list(parameters.keys())
            values = list(parameters.values())
        elif isinstance(parameters, items_types):
            values = list(parameters)
        for i, item in enumerate(values):
            if item is None or pd.isnull(item):
                values[i] = '.'
            if isinstance(item, text_types):
                values[i] = "'%s'" % item.replace("'", "''")
            elif isinstance(item, binary_types):
                values[i] = b"'%s'" % item.replace("'", "''")
            elif isinstance(item, datetime.datetime):
                values[i] = "'%s'dt" % item.strftime('%d%b%Y:%H:%M:%S')
            elif isinstance(item, datetime.date):
                values[i] = "'%s'd" % item.strftime('%d%b%Y')
            elif isinstance(item, datetime.time):
                values[i] = "'%s't" % item.strftime('%H:%M:%S')
            elif isinstance(item, int32_types):
                values[i] = '%s' % int32(item)
            elif isinstance(item, int64_types):
                values[i] = '%s' % int64(item)
            elif isinstance(item, float64_types):
                values[i] = '%s' % float64(item)
            elif item is True:
                values[i] = '1'
            elif item is False:
                values[i] = '0'
            else:
                raise TypeError('Unrecognized data type: %s' % item)
        if keys:
            return dict(zip(keys, values))
        return tuple(values)

    def execute(self, operation, parameters=None):
        ''' Execute a database operation '''
        if parameters is None:
            parameters = []
        self._reset_output()
        out = self._retrieve('fedsql.execdirect',
                             query=operation % self._format_params(parameters),
                             casout=self._get_table())
        self.messages.extend(out.messages)
        self._set_description()
        return self

    def executemany(self, operation, seq_of_parameters=None):
        ''' Execute multiple database operations '''
        # if parameters is None:
        #     parameters = []
        self._reset_output()
        for parameters in seq_of_parameters:
            out = self.execute(operation, parameters)
            self.messages.extend(out.messages)
        self._set_description()
        return self

    def fetchone(self):
        ''' Fetch a single row of the result '''
        del self.messages[:]
        if self._casout is None:
            return
        out = self._retrieve('table.fetch', table=self._casout,
                             from_=self._rowid, to=self._rowid,
                             sastypes=False, noindex=True)
        self.messages.extend(out.messages)
        self._rowid += 1
        if self._rowid > self.rowcount:
            self._retrieve('table.droptable', table=self._casout)
            self._casout = None
            self._rowid = 1
        return [self.row_factory(x) for x in out['Fetch'].values][0]

    def fetchmany(self, size=None):
        ''' Fetch `size` rows of the result '''
        del self.messages[:]
        if self._casout is None:
            return
        if size is None:
            size = max(self.arraysize, 1)
        out = self._retrieve('table.fetch', table=self._casout,
                             from_=self._rowid,
                             to=self._rowid + size,
                             sastypes=False, noindex=True)
        self.messages.extend(out.messages)
        self._rowid += size
        if self._rowid > self.rowcount:
            self._retrieve('table.droptable', table=self._casout)
            self._casout = None
            self._rowid = 1
        return [self.row_factory(x) for x in out['Fetch'].values]

    def fetchall(self):
        ''' Fetch all remaining rows of the result '''
        del self.messages[:]
        if self._casout is None:
            return
        out = self._retrieve('table.fetch', table=self._casout,
                             from_=self._rowid, to=self.rowcount,
                             sastypes=False, noindex=True)
        self.messages.extend(out.messages)
        self._retrieve('table.droptable', table=self._casout)
        self._casout = None
        self._rowid = 1
        return [self.row_factory(x) for x in out['Fetch'].values]

    def nextset(self):
        ''' Return the next result set '''
        return

    def setinputsizes(self, sizes):
        ''' Set input sizes '''
        return

    def setoutputsize(self, size, column):
        ''' Set output sizes '''
        return

    @property
    def connection(self):
        ''' Return Connection object '''
        return self._connection

    @property
    def rownumber(self):
        ''' Return current row number in the result '''
        return self._rowid

    def scroll(self, value, mode='relative'):
        ''' Scroll the cursor '''
        return self._raise_error(NotSupportedError, 'scroll')

    @property
    def messages(self):
        ''' Return messages returned by database '''
        return self._messages

    def next(self):
        ''' Return the next row in the result '''
        out = self.fetchone()
        if out is None:
            raise StopIteration
        return out

    def __iter__(self):
        ''' Return an iterator of the result '''
        while True:
            yield self.next()
        self._reset_output()

    @property
    def lastrowid(self):
        ''' Return the ID of the last modified row '''
        return

    def _raise_error(self, errorclass, errorvalue):
        ''' Raise the specified error '''
        # TODO: Insert messages
        if self.errorhandler is not None:
            return self.errorhandler(self._connection, self, errorclass, errorvalue)
        raise errorclass(errorvalue)


# Type Objects and Constructors

def Date(year, month, day):
    ''' Construct a date value '''
    return datetime.date(year, month, day)


def Time(hour, minute, second):
    ''' Construct a time value '''
    return datetime.time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    ''' Construct a timestamp value '''
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    ''' Construct a date object from ticks '''
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    ''' Construct a time object from ticks '''
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    ''' Construct a timestamp object from ticks '''
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(string):
    ''' Construct a binary value '''
    return bytes(string)


class DBAPITypeObject:
    ''' Base class for all column types '''

    def __init__(self, *values):
        self._values = values

    def __cmp__(self, other):
        if other in self._values:
            return 0
        if other < self._values:
            return 1
        return -1


# Standard Types

STRING = DBAPITypeObject('char', 'varchar')
BINARY = DBAPITypeObject('binary', 'varbinary')
NUMBER = DBAPITypeObject('double', 'int32', 'int64', 'decsext', 'decquad')
DATETIME = DBAPITypeObject('datetime', 'date', 'time')
ROWID = DBAPITypeObject('rowid')

# Extended Types

CHAR = DBAPITypeObject('char')
FIXEDCHAR = DBAPITypeObject('char')
VARCHAR = DBAPITypeObject('varchar')
FIXEDBINARY = DBAPITypeObject('binary')
VARBINARY = DBAPITypeObject('varbinary')
INT32 = DBAPITypeObject('int32')
INT64 = DBAPITypeObject('int64')
INTEGER = DBAPITypeObject('int32', 'int64')
FLOAT = DBAPITypeObject('double', 'decsext', 'decquad')
DOUBLE = DBAPITypeObject('double')
DECIMAL = DBAPITypeObject('decsext', 'decquad')
DECSEXT = DBAPITypeObject('decsext')
DECQUAD = DBAPITypeObject('decquad')
DATE = DBAPITypeObject('date')
TIME = DBAPITypeObject('time')
