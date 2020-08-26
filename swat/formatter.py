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
SAS Data Formatter

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import datetime
import numpy as np
import re
from pandas import Timestamp
from . import clib
from .cas import utils
from .clib import errorcheck
from .cas.table import CASTable
from .exceptions import SWATError
from .utils import getsoptions
from .utils.compat import (a2n, a2u, int32, int64, float64, float64_types,
                           int32_types, int64_types, text_types, binary_types,
                           bool_types)

# pylint: disable=C0330


class SASFormatter(object):
    '''
    Create a locale-aware SAS value formatter

    This class is typically constructed by calling the :meth:`CAS.SASFormatter`
    method.  When used in that way, the options for the :class:`SASFormatter`
    match the :class:`CAS` settings.

    Parameters
    ----------
    locale : string, optional
        POSIX locale to use for formatting.
    soptions : string, optional
        SOptions string from connection object (internal use only).

    Notes
    -----
    This class requires the binary SAS support libraries to function. It will
    not work in pure Python mode.

    Returns
    -------
    :class:`SASFormatter` object

    '''

    def __init__(self, locale=None, soptions=None):
        if soptions is not None:
            pass
        elif locale is None:
            soptions = ''
        else:
            soptions = getsoptions(locale=locale)
        self._soptions = soptions
        self._sw_formatter = None
        self._load_attempted = False

    def _load_formatter(self):
        ''' Allow lazy loading of formatter '''
        if self._load_attempted:
            return
        try:
            self._load_attempted = True
            _sw_error = clib.SW_CASError(a2n(self._soptions))
            self._sw_formatter = errorcheck(
                clib.SW_CASFormatter(a2n(self._soptions), _sw_error), _sw_error)
        except Exception:
            pass

    def format(self, value, sasfmt=None, width=12):
        '''
        Format the given value

        Parameters
        ----------
        value : any
            The value to format.
        sasfmt : string, optional
            The SAS format to use.
        width : int, optional
            The width of the field to format to.

        Examples
        --------
        >>> fmt = SASFormatter()

        >>> fmt.format(123.45678, 'F8.2')
        123.45

        >>> fmt.format(123.45678, 'DATE.')
        '03-MAY-1960'

        Returns
        -------
        string
            Formatted form of input value.

        '''
        self._load_formatter()

        out = None

        if self._sw_formatter is None:
            return self._generic_format(value, sasfmt=sasfmt, width=width)

        if isinstance(value, float64_types):
            if np.isnan(value) or value is None:
                out = a2u(str(value))
            else:
                try:
                    out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                         float64(value), a2n(sasfmt), int32(width)),
                                         a2n('utf-8')),
                                     self._sw_formatter)
                except SWATError:
                    out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                         float64(value), a2n('best12.'), int32(width)),
                                         a2n('utf-8')),
                                     self._sw_formatter)
        elif isinstance(value, int64_types):
            try:
                out = errorcheck(a2u(self._sw_formatter.formatInt64(
                                     int64(value), a2n(sasfmt), int32(width)),
                                     a2n('utf-8')),
                                 self._sw_formatter)
            except SWATError:
                out = errorcheck(a2u(self._sw_formatter.formatInt64(
                                     int64(value), a2n('best12.'), int32(width)),
                                     a2n('utf-8')),
                                 self._sw_formatter)
        elif isinstance(value, int32_types):
            try:
                try:
                    out = errorcheck(a2u(self._sw_formatter.formatInt32(
                                         int32(value), a2n(sasfmt), int32(width)),
                                         a2n('utf-8')),
                                     self._sw_formatter)
                except SWATError:
                    out = errorcheck(a2u(self._sw_formatter.formatInt32(
                                         int32(value), a2n('best12.'), int32(width)),
                                         a2n('utf-8')),
                                     self._sw_formatter)
            except OverflowError:
                try:
                    out = errorcheck(a2u(self._sw_formatter.formatInt64(
                                         int64(value), a2n(sasfmt), int32(width)),
                                         a2n('utf-8')),
                                     self._sw_formatter)
                except SWATError:
                    out = errorcheck(a2u(self._sw_formatter.formatInt64(
                                         int64(value), a2n('best12.'), int32(width)),
                                         a2n('utf-8')),
                                     self._sw_formatter)
        elif isinstance(value, text_types):
            try:
                out = errorcheck(a2u(self._sw_formatter.formatString(
                                     a2n(value), a2n(sasfmt),
                                     int32(width)), a2n('utf-8')),
                                 self._sw_formatter)
            except SWATError:
                out = value
        # TODO: Should binary types ever get here?
        elif isinstance(value, binary_types):
            out = errorcheck(a2u(self._sw_formatter.formatString(
                                 a2n(value), a2n(sasfmt), int32(width)), a2n('utf-8')),
                             self._sw_formatter)
        elif isinstance(value, bool_types):
            out = errorcheck(a2u(self._sw_formatter.formatInt32(
                                 int32(value), a2n(sasfmt), int32(width)), a2n('utf-8')),
                             self._sw_formatter)
        elif isinstance(value, (datetime.datetime, Timestamp)):
            out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                 utils.datetime.python2sas_datetime(value),
                                 a2n(sasfmt), int32(width)),
                                 a2n('utf-8')),
                             self._sw_formatter)
        elif isinstance(value, datetime.date):
            out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                 utils.datetime.python2sas_date(value),
                                 a2n(sasfmt), int32(width)),
                                 a2n('utf-8')),
                             self._sw_formatter)
        elif isinstance(value, datetime.time):
            out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                 utils.datetime.python2sas_time(value),
                                 a2n(sasfmt), int32(width)),
                                 a2n('utf-8')),
                             self._sw_formatter)
        elif value is None:
            out = errorcheck(a2u(self._sw_formatter.formatString(
                                 a2n(''), a2n(sasfmt), int32(width)), a2n('utf-8')),
                             self._sw_formatter)

        # For CASTable columns in dataframes
        elif isinstance(value, CASTable):
            return a2u(str(value))

        if out is None:
            raise TypeError(type(value))

        return out

    __call__ = format

    def _format_numeric(self, value, sasfmt, width=12, commas=False):
        ''' Format fixed with numerics '''
        m = re.match(r'^([A-Za-z]*)(\d*)\.(\d*)$', sasfmt)
        name = m.group(1).strip().lower()
        a = m.group(2).strip()
        b = m.group(3).strip() or '0'
        if commas:
            a = ','
        out = a2u(('{:' + a + '.' + b + 'f}').format(value).strip())
        if name in ['dollar', 'nldollar', 'mny', 'nlmny']:
            return '$' + out
        if name in ['euro']:
            return '\u20ac' + out
        if name in ['best', 'int']:
            return re.sub(r'\.0*$', r'', out)
        return out

    def _generic_format(self, value, sasfmt=None, width=12):
        ''' Generic formatter for when tkefmt isn't available '''
        out = None

        if sasfmt and '.' not in sasfmt:
            sasfmt = sasfmt + '.'

        num_fmts = r'^(d|f|int|best)?\d*\.\d*$'
        money_fmts = r'^(nl)?(comma|mny|mny|dollar|euro)\d*\.\d*$'

        if isinstance(value, float64_types):
            if np.isnan(value) or value is None:
                out = a2u(str(value))
            elif sasfmt and re.match(num_fmts, sasfmt, flags=re.I):
                out = self._format_numeric(value, sasfmt, width=width)
            elif sasfmt and re.match(money_fmts, sasfmt, flags=re.I):
                out = self._format_numeric(value, sasfmt, width=width, commas=True)
            else:
                out = a2u(str(float64(value)))
        elif isinstance(value, int64_types):
            if sasfmt and re.match(num_fmts, sasfmt):
                out = self._format_numeric(value, sasfmt, width=width)
            elif sasfmt and re.match(money_fmts, sasfmt, flags=re.I):
                out = self._format_numeric(value, sasfmt, width=width, commas=True)
            else:
                out = a2u(str(int64(value)))
        elif isinstance(value, int32_types):
            try:
                if sasfmt and re.match(num_fmts, sasfmt, flags=re.I):
                    out = self._format_numeric(int32(value), sasfmt, width=width)
                elif sasfmt and re.match(money_fmts, sasfmt, flags=re.I):
                    out = self._format_numeric(value, sasfmt, width=width, commas=True)
                else:
                    out = a2u(str(int32(value)))
            except OverflowError:
                if sasfmt and re.match(num_fmts, sasfmt, flags=re.I):
                    out = self._format_numeric(int64(value), sasfmt, width=width)
                elif sasfmt and re.match(money_fmts, sasfmt, flags=re.I):
                    out = self._format_numeric(value, sasfmt, width=width, commas=True)
                else:
                    out = a2u(str(int64(value)))
        elif isinstance(value, text_types):
            out = a2u(value)
        # TODO: Should binary types ever get here?
        elif isinstance(value, binary_types):
            out = a2u(value)
        elif isinstance(value, bool_types):
            out = a2u(str(value))
        elif isinstance(value, (datetime.date, datetime.time, datetime.datetime,
                                Timestamp)):
            out = a2u(str(value))
        elif value is None:
            out = a2u('')

        # For CASTable columns in dataframes
        elif isinstance(value, CASTable):
            return a2u(str(value))

        if out is None:
            raise TypeError(type(value))

        return out
