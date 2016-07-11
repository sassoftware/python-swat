#!/usr/bin/env python
# encoding: utf-8

'''
SAS Data Formatter

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import datetime
import numpy as np
from . import clib
from .cas import utils
from .clib import errorcheck
from .cas.table import CASTable
from pandas import Timestamp
from .utils import getsoptions
from .utils.compat import (a2n, a2u, int32, int64, float64, float64_types,
                           int32_types, int64_types, text_types, binary_types,
                           bool_types)

# pylint: disable=C0330


class SASFormatter(object):
    '''
    Create a locale-aware SAS value formatter

    Parameters
    ----------
    locale : string, optional
       POSIX locale to use for formatting
    soptions : string, optional
       SOptions string from connection object (internal use only)

    Returns
    -------
    SASFormatter object

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
        except:
            pass

    def format(self, value, sasfmt=None, width=12):
        '''
        Format the given value

        Parameters
        ----------
        value : any
           The value to format

        sasfmt : string, optional
           The SAS format to use
        width : int, long, optional
           The width of the field to format to

        Returns
        -------
        string
           Formatted form of input value

        '''
        self._load_formatter()

        out = None

        if self._sw_formatter is None:
            return self._generic_format(value, sasfmt=None, width=12)

        if isinstance(value, float64_types):
            if np.isnan(value) or value is None:
                out = a2u(str(value))
            else:
                out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                     float64(value), a2n(sasfmt), int32(width)),
                                     a2n('utf-8')),
                                 self._sw_formatter)
        elif isinstance(value, int64_types):
            out = errorcheck(a2u(self._sw_formatter.formatInt64(
                                 int64(value), a2n(sasfmt), int32(width)), a2n('utf-8')),
                             self._sw_formatter)
        elif isinstance(value, int32_types):
            try:
                out = errorcheck(a2u(self._sw_formatter.formatInt32(
                                     int32(value), a2n(sasfmt), int32(width)),
                                     a2n('utf-8')),
                                 self._sw_formatter)
            except OverflowError:
                out = errorcheck(a2u(self._sw_formatter.formatInt64(
                                     int64(value), a2n(sasfmt), int32(width)),
                                     a2n('utf-8')),
                                 self._sw_formatter)
        elif isinstance(value, text_types):
            out = errorcheck(a2u(self._sw_formatter.formatString(
                                 a2n(value), a2n(sasfmt),
                                 int32(width)), a2n('utf-8')),
                             self._sw_formatter)
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
                                 utils.python2sas_datetime(value),
                                 a2n(sasfmt), int32(width)),
                                 a2n('utf-8')),
                             self._sw_formatter)
        elif isinstance(value, datetime.date):
            out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                 utils.python2sas_date(value),
                                 a2n(sasfmt), int32(width)),
                                 a2n('utf-8')),
                             self._sw_formatter)
        elif isinstance(value, datetime.time):
            out = errorcheck(a2u(self._sw_formatter.formatDouble(
                                 utils.python2sas_time(value),
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

    def _generic_format(self, value, sasfmt=None, width=12):
        ''' Generic formatter for when tkefmt isn't available '''
        if isinstance(value, float64_types):
            if np.isnan(value) or value is None:
                out = a2u(str(value))
            else:
                out = a2u(str(float64(value)))
        elif isinstance(value, int64_types):
            out = a2u(str(int64(value)))
        elif isinstance(value, int32_types):
            try:
                out = a2u(str(int32(value)))
            except OverflowError:
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
