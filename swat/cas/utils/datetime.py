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
Datetime utilities for interfacing with CAS

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import datetime
import pytz
import time
import numpy as np
import pandas as pd
from ...config import get_option
from ...utils.compat import int64, int32, text_types, binary_types


CAS_EPOCH = datetime.datetime(month=1, day=1, year=1960)
UTC_TZ = pytz.timezone('UTC')


def _astimezone(dt, tz):
    '''
    Convert date/time to given timezone

    Parameters
    ----------
    dt : date or time or datetime
        The timestamp to convert
    tz : tzinfo
        The timezone to apply

    Returns
    -------
    date or time or datetime

    '''
    if not isinstance(dt, datetime.datetime):
        return dt
    if tz is None:
        return dt.replace(tzinfo=tz)
    if dt.tzinfo is None:
        dt = UTC_TZ.localize(dt)
    return dt.astimezone(tz)


# str to CAS/SAS


def str2cas_timestamp(dts):
    '''
    Convert a string to a CAS timestamp

    Parameters
    ----------
    dts : string
        The string representation of a timestamp.

    Examples
    --------
    >>> str2cas_timestamp('19700101T12:00')
    315662400000000

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    int
        CAS timestamp

    '''
    return python2cas_datetime(_astimezone(pd.to_datetime('%s' % dts), UTC_TZ))


str2cas_datetime = str2cas_timestamp


def str2cas_date(dts):
    '''
    Convert a string to a CAS date

    Parameters
    ----------
    dts : string
        The string representation of a date.

    Examples
    --------
    >>> str2cas_date('19700101T12:00')
    3653

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    int
        CAS date

    '''
    return python2cas_date(_astimezone(pd.to_datetime('%s' % dts), UTC_TZ))


def str2cas_time(dts):
    '''
    Convert a string to a CAS time

    Parameters
    ----------
    dts : string
        The string representation of a time.

    Examples
    --------
    >>> str2cas_time('19700101T12:00')
    43200000000

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    int
        CAS time

    '''
    return python2cas_time(_astimezone(pd.to_datetime('%s' % dts), UTC_TZ))


def str2sas_timestamp(dts):
    '''
    Convert a string to a SAS timestamp

    Parameters
    ----------
    dts : string
        The string representation of a timestamp.

    Examples
    --------
    >>> str2sas_timestamp('19700101T12:00')
    315662400.0

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    float
        SAS timestamp

    '''
    return python2sas_datetime(_astimezone(pd.to_datetime('%s' % dts), UTC_TZ))


str2sas_datetime = str2sas_timestamp


def str2sas_date(dts):
    '''
    Convert a string to a SAS date

    Parameters
    ----------
    dts : string
        The string representation of a date.

    Examples
    --------
    >>> str2sas_date('19700101T12:00')
    3653.0

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    float
        SAS date

    '''
    return python2sas_date(_astimezone(pd.to_datetime('%s' % dts), UTC_TZ))


def str2sas_time(dts):
    '''
    Convert a string to a SAS time

    Parameters
    ----------
    dts : string
        The string representation of a time.

    Examples
    --------
    >>> str2sas_time('12:00')
    43200.0

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    float
        SAS time

    '''
    return python2sas_time(_astimezone(pd.to_datetime('%s' % dts), UTC_TZ))


# SAS to Python/CAS


def sas2python_timestamp(sts, tz=None):
    '''
    Convert a SAS datetime to Python datetime

    Parameters
    ----------
    sts : float
        SAS timestamp.

    Examples
    --------
    >>> sas2python_timestamp(315662400.0)
    datetime.datetime(1970, 1, 1, 12, 0)

    Returns
    -------
    :class:`datetime.datetime`

    '''
    if pd.isnull(sts):
        return pd.NaT
    return cas2python_timestamp(sas2cas_timestamp(sts), tz=tz)


sas2python_datetime = sas2python_timestamp


def sas2python_date(sdt):
    '''
    Convert a SAS date to a Python date

    Parameters
    ----------
    sts : float
        SAS date.

    Examples
    --------
    >>> sas2python_date(3653.0)
    datetime.date(1970, 1, 1)

    Returns
    -------
    :class:`datetime.date`

    '''
    if pd.isnull(sdt):
        return pd.NaT
    return cas2python_date(sas2cas_date(sdt))


def sas2python_time(sts):
    '''
    Convert a SAS time to a Python time

    Parameters
    ----------
    sts : float
        SAS time.

    Examples
    --------
    >>> sas2python_time(43200.0)
    datetime.time(12, 0)

    Returns
    -------
    :class:`datetime.time`

    '''
    if pd.isnull(sts):
        return pd.NaT
    return cas2python_time(sas2cas_time(sts))


def sas2cas_timestamp(sts):
    '''
    Convert a SAS datetime to CAS datetime

    Parameters
    ----------
    sts : float
        SAS timestamp.

    Examples
    --------
    >>> sas2cas_timestamp(315662400.0)
    315662400000000

    Returns
    -------
    int
        CAS timestamp

    '''
    return int64(sts * 10**6)


sas2cas_datetime = sas2cas_timestamp


def sas2cas_date(sdt):
    '''
    Convert a SAS date to a CAS date

    Parameters
    ----------
    sdt : float
        SAS date.

    Examples
    --------
    >>> sas2cas_date(3653.0)
    3653

    Returns
    -------
    int
        CAS date

    '''
    return int32(sdt)


def sas2cas_time(sts):
    '''
    Convert a SAS time to a CAS time

    Parameters
    ----------
    sts : float
        SAS time.

    Examples
    --------
    >>> sas2cas_time(43200.0)
    43200000000

    Returns
    -------
    int
        CAS time

    '''
    return int64(sts * 10**6)


# CAS to Python/SAS


def cas2python_timestamp(cts, tz=None):
    '''
    Convert a CAS datetime to Python datetime

    Parameters
    ----------
    cts : int
        CAS timestamp.

    Examples
    --------
    >>> cas2python_timestamp(315662400000000)
    datetime.datetime(1970, 1, 1, 12, 0)

    Returns
    -------
    :class:`datetime.datetime`

    '''
    if tz is None:
        tz = get_option('timezone')
    elif isinstance(tz, (text_types, binary_types)):
        tz = pytz.timezone(tz)
    return _astimezone(CAS_EPOCH + datetime.timedelta(microseconds=cts), tz)


cas2python_datetime = cas2python_timestamp


def cas2python_date(cdt):
    '''
    Convert a CAS date to a Python date

    Parameters
    ----------
    cdt : int
        CAS date.

    Examples
    --------
    >>> sas2python_date(3653)
    datetime.date(1970, 1, 1)

    Returns
    -------
    :class:`datetime.date`

    '''
    return (CAS_EPOCH + datetime.timedelta(days=cdt)).date()


def cas2python_time(ctm):
    '''
    Convert a CAS time to a Python time

    Parameters
    ----------
    cdt : int
        CAS time.

    Examples
    --------
    >>> cas2python_time(43200000000)
    datetime.time(12, 0)

    Returns
    -------
    :class:`datetime.time`

    '''
    return _astimezone(cas2python_datetime(ctm), UTC_TZ).time()


def cas2sas_timestamp(cdt):
    '''
    Convert a CAS timestamp to a SAS timestamp

    Parameters
    ----------
    cdt : int
        CAS timestamp.

    Examples
    --------
    >>> cas2sas_timestamp(43200000000)
    43200.0

    Returns
    -------
    float
        SAS timestamp

    '''
    return cdt / float(10**6)


cas2sas_datetime = cas2sas_timestamp


def cas2sas_date(cdt):
    '''
    Convert a CAS date to a SAS date

    Parameters
    ----------
    cdt : int
        CAS date.

    Examples
    --------
    >>> cas2sas_date(3653)
    3653.0

    Returns
    -------
    float
        SAS date

    '''
    return float(cdt)


def cas2sas_time(cdt):
    '''
    Convert a CAS time to a SAS time

    Parameters
    ----------
    cdt : int
        CAS time.

    Examples
    --------
    >>> cas2sas_time(43200000000)
    43200.0

    Returns
    -------
    float
        SAS time

    '''
    return cdt / float(10**6)


# Python to CAS/SAS


def python2cas_timestamp(pyts):
    '''
    Convert a Python datetime to CAS datetime

    Parameters
    ----------
    pyts : :class:`datetime.datetime`
        Python timestamp.

    Examples
    --------
    >>> python2cas_timestamp(datetime.datetime(1970, 1, 1, 12, 0))
    315662400000000

    Returns
    -------
    int
        CAS timestamp

    '''
    delta = _astimezone(pyts, UTC_TZ) - _astimezone(CAS_EPOCH, UTC_TZ)
    if isinstance(delta, type(pd.NaT)):
        # TODO: Change when integers support missing values
        return 0
    return int64((delta.days * 24 * 60 * 60 * 10**6)
                 + (delta.seconds * 10**6) + delta.microseconds)


python2cas_datetime = python2cas_timestamp


def python2cas_time(pytm):
    '''
    Convert a Python time to a CAS time

    Parameters
    ----------
    pyts : :class:`datetime.time`
        Python time.

    Examples
    --------
    >>> cas2python_time(datetime.time(12, 0))
    43200000000

    Returns
    -------
    int
        CAS time

    '''
    if isinstance(pytm, datetime.datetime):
        pytm = _astimezone(pytm, UTC_TZ)
    return int64(pytm.hour * (60 * 60 * 10**6) + (pytm.minute * 60 * 10**6)
                 + (pytm.second * 10**6) + pytm.microsecond)


def python2cas_date(pydt):
    '''
    Convert a Python date to a CAS date

    Parameters
    ----------
    pyts : :class:`datetime.date`
        Python date.

    Examples
    --------
    >>> cas2python_date(datetime.date(1970, 1, 1))
    3653

    Returns
    -------
    int
        CAS date

    '''
    if isinstance(pydt, datetime.datetime):
        delta = _astimezone(pydt, UTC_TZ).date() - CAS_EPOCH.date()
    elif isinstance(pydt, datetime.time):
        delta = datetime.date.today() - CAS_EPOCH.date()
    else:
        delta = pydt - CAS_EPOCH.date()
    if isinstance(delta, type(pd.NaT)):
        # TODO: Change when integers support missing values
        return 0
    return int32(delta.days)


def python2sas_timestamp(pyts, tz=None):
    '''
    Convert a Python datetime to SAS datetime

    Parameters
    ----------
    pyts : :class:`datetime.datetime`
        Python timestamp.

    Examples
    --------
    >>> python2sas_timestamp(datetime.datetime(1970, 1, 1, 12, 0))
    315662400.0

    Returns
    -------
    float
        SAS timestamp

    '''
    return python2cas_timestamp(pyts) / float(10**6)


python2sas_datetime = python2sas_timestamp


def python2sas_date(pydt):
    '''
    Convert a Python date to a SAS date

    Parameters
    ----------
    pydt : :class:`datetime.date`
        Python date.

    Examples
    --------
    >>> python2sas_date(datetime.datetime(1970, 1, 1, 12, 0))
    3653.0

    Returns
    -------
    float
        SAS date

    '''
    if isinstance(pydt, datetime.datetime):
        delta = _astimezone(pydt, UTC_TZ).date() - CAS_EPOCH.date()
    elif isinstance(pydt, datetime.time):
        delta = datetime.date.today() - CAS_EPOCH.date()
    else:
        delta = pydt - CAS_EPOCH.date()
    if isinstance(delta, type(pd.NaT)):
        return np.nan
    return float(delta.days)


def python2sas_time(pytm):
    '''
    Convert a Python time to a SAS time

    Parameters
    ----------
    pytm : :class:`datetime.time`
        Python time.

    Examples
    --------
    >>> python2sas_time(datetime.datetime(1970, 1, 1, 12, 0))
    43200.0

    Returns
    -------
    float
        SAS time

    '''
    return python2cas_time(pytm) / float(10**6)


def _local_time_offset(timestamp):
    '''
    Return offset of local zone from GMT

    Parameters
    ----------
    timestamp : float
       Timestamp to give the GMT offset of.

    Returns
    -------
    int
        Number of offset seconds

    '''
    if time.localtime(timestamp).tm_isdst:
        return -time.altzone
    return -time.timezone
