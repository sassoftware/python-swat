#!/usr/bin/env python
# encoding: utf-8

'''
Datetime utilities for interfacing with CAS

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import datetime
import pandas as pd
import six
import time
from ...utils.compat import int64, int32


CAS_EPOCH = datetime.datetime(month=1, day=1, year=1960)


# str to CAS/SAS


def str2cas_timestamp(dts):
    ''' 
    Convert a string to a CAS timestamp

    Parameters
    ----------
    dts : string
        The string representation of a timestamp.

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    int
        CAS timestamp

    '''
    return python2cas_datetime(pd.to_datetime(dts))

str2cas_datetime = str2cas_timestamp


def str2cas_date(dts):
    '''
    Convert a string to a CAS date

    Parameters
    ----------
    dts : string
        The string representation of a date.

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    int
        CAS date

    '''
    return python2cas_date(pd.to_datetime(dts))


def str2cas_time(dts):
    ''' 
    Convert a string to a CAS time

    Parameters
    ----------
    dts : string
        The string representation of a time.

    See Also
    --------
    :func:`pandas.to_datetime`

    Returns
    -------
    int
        CAS time

    '''
    return python2cas_time(pd.to_datetime(dts))


def str2sas_timestamp(dts):
    ''' Convert a string to a SAS timestamp '''
    return python2sas_datetime(pd.to_datetime(dts))

str2sas_datetime = str2sas_timestamp


def str2sas_date(dts):
    ''' Convert a string to a SAS date '''
    return python2sas_date(pd.to_datetime(dts))


def str2sas_time(dts):
    ''' Convert a string to a SAS time '''
    return python2sas_time(pd.to_datetime(dts))


# SAS to Python/CAS


def sas2python_timestamp(sts):
    ''' Convert a SAS datetime to Python datetime '''
    return cas2python_timestamp(sas2cas_timestamp(sts))

sas2python_datetime = sas2python_timestamp


def sas2python_date(sdt):
    ''' Convert a SAS date to a Python date '''
    return cas2python_date(sas2cas_date(sdt))


def sas2python_time(sts):
    ''' Convert a SAS time to a Python time '''
    return cas2python_time(sas2cas_time(sts))


def sas2cas_timestamp(sts):
    ''' Convert a SAS datetime to CAS datetime '''
    return int64(sts * 10**6)

sas2cas_datetime = sas2cas_timestamp


def sas2cas_date(sdt):
    ''' Convert a SAS date to a CAS date '''
    return int32(sdt)


def sas2cas_time(sts):
    ''' Convert a SAS time to a CAS time '''
    return int64(sts * 10**6)


# CAS to Python/SAS


def cas2python_timestamp(cts):
    '''
    Convert a CAS datetime to Python datetime

    Parameters
    ----------
    cts : int
        CAS timestamp.

    Returns
    -------
    :class:`python.datetime.datetime`

    '''
    return CAS_EPOCH + datetime.timedelta(microseconds=cts)

cas2python_datetime = cas2python_timestamp


def cas2python_date(cdt):
    '''
    Convert a CAS date to a Python date

    Parameters
    ----------
    cdt : int
        CAS date.

    Returns
    -------
    :class:`python.datetime.date`

    '''
    return (CAS_EPOCH + datetime.timedelta(days=cdt)).date()


def cas2python_time(ctm):
    '''
    Convert a CAS time to a Python time

    Parameters
    ----------
    cdt : int
        CAS time.

    Returns
    -------
    :class:`python.datetime.time`

    '''
    return cas2python_datetime(ctm).time()


def cas2sas_timestamp(cdt):
    ''' Convert a CAS timestamp to a SAS timestamp '''
    return cdt / float(10**6)

cas2sas_datetime = cas2sas_timestamp


def cas2sas_date(cdt):
    ''' Convert a CAS date to a SAS date '''
    return float(cdt)


def cas2sas_time(cdt):
    ''' Convert a CAS time to a SAS time '''
    return cdt / float(10**6)


# Python to CAS/SAS


def python2cas_timestamp(pyts):
    '''
    Convert a Python datetime to CAS datetime
 
    Parameters
    ----------
    pyts : :class:`python.datetime.datetime`
        Python timestamp.

    Returns
    -------
    int
        CAS timestamp

    '''
    delta = pyts - CAS_EPOCH
    if isinstance(delta, pd.tslib.NaTType):
        # TODO: Change when integers support missing values
        return 0
    return int64((delta.days * 24 * 60 * 60 * 10**6) +
                 (delta.seconds * 10**6) + delta.microseconds)

python2cas_datetime = python2cas_timestamp


def python2cas_time(pytm):
    '''
    Convert a Python time to a CAS time

    Parameters
    ----------
    pyts : :class:`python.datetime.time`
        Python time.

    Returns
    -------
    int
        CAS time

    '''
    return int64(pytm.hour * (60 * 60 * 10**6) + (pytm.minute * 60 * 10**6) +
                 (pytm.second * 10**6) + pytm.microsecond)


def python2cas_date(pydt):
    '''
    Convert a Python date to a CAS date

    Parameters
    ----------
    pyts : :class:`python.datetime.date`
        Python date.

    Returns
    -------
    int
        CAS date

    '''
    if isinstance(pydt, datetime.datetime):
        delta = pydt.date() - CAS_EPOCH.date()
    elif isinstance(pydt, datetime.time):
        delta = datetime.date.today() - CAS_EPOCH.date()
    else:
        delta = pydt - CAS_EPOCH.date()
    if isinstance(delta, pd.tslib.NaTType):
        # TODO: Change when integers support missing values
        return 0
    return int32(delta.days)


def python2sas_timestamp(pyts):
    ''' Convert a Python datetime to SAS datetime '''
    return python2cas_timestamp(pyts) / float(10**6)

python2sas_datetime = python2sas_timestamp


def python2sas_date(pydt):
    ''' Convert a Python date to a SAS date '''
    return float((pydt - CAS_EPOCH.date()).days)


def python2sas_time(pytm):
    ''' Convert a Python time to a SAS time '''
    return python2cas_time(pytm) / float(10**6)


def _local_time_offset(timestamp):
    '''
    Return offset of local zone from GMT

    Parameters
    ----------
    t : float, optional
       Timestamp to give the GMT offset of

    Returns
    -------
    int
        Number of offset seconds

    '''
    if time.localtime(timestamp).tm_isdst:
        return -time.altzone
    return -time.timezone
