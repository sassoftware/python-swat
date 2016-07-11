#!/usr/bin/env python
# encoding: utf-8

'''
General utilities for interfacing with CAS

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import copy
import datetime
import json
import pandas as pd
import six
import time
from .. import clib
from ..utils.compat import a2n, int_types, int64, int32
from ..utils.args import dict2kwargs, iteroptions
from ..utils.xdict import xadict


def InitializeTK(path):
    '''
    Initialize the TK subsystem

    Parameters
    ----------
    path : string
       Colon (semicolon on Windows) separated list of directories to
       search for TK components


    Examples
    --------
    Set the TK search path to look through /usr/local/tk/ and /opt/sas/tk/.

    >>> swat.InitializeTK('/usr/local/tk:/opt/sas/tk')

    '''
    clib.InitializeTK(a2n(path, 'utf-8'))


initialize_tk = InitializeTK


def vl(**kwargs):
    '''
    Value List

    This is simply a shorter named version of the `dict` function
    that only takes keyword parameters as arguments.

    Parameters
    ----------
    **kwargs : any, optional
       Arbitrary keyword arguments

    Returns
    -------
    dict
       Dictionary containing keyword arguments

    Examples
    --------
    >>> vl(arg1=1, arg2=2, arg3='foo')
    {'arg1': 1, 'arg2': 2, 'arg3': 'foo'}

    '''
    return xadict(**kwargs)


def table(name, **kwargs):
    '''
    Convenience wrapper for connectionless table parameters

    Parameters
    ----------
    name : string
       The name of the table in CAS
    **kwargs : any, optional
       Arbitrary keyword arguments

    Returns
    -------
    dict
       Dictionary of keyword arguments and table name

    Examples
    --------
    >>> tbl('my.table')
    {'name': 'my.table'}
    >>> tbl('my.table', varlist=['var1','var2','var3'])
    {'name': 'my.table', 'varlist': ['var1', 'var2', 'var3']}

    '''
    out = xadict(**kwargs)
    out['name'] = name
    return out


#
# Datetime functions
#

CAS_EPOCH = datetime.datetime(month=1, day=1, year=1960)


# str to CAS/SAS


def str2cas_timestamp(dts):
    ''' Convert a string to a CAS timestamp '''
    return python2cas_datetime(pd.to_datetime(dts))

str2cas_datetime = str2cas_timestamp


def str2cas_date(dts):
    ''' Convert a string to a CAS date '''
    return python2cas_date(pd.to_datetime(dts))


def str2cas_time(dts):
    ''' Convert a string to a CAS time '''
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
    ''' Convert a CAS datetime to Python datetime '''
    return CAS_EPOCH + datetime.timedelta(microseconds=cts)

cas2python_datetime = cas2python_timestamp


def cas2python_date(cdt):
    ''' Convert a CAS date to a Python date '''
    return (CAS_EPOCH + datetime.timedelta(days=cdt)).date()


def cas2python_time(ctm):
    ''' Convert a CAS time to a Python time '''
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
    ''' Convert a Python datetime to CAS datetime '''
    delta = pyts - CAS_EPOCH
    if isinstance(delta, pd.tslib.NaTType):
        # TODO: Change when integers support missing values
        return 0
    return int64((delta.days * 24 * 60 * 60 * 10**6) +
                 (delta.seconds * 10**6) + delta.microseconds)

python2cas_datetime = python2cas_timestamp


def python2cas_time(pytm):
    ''' Convert a Python time to a CAS time '''
    return int64(pytm.hour * (60 * 60 * 10**6) + (pytm.minute * 60 * 10**6) +
                 (pytm.second * 10**6) + pytm.microsecond)


def python2cas_date(pydt):
    ''' Convert a Python date to a CAS date '''
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


#
# Parameter Manager classes
#

class ActionParamManager(object):
    '''
    Base class for CAS action parameter manager

    '''

    def __init__(self):
        self._action_params = {}

    def get_action_params(self, name, *default):
        ''' Return parameters for specified action name '''
        try:
            return self._action_params[name]
        except KeyError:
            if default:
                return default[0]
        return None

    def set_action_params(self, name, **kwargs):
        ''' Set parameters for specified action name '''
        self._action_params.setdefault(name, {}).update(kwargs)

    def del_action_params(self, *names):
        ''' Delete parameters for specified action names '''
        for name in names:
            try:
                del self._action_params[name]
            except KeyError:
                pass


@six.python_2_unicode_compatible
class ParamManager(object):
    '''
    Base class for objects that take CAS action parameters

    Parameters
    ----------
    *args : key / value pair strings, optional
        Parameter names and values can be specified as subsequent
        arguments (not tuples).
    **kwargs : dict, optional
        Key / value pairs of parameters

    Returns
    -------
    ParamManager object

    '''

    param_names = []

    def __init__(self, *args, **kwargs):
        self._contexts = []
        self.params = xadict()
        self.set_params(*args, **kwargs)

    def __enter__(self):
        self._contexts.append(copy.deepcopy(self.params))
        return self

    def __exit__(self, type, value, traceback):
        self.params = self._contexts.pop()

    def _cast_value(self, val):
        '''
        Convert values in dictionary to proper parameter types

        The primary purpose of this function is to convert dictionaries
        that have only integer keys into lists.  This operation is done
        recursively over the entire structure.

        Parameters
        ----------
        val : any
            The value to convert

        Returns
        -------
        any

        '''
        if isinstance(val, dict):
            if len(val) and all(isinstance(key, int_types) for key in val.keys()):
                return [self._cast_value(val[k]) for k in sorted(six.iterkeys(val))]
            return {k: self._cast_value(v) for k, v in six.iteritems(val)}

        if isinstance(val, list):
            return [self._cast_value(x) for x in val]

        return val

    def to_dict(self):
        ''' Return the parameters as a dictionary '''
        return self._cast_value(self.params)

    to_params = to_dict

    def to_json(self, *args, **kwargs):
        '''
        Convert parameters to JSON

        Parameters
        ----------
        *args : any, optional
            Additional arguments to json.dumps
        **kwargs : any, optional
            Additional arguments to json.dumps

        Returns
        -------
        string

        '''
        return json.dumps(self.to_dict(), *args, **kwargs)

    def set_params(self, *args, **kwargs):
        '''
        Set paramaters according to key/value pairs

        Parameters
        ----------
        *args : any, optional
           Key / value pairs specified as sequential arguments (not in tuples).
        **kwargs : any, optional
           Arbitrary keyword arguments.  These key/value pairs of
           parameters added to the CAS action's set of parameters.

        Returns
        -------
        None

        '''
        for key, value in iteroptions(*args, **kwargs):
            self.params[key] = value

    set_param = set_params

    def del_params(self, *keys):
        '''
        Delete parameters

        Parameters
        ----------
        *keys : strings
           Names of parameters to delete

        Returns
        -------
        None

        '''
        for key in keys:
            self.params.pop(key, None)

    del_param = del_params

    def get_param(self, key, *default):
        '''
        Return the value of a parameter

        Parameters
        ----------
        key : string
           Name of parameter
        default : any
           The value to return if the parameter doesn't exist

        Returns
        -------
        any
           Value of parameter

        '''
        try:
            return self.params[key]
        except KeyError:
            if default:
                return default[0]
            raise

    def get_params(self, *keys):
        '''
        Return the values of one or more parameters

        Parameters
        ----------
        *keys : one or more strings
           Names of parameters

        Returns
        -------
        dict
           Dictionary of requested parameters

        '''
        out = {}
        for key in keys:
            out[key] = self.params[key]
        return out

    def has_params(self, *keys):
        '''
        Return a boolean indicating whether or not the parameters exist

        Parameters
        ----------
        *keys : one or more strings
            Names of parameters

        Returns
        -------
        True or False

        '''
        for key in keys:
            if key not in self.params:
                return False
        return True

    has_param = has_params

    def __setattr__(self, name, value):
        ''' Set an attribute '''
        if name in type(self).param_names:
            self.params[name] = value
            return
        return object.__setattr__(self, name, value)

    def __delattr__(self, name):
        ''' Delete an attribute '''
        if name in type(self).param_names:
            if name in self.params:
                del self.params[name]
                return
            raise AttributeError(name)
        return object.__delattr__(self, name)

    def __getattr__(self, name):
        ''' Get named attribute '''
        if name in type(self).param_names:
            if name not in self.params:
                self.params[name] = xadict()
            return self.params[name]
        return object.__getattribute__(self, name)

    def __str__(self):
        return '?.%s(%s)' % (type(self).__name__, dict2kwargs(self.to_params(),
                                                              fmt='%s'))

    def __repr__(self):
        return str(self)
