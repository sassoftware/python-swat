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
Parameter utilities for interfacing with CAS

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import copy
import json
import six
from ...utils.compat import int_types
from ...utils.args import dict2kwargs, iteroptions
from ...utils.xdict import xadict


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
    >>> tbl('my.table', vars=['var1','var2','var3'])
    {'name': 'my.table', 'vars': ['var1', 'var2', 'var3']}

    '''
    out = xadict(**kwargs)
    out['name'] = name
    return out


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
        Set paramaters according to key-value pairs

        Parameters
        ----------
        *args : any, optional
           Key-value pairs specified as sequential arguments (not in tuples).
        **kwargs : any, optional
           Arbitrary keyword arguments.  These key-value pairs of
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
        Return True if the specified parameters exist

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
