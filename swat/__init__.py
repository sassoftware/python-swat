#!/usr/bin/env python
# encoding: utf-8

'''
SWAT (SAS Wrapper for Analytics Transfer)
=========================================

This package allows you to connect to a SAS CAS host and call actions.
The responses and results are returned as Python objects.

Create a connection
-------------------

>>> s = CAS('myhost.com', 12345, 'username', 'password')

Load a data table
-----------------

>>> tbl = s.read_path('datasources/cars_single.sashdat')

Load an action set
------------------

>>> s.loadactionset(actionset='simple')

Get help for an action set
--------------------------

>>> help(s.simple)  # or s.simple? in IPython

Get help for an action
----------------------

>>> help(s.summary)  # or s.summary? in IPython

Call an action from the library
-------------------------------

>>> result = tbl.summary()
>>> print(result)
>>> print(result.Summary)

'''

from __future__ import print_function, division, absolute_import, unicode_literals

__version__ = '0.9.0'

# Configuration
from . import config
from .config import (set_option, get_option, reset_option, describe_option, options,
                     option_context)

# CAS utilities
from .cas import (InitializeTK, CAS, vl, nil, getone, getnext, CASResults, CASAction,
                  CASActionSet, datamsghandlers, blob, initialize_tk)
from .cas.table import CASTable

# Conflicts with .cas.table, so we import it excplicitly here
from .cas.utils import table

# DataFrame with SAS metadata
from .dataframe import SASDataFrame, concat, reshape_bygroups

# Exceptions
from .exceptions import SWATError, SWATOptionError, SWATCASActionError

# SAS Formatter
from .formatter import SASFormatter

__all__ = ['config', 'set_option', 'get_option', 'reset_option', 'describe_option',
           'options', 'option_context', 'InitializeTK', 'CAS', 'vl', 'nil', 'getone',
           'getnext', 'CASResults', 'CASAction', 'CASActionSet', 'datamsghandlers',
           'blob', 'initialize_tk', 'table', 'SASDataFrame', 'SWATError', 
           'SWATOptionError', 'SWATCASActionError', 'SASFormatter', 'CASTable',
           'concat', 'reshape_bygroups']
