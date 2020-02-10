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
SAS Scripting Wrapper for Analytics Transfer (SWAT)
===================================================

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

# Make sure we meet the minimum requirements
import sys

if sys.hexversion < 0x02070000:
    raise RuntimeError('Python 2.7 or newer is required to use this package.')

# Configuration
from . import config    # noqa: E402
from .config import (set_option, get_option, reset_option, describe_option,
                     options, option_context)    # noqa: E402

# CAS utilities
from .cas import (CAS, vl, nil, getone, getnext, datamsghandlers, blob)    # noqa: E402
from .cas import (dir_actions, dir_members)    # noqa: E402
from .cas.table import CASTable    # noqa: E402

# Conflicts with .cas.table, so we import it excplicitly here
from .cas.utils import table    # noqa: E402

# DataFrame with SAS metadata
from .dataframe import SASDataFrame, reshape_bygroups    # noqa: E402

# Functions
from .functions import concat, merge    # noqa: E402

# Exceptions
from .exceptions import SWATError, SWATOptionError, SWATCASActionError    # noqa: E402

# SAS Formatter
from .formatter import SASFormatter    # noqa: E402

__version__ = '1.6.1'
