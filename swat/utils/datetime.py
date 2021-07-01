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
Datetime utilities

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import re
import six
from ..config import get_option


def is_datetime_format(fmt):
    '''
    Is the given format name a datetime format?

    Parameters
    ----------
    fmt : string
        Name of a SAS format

    Returns
    -------
    bool

    '''
    if not fmt:
        return False
    dt_formats = get_option('cas.dataset.datetime_formats')
    if isinstance(dt_formats, six.string_types):
        dt_formats = [dt_formats]
    datetime_regex = re.compile(r'^(%s)(\d*\.\d*)?$' % '|'.join(dt_formats), flags=re.I)
    return bool(datetime_regex.match(fmt))


def is_date_format(fmt):
    '''
    Is the given format name a date format?

    Parameters
    ----------
    fmt : string
        Name of a SAS format

    Returns
    -------
    bool

    '''
    if not fmt:
        return False
    d_formats = get_option('cas.dataset.date_formats')
    if isinstance(d_formats, six.string_types):
        d_formats = [d_formats]
    date_regex = re.compile(r'^(%s)(\d*\.\d*)?$' % '|'.join(d_formats), flags=re.I)
    return bool(date_regex.match(fmt))


def is_time_format(fmt):
    '''
    Is the given format name a time format?

    Parameters
    ----------
    fmt : string
        Name of a SAS format

    Returns
    -------
    bool

    '''
    if not fmt:
        return False
    t_formats = get_option('cas.dataset.time_formats')
    if isinstance(t_formats, six.string_types):
        t_formats = [t_formats]
    time_regex = re.compile(r'^(%s)(\d*\.\d*)?$' % '|'.join(t_formats), flags=re.I)
    return bool(time_regex.match(fmt))
