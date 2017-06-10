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
Utilities for dealing with function/method arguments

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import re
import six
import locale as loc
from .compat import items_types


def mergedefined(*args):
    '''
    Merge the defined key/value pairs of multiple dictionaries

    Parameters
    ----------
    *args : list of dicts
       One or more dictionaries

    Returns
    -------
    dict
       Dictionary of merged key/value pairs

    '''
    out = {}
    for kwarg in args:
        for key, value in six.iteritems(kwarg):
            if value is not None:
                out[key] = value
    return out


def dict2kwargs(dct, fmt='dict(%s)', nestfmt='dict(%s)', ignore=None):
    '''
    Create a string from a dict-like object using keyword argument syntax

    Parameters
    ----------
    dct : dict-like object
       The dictionary to represent as a string
    fmt : string, optional
       The format string to use at the top level
    nestfmt : string, optional
       The format string to use for nested dictionaries
    ignore : list
       Keys to ignore at the top level

    Returns
    -------
    string
       String representation of a dict

    '''
    if ignore is None:
        ignore = []
    out = []
    for key, value in sorted(six.iteritems(dct)):
        if key in ignore:
            continue
        if isinstance(value, dict):
            out.append('%s=%s' % (key, dict2kwargs(value, fmt=nestfmt)))
        elif isinstance(value, items_types):
            sublist = []
            for item in value:
                if isinstance(item, (dict, items_types)):
                    sublist.append(dict2kwargs(item))
                else:
                    sublist.append(repr(item))
            if isinstance(value, tuple):
                fmtstr = '%s=(%s)'
            elif isinstance(value, set):
                fmtstr = '%s={%s}'
            else:
                fmtstr = '%s=[%s]'
            out.append(fmtstr % (key, ', '.join(sublist)))
        else:
            out.append('%s=%s' % (key, repr(value)))
    return fmt % ', '.join(out)


def getsoptions(**kwargs):
    '''
    Convert keyword arguments to soptions format

    Paramaters
    ----------
    **kwargs : any, optional
       Arbitrary keyword arguments

    Returns
    -------
    string
       Formatted string of all options

    '''
    soptions = []
    for key, value in six.iteritems(kwargs):
        if value is None:
            continue
        if key == 'locale':
            value = getlocale(**dict(locale=value))
        soptions.append('%s=%s' % (key, value))
    return ' '.join(soptions)


def parsesoptions(soptions):
    '''
    Convert soptions string to dictionary

    Parameters
    ----------
    soptions : string
        Formatted string of options

    Returns
    -------
    dict

    '''
    out = {}
    if not soptions:
        return out
    soptions = soptions.strip()
    if not soptions:
        return out
    while re.match(r'^[\w+-]+\s*=', soptions):
        name, soptions = re.split(r'\s*=\s*', soptions, 1)
        if soptions.startswith('{'):
            match = re.match(r'^\{\s*([^\}]*)\s*\}\s*(.*)$', soptions)
            value = re.split(r'\s+', match.group(1))
            soptions = match.group(2) or ''
        elif ' ' in soptions:
            value, soptions = re.split(r'\s+', soptions, 1)
        else:
            value = soptions
            soptions = ''
        out[name] = value
    return out


def getlocale(locale=None):
    '''
    Get configured language code for locale

    If a locale argument is specified, that code is returned.
    Otherwise, Python's locale module is used to acquire
    the currently configured language code.

    Parameters
    ----------
    locale : string, optional
       POSIX language code

    Returns
    -------
    string
       String containing locale

    '''
    if locale:
        return locale
    locale = loc.getlocale()[0]
    if locale:
        return locale
    return loc.getdefaultlocale()[0]


def iteroptions(*args, **kwargs):
    '''
    Iterate through name / value pairs of options

    Options can come in several forms.  They can be consecutive arguments
    where the first argument is the name and the following argument is
    the value.  They can be two-element tuples (or lists) where the first
    element is the name and the second element is the value.  You can
    also pass in a dictionary of key / value pairs.  And finally, you can
    use keyword arguments.

    Parameters
    ----------
    *args : any, optional
        See description above.
    **kwargs : key / value pairs, optional
        Arbitrary keyword arguments.

    Returns
    -------
    generator
        Each iteration returns a name / value pair in a tuple

    '''
    args = list(args)
    while args:
        item = args.pop(0)
        if isinstance(item, (list, tuple)):
            yield item[0], item[1]
        elif isinstance(item, dict):
            for key, value in six.iteritems(item):
                yield key, value
        else:
            yield item, args.pop(0)
    for key, value in six.iteritems(kwargs):
        yield key, value
