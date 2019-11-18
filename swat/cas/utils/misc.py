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
Utilities for CAS modules

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import os


def super_dir(cls, obj):
    '''
    Return the super's dir(...) list

    Parameters
    ----------
    cls : type
        The type of the object for the super(...) call
    obj : instance
        The instance object for the super(...) call

    Returns
    -------
    list-of-strings

    '''
    if obj is None:
        return []

    try:
        return sorted(x for x in super(cls, obj).__dir__() if not x.startswith('_'))

    except AttributeError:

        def get_attrs(o):
            try:
                return list(o.__dict__.keys())
            except AttributeError:
                return []

        out = set(get_attrs(cls))
        for basecls in cls.__bases__:
            out.update(get_attrs(basecls))
            out.update(super_dir(basecls, obj))
        out.update(get_attrs(obj))

        return list(str(x).decode('utf8') for x in sorted(out) if not x.startswith('_'))


def any_file_exists(files):
    '''
    Determine if any specified files actually exist

    Parameters
    ----------
    files : string or list-of-strings or None
        If string, the value is the filename.  If list, a boolean is returned
        indicating if any of the files exist.

    '''
    if isinstance(files, (list, tuple, set)):
        for item in files:
            if os.path.isfile(os.path.expanduser(item)):
                return True

    elif os.path.isfile(os.path.expanduser(files)):
        return True

    return False
