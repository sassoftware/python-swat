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
Utilities for reading authinfo/netrc files

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import os
import re
import sys
from .compat import items_types

_AUTHINFO_PATHS = [
    '_authinfo.gpg',
    '.authinfo.gpg',
    '_netrc.gpg',
    '.netrc.gpg',
    '_authinfo',
    '.authinfo',
    '_netrc',
    '.netrc',
]

if 'win' not in sys.platform.lower():
    _AUTHINFO_PATHS = [aipath for aipath in _AUTHINFO_PATHS if not aipath.startswith('_')]

_ALIASES = {
    'machine': 'host',
    'login': 'user',
    'account': 'user',
    'port': 'protocol',
}


def _chunker(seq, size):
    ''' Read sequence `seq` in `size` sized chunks '''
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def _matches(params, **kwargs):
    ''' See if keyword arguments are a subset of `params` '''
    for key, value in kwargs.items():
        if value is None:
            continue
        if key not in params:
            continue
        if params.get(key) != value:
            return False
    return True


def parseparams(param):
    '''
    Parse the next parameter from the string

    Parameters
    ----------
    param : string
        The string to parse

    Returns
    -------
    dict
        Key/value pairs parsed from the string

    '''
    out = {}

    if not param:
        return out

    siter = iter(param)

    name = []
    for char in siter:
        if not char.strip():
            break
        name.append(char)

    value = []
    for char in siter:
        if not char.strip():
            break
        if char == '"':
            for subchar in siter:
                if subchar == '\\':
                    value.append(next(siter))
                elif subchar == '"':
                    break
        else:
            value.append(char)

    name = ''.join(name)
    value = ''.join(value)

    out[_ALIASES.get(name, name)] = value
    out.update(parseparams((''.join(list(siter))).strip()))
    return out


def query_authinfo(host, user=None, protocol=None, path=None):
    '''
    Look for a matching host definition in authinfo/netrc files

    Parameters
    ----------
    host : string
        The host name or IP address to match
    user : string, optional
        The username to match
    protocol : string or int, optional
        The protocol or port to match
    path : string or list of strings, optional
        The paths to look for instead of the automatically detected paths

    Returns
    -------
    dict
        Connection information

    '''
    paths = []

    # Construct list of authinfo/netrc paths
    if path is None:
        if os.environ.get('AUTHINFO'):
            paths = [os.path.expanduser(x)
                     for x in os.environ.get('AUTHINFO').split(os.path.sep)]
        elif os.environ.get('NETRC'):
            paths = [os.path.expanduser(x)
                     for x in os.environ.get('NETRC').split(os.path.sep)]
        else:
            home = os.path.expanduser('~')
            for item in _AUTHINFO_PATHS:
                paths.append(os.path.join(home, item))

    elif not isinstance(path, items_types):
        paths = [os.path.expanduser(path)]

    else:
        paths = [os.path.expanduser(x) for x in path]

    # Parse each file
    for path in paths:

        if not os.path.exists(path):
            continue

        # Remove comments and macros
        lines = []

        with open(path) as info:
            infoiter = iter(info)
            for line in infoiter:
                line = line.strip()

                # Bypass comments
                if line.startswith('#'):
                    continue

                # Bypass macro definitions
                if line.startswith('macdef'):
                    for line in infoiter:
                        if not line.strip():
                            break
                    continue

                lines.append(line)

        line = ' '.join(lines)

        # Parse out definitions and look for matches
        defs = [x for x in re.split(r'\b(host|machine|default)\b\s*', line) if x.strip()]

        for name, value in _chunker(defs, 2):
            if name in ['host', 'machine']:
                hostname, value = re.split(r'\s+', value, 1)
                out = parseparams(value)
                out['host'] = hostname.lower()
                if _matches(out, host=host.lower(), user=user, protocol=protocol):
                    return out
            else:
                out = parseparams(value)
                if _matches(out, user=user, protocol=protocol):
                    return out

    return {}
