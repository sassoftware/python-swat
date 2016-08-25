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
General utilities for dealing with keywords

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import keyword

DEKEYWORDS = set([(x + '_') for x in keyword.kwlist])


def dekeywordify(name):
    '''
    Add an underscore to names that are keywords

    Parameters
    ----------
    name : string
        The string to check against keywords

    Returns
    -------
    string
        Name changed to avoid keywords

    '''
    if keyword.iskeyword(name):
        return name + '_'
    return name


def keywordify(name):
    '''
    Convert name that has been dekeywordified to a keyword

    Parameters
    ----------
    name : string
        The string to convert to a keyword if needed

    Returns
    -------
    string
        Name changed to not avoid keywords

    '''
    if name in DEKEYWORDS:
        return name[:-1]
    return name
