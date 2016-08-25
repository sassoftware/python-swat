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
Utilities for dealing with requests from a CAS action

'''

from __future__ import print_function, division, absolute_import, unicode_literals

from ..clib import errorcheck
from .transformers import casvaluelist2py


class CASRequest(object):
    '''
    Create a CASRequest object

    Parameters
    ----------
    _sw_request : SWIG CASRequest object
       The SWIG request object

    soptions : string, optional
       soptions of the connection object

    Returns
    -------
    CASRequest object

    '''

    def __init__(self, _sw_request, soptions=''):
        self._sw_request = _sw_request
        self._soptions = soptions

        nparams = errorcheck(_sw_request.getNParameters(), _sw_request)
        params = errorcheck(_sw_request.getParameters(), _sw_request)

        self.parameters = casvaluelist2py(params, self._soptions, nparams)
