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
SWAT library exceptions

'''

from __future__ import print_function, division, absolute_import, unicode_literals


class SWATError(Exception):
    '''
    Base class for all SWAT exceptions

    '''
    pass


class SWATOptionError(SWATError):
    '''
    SWAT configuration option error

    '''
    pass


class SWATCASActionError(SWATError):
    '''
    CAS action error exception

    Parameters
    ----------
    message : string
        The error message
    response : CASResponse
        The response object that contains the error
    connection : CAS
        The connection object
    results : CASResults or any
        The compiled results so far

    '''
    def __init__(self, message, response, connection, results=None, events=None):
        super(SWATCASActionError, self).__init__(message)
        self.message = message
        self.response = response
        self.connection = connection
        self.results = results
        self.events = events


class SWATCASActionRetry(SWATError):
    '''
    CAS action must be resubmitted

    '''
