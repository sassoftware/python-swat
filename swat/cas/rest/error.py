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
Class for CAS errors

'''

from __future__ import print_function, division, absolute_import, unicode_literals


class REST_CASError(object):
    '''
    Create a CASError object

    Parameters
    ----------
    soptions : string
        Instantiation options

    Returns
    -------
    REST_CASError

    '''

    def __init__(self, soptions=''):
        self._soptions = soptions
        self._message = ''

    def getTypeName(self):
        ''' Get object type '''
        return 'error'

    def getSOptions(self):
        ''' Get SOptions value '''
        return self._soptions

    def isNULL(self):
        ''' Is this a NULL object? '''
        return False

    def getLastErrorMessage(self):
        ''' Get the last generated error message '''
        return self._message

    def setErrorMessage(self, msg):
        ''' Set the last generated error message '''
        self._message = msg
