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
Class for simulating CAS messages

'''

from __future__ import print_function, division, absolute_import, unicode_literals

from .response import REST_CASResponse


class REST_CASMessage(object):
    ''' CASMessage wrapper '''

    def __init__(self, obj, connection=None):
        '''
        Create a CASMessage object

        Parameters
        ----------
        obj : any
            The object returned by the CAS connection
        connection : REST_CASConnection
            The connection the object came from

        Returns
        -------
        REST_CASMessage

        '''
        self._obj = obj
        self._connection = connection

    def getTypeName(self):
        ''' Get the object type '''
        return 'message'

    def getSOptions(self):
        ''' Get the SOptions value '''
        return ''

    def isNULL(self):
        ''' Is this a NULL object? '''
        return False

    def getTag(self):
        ''' Get the message tag '''
        return ''

    def getType(self):
        ''' Get the message type '''
        return 'response'

    def getFlags(self):
        ''' Get the message flags '''
        return []

    def toResponse(self, connection=None):
        ''' Convert the message to a response '''
        return REST_CASResponse(self._obj)

    def toRequest(self):
        ''' Convert the message to a request '''
        raise NotImplementedError('Not supported in the REST interface')

    def getLastErrorMessage(self):
        ''' Return the last generated error message '''
        return ''
