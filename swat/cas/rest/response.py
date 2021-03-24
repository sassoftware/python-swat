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
Class for receiving responses from a CAS action

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import re
from ...config import options
from .value import REST_CASValue


_SEVERITY_MAP = {
    'Normal': 0,
    'Warning': 1,
    'Error': 2,
}


def camel2underscore(text):
    ''' Convert camelcase name to underscore delimited '''
    return re.sub(r'^_([A-Z])', r'\1', re.sub(r'([A-Z])', r'_\1', text)).lower()


def decrement_index(match):
    ''' Decrement number in group 2 '''
    return '%s%d%s' % (match.group(1), int(match.group(2)) - 1, match.group(3))


def process_parameter_indexes(status_code, msg):
    ''' Decrement parameter index values '''
    # Only process parameter error messages
    if int(status_code / 10000) == 272:
        return re.sub(r'(\w+\[)(\d+)(\])', decrement_index, msg)
    return msg


class REST_CASResponse(object):
    '''
    Create a CASResponse object

    Parameters
    ----------
    obj : dict
        Object returned by CAS server

    Returns
    -------
    REST_CASResponse

    '''

    def __init__(self, obj):
        obj = obj or {}

        disp = obj.get('disposition', None) or {}

        self._disposition = {}
        self._disposition['debug'] = disp.get('debugInfo')
        self._disposition['status'] = disp.get('formattedStatus')
        self._disposition['reason'] = disp.get('reason', '').lower()
        if self._disposition['reason'] == 'ok':
            self._disposition['reason'] = None
        self._disposition['severity'] = _SEVERITY_MAP.get(disp.get('severity'))
        self._disposition['status_code'] = disp.get('statusCode')

        # TODO: Map names for consistency
        self._update_flags = [camel2underscore(x)
                              for x in obj.get('changedResources', [])]

        self._messages = [x['message'] for x in obj.get('logEntries', [])]

        metrics = obj.get('metrics', {}) or {}
        self._metrics = {camel2underscore(k): v for k, v in metrics.items()}

        self._results = obj.get('results', {})

        def getNextMessage(self):
            ''' Iterator for getting next message '''
            status_code = self._disposition['status_code']
            for item in self._messages:
                if not item:
                    continue
                item = process_parameter_indexes(status_code, item)
                if options.cas.print_messages:
                    print(item)
                yield item
        self._getNextMessage = getNextMessage(self)

        def getNextUpdateFlag(self):
            ''' Iterator for getting next update flag '''
            for item in self._update_flags:
                yield item
        self._getNextUpdateFlag = getNextUpdateFlag(self)

        def getNextResult(self):
            ''' Iterator for getting next result '''
            if isinstance(self._results, list):
                for i, item in enumerate(self._results):
                    yield REST_CASValue(i, item)
            else:
                for key, value in self._results.items():
                    yield REST_CASValue(key, value)
        self._getNextResult = getNextResult(self)

    def getNextMessage(self):
        ''' Get the next message '''
        for item in self._getNextMessage:
            return item

    def getNextUpdateFlag(self):
        ''' Get the next update flag '''
        for item in self._getNextUpdateFlag:
            return item

    def getNextResult(self):
        ''' Get the next result '''
        for item in self._getNextResult:
            return item

    def getTypeName(self):
        ''' Get the object type '''
        return 'response'

    def getSOptions(self):
        ''' Get the SOptions value '''
        return ''

    def isNULL(self):
        ''' Is this a NULL object? '''
        return False

    def getNMessages(self):
        ''' Get the number of messages '''
        return len(self._messages)

    def getNUpdateFlags(self):
        ''' Get the number of update flags '''
        return len(self._update_flags)

    def getNResults(self):
        ''' Get the number of results '''
        return len(self._results)

    def getDispositionSeverity(self):
        ''' Get the disposition severity '''
        return self._disposition.get('severity')

    def getDispositionReason(self):
        ''' Get the disposition reason '''
        return self._disposition.get('reason')

    def getDispositionDebug(self):
        ''' Get the disposition debug flags '''
        return self._disposition.get('debug')

    def getDispositionStatusCode(self):
        ''' Get the disposition status code '''
        return self._disposition.get('status_code')

    def getDispositionStatus(self):
        ''' Get the disposition status '''
        return self._disposition.get('status')

    def getElapsedTime(self):
        ''' Get the elapsed time '''
        return self._metrics.get('elapsed_time')

    def getDataMovementTime(self):
        ''' Get the amount of time for data movement '''
        return self._metrics.get('data_movement_time')

    def getDataMovementBytes(self):
        ''' Get the number of bytes of data movement '''
        return self._metrics.get('data_movement_bytes')

    def getCPUUserTime(self):
        ''' Get the amount of CPU user time '''
        return self._metrics.get('cpu_user_time')

    def getCPUSystemTime(self):
        ''' Get the amount of CPU system time '''
        return self._metrics.get('cpu_system_time')

    def getSystemTotalMemory(self):
        ''' Get the amount of system memory '''
        return self._metrics.get('system_total_memory')

    def getSystemNodes(self):
        ''' Get the number of system nodes '''
        return self._metrics.get('system_nodes')

    def getSystemCores(self):
        ''' Get the number of system cores '''
        return self._metrics.get('system_cores')

    def getMemory(self):
        ''' Get the amount of memory used '''
        return self._metrics.get('memory')

    def getMemoryOS(self):
        ''' Get the amount of OS memory used '''
        return self._metrics.get('memory_os')

    def getMemorySystem(self):
        ''' Get the amount of system memory used '''
        return self._metrics.get('memory_system')

    def getMemoryQuota(self):
        ''' Get the memory quota '''
        return self._metrics.get('memory_quota')

    def getLastErrorMessage(self):
        ''' Get the last generated error message '''
        return ''
