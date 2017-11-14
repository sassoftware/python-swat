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

import weakref
import six
from ..utils.compat import a2u, binary_types
from ..utils import cachedproperty
from ..clib import errorcheck
from .transformers import cas2py


@six.python_2_unicode_compatible
class CASDisposition(object):
    '''
    Disposition of a CAS response

    This class is never instantiated directly.  It is instantiated behind the
    scenes when the :class:`CASResponse` is created.

    Attributes
    ----------
    severity : int
        The severity of the action result.  A value of zero means that no
        problems were reported.  A value of one means that warnings were
        reported.  A value of two means that errors were reported.
    reason : string
        Reason for the error (if any).
    status : string
        Human-readable message for the response.
    status_code : string
        Status code for technical support assistance.

    Returns
    -------
    :class:`CASDisposition` object

    '''

    def __init__(self, _sw_response):
        self._sw_response = _sw_response

    def to_dict(self):
        '''
        Return dispositoin attributes as a dictionary

        Returns
        -------
        dict
            Key/value pairs of fields in :class:`CASDisposition`

        '''
        out = {}
        for key in ['severity', 'reason', 'status', 'debug', 'status_code']:
            out[key] = getattr(self, key)
        return out

    @cachedproperty
    def severity(self):
        ''' Disposition severity '''
        return errorcheck(self._sw_response.getDispositionSeverity(),
                          self._sw_response)

    @cachedproperty
    def reason(self):
        ''' Disposition reason '''
        return errorcheck(a2u(self._sw_response.getDispositionReason(), 'utf-8'),
                          self._sw_response)

    @cachedproperty
    def status(self):
        ''' Disposition status '''
        return errorcheck(a2u(self._sw_response.getDispositionStatus(), 'utf-8'),
                          self._sw_response)

    @cachedproperty
    def status_code(self):
        ''' Disposition status code '''
        return errorcheck(self._sw_response.getDispositionStatusCode(),
                          self._sw_response)

    @cachedproperty
    def debug(self):
        ''' Disposition debug information '''
        return errorcheck(a2u(self._sw_response.getDispositionDebug(), 'utf-8'),
                          self._sw_response)

    def __str__(self):
        out = []
        for key, value in sorted(six.iteritems(self.to_dict())):
            out.append('%s=%s' % (key, value))
        return 'CASDisposition(%s)' % ', '.join(out)

    def __repr__(self):
        return str(self)


@six.python_2_unicode_compatible
class CASPerformance(object):
    '''
    Performance metrics of a CAS response

    This class is never instantiated directly.  It is instantiated behind the
    scenes when the :class:`CASResponse` is created.

    Attributes
    ----------
    elapsed_time : float
    cpu_user_time : float
    cpu_system_time : float
    system_total_memory : int
    system_nodes : int
    system_cores : int
    memory : int
    memory_os : int
    memory_quota : int
    data_movement_time : float
    date_movement_bytes : int

    Parameters
    ----------
    _sw_response : SWIG CASResponse
        SWIG CASResponse object

    Returns
    -------
    :class:`CASPerformance` object

    '''

    def __init__(self, _sw_response):
        self._sw_response = _sw_response

    def to_dict(self):
        '''
        Return performance attributes as a dictionary

        Returns
        -------
        dict
            Key/value pairs of fields in :class:`CASPerformance` object

        '''
        out = {}
        for key in ['elapsed_time', 'cpu_user_time', 'cpu_system_time',
                    'system_total_memory', 'system_nodes', 'system_cores', 'memory',
                    'memory_os', 'memory_quota',
                    'data_movement_time', 'data_movement_bytes']:
            out[key] = getattr(self, key)
        return out

    @cachedproperty
    def data_movement_time(self):
        ''' Data movement time '''
        return errorcheck(self._sw_response.getDataMovementTime(), self._sw_response)

    @cachedproperty
    def data_movement_bytes(self):
        ''' Data movement bytes '''
        return errorcheck(self._sw_response.getDataMovementBytes(), self._sw_response)

    @cachedproperty
    def rows_read(self):
        ''' Unique rows read '''
        return errorcheck(self._sw_response.getRowsRead(), self._sw_response)

    @cachedproperty
    def rows_written(self):
        ''' Unique rows written '''
        return errorcheck(self._sw_response.getRowsWritten(), self._sw_response)

    @cachedproperty
    def elapsed_time(self):
        ''' Elapsed time '''
        return errorcheck(self._sw_response.getElapsedTime(), self._sw_response)

    @cachedproperty
    def cpu_user_time(self):
        ''' CPU User Time '''
        return errorcheck(self._sw_response.getCPUUserTime(), self._sw_response)

    @cachedproperty
    def cpu_system_time(self):
        ''' CPU System Time '''
        return errorcheck(self._sw_response.getCPUSystemTime(), self._sw_response)

    @cachedproperty
    def system_total_memory(self):
        ''' Total System Memory '''
        return errorcheck(self._sw_response.getSystemTotalMemory(), self._sw_response)

    @cachedproperty
    def system_nodes(self):
        ''' Number of nodes '''
        return errorcheck(self._sw_response.getSystemNodes(), self._sw_response)

    @cachedproperty
    def system_cores(self):
        ''' Number of cores '''
        return errorcheck(self._sw_response.getSystemCores(), self._sw_response)

    @cachedproperty
    def memory(self):
        ''' Memory '''
        return errorcheck(self._sw_response.getMemory(), self._sw_response)

    @cachedproperty
    def memory_os(self):
        ''' OS Memory '''
        return errorcheck(self._sw_response.getMemoryOS(), self._sw_response)

    @cachedproperty
    def memory_quota(self):
        ''' Memory Quota '''
        return errorcheck(self._sw_response.getMemoryQuota(), self._sw_response)

    def __str__(self):
        out = []
        for key, value in sorted(six.iteritems(self.to_dict())):
            out.append('%s=%s' % (key, value))
        return 'CASPerformance(%s)' % ', '.join(out)

    def __repr__(self):
        return str(self)


@six.python_2_unicode_compatible
class CASResponse(object):
    '''
    Response from a CAS action

    This class is never instantiated directly.  It is created behind
    the scenes and surfaced when iterating over the responses from a
    CAS action.

    Attributes
    ----------
    disposition : :class:`CASDisposition`
        The disposition of a CAS response.  This includes the attributes
        severity, reason, status, debug, and status_code.
    performance : :class:`CASPerformance`
        Performance metrices of a CAS action.
    messages : list-of-strings
        The messages returned by the CAS action.
    updateflags : set-of-strings
        The update flags sent by the CAS server.

    Examples
    --------
    >>> conn = swat.CAS()
    >>> conn.invoke('serverstatus')
    >>> for response in conn:
    ...     for k, v in response:
    ...         print(k, v)

    Parameters
    ----------
    _sw_response : SWIG CASResponse object
        The SWIG response object.
    soptions : string, optional
        soptions string of the connection object.

    Returns
    -------
    :class:`CASResponse` object

    '''
    def __init__(self, _sw_response, soptions='', connection=None):
        self._sw_response = _sw_response
        self.soptions = soptions
        self._connection = weakref.proxy(connection)

    @cachedproperty
    def messages(self):
        ''' Return a list of messages '''
        messages = []
        nmessages = errorcheck(self._sw_response.getNMessages(), self._sw_response)
        for i in range(nmessages):
            messages.append(errorcheck(
                a2u(self._sw_response.getNextMessage(), 'utf-8'), self._sw_response))
        return messages

    @cachedproperty
    def updateflags(self):
        ''' Return a set of update flags '''
        flags = set()
        nflags = errorcheck(self._sw_response.getNUpdateFlags(), self._sw_response)
        for i in range(nflags):
            flags.add(errorcheck(
                a2u(self._sw_response.getNextUpdateFlag(), 'utf-8'), self._sw_response))
        return flags

    @cachedproperty
    def disposition(self):
        ''' Return a :class:`CASDisposition` object '''
        return CASDisposition(self._sw_response)

    @cachedproperty
    def performance(self):
        ''' Return a :class:`CASPerformance` object '''
        return CASPerformance(self._sw_response)

    def __iter__(self):
        ''' Iterate over all results in the response '''
        _sw_result = errorcheck(self._sw_response.getNextResult(), self._sw_response)
        while _sw_result:
            key = errorcheck(_sw_result.getKey(), _sw_result)
            if key is None:
                key = 0
            elif isinstance(key, binary_types):
                key = a2u(key, 'utf-8')
            yield key, cas2py(_sw_result, self.soptions, connection=self._connection)
            _sw_result = errorcheck(self._sw_response.getNextResult(), self._sw_response)

    def __str__(self):
        return 'CASResponse(messages=%s, disposition=%s, performance=%s)' % \
               (self.messages, self.disposition, self.performance)

    def __repr__(self):
        return 'CASResponse(messages=%s, disposition=%s, performance=%s)' % \
               (self.messages, repr(self.disposition), repr(self.performance))
