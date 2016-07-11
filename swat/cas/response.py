#!/usr/bin/env python
# encoding: utf-8

'''
Class for receiving responses from a CAS action

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import six
import weakref
from ..utils.compat import a2u, binary_types
from ..utils import cachedproperty
from ..clib import errorcheck
from .transformers import cas2py


@six.python_2_unicode_compatible
class CASResponse(object):
    '''
    Response from CAS

    Information returned from CAS includes messages (self.messages),
    disposition (self.disposition), and performance (self.performance).

    To get the results objects, you simply iterate over this object::

    >>> for result in response:
    ...    print result

    If you want them in a list, simply use the list function list(response).

    Parameters
    ----------
    _sw_response : SWIG CASResponse object
       The SWIG response

    soptions : string, optional
       soptions of the connection object

    Returns
    -------
    CASResponse object

    '''

    @six.python_2_unicode_compatible
    class CASDisposition(object):
        ''' Disposition of a CAS response '''

        def __init__(self, _sw_response):
            self._sw_response = _sw_response

        def to_dict(self):
            '''
            Return dispositoin attributes as a dictionary

            Returns
            -------
            dict
               Key/value pairs of fields in CASDisposition

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

        Parameters
        ----------
        _sw_response : SWIG CASResponse
           CASResponse object

        Returns
        -------
        CASPerformance object

        '''

        def __init__(self, _sw_response):
            self._sw_response = _sw_response

        def to_dict(self):
            '''
            Return performance attributes as a dictionary

            Returns
            -------
            dict
               Key/value pairs of fields in CASPerformance object

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
        ''' Return a CASDisposition object '''
        return type(self).CASDisposition(self._sw_response)

    @cachedproperty
    def performance(self):
        ''' Return a CASPerformance object '''
        return type(self).CASPerformance(self._sw_response)

    def __iter__(self):
        ''' Loop over all results in the response '''
        _sw_result = errorcheck(self._sw_response.getNextResult(), self._sw_response)
        while _sw_result:
            output = {}
            key = errorcheck(_sw_result.getKey(), _sw_result)
            if key is None:
                key = 0
            elif isinstance(key, binary_types):
                key = a2u(key, 'utf-8')
            output[key] = cas2py(_sw_result, self.soptions, connection=self._connection)
            yield output
            _sw_result = errorcheck(self._sw_response.getNextResult(), self._sw_response)

    def __str__(self):
        return 'CASResponse(messages=%s, disposition=%s, performance=%s)' % \
               (self.messages, self.disposition, self.performance)

    def __repr__(self):
        return 'CASResponse(messages=%s, disposition=%s, performance=%s)' % \
               (self.messages, repr(self.disposition), repr(self.performance))
