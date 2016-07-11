#!/usr/bin/env python
# encoding: utf-8

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
