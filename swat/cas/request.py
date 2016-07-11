#!/usr/bin/env python
# encoding: utf-8

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
