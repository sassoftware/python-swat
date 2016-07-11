#!/usr/bin/env python
# encoding: utf-8

'''
TK utilities for interfacing with CAS

'''

from __future__ import print_function, division, absolute_import, unicode_literals

from ... import clib


def InitializeTK(path):
    '''
    Initialize the TK subsystem

    Parameters
    ----------
    path : string
       Colon (semicolon on Windows) separated list of directories to
       search for TK components


    Examples
    --------
    Set the TK search path to look through /usr/local/tk/ and /opt/sas/tk/.

    >>> swat.InitializeTK('/usr/local/tk:/opt/sas/tk')

    '''
    clib.InitializeTK(a2n(path, 'utf-8'))


initialize_tk = InitializeTK
