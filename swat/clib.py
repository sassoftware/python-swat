#!/usr/bin/env python
# encoding: utf-8

'''
SWAT C library functions

'''

from __future__ import print_function, division, absolute_import, unicode_literals

from .utils.compat import PY3, WIDE_CHARS, a2u
from .exceptions import SWATError

# pylint: disable=E1101

_pyswat = None


def _import_pyswat():
    ''' Import version-specific _pyswat package '''
    global _pyswat
    import sys
    platform = 'linux'
    if sys.platform.lower().startswith('win'):
        platform = 'win'
    elif sys.platform.lower().startswith('darwin'):
        platform = 'mac'
    _globals, _locals = globals(), locals()
    try:
        if PY3:
            exec('from .lib.%s import _py%d%dswat as _pyswat' %
                 (platform, sys.version_info[0], sys.version_info[1]),
                 _globals, _locals)
        elif WIDE_CHARS:
            exec('from .lib.%s import _pyswatw as _pyswat' % platform, _globals, _locals)
        else:
            exec('from .lib.%s import _pyswat as _pyswat' % platform, _globals, _locals)
    except ImportError:
        raise ValueError(('Could not import import _py%d%dswat.  This is likely due to an '
                          'incorrect TK path or an error while loading the TK subsystem. ' 
                          'Try using the HTTP port for the REST interface.') %
                         sys.version_info[:2])
    _pyswat = _locals['_pyswat']


def SW_CASConnection(*args, **kwargs):
    ''' Return a CASConnection (importing _pyswat as needed) '''
    if _pyswat is None:
        _import_pyswat()
    return _pyswat.SW_CASConnection(*args, **kwargs)


def SW_CASValueList(*args, **kwargs):
    ''' Return a CASValueList (importing _pyswat as needed) '''
    if _pyswat is None:
        _import_pyswat()
    return _pyswat.SW_CASValueList(*args, **kwargs)


def SW_CASFormatter(*args, **kwargs):
    ''' Return a CASFormatter (importing _pyswat as needed) '''
    if _pyswat is None:
        _import_pyswat()
    return _pyswat.SW_CASFormatter(*args, **kwargs)


def SW_CASConnectionEventWatcher(*args, **kwargs):
    ''' Return a CASConnectionEventWatcher (importing _pyswat as needed) '''
    if _pyswat is None:
        _import_pyswat()
    return _pyswat.SW_CASConnectionEventWatcher(*args, **kwargs)


def SW_CASDataBuffer(*args, **kwargs):
    ''' Return a CASDataBuffer (importing _pyswat as needed) '''
    if _pyswat is None:
        _import_pyswat()
    return _pyswat.SW_CASDataBuffer(*args, **kwargs)


def SW_CASError(*args, **kwargs):
    ''' Return a CASError (importing _pyswat as needed) '''
    if _pyswat is None:
        _import_pyswat()
    return _pyswat.SW_CASError(*args, **kwargs)


def InitializeTK(*args, **kwargs):
    ''' Initialize the TK subsystem (importing _pyswat as needed) '''
    if _pyswat is None:
        _import_pyswat()
    return _pyswat.InitializeTK(*args, **kwargs)


def errorcheck(expr, obj):
    '''
    Check for generated error message

    Parameters
    ----------
    expr : any
       Result to return if no error happens
    obj : SWIG-based class
       Object to check for messages

    Raises
    ------
    SWATError
       If error message exists

    Returns
    -------
    `expr` argument
       The result of `expr`

    '''
    if obj is not None:
        msg = obj.getLastErrorMessage()
        if msg:
            raise SWATError(a2u(msg, 'utf-8'))
    return expr
