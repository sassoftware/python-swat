#!/usr/bin/env python
# encoding: utf-8

'''
General utility decorators

'''

from __future__ import print_function, division, absolute_import, unicode_literals


class cachedproperty(object):
    ''' Property whose value is only calculated once and cached '''

    def __init__(self, func):
        self._func = func
        self.__doc__ = func.__doc__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return getattr(obj, '@%s' % self._func.__name__)
        except AttributeError:
            result = self._func(obj)
            setattr(obj, '@%s' % self._func.__name__, result)
            return result


class getattr_safe_property(object):
    ''' Property that safely coexists with __getattr__ '''

    def __init__(self, func):
        self._func = func
        self.__doc__ = func.__doc__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return self._func(obj)
        except AttributeError as exc:
            raise RuntimeError(str(exc))
