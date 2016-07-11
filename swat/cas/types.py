#!/usr/bin/env python
# encoding: utf-8

'''
Additional types for CAS support

'''

from __future__ import (print_function, division, absolute_import,
                        unicode_literals)

import six


@six.python_2_unicode_compatible
class NilType(object):
    '''
    Type for `nil` valued parameters

    The swat module contains a singleton of the NilType class called `nil`.

    Examples
    --------
    Send a nil as a parameter value

    >>> s.action(param=swat.nil)

    '''

    def __repr__(self):
        return str(self)

    def __str__(self):
        return 'nil'


# nil singleton
nil = NilType()


class blob(bytes):
    '''
    Explicitly defined type for blob parameters

    '''
