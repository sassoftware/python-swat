#!/usr/bin/env python
# encoding: utf-8

'''
General JSON processing utilities

'''

from __future__ import print_function, division, absolute_import, unicode_literals


def escapejson(jsonstr):
    '''
    Escape quotes in JSON strings

    Parameters
    ----------
    jsonstr : JSON string

    Returns
    -------
    string
       String with quotes and newlines escaped

    '''
    jsonstr = jsonstr.replace('\\', '\\\\')
    jsonstr = jsonstr.replace(r'\"', r'\\"')
#   jsonstr = jsonstr.replace(r'\b', r'\\b')
#   jsonstr = jsonstr.replace(r'\f', r'\\f')
#   jsonstr = jsonstr.replace(r'\n', r'\\n')
#   jsonstr = jsonstr.replace(r'\r', r'\\r')
#   jsonstr = jsonstr.replace(r'\t', r'\\t')
    jsonstr = jsonstr.replace('\b', r'\u0008')
    jsonstr = jsonstr.replace('\f', r'\u000C')
    jsonstr = jsonstr.replace('\n', r'\u000A')
    jsonstr = jsonstr.replace('\r', r'\u000D')
    jsonstr = jsonstr.replace('\t', r'\u0009')
    return jsonstr
