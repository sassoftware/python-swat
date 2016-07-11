#!/usr/bin/env python
# encoding: utf-8

'''
Generic rendering functions

'''

from __future__ import print_function, division, absolute_import, unicode_literals

from .html import render_html


def render(results):
    render_html(results)
