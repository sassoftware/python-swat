#!/usr/bin/env python
# encoding: utf-8

'''
General utilities for the SWAT module

'''

from __future__ import print_function, division, absolute_import, unicode_literals

from . import compat
from . import config
from .decorators import cachedproperty, getattr_safe_property
from .args import mergedefined, dict2kwargs, getsoptions, getlocale, parsesoptions
from .json import escapejson
from .keyword import dekeywordify, keywordify
