#!/usr/bin/env python
# encoding: utf-8
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

'''
TK utilities for interfacing with CAS

'''

from __future__ import print_function, division, absolute_import, unicode_literals

from ... import clib
from ...utils.compat import a2n


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
