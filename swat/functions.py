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
Global functions

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import pandas as pd
from . import dataframe
from .cas import table


def concat(objs, **kwargs):
    '''
    Concatenate data in given objects

    Parameters
    ----------
    objs : list, optional
        List of CASTable or DataFrame objects
    **kwargs : keyword arguments, optional
        Optional arguments to concatenation function

    See Also
    --------
    :func:`pd.concat`

    Notes
    -----
    All input objects must be of the same type.

    Returns
    -------
    :class:`CASTable`
        If first input is a CASTable
    :class:`SASDataFrame`
        If first input is a SASDataFrame
    :class:`DataFrame`
        If first input is a pandas.DataFrame
    :class:`Series`
        If first input is a pandas.Series

    '''
    objs = [x for x in objs if x is not None]
    if not objs:
        raise ValueError('There are no non-None objects in the given sequence')

    if isinstance(objs[0], table.CASTable):
        return table.concat(objs, **kwargs)

    if isinstance(objs[0], dataframe.SASDataFrame):
        return dataframe.concat(objs, **kwargs)

    return pd.concat(objs, **kwargs)


def merge(left, right, **kwargs):
    '''
    Merge data in given objects

    Parameters
    ----------
    left : CASTable or SASDataFrame or DataFrame, optional
        CASTable or (SAS)DataFrame object
    right : CASTable or SASDataFrame or DataFrame, optional
        CASTable or (SAS)DataFrame object to merge with
    **kwargs : keyword arguments, optional
        Optional arguments to merge function

    See Also
    --------
    :func:`pd.merge`

    Notes
    -----
    All input objects must be of the same type.

    Returns
    -------
    :class:`CASTable`
        If first input is a CASTable
    :class:`SASDataFrame`
        If first input is a SASDataFrame
    :class:`DataFrame`
        If first input is a pandas.DataFrame
    :class:`Series`
        If first input is a pandas.Series

    '''
    if isinstance(left, table.CASTable):
        return table.merge(left, right, **kwargs)
    return pd.merge(left, right, **kwargs)
