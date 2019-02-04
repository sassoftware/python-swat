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
Utilities for collecting results from a CAS action

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import pprint
import re
import six
import pandas as pd
from ..dataframe import SASDataFrame, concat
from ..notebook.zeppelin import show as z_show
from ..utils.compat import OrderedDict
from ..utils.xdict import xadict


@six.python_2_unicode_compatible
class RendererMixin(object):
    ''' Mixin for rendering dictionaries of results '''

    def _z_show_(self, **kwargs):
        ''' Display Zeppelin notebook rendering '''
        i = 0
        for key, value in six.iteritems(self):
            if i == 0:
                print('%%html <div class="cas-results-key">'
                      '<b>&#167; %s</b></div>' % key)
            else:
                print('%%html <div class="cas-results-key">'
                      '<b><hr/>&#167; %s</b></div>' % key)

            print('')

            if hasattr(value, '_z_show_'):
                value._z_show_(**kwargs)
            else:
                z_show(value, **kwargs)

            print('')
            i = i + 1

        if getattr(self, 'performance'):
            stats = []
            if getattr(self.performance, 'elapsed_time'):
                stats.append('<span class="cas-elapsed">elapsed %.3gs</span>' %
                             self.performance.elapsed_time)
            if getattr(self.performance, 'cpu_user_time'):
                stats.append('<span class="cas-user">user %.3gs</span>' %
                             self.performance.cpu_user_time)
            if getattr(self.performance, 'cpu_system_time'):
                stats.append('<span class="cas-sys">sys %.3gs</span>' %
                             self.performance.cpu_system_time)
            if getattr(self.performance, 'memory'):
                stats.append('<span class="cas-memory">mem %.3gMB</span>' %
                             (self.performance.memory / 1048576.0))
            if stats:
                print('%%html <p class="cas-results-performance"><small>%s</small></p>' %
                      ' &#183; '.join(stats))

    def _repr_html_(self):
        '''
        Create an HTML representation for IPython

        Returns
        -------
        string
           HTML representation of CASResults object

        '''
        try:
            import IPython
            from pandas.io.formats import console
            from distutils.version import LooseVersion
        except ImportError:
            pass
        else:
            if LooseVersion(IPython.__version__) < LooseVersion('3.0'):
                if console.in_qtconsole():
                    # 'HTML output is disabled in QtConsole'
                    return None

        if not pd.get_option('display.notebook.repr_html'):
            return None

        output = []

        i = 0
        for key, item in six.iteritems(self):
            if i:
                sfmt = '<div class="cas-results-key"><hr/><b>&#167; %s</b></div>'
            else:
                sfmt = '<div class="cas-results-key"><b>&#167; %s</b></div>'
            output.append(sfmt % key)
            output.append('<div class="cas-results-body">')
            if hasattr(item, '_repr_html_'):
                res = item._repr_html_()
                if res is None:
                    output.append('<div>%s</div>' % res)
                else:
                    output.append(res)
            else:
                output.append('<div>%s</div>' % item)
            output.append('</div>')
            i += 1

        output.append('<div class="cas-output-area"></div>')

        if getattr(self, 'performance'):
            stats = []
            if getattr(self.performance, 'elapsed_time'):
                stats.append('<span class="cas-elapsed">elapsed %.3gs</span>' %
                             self.performance.elapsed_time)
            if getattr(self.performance, 'cpu_user_time'):
                stats.append('<span class="cas-user">user %.3gs</span>' %
                             self.performance.cpu_user_time)
            if getattr(self.performance, 'cpu_system_time'):
                stats.append('<span class="cas-sys">sys %.3gs</span>' %
                             self.performance.cpu_system_time)
            if getattr(self.performance, 'memory'):
                stats.append('<span class="cas-memory">mem %.3gMB</span>' %
                             (self.performance.memory / 1048576.0))
            if stats:
                output.append('<p class="cas-results-performance"><small>%s</small></p>' %
                              ' &#183; '.join(stats))

        return '\n'.join(output)

    def __str__(self):
        try:
            from IPython.lib.pretty import pretty
            return pretty(self)
        except ImportError:
            out = []

            for key, item in six.iteritems(self):
                out.append('[%s]' % key)
                out.append('')
                out.append('%s' % item)
                out.append('')
                out.append('')

            if getattr(self, 'performance'):
                out.append(self._performance_str_())

            return '\n'.join(out)

    def _repr_pretty_(self, p, cycle):
        from IPython.lib.pretty import pretty

        if cycle:
            p.text('...')
            return

        for key, item in six.iteritems(self):
            p.text('[%s]' % key)
            p.break_()
            with p.indent(1):
                for line in pretty(item).splitlines():
                    p.break_()
                    p.text(line)
            p.break_()
            p.break_()

        if getattr(self, 'performance'):
            p.text(self._performance_str_())

    def _performance_str_(self):
        if getattr(self, 'performance'):
            stats = []
            if getattr(self.performance, 'elapsed_time'):
                stats.append('elapsed: %.3gs' % self.performance.elapsed_time)
            if getattr(self.performance, 'cpu_user_time'):
                stats.append('user: %.3gs' % self.performance.cpu_user_time)
            if getattr(self.performance, 'cpu_system_time'):
                stats.append('sys: %.3gs' % self.performance.cpu_system_time)
            if getattr(self.performance, 'memory'):
                stats.append('mem: %.3gMB' % (self.performance.memory / 1048576.0))
            if stats:
                return '+ ' + (', '.join(stats)).capitalize()
        return ''

    def _make_byline(self, attrs):
        ''' Create a SAS Byline from DataFrame metadata '''
        if 'ByGroup' in attrs:
            return attrs['ByGroup']
        return ''

    def _render_html_(self):
        ''' Create an ODS-like report of the results '''
        byline = ''
        output = []

        output.append('<div class="cas-results">')

        for key, value in self.items():
            if key.startswith('$'):
                continue
            if key.endswith('ByGroupInfo'):
                continue

            if isinstance(value, SASDataFrame):
                newbyline = self._make_byline(value.attrs)
                if newbyline != byline:
                    byline = newbyline
                    output.append('<h3 class="byline">%s</h3>' % byline)
                output.append(value._render_html_())
                continue

            if hasattr(value, '_render_html_'):
                result = value._render_html_()
                if result is None:
                    output.append(result)
                    continue

            if hasattr(value, '_repr_html_'):
                result = value._repr_html_()
                if result is None:
                    output.append(result)
                    continue

            output.append('<pre>%s</pre>' % pprint.pformat(value))

        output.append('</div>')

        return ''.join(output)


class RenderableXADict(RendererMixin, xadict):
    ''' Renderable xadict object '''
    pass


class CASResults(RendererMixin, OrderedDict):
    '''
    Ordered collection of results from a CAS action

    The output of all CAS actions is a :class:`CASResults` object.
    This is a Python ordered dictionary with a few methods added to
    assist in handling the output keys, and attributes added to
    report information from the CAS action.

    Attributes
    ----------
    performance : CASPerformance object
       Performance metrics of the action.
    messages : list-of-strings
       List of message strings.
    signature : dict
       The action call's signature.
    session : string
       Unique identifier of CAS session where action was executed.
    updateflags : set-of-strings
       Set of resources updated in the last action.
    severity : int
       Severity level of the action response. A value of zero means
       that no problems were reported.  A value of 1 means that warnings
       were reported.  A value of 2 means that errors were reported.
    reason : string
       Reason for error.
    status : string
       Formatted status message.
    status_code : int
       Status code for the result.

    Parameters
    ----------
    *args : any
       Positional argument passed to OrderedDict constructor
    **kwargs : any
       Arbitrary keyword arguments passed to OrderedDict constructor

    Examples
    --------
    >>> conn = swat.CAS()
    >>> out = conn.serverstatus()

    Calling standard Python dictionary methods.

    >>> print(list(out.keys()))
    ['About', 'server', 'nodestatus']

    Accessing keys.

    >>> print(out['About'])
    {'license': {'siteNum': 1, 'warningPeriod': 31,
     'expires': '08Sep2016:00:00:00', ... }}

    You can also access keys using attribute syntax as long as the
    key name doesn't collide with an existing attribute or method.

    >>> print(out.About)
    {'license': {'siteNum': 1, 'warningPeriod': 31,
     'expires': '08Sep2016:00:00:00', ... }}

    Iterating over items.

    >>> for key, value in out.items():
    ...     print(key)
    About
    server
    nodestatus

    Display status information and performance metrics for the CAS action.

    >>> print(out.status)
    None

    >>> print(out.severity)
    0

    >>> print(out.performance)
    CASPerformance(cpu_system_time=0.004999, cpu_user_time=0.020997,
                   data_movement_bytes=0, data_movement_time=0.0,
                   elapsed_time=0.025089, memory=704160, memory_os=9228288,
                   memory_quota=19746816, system_cores=24, system_nodes=1,
                   system_total_memory=101427879936)

    Returns
    -------
    :class:`CASResults` object

    '''

    def __init__(self, *args, **kwargs):
        super(CASResults, self).__init__(*args, **kwargs)
        self.performance = None
        self.messages = None
        self.events = collections.OrderedDict()
        self.signature = None
        self.session = None
        self.sessionname = None
        self.updateflags = None
        # disposition fields
        self.severity = None
        self.reason = None
        self.status = None
        self.status_code = None
        self.debug = None

    def __getattr__(self, name):
        if name in self:
            return self[name]
        return super(CASResults, self).__getattribute__(name)

    def get_set(self, num):
        '''
        Return a :class:`CASResults` object of the By group set

        Some CAS actions support multiple By group sets.  This
        method can be used to retrieve the values for a particular
        set index.

        Parameters
        ----------
        num : int
            The By group set index to return.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_csv('data/cars.csv')
        >>> out = tbl.mdsummary(sets=[dict(groupby=['Origin']),
                                      dict(groupby=['Cylinders'])])

        Return the first By group set objects

        >>> print(out.get_set(1))

        Return the second By group set objects

        >>> print(out.get_set(2))

        Returns
        -------
        :class:`CASResults`

        '''
        if 'ByGroupSet1.ByGroupInfo' not in self:
            raise IndexError('There are no By group sets defined.')

        out = CASResults()
        prefix = 'ByGroupSet%s.' % num
        for key, value in six.iteritems(self):
            if key.startswith(prefix):
                out[key.replace(prefix, '', 1)] = value

        if out:
            return out

        raise IndexError('No By group set matched the given index.')

    def get_group(_self_, *name, **kwargs):
        '''
        Return a :class:`CASResults` object of the specified By group tables

        Parameters
        ----------
        name : string or tuple-of-strings, optional
            The values of the By variable to choose.
        **kwargs : any, optional
            Key / value pairs containing the variable name and value
            of the desired By group.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_csv('data/cars.csv')
        >>> out = tbl.groupby(['Origin', 'Cylinders']).summary()

        Specify the By group values (in order).

        >>> print(out.get_group(('Asia', 4)))

        Or, specify the By group values as keyword parameters.

        >>> print(out.get_group(Origin='Asia', Cylinders=4))

        Returns
        -------
        :class:`CASResults`

        '''
        self = _self_

        if 'ByGroupSet1.ByGroupInfo' in self:
            raise IndexError('Multiple By group sets are defined, use get_set to '
                             'to select a By group set first.')

        # Convert list of positionals to a scalar
        if name:
            name = name[0]

        if not isinstance(name, (tuple, list)):
            name = tuple([name])
        else:
            name = tuple(name)

        out = CASResults()

        bykey = []

        def set_bykey(attrs):
            ''' Locate By variable keys '''
            if bykey:
                return bykey
            i = 1
            while True:
                if 'ByVar%s' % i not in attrs:
                    break
                bykey.append(attrs['ByVar%s' % i])
                i = i + 1
            return bykey

        for key, value in six.iteritems(self):
            if not isinstance(value, SASDataFrame):
                continue
            if not re.match(r'^ByGroup\d+\.', key):
                continue
            attrs = value.attrs
            match = True
            i = 1
            for byname in set_bykey(attrs):
                if kwargs:
                    if attrs['ByVar%sValue' % i] != kwargs[byname] and \
                            attrs['ByVar%sValueFormatted' % i] != kwargs[byname]:
                        match = False
                        break
                elif name:
                    try:
                        if attrs['ByVar%sValue' % i] != name[i - 1] and \
                                attrs['ByVar%sValueFormatted' % i] != name[i - 1]:
                            match = False
                            break
                    except IndexError:
                        raise KeyError('No matching By group keys were found.')
                i = i + 1
            if match:
                out[re.sub(r'^ByGroup\d+\.', '', key)] = value

        if out:
            return out

        raise KeyError('No matching By group keys were found.')

    def get_tables(self, name, set=None, concat=False, **kwargs):
        '''
        Return all tables ending with `name` in all By groups

        Parameters
        ----------
        name : string
            The name of the tables to retrieve.  This name does not include
            the "ByGroup#." prefix if By groups are involved.  It also does
            not include "ByGroupSet#." if By group sets are involved.
        set : int, optional
            The index of the By group set (if the action supports multiple
            sets of By groups).
        concat : boolean, optional
            Should the tables be concatenated into one DataFrame?
        **kwargs : any, optional
            Additional parameters to pass to :func:`pandas.concat`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_csv('data/cars.csv')

        >>> out = tbl.summary()
        >>> print(list(out.keys()))
        ['Summary']
        >>> print(len(out.get_tables('Summary')))
        1

        >>> out = tbl.groupby('Origin').summary()
        >>> print(list(out.keys()))
        ['ByGroupInfo', 'ByGroup1.Summary', 'ByGroup2.Summary', 'ByGroup3.Summary']
        >>> print(len(out.get_tables('Summary')))
        3

        Returns
        -------
        list of DataFrames

        '''
        if name in self:
            return [self[name]]

        if set is None and 'ByGroupSet1.ByGroupInfo' in self:
            raise ValueError('Multiple By group sets exist, but no set '
                             'index was specified.')

        out = []
        by_re = re.compile(r'^(ByGroupSet%s\.)?ByGroup\d+\.%s$' % (set, re.escape(name)))
        for key, value in six.iteritems(self):
            if by_re.match(key):
                out.append(value)

        if concat and out:
            if isinstance(out[0], SASDataFrame):
                attrs = out[0].attrs.copy()
                attrs.pop('ByGroup', None)
                attrs.pop('ByGroupIndex', None)
                i = 1
                while 'ByVar%d' % i in attrs:
                    attrs.pop('ByVar%dValue' % i, None)
                    attrs.pop('ByVar%dValueFormatted' % i, None)
                    i = i + 1
                return SASDataFrame(pd.concat(out, **kwargs), name=out[0].name,
                                    label=out[0].label, title=out[0].title,
                                    formatter=out[0].formatter,
                                    attrs=attrs, colinfo=out[0].colinfo.copy())
            return pd.concat(out, **kwargs)

        return out

    def concat_bygroups(self, inplace=False, **kwargs):
        '''
        Concatenate all tables within a By group into a single table

        Parameters
        ----------
        inplace : boolean, optional
            Should the :class:`CASResults` object be modified in place?
        **kwargs : keyword arguments, optional
            Additional parameters to the :func:`concat` function.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_csv('data/cars.csv')
        >>> out = tbl.groupby('Origin').summary()

        >>> print(list(out.keys()))
        ['ByGroupInfo', 'ByGroup1.Summary', 'ByGroup2.Summary', 'ByGroup3.Summary']

        >>> out.concat_bygroups(inplace=True)
        >>> print(list(out.keys()))
        ['Summary']

        Returns
        -------
        :class:`CASResults`
            If inplace == False
        ``None``
            If inplace == True

        '''
        if 'ByGroupSet1.ByGroupInfo' in self:
            keyfmt = 'ByGroupSet%(set)s.%(name)s'
        else:
            keyfmt = '%(name)s'

        tables = collections.OrderedDict()
        delkeys = []
        info_re = re.compile(r'^(?:ByGroupSet\d+\.)?ByGroupInfo')
        tbl_re = re.compile(r'^(?:ByGroupSet(\d+)\.)?ByGroup(\d+)\.(.+?)$')

        out = self
        if not inplace:
            out = CASResults()

        for key, value in six.iteritems(self):
            if info_re.match(key):
                if inplace:
                    delkeys.append(key)
                continue

            tblmatch = tbl_re.match(key)
            if tblmatch:
                tables.setdefault(keyfmt % dict(set=tblmatch.group(1),
                                                name=tblmatch.group(3)), []).append(value)
                if inplace:
                    delkeys.append(key)
                else:
                    out[keyfmt % dict(set=tblmatch.group(1),
                                      name=tblmatch.group(3))] = None

            elif not inplace:
                out[key] = value

        for key in delkeys:
            self.pop(key, None)

        for key, value in six.iteritems(tables):
            out[key] = concat(value, **kwargs)

        if not inplace:
            return out

#   def _my_repr_json_(self):
#       '''
#       Create a JSON representation of the CASResults object

#       Returns
#       -------
#       string
#          JSON representation of CASResults object

#       '''
#       out = {}
#       out['session'] = self.session
#       out['sessionname'] = self.sessionname
#       out['performance'] = self.performance.to_dict()
#       out['signature'] = self.signature
#       out['messages'] = self.messages
#       # disposition fields
#       out['debug'] = self.debug
#       out['status'] = self.status
#       out['status_code'] = self.status_code
#       out['reason'] = self.reason
#       out['severity'] = self.severity

#       def default(obj):
#           ''' Convert unknown objects to known types '''
#           if isinstance(obj, set):
#               return list(obj)
#           return obj

#       return escapejson(json.dumps(out, default=default))

#   def _repr_javascript_(self):
#       '''
#       Create a Javascript representation for IPython

#       Returns
#       -------
#       string
#          Javascript representation of CASResults object

#       '''
#       if not get_option('display.notebook.repr_javascript'):
#           return None

#       children = []

#       for key, item in six.iteritems(self):
#           children.append("element.append($.elem('div', " +
#                           "{text:'%s', class:'cas-results-key'}));" %
#                           escapejson('%s' % key))

#           if hasattr(item, '_repr_javascript_'):
#               output = item._repr_javascript_()
#               if output is not None:
#                   children.append(item._repr_javascript_())
#                   continue

#           if hasattr(item, '_repr_html_'):
#               output = item._repr_html_()
#               if output is not None:
#                   children.append("element.append($.elem('div', {html:'%s'}));" %
#                                   item._repr_html_().replace("'", "\\'").replace('\n',
#                                                                                  r'\n'))
#                   continue

#           children.append("element.append($.elem('pre', {text:'%s'}));" %
#                           escapejson(str(item)).replace("'", "\\'").replace('\n',
#                                                                             r'\n'))

#       children = '\n'.join(children)

#       return notebook.bootstrap(r'''
#        require(['swat'], function (swat) {
#           new swat.CASResults(element, JSON.parse('%s'), function (element) {
#              %s
#           }, function () { console.log('Could not load swat.js') } );

#        });''' % (self._my_repr_json_().replace("'", "\\'"), children))
