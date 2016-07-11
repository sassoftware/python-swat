#!/usr/bin/env python
# encoding: utf-8

'''
Utilities for collecting results from a CAS action

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import json
import pandas as pd
import pandas.core.common as pdcom
import pprint
import re
import six
from ..config import get_option
from ..dataframe import SASDataFrame, concat
from ..exceptions import SWATError
from ..utils.compat import OrderedDict, items_types
from ..utils import escapejson
from ..utils.xdict import xadict


class RendererMixin(object):
    ''' Mixin for rendering dictionaries of results '''

    def _repr_html_(self):
        '''
        Create an HTML representation for IPython

        Returns
        -------
        string
           HTML representation of CASResults object

        '''
        if pdcom.in_qtconsole():
            return None

        if not pd.get_option('display.notebook.repr_html'):
            return None

        output = []

        i = 0
        for key, item in six.iteritems(self):
            if not isinstance(key, int):
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
        from IPython.lib.pretty import pretty
        return pretty(self)

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
                p.text('+ ' + (', '.join(stats)).capitalize())

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
            if key.startswith('ByGroupInfo'):
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


class RenderableXADict(xadict, RendererMixin):
    ''' Renderable xadict object '''
    pass


@six.python_2_unicode_compatible
class CASResults(OrderedDict, RendererMixin):
    '''
    Ordered collection of results from a CAS action

    Attributes
    ----------
    performance : CASPerformance object
       Performance metrics of the action
    messages : list
       List of message strings
    signature : dict
       The action call's signature
    session : string
       Unique identifier of CAS session where action was executed
    updateflags : set
       Set of resources updated in the last action
    severity : int or long
       Severity level of the action response
    reason : string
       Reason for error
    status : string
       Formatted status message
    status_code : int
       Status code for the result

    Parameters
    ----------
    *args : any
       Positional argument passed to OrderedDict constructor
    **kwargs : any
       Arbitrary keyword arguments passed to OrderedDict constructor

    Returns
    -------
    CASResults object

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
        self._bykeys = None
        self._bygroups = None
        self._fmt_bygroups = None
        self._raw_bygroups = None

    def __getattr__(self, name):
        if name in self:
            return self[name]
        return super(CASResults, self).__getattribute__(name)

    @property
    def groups(self, set=1):
        ''' Return unique values of all By group variables '''
        if 'ByGroupInfo' not in self:
            return {}
        self._cache_bygroups()
        info = self['ByGroupInfo']
        out = {}
        for item in self._bykeys:
            out[item] = list(info.ix[:, item].unique())
        return out

    def _bygroup_keys(self, tbl):
        ''' Construct a unique By group key for raw and formatted values '''
        set_bykeys = False
        if not self._bykeys:
            set_bykeys = True
        fmtkey = []
        rawkey = []
        nstrip = lambda x: hasattr(x, 'strip') and x.strip() or x
        i = 1
        while True:
            bykey = 'ByVar%d' % i
            if bykey not in tbl.attrs:
                break
            if set_bykeys:
                self._bykeys.append(tbl.attrs[bykey])
            rawkey.append((tbl.attrs[bykey],
                           nstrip(tbl.attrs[bykey + 'Value'])))
            fmtkey.append((tbl.attrs[bykey],
                           tbl.attrs[bykey + 'ValueFormatted'].strip()))
            i += 1
        if rawkey and fmtkey:
            return tuple(sorted(rawkey, key=lambda x: x[0])), \
                   tuple(sorted(fmtkey, key=lambda x: x[0])) 
        if fmtkey:
            return None, tuple(sorted(fmtkey, key=lambda x: x[0]))
        return tuple(sorted(rawkey, key=lambda x: x[0])), None

    def _cache_bygroups(self):
        ''' Cache results by By group '''
        if self._bygroups is not None:
            return self._bygroups

        self._bykeys = []
        self._bygroups = OrderedDict()
        self._fmt_bygroups = OrderedDict()
        self._raw_bygroups = OrderedDict()

        if 'ByGroupInfo' not in self and 'ByGroupSet1.ByGroupInfo' not in self:
            return self._bygroups

        bygroup_re = re.compile('^(?:ByGroupSet(\d+)\.)?ByGroup(\d+)\.')
        for key, value in six.iteritems(self):
            if re.match(r'^(ByGroupSet\d+\.)?ByGroupInfo', key):
                continue

            # Table name
            tblname = re.search(r'((?:\.\w+)+)$', key).group(1)[1:]

            # Get bygroup index
            setnum = 1
            if 'ByGroupIndex' in getattr(value, 'attrs', {}):
                groupnum = value.attrs['ByGroupIndex']
                if 'ByGroupSet' in getattr(value, 'attrs', {}):
                    setnum = value.attrs['ByGroupSet']
            elif bygroup_re.match(key):
                keymatch = bygroup_re.match(key)
                setnum = int(keymatch.group(1))
                groupnum = int(keymatch.group(2))

            # Add group number key
            if groupnum not in self._bygroups:
                self._bygroups[groupnum] = RenderableXADict()
            self._bygroups[groupnum][tblname] = value

            rawkey, fmtkey = self._bygroup_keys(value)

            # Add group formatted value key
            if fmtkey not in self._fmt_bygroups:
                self._fmt_bygroups[fmtkey] = RenderableXADict()
            self._fmt_bygroups[fmtkey][tblname] = value

            # Add group raw value key
            if rawkey not in self._raw_bygroups:
                self._raw_bygroups[rawkey] = RenderableXADict()
            self._raw_bygroups[rawkey][tblname] = value

        return self._bygroups

    def get_group(_self_, *_num_, **kwargs):
        ''' 
        Return a dictionary of the tables in By group `num` 

        Parameters
        ----------
        _num_ : int or long, optional
            The index of the By group to retrieve.

        **kwargs : any, optional
            Key value pairs containing the variable name and value
            of the desired By group.    

        Returns
        -------
        list of results

        '''
        bygroups = _self_._cache_bygroups()

        if _num_:
            if _num_[0] in bygroups:
                return bygroups[_num_[0]]
            raise SWATError('%s is an invalid By group number' % _num_[0])

        if kwargs:
            bykey = tuple(sorted(six.iteritems(kwargs)))
            if bykey in _self_._fmt_bygroups:
                return _self_._fmt_bygroups[bykey]
            if bykey in _self_._raw_bygroups:
                return _self_._raw_bygroups[bykey]
            raise SWATError('%s is an invalid By group key' % kwargs)

        raise SWATError('No By group identifiers were specified')

    def get_tables(self, name, set=None, concat=False, **kwargs):
        '''
        Return all tables ending with `name` in all By groups

        Parameters
        ----------
        name : string
            The name of the tables to retrieve.  This name does not include
            the "ByGroup#." prefix if By groups are involved.
        set : int or long, optional
            The index of the By group set (if the action supports multiple
            sets of By groups).
        concat : boolean, optional
            Should the tables be concatenated into one DataFrame?
        **kwargs : any, optional
            Additional parameters to pass to pd.concat.

        Returns
        -------
        list of DataFrames

        '''
        if name in self:
            return [self[name]]

        if set is None and 'ByGroupSet1.ByGroupInfo' in self:
            raise ValueError('Multiple By group sets exist, but no set index was specified.')

        out = []
        by_re = re.compile(r'^(ByGroupSet%s\.)?ByGroup\d+\.%s' % (set, re.escape(name)))
        for key, value in six.iteritems(self):
            if by_re.match(key):
                out.append(value)

        if concat and out:
            if isinstance(out[0], SASDataFrame):
                attrs = out[0].attrs.copy()
                attrs.pop('ByGroup', None)
                attrs.pop('ByGroupIndex', None)
                i = 1
                while ('ByVar%d' % i) in attrs:
                    attrs.pop('ByVar%dValue' % i, None)
                    attrs.pop('ByVar%dValueFormatted' % i, None)
                    i = i + 1
                return SASDataFrame(pd.concat(out, **kwargs), name=out[0].name,
                                    label=out[0].label, title=out[0].title,
                                    formatter=out[0].formatter,
                                    attrs=attrs, colinfo=out[0].colinfo.copy())
            else:
                return pd.concat(out, **kwargs)

        return out 

    def concat_bygroups(self, inplace=False, **kwargs):
        '''
        Concatenate all tables within a By group into a single table

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

    def _repr_html_(self):
        '''
        Create an HTML representation for IPython

        Returns
        -------
        string
           HTML representation of CASResults object

        '''
        if pdcom.in_qtconsole():
            return None

        if not pd.get_option('display.notebook.repr_html'):
            return None

        output = []

        i = 0
        for key, item in six.iteritems(self):
            if not isinstance(key, int):
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
