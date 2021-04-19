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
Classes for interfacing with CAS actions and action sets

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import json
import keyword
import os
import re
import textwrap
import weakref
import six
import pandas as pd
from ..config import get_option
from ..exceptions import SWATError
from ..utils import mergedefined
from ..utils.compat import a2n
from ..utils.keyword import dekeywordify
from ..utils.xdict import xadict
from .utils.params import ParamManager

# pylint: disable=C0330

SET_PARAM_DOCSTRING = '''
Set one or more action parameters

Parameters
----------
*args : string / any pairs, optional
    Parameters can be specified as fully-qualified names (e.g, table.name)
    and values as subsequent arguments.  Any number of name / any pairs
    can be specified.
**kwargs : any, optional
    Parameters can be specified as any number of keyword arguments.

Examples
--------
#
# String / any pairs
#
> summ = s.simple.Sumamry()
> summ.set_param('table.name', 'iris',
                 'table.singlepass', True,
                 'casout.name', 'iris_summary')
> print(summ)
?.simple.Summary(table={'name': 'iris', 'singlepass': True},
                 casout={'name': 'iris_summary'})

#
# Keywords
#
> summ.set_param(casout=dict(name='iris_out'))
> print(summ)
?.simple.Summary(table={'name': 'iris', 'singlepass': True},
                 casout={'name': 'iris_out'})

Valid Parameters
----------------
%s

Returns
-------
None

'''

SET_PARAMS_DOCSTRING = SET_PARAM_DOCSTRING

GET_PARAM_DOCSTRING = '''
Get the value of an action parameter

Parameters
----------
key : string
    The fully-qualified name (e.g., table.name) of the parameter to retrieve.

Valid Parameters
----------------
%s

Returns
-------
any
    The value of the speciifed parameter.

'''

GET_PARAMS_DOCSTRING = '''
Get the value of one or more action parameters

Parameters
----------
*keys : one or more strings
    The fully-qualified names (e.g., table.name) of the parameters to retrieve.

Valid Parameters
----------------
%s

Returns
-------
dict
    A dictionary of key value pairs containing the requested parameters.

'''


def _format_param(param, connection, indent=0, selector=None, path='', output=None,
                  suppress_subparams=None, param_names=None, results_format=False):
    '''
    Format a docstring for an action parameter and its sub-parameters

    Parameters
    ----------
    param : dict
        The reflection information for the parameter
    indent : int, optional
        The number of indentation levels to use
    selector : string, optional
        If the parameter being documented is only selected based on
        a selector value of the parent parameter, this string
        indicates the name of the selector.
    path : string, optional
        The absolute path of the parameter
    output : list, optional
        The list where pieces of documentation are appended
    suppress_subparams : list of strings, optional
        A list of absolute parameter names that should not have
        their sub-parameters documented.
    results_format : boolean, optional
        Is this description being used for results rather than
        parameter formatting?

    Returns
    -------
    None

    '''
    if output is None:
        output = []
    if param_names is None:
        param_names = []

    thisindent = ' ' * (indent * 4)
    nextindent = ' ' * ((indent + 1) * 4)
    nextnextindent = ' ' * ((indent + 2) * 4)

    # Set up fully-qualified option name
    if path and selector is None:
        path = '%s.%s' % (path, param['name'].lower())
    elif selector is not None:
        pass
    else:
        path = param['name'].lower()

    # Check for required or optional
    optional = ', optional'
    if param.get('isRequired'):
        optional = ''
    if results_format:
        optional = ''

    # Determine proper Python data type
    selector_values = []
    selector_type = ''
    alttypes = [param['parmType'].replace('value_', '')]

    if 'alternatives' in param:
        alttypes = list(set([prm['parmType'].replace('value_', '')
                             for prm in param['alternatives']
                             if not prm.get('hidden')]))
        if alttypes == ['list'] and param.get('selector'):
            alttypes = ['dict']
            for alt in param['alternatives']:
                if alt.get('hidden'):
                    continue
                for prm in alt['parmList']:
                    if prm['name'] == param['selector']:
                        selector_values.extend(prm['allowedValues'])
                        selector_type = prm['parmType']
                        continue
        elif 'string' in alttypes:
            for alt in param['alternatives']:
                if alt.get('hidden'):
                    continue
                if alt['parmType'] == 'string' and 'allowedValues' in alt:
                    param.setdefault('allowedValues', []).extend(alt['allowedValues'])
                if 'default' in alt:
                    param.setdefault('default', alt['default'])

    elif alttypes == ['list']:
        if 'parmList' in param:
            alttypes = ['dict']
        elif 'exemplar' in param:
            alttypes = ['list of dicts']
        elif param.get('isVar') or param.get('isCompVar') or \
                param.get('isGroupBy') or param.get('isOrderBy'):
            alttypes = ['list of strings']

    # If this is a table, allow CASTable objects too
    if param.get('isTableName') or param.get('isTableDef') or param.get('isOutTableDef'):
        alttypes.append('CASTable')

    # Handle alternatives
    if selector is None:
        if keyword.iskeyword(path):
            # TODO: I think the dekeywordify here needs work, it's doing the path
            #       not just the last component.
            output.append('%s%s, %s : %s%s' % (thisindent, path, dekeywordify(path),
                          ' or '.join(alttypes), optional))
            param_names.append(path)
            param_names.append(dekeywordify(path))
        else:
            output.append('%s%s : %s%s' % (thisindent, path,
                          ' or '.join(alttypes), optional))
            param_names.append(path)
    else:
        # TODO: Check selector against keywords
        svalue = None
        num_params = 0
        for prm in param['parmList']:
            if prm['name'] == selector:
                svalue = prm['allowedValues'][0]
            num_params = num_params + 1
        if num_params > 1:
            output.append('%sif %s.%s == %s:' % (thisindent, path,
                                                 selector.lower(), svalue))
        param_names.append('%s.%s' % (path, selector.lower()))

    width = 72
    wraptext = None
    if hasattr(connection._sw_connection, 'wraptext'):
        wraptext = connection._sw_connection.wraptext

    # Print description and other meta-data
    if 'desc' in param:
        if wraptext:
            output.append(''.join(wraptext(a2n(param['desc'], 'utf-8'),
                                           width, a2n(nextindent, 'utf-8'),
                                           a2n(nextindent, 'utf-8'))))
        else:
            output.append(textwrap.fill(param['desc'], width, initial_indent=nextindent,
                                        subsequent_indent=nextindent))

    if not(results_format) and 'default' in param:
        if param['parmType'] == 'boolean':
            output.append('%sDefault: %s' % (nextindent,
                                             param['default'] and True or False))
        else:
            output.append('%sDefault: %s' % (nextindent, param['default']))

    if selector_values:
        output.append('')
        output.append('%s%s.%s : %s' % (nextindent, path,
                                        param['selector'].lower(), selector_type))
        output.append('%sDefault: %s' % (nextnextindent, selector_values[0]))
        value = a2n('Values: %s' %
                    (', '.join([('%s' % x) for x in selector_values])), 'utf-8')
        if wraptext:
            output.append(''.join(wraptext(value, width,
                                           a2n(nextnextindent, 'utf-8'),
                                           a2n(nextnextindent + ' ' * 8, 'utf-8'))))
        else:
            output.append(textwrap.fill('Values: %s' %
                                        (', '.join([('%s' % x)
                                         for x in selector_values])),
                                        width, initial_indent=nextnextindent,
                                        subsequent_indent=nextnextindent + ' ' * 8))
    elif 'allowedValues' in param:
        value = a2n('Values: %s' % (', '.join([('%s' % x)
                                    for x in param['allowedValues']])), 'utf-8')
        if wraptext:
            output.append(''.join(wraptext(value, width,
                                           a2n(nextindent, 'utf-8'),
                                           a2n(nextindent + ' ' * 8, 'utf-8'))))
        else:
            output.append(textwrap.fill('Values: %s' %
                                        (', '.join([('%s' % x)
                                         for x in param['allowedValues']])),
                                        width, initial_indent=nextindent,
                                        subsequent_indent=nextindent + ' ' * 8))

#   if 'isVar' in param and param['isVar']:
#       output.append('%sNote: Must be a valid variable name' % nextindent)
#   if 'isCompVar' in param and param['isCompVar']:
#       output.append('%sNote: Must be a valid computed variable name' % nextindent)
#   if 'isGroupBy' in param and param['isGroupBy']:
#       output.append('%sNote: Must be a valid variable name' % nextindent)
#   if 'isOrderBy' in param and param['isOrderBy']:
#       output.append('%sNote: Must be a valid variable name' % nextindent)
#   if 'isWhere' in param and param['isWhere']:
#       output.append('%sNote: Must be a valid where clause' % nextindent)
#   if 'isTableName' in param and param['isTableName']:
#       output.append('%sNote: Must be a valid table name' % nextindent)
#   if 'isCasLib' in param and param['isCasLib']:
#       output.append('%sNote: Must be a valid CASLib name' % nextindent)
#   if 'uiGroup' in param:
#       print('%sNote: %s' % (nextindent, param['uiGroup']))

    if 'valueMin' in param or 'valueMax' in param:
        maxval = 'inf'
        minval = '-inf'
        if param['parmType'] == 'int32':
            maxval = 2 ** 31 - 1
            minval = -2 ** 31
        elif param['parmType'] == 'int64':
            maxval = 2 ** 63 - 1
            minval = -2 ** 63
        elif param['parmType'] == 'double':
            maxval = 'max-double'
            minval = 'min-double'
        low = param.get('valueMin', minval)
        high = param.get('valueMax', maxval)
        lowop = '<'
        highop = '<'
        if param.get('hasInclMin'):
            lowop = '<='
        if param.get('hasInclMax'):
            highop = '<='
        output.append('%sNote: Value range is %s %s n %s %s' %
                      (nextindent, low, lowop, highop, high))

    if not(results_format) and output[-1]:
        output.append('')

    if suppress_subparams and path in suppress_subparams:
        return

    # Sub-parameters
    if 'parmList' in param:
        for prm in param['parmList']:
            if selector is not None and prm['name'] == selector:
                continue
            _format_param(prm, connection, indent=indent + 1, path=path, output=output,
                          suppress_subparams=suppress_subparams,
                          param_names=param_names)
            if not(results_format) and output[-1]:
                output.append('')

    # Print alternative parameter lists
    if 'alternatives' in param and 'dict' in alttypes:
        for prm in param['alternatives']:
            if 'parmList' in prm:
                _format_param(prm, connection, indent=indent + 1,
                              selector=param['selector'],
                              path=path, output=output,
                              suppress_subparams=suppress_subparams,
                              param_names=param_names)
                if not(results_format) and output[-1]:
                    output.append('')

    # Print exemplars
    if 'exemplar' in param:
        for prm in param['exemplar']:
            _format_param(prm, connection, indent=indent + 1, path=path + '[*]',
                          output=output, suppress_subparams=suppress_subparams,
                          param_names=param_names)
            if not(results_format) and output[-1]:
                output.append('')


def format_params(params, connection, suppress_subparams=None, param_names=None,
                  results_format=False):
    '''
    Format a docstring for a list of parameters

    Parameters
    ----------
    params : list
        A list of parameter information reflected from the server
    suppress_subparams : list of strings, optional
        A list of absolute parameter names that should not have
        their sub-parameters documented
    param_names : list, optional
       A list that is populated with all absolute parameter names
       in the resulting docstring
    results_format : boolean, optional
        Is this description being used for results rather than
        parameter formatting?

    Returns
    -------
    string

    '''
    if not get_option('interactive_mode'):
        return ''
    output = []
    for param in params:
        _format_param(param, connection, indent=0, output=output,
                      suppress_subparams=suppress_subparams,
                      param_names=param_names, results_format=results_format)
        if not(results_format) and output[-1]:
            output.append('')
    return '\n'.join(output)


class CASActionSet(object):
    '''
    CASActionSet container

    Attributes and methods of this object are CASAction instances
    and methods that call CAS actions, respectively.

    '''

    trait_names = None  # Block IPython's lookup of this
    _connection = None

    @classmethod
    def from_reflection(cls, asinfo, connection):
        '''
        Create a CASActionSet class from reflection information

        Parameters
        ----------
        asinfo : dict
            Reflection information from the server
        connection : CAS object
            The connection object to associate with the CASActionSet

        Returns
        -------
        CASActionSet class

        '''
        asname = asinfo['name'].lower()

        actions = {}

        members = {
            '_connection': weakref.ref(connection),
            '__doc__': cls._format_actionset_doc(asinfo),
            'actions': actions,
        }

        # Create a class for each action
        for act in asinfo.get('actions', []):
            actcls = CASAction.from_reflection(asname, act, connection)
            clsname = actcls.__name__.split('.', 1)[-1].lower()

            # Don't include table.upload, it can't be called directly.
            if asname == 'table' and clsname.lower() == 'upload':
                continue

            actions[clsname] = actcls

        # Generate action set class
        return type(str(asname).title(), (CASActionSet,), members)

    @classmethod
    def _format_actionset_doc(cls, asinfo):
        '''
        Generate action set documentation string

        Parameters
        ----------
        asinfo : dict
           Reflection information to generate documentation from

        Returns
        -------
        string
           Documentation derived from action set dictionary

        '''
        actions = asinfo.get('actions', [])

        doc = []
        doc.append(asinfo.get('desc', asinfo.get('label', asinfo['name'])))
        doc.append('')
        doc.append('Actions')
        doc.append('-------')
        display_width = pd.options.display.width
        width = 0
        for item in actions:
            width = max(width, len(item['name']))
        for item in sorted(actions, key=lambda x: x['name']):
            doc.append(textwrap.fill(item.get('desc', item.get('label', '')),
                                     width=display_width,
                                     initial_indent='%s : ' % item['name'].ljust(width),
                                     subsequent_indent=' ' * (width + 3)))
        return '\n'.join(doc)

    @classmethod
    def get_connection(cls):
        '''
        Retrieve the registered connection

        Since the connection is only held using a weak reference,
        this method will raise a SWATError if the connection object
        no longer exists.

        Returns
        -------
        CAS object
            The registered connection object

        Raises
        ------
        SWATError
            If the connection object no longer exists

        '''
        try:
            if cls._connection is not None:
                conn = cls._connection()
        except AttributeError:
            pass
        if conn is None:
            raise SWATError('Connection object is no longer valid')
        return conn

    def __getattr__(self, name):
        origname = name
        name = name.lower()
        enabled = ['yes', 'y', 'on', 't', 'true', '1']

        if name in type(self).actions:

            cls = type(self).actions[name]

            # Check for un-reflected actions
            if cls is None:
                if os.environ.get('CAS_ACTION_TEST_MODE', '').lower() in enabled:
                    return CASActionRaw('%s.%s' % (type(self).__name__.lower(), name),
                                        self.get_connection())

            # Create action class
            if hasattr(self, 'default_params') and self.default_params is not None:
                members = {'default_params': getattr(self, 'default_params', {})}
                cls = type(cls.__name__, (cls,), members)

            # Return action class
            if re.match(r'^[A-Z]', origname):
                return cls

            # Return action instance
            return cls()

        elif os.environ.get('CAS_ACTION_TEST_MODE', '').lower() in enabled:
            return CASActionRaw('%s.%s' % (type(self).__name__.lower(), name),
                                self.get_connection())

        raise AttributeError(origname)

    def __call__(self, *args, **kwargs):
        return getattr(self, self.__class__.__name__.lower())(*args, **kwargs)

    def __dir__(self):
        return list(type(self).actions.keys())


class CASAction(ParamManager):
    '''
    Create a CASAction object

    Parameters
    ----------
    *args : string / value pairs, optional
       Pairs of parameter name strings and values given as subsequent
       arguments (not as tuples).

    **kwargs : any, optional
       Arbitrary keyword arguments.  These key/value pairs are
       the initial parameters to the CAS action.

    Returns
    -------
    CASAction object

    '''

    trait_names = None  # Block IPython's lookup of this
    _connection = None
    all_params = set()

    def __init__(self, *args, **kwargs):
        super(CASAction, self).__init__(*args, **kwargs)
        self.params.set_dir_values(type(self).all_params)

        # Add doc to params
        if type(self).__doc__:
            idx = 0
            if 'Parameters' in type(self).__doc__:
                idx = 1
            self.params.set_doc(re.split(r'\w+\s+----+',
                                         type(self).__doc__)[idx].strip())
        elif self.__init__.__doc__:
            idx = 0
            if 'Parameters' in self.__init__.__doc__:
                idx = 1
            self.params.set_doc(re.split(r'\w+\s+----+',
                                         self.__init__.__doc__)[idx].strip())

    @classmethod
    def from_reflection(cls, asname, actinfo, connection):
        '''
        Construct a CASAction class from reflection information

        Parameters
        ----------
        asname : string
            The action set name
        actinfo : dict
            The reflection information for the action
        connection : CAS object
            The connection to associate with the CASAction
        defaults : dict
            Default parameters for the action

        Returns
        -------
        CASAction class

        '''
        _globals = globals()
        _locals = locals()

        name = actinfo['name'].split('.', 1)[-1]

        clsname = name.title()

        # Create call signatures
        params = actinfo.get('params', [])
        results = actinfo.get('results', [])
        params = [x for x in params if not x['name'].startswith('_')]
        params = sorted(params, key=lambda x: (int(not x.get('isRequired', 0))))
        pkeys = [param['name'] for param in params]
        sig = ', '.join([('%s=None' % dekeywordify(x)) for x in pkeys] + ['**kwargs'])
        callargs = ', '.join([('%s: %s' % (repr(x), dekeywordify(x))) for x in pkeys])

        # Save these for testing valid default_params
        param_names = [prm.lower() for prm in pkeys]

        # __init__ and action methods
        funcargs = ('**mergedefined(_self_._get_default_params(), '
                    + '{%s}, kwargs)') % callargs
        six.exec_(('''def __init__(_self_, %s):'''
                   + '''    CASAction.__init__(_self_, %s)''')
                  % (sig, funcargs), _globals, _locals)
        six.exec_(('''def __call__(_self_, %s):'''
                   + '''    return CASAction.__call__(_self_, %s)''')
                  % (sig, funcargs), _globals, _locals)

        # Generate documentation
        all_params = []
        setget_doc = format_params(params, connection,
                                   suppress_subparams=['table.importoptions'],
                                   param_names=all_params).rstrip()
        action_doc = cls._format_action_doc(actinfo, setget_doc).rstrip()
        if results:
            results_doc = '\n\nResults Keys\n------------\n' + \
                          format_params(results, connection, results_format=True).rstrip()
        else:
            results_doc = ''

        # Generate set/del methods for scalar parameters
        def set_params(_self_, *args, **kwargs):
            ''' Set parameters '''
            return CASAction.set_params(_self_, *args, **kwargs)

        def set_param(_self_, *args, **kwargs):
            ''' Set parameter '''
            return CASAction.set_param(_self_, *args, **kwargs)

        def get_params(_self_, *keys):
            ''' Get parameters '''
            return CASAction.get_params(_self_, *keys)

        def get_param(_self_, key):
            ''' Get parameter '''
            return CASAction.get_param(_self_, key)

        # Set docstrings
        set_params.__doc__ = SET_PARAMS_DOCSTRING % setget_doc
        set_param.__doc__ = SET_PARAM_DOCSTRING % setget_doc
        get_params.__doc__ = GET_PARAMS_DOCSTRING % setget_doc
        get_param.__doc__ = GET_PARAM_DOCSTRING % setget_doc
        _locals['__call__'].__doc__ = re.sub(r'\w+ object$',
                                             r'CASResults object%s' % results_doc,
                                             action_doc.rstrip())
        _locals['__init__'].__doc__ = action_doc.rstrip()

        for name in list(param_names):
            if keyword.iskeyword(name):
                param_names.append(dekeywordify(name))

        # CASAction members and methods
        actmembers = {
            '_connection': weakref.ref(connection),
            '__init__': _locals['__init__'],
            '__call__': _locals['__call__'],
            '__doc__': action_doc,
            'set_params': set_params,
            'set_param': set_param,
            'get_params': get_params,
            'get_param': get_param,
            'param_names': param_names,
            'all_params': set(all_params)
        }

        # Generate action class
        actcls = type(str(asname + '.' + clsname), (CASAction,), actmembers)

        return actcls

    @classmethod
    def _format_action_doc(cls, actinfo, paramdoc):
        '''
        Create a docstring for action class

        Parameters
        ----------
        actinfo : dict
            Action reflection information
        paramdoc : string
            String containing parameter documentation

        Returns
        -------
        string

        '''
        doc = []
        doc.append(actinfo.get('desc', actinfo.get('label', actinfo['name'])))
        doc.append('')
        if paramdoc.rstrip():
            doc.append('Parameters')
            doc.append('----------')
            doc.append(paramdoc.rstrip())
            doc.append('')
        doc.append('Returns')
        doc.append('-------')
        doc.append('%s object' % actinfo['name'].split('.', 1)[-1].title())
        return '\n'.join(doc)

    @classmethod
    def _format_call_doc(cls, actinfo, paramdoc):
        ''' Generate action call documentation '''
        return re.sub(r'\w+ object$', r'CASResults object',
                      cls._format_action_doc(actinfo, paramdoc).rstrip())

    @classmethod
    def get_connection(cls):
        '''
        Return the registered connection

        The connection is only held by a weak reference.  If the
        connection no longer exists, a SWATError is raised.

        Raises
        ------
        SWATError
            If the registered connection no longer exists

        '''
        try:
            if cls._connection is not None:
                conn = cls._connection()
        except AttributeError:
            pass
        if conn is None:
            raise SWATError('Connection object is no longer valid')
        return conn

    def _get_default_params(self):
        ''' Get a dictionary of only valid default paramaters '''
        out = xadict()
        params = getattr(self, 'default_params', {})
        if isinstance(params, dict):
            for key, value in six.iteritems(params):
                if key.lower() in type(self).param_names:
                    out[key] = value
                elif key.lower() == '__table__':
                    out[key] = value
            return out
        return out

    def __iter__(self):
        ''' Call the action and iterate over the results '''
        return iter(self.invoke())

    def invoke(self, **kwargs):
        '''
        Invoke the action

        Parameters
        ----------
        **kwargs : any, optional
            Arbitrary key/value pairs to add to the arguments sent to the
            action.  These key/value pairs are not added to the collection
            of parameters set on the action object.  They are only used in
            this call.

        Returns
        -------
        self
            Returns the CASAction object itself

        '''
        # Decode from JSON as needed
        if '_json' in kwargs:
            newargs = json.loads(kwargs['_json'])
            newargs.update(kwargs)
            del newargs['_json']
            kwargs = newargs

        conn = type(self).get_connection()
        conn.invoke(type(self).__name__.lower(), **mergedefined(self.to_params(), kwargs))
        return conn

    def __call__(self, **kwargs):
        '''
        Call the action

        Parameters
        ----------
        **kwargs : any, optional
            Arbitrary key/value pairs to add to the arguments sent to the
            action.  These key/value pairs are not added to the collection
            of parameters set on the action object.  They are only used in
            this call.

        Returns
        -------
        CASResults object
            Collection of results from the action call

        '''
        return type(self).get_connection().retrieve(type(self).__name__.lower(),
                                                    **mergedefined(self.to_params(),
                                                    kwargs))

    retrieve = __call__


class CASActionRaw(ParamManager):
    '''
    Generic action object

    This object can be created to call a CAS action without knowing the
    reflection information. It will simply call the action with the given
    parameters without any processing at all.

    Parameters
    ----------
    name : string
        The name of the action.
    connection : CAS
        The CAS connection object.
    *args, **kwargs : additional parameters
        Parameters for the action.

    '''

    trait_names = None  # Block IPython's lookup of this
    _connection = None

    def __init__(self, name, connection, *args, **kwargs):
        super(CASActionRaw, self).__init__(*args, **kwargs)
        self._name = name
        type(self)._connection = weakref.ref(connection)

    @classmethod
    def get_connection(cls):
        '''
        Return the registered connection

        The connection is only held by a weak reference.  If the
        connection no longer exists, a SWATError is raised.

        Raises
        ------
        SWATError
            If the registered connection no longer exists

        '''
        try:
            if cls._connection is not None:
                conn = cls._connection()
        except AttributeError:
            pass
        if conn is None:
            raise SWATError('Connection object is no longer valid')
        return conn

    def __iter__(self):
        ''' Call the action and iterate over the results '''
        return iter(self.invoke())

    def invoke(self, **kwargs):
        '''
        Invoke the action

        Parameters
        ----------
        **kwargs : any, optional
            Arbitrary key/value pairs to add to the arguments sent to the
            action.  These key/value pairs are not added to the collection
            of parameters set on the action object.  They are only used in
            this call.

        Returns
        -------
        self
            Returns the CASAction object itself

        '''
        # Decode from JSON as needed
        if '_json' in kwargs:
            newargs = json.loads(kwargs['_json'])
            newargs.update(kwargs)
            del newargs['_json']
            kwargs = newargs

        conn = type(self).get_connection()
        conn._raw_invoke(self._name, **mergedefined(self.to_params(), kwargs))
        return conn

    def __call__(self, **kwargs):
        '''
        Call the action

        Parameters
        ----------
        **kwargs : any, optional
            Arbitrary key/value pairs to add to the arguments sent to the
            action.  These key/value pairs are not added to the collection
            of parameters set on the action object.  They are only used in
            this call.

        Returns
        -------
        CASResults object
            Collection of results from the action call

        '''
        return type(self).get_connection()._raw_retrieve(self._name,
                                                         **mergedefined(self.to_params(),
                                                                        kwargs))

    retrieve = __call__


def cvar(*varlist, **kwargs):
    '''
    Classification variables to be used as explanatory variables in an analysis

    Parameters
    ----------
    *varlist : one or more strings
        The classification variable names.
    param : string, optional
        specifies the parameterization method for the classification
        variable or variables. GLM is the default.
        Values: BTH, EFFECT, GLM, ORDINAL, ORTHBTH, ORTHEFFECT,
                ORTHORDINAL, ORTHPOLY, ORTHREF, POLYNOMIAL, REFERENCE
    order : string, optional
        specifies the sort order for the levels of classification
        variable.  This ordering determines which parameters in the model
        correspond to each level in the data.
        Values: FORMATTED, FREQ, FREQFORMATTED, FREQINTERNAL, INTERNAL
    maxlev : int32, optional
        specifies the maximum number of levels to allow. Default value
        of 0 means unlimited.
        Default: 0
    levelizeraw : boolean, optional
        specifies that for this variable levelization should be based on
        raw values.
        Default: False
    countmissing : boolean, optional
        specifies that for this variable missing is a valid level.
        Default: False
    ignoremissing : boolean, optional
        requests that even though some variables in the observation are
        missing, ignore that fact and honor the values in the
        observation.
        Default: False
    descending : boolean, optional
        reverses the sort order of the classification variable.  If both
        DESCENDING and ORDER options are specified, the action orders the
        categories according to the ORDER= option and then reverses that
        order.
        Default: False
    split : boolean, optional
        requests that columns of the design matrix that correspond to
        any effect that contains a split classification variable can be
        selected to enter or leave a model independently of the other
        design columns of that effect.
        Default: False
    ref : string, optional
        specifies the reference level that is used when you specify
        PARAM=REFERENCE. For an individual variable you can specify the
        level of the variable to use as the reference level. For the
        global option you can specify FIRST or LAST.

    Returns
    -------
    dict

    '''
    return dict(varlist=list(varlist), **kwargs)


def dvar(name, **kwargs):
    '''
    Specifies a response variable and its options

    Parameters
    ----------
    name : string, optional
        defines a response variable.
    order : string, optional
        specifies the sort order for the levels of the response
        variable.  This ordering determines which parameters in
        the model correspond to each level in the data.
        Values: FORMATTED, FREQ, FREQFORMATTED, FREQINTERNAL,
                INTERNAL
    event : string, optional
        specifies the event category for the binary response
        model. If you specify FIRST or LAST, these strings are
        interpreted to be the first or last ordered value
        respectively of the response. The default is event=FIRST.
    ref : string, optional
        specifies the reference level that is used for your
        response variable. You can specify the level of the
        variable. If you specify FIRST or LAST, these strings are
        interpreted to refer to the first or last ordered value
        respectively of the variable.
    descending : boolean, optional
        reverses the sort order of the response variable.  If
        both DESCENDING and ORDER options are specified, the
        action orders the categories according to the ORDER=
        option and then reverses that order.
        Default: False
    leveltype : string, optional
        specifies the type of the response variable. By default
        response variables are INTERVAL. The types NOMINAL,
        ORDINAL and BINARY specify that the response variable
        should be levelized. When INTERVAL is specified as
        LEVELTYPE, all other options specified for this response
        variable are ignored and response variable is not
        levelized.
        Default: INTERVAL
        Values: BINARY, INTERVAL, NOMINAL, ORDINAL

    Returns
    -------
    dict

    '''
    return dict(name=name, **kwargs)


class terms(dict):
    ''' A combination of terms in an effect list '''

    def __init__(self, *terms, **kwargs):
        self['eff'] = []
        for term in terms:
            for eff in term['eff']:
                self['eff'].append(eff)
        self['flags'] = kwargs.get('flags', 'individual')
        self['maxinteract'] = max(0, int(kwargs.get('maxinteract', 0)))

    def __or__(self, other):
        return terms(self, other, flags='bar')

    def __mul__(self, other):
        return terms(self, other, flags='cross')

    def __lt__(self, other):
        return terms(self, flags=self['flags'],
                     maxinteract=int(other) - 1)

    def __le__(self, other):
        return terms(self, flags=self['flags'],
                     maxinteract=int(other))


class term(dict):
    ''' A term of an effect list '''

    def __init__(self, name, nest=None):
        if nest:
            if isinstance(nest, term):
                nest = nest['eff']['varlist']
            elif not isinstance(nest, (list, tuple, set)):
                nest = [nest]
            self['eff'] = [dict(varlist=[name], nest=nest)]
            return
        self['eff'] = [dict(varlist=[name])]

    def __or__(self, other):
        return terms(self, other, flags='bar')

    def __mul__(self, other):
        return terms(self, other, flags='cross')


def collection(name, *varlist, **kwargs):
    '''
    Defines a set of variables that are treated as a single effect with multiple DOF

    Parameters
    ----------
    name : string
        Specifies the name of the effect
    *varlist : one or more strings
        Defines a set of variables that are treated as a single effect
        with multiple degrees of freedom. The columns in the design
        matrix that are contributed by a collection effect are the design
        columns of its constituent variables in the order in which they
        appear in the definition of the collection effect.
        Default: []
    details : boolean, optional
        requests a table that shows additional details related to this
        effect.
        Default: False

    Returns
    -------
    dict

    '''
    return dict(name=name, varlist=list(varlist), **kwargs)


def multimember(name, *varlist, **kwargs):
    '''
    A classification effect whose levels are determined by one or more class variables

    Parameters
    ----------
    name : string
        specifies the name of the effect.
    details : boolean, optional
        requests a table that shows additional details related to this
        effect.
        Default: False
    varlist : list of strings
        specifies classification variables for the multimember effect.
        The levels of a multimember effect consist of the union of
        formatted values of the variables that define this effect. Each
        such level contributes one column to the design matrix. For each
        observation, the value that corresponds to each level of the
        multimember effect in the design matrix is the number of times
        that this level occurs for the observation.
        Default: []
    noeffect : boolean, optional
        when set to True, specifies that for observations with all
        missing levels of the multimember variables, the values in the
        corresponding design matrix columns be set to zero.
        Default: False
    stdize : boolean, optional
        when set to True, specifies that for each observation, the
        entries in the design matrix that corresponds to the multimember
        effect be scaled to have a sum of one.
        Default: False
    weight : list of strings, optional
        specifies numeric variables used to weigh the contributions of
        each classification variable that define the multimember effect.
        The number of weight variables must match the number of
        classification variables that define the effect.
        Default: []

    Returns
    -------
    dict

    '''
    return dict(name=name, varlist=list(varlist), **kwargs)


mm = multimember


def polynomial(name, *varlist, **kwargs):
    '''
    Multivariate polynomial effect in the specified numeric variables

    Parameters
    ----------
    name : string
        specifies the name of the effect.
    *varlist : one or more strings
        specifies numeric variables for the multivariate polynomial
        effect.
        Default: []
    details : boolean, optional
        requests a table that shows additional details related to this
        effect.
        Default: False
    degree : int32, optional
        specifies the degree of the polynomial. The degree must be a
        positive integer.
        Default: 1
        Note: Value range is 0 <= n < 2147483647
    labelstyle : dict, optional
        specifies a list of options that control the terms in the
        polynomial are labeled.

        expand : boolean, optional
            when set to True, specifies that each variable with an
            exponent greater than 1 be written as products of that
            variable.
            Default: False
        exponent : string, optional
            specifies that each variable with an exponent greater than 1
            be written using exponential notation with the specified
            exponentiation string. By default, the symbol ^ is used as
            the exponentiation operator.
        includename : boolean, optional
            when set to True, specifies that the name of the effect
            followed by an underscore be used as a prefix for term
            labels.
            Default: False
        productsymbol : string, optional
            specifies that the supplied string be used as the product
            symbol.

    mdegree : int32, optional
        specifies the maximum degree of any variable in a term of the
        polynomial. This degree must be a positive integer. The default
        is the degree of the specified polynomial.
        Default: 1
        Note: Value range is 1 <= n < 2147483647
    noseparate : boolean, optional
        specifies that the polynomial be treated as a single effect with
        multiple degrees of freedom. The effect name that you specify is
        used as the constructed effect name, and the labels of the terms
        are used as labels of the corresponding parameters.
        Default: False
    standardize : dict, optional
        specifies a list of options that control how the variables
        forming the polynomial are standardized.

        method : string, optional
            specifies the method by which the variables that define the
            polynomial be standardized.
            Default: MRANGE
            Values: MOMENTS, MRANGE, WMOMENTS
        prefix : string, optional
            specifies the prefix that is appended to standardized
            variables when forming the term labels.
        options : string, optional
            controls whether the standardization is to center, scale, or
            both center and scale.
            Default: CENTERSCALE
            Values: CENTER, CENTERSCALE, NONE, SCALE

    Returns
    -------
    dict

    '''
    dict(name=name, varlist=list(varlist), **kwargs)


poly = polynomial


def spline(name, *varlist, **kwargs):
    '''
    Expands variables into spline bases whose form depends on options you specify

    Parameters
    ----------
    name : string
        specifies the name of the effect.
    varlist : list of strings
        specifies numeric variables for the spline effect.  By default,
        the spline basis that is generated for each variable is a cubic
        B-spline basis with three equally spaced knots positioned between
        the minimum and maximum values of that variable.
        Default: []
    details : boolean, optional
        requests a table that shows additional details related to this
        effect.
        Default: False
    basis : string, optional
        specifies the basis for the spline expansion.
        Default: BSPLINE
        Values: BSPLINE, TPF_DEFAULT, TPF_NOINT, TPF_NOINTANDNOPOWERS,
                TPF_NOPOWERS
    databoundary : boolean, optional
        specifies that the extremes of the data be used as boundary
        knots when building a B-spline basis.
        Default: False
    degree : int32, optional
        specifies the degree of the spline transformation.  The degree
        must be a nonnegative integer. The default degree is 3.
        Default: 3
        Note: Value range is 0 <= n < 2147483647
    knotmin : double, optional
        specifies that for each variable, the left-side boundary knots
        be equally spaced starting at the specified value and ending at
        the minimum of the variable.
        Default: 0.0
    knotmax : double, optional
        specifies that for each variable, the right-side boundary knots
        be equally spaced starting at the maximum of the variable and
        ending at the specified value.
        Default: 0.0
    knotmethod : dict, optional
        specifies how to construct the knots for spline effects.

        equal : int32, optional
            specifies the number of equally spaced knots be positioned
            between the extremes of the data. The default is 3. For a
            B-spline basis, any needed boundary knots continue to be
            equally spaced unless the DATABOUNDARY option has also been
            specified. KNOTMETHOD=EQUAL is the default if no knot-method
            is specified.
            Default: 3
        list : list, optional
            specifies the list of internal knots to be used in forming
            the spline basis columns. For a B-spline basis, the data
            extremes are used as boundary knots.
            Default: []
        listwithboundary : list, optional
            specifies the list of all knots that are used in forming the
            spline basis columns. When you use a truncated power function
            basis, this list is interpreted as the list of internal
            knots. When you use a B-spline basis of degree d, then the
            first d entries are used as left-side boundary knots and the
            last MAX(d,1) entries in the list are used as right-side
            boundary knots.
            Default: []
        multiscale : dict, optional
            specifies that multiple B-spline bases be generated,
            corresponding to sets with an increasing number of internal
            knots. For scale i, the spline basis corresponds to 2 to the
            power of i equally spaced internal knots. By default, the
            bases for scales 0 to 7 are generated. For each scale, a
            separate spline effect is generated.

            startscale : int32, optional
                specifies the start scale for a multiscale spline
                effect. The default is STARTSCALE=0.
                Default: 0
            endscale : int32, optional
                specifies the end scale for a multiscale spline effect.
                The default is ENDSCALESCALE=7.
                Default: 7

        percentiles : int32, optional
            specifies the number of equally spaced percentiles of the
            range of the variable or variables defining the spline effect
            at which knots are placed.
            Default: 0
        rangefractions : list, optional
            specifies a list of fractions. For each variable specified
            in the spline effect, internal knots are placed at each
            specified fraction of the ranges of those variables.
            Default: []

    naturalcubic : boolean, optional
        specifies a natural cubic spline basis for the spline expansion.
         Natural cubic splines, also known as restricted cubic splines,
        are cubic splines that are constructed to be linear beyond the
        extreme knots.
        Default: False
    separate : boolean, optional
        specifies that when multiple variables are specified, the spline
        basis for each variable is treated as a separate effect.
        Default: False
    split : boolean, optional
        specifies that each individual column in the design matrix that
        corresponds to the spline effect be treated as a separate effect
        that can enter or leave the model independently.
        Default: False

    Returns
    -------
    dict

    '''
    return dict(name=name, varlist=list(varlist), **kwargs)
