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
Options interface for SWAT

Options can be set and retrieved using set_option(...), get_option(...), and
reset_option(...).  The describe_option(...) function can be used to display
a description of one or more options.

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import contextlib
import os
import re
import six
import types
import weakref
from six.moves.urllib.parse import urlparse
from .compat import a2u, text_types, binary_types, int_types, items_types
from .args import iteroptions
from .xdict import xdict
from ..exceptions import SWATOptionError

# pylint: disable=C0330


# Container for options
_config = xdict()

# Subscribers to option changes
_subscribers = weakref.WeakKeyDictionary()


def _getenv(names, *args):
    '''
    Check for multiple environment variable values

    Two forms of the environment variable name will be checked,
    both with and without underscores.  This allows for aliases
    such as CAS_HOST and CASHOST.

    Parameters
    ----------
    names : string or list-of-strings
        Names of environment variables to look for
    *args : any, optional
        The default return value if no matching environment
        variables exist

    Returns
    -------
    string or default value

    '''
    if not isinstance(names, items_types):
        names = [names]
    for name in names:
        if name in os.environ:
            return os.environ[name]
        name = name.replace('_', '')
        if name in os.environ:
            return os.environ[name]
    if args:
        return args[0]
    raise KeyError(names[0])


def _setenv(names, value):
    '''
    Set environment variable

    The environment is first checked for an existing variable
    that is set.  If it finds one, it uses that name.
    If no variable is found, the first one in the `names`
    list is used.

    Just as with _getenv, the variable name is checked both
    with and without underscores to allow aliases.

    '''
    if not isinstance(names, items_types):
        names = [names]
    for name in names:
        if name in os.environ:
            os.environ[name] = value
        name = name.replace('_', '')
        if name in os.environ:
            os.environ[name] = value


def _delenv(names):
    ''' Delete given environment variables '''
    if not isinstance(names, items_types):
        names = [names]
    for name in names:
        os.environ.pop(name, None)
        os.environ.pop(name.replace('_', ''), None)


def subscribe(func):
    '''
    Add a subscriber function to option events

    Parameters
    ----------
    func : callable
        A callable object that takes two parameters: key and value.
        This function is called with the name and value of any option
        that is set.

    Returns
    -------
    None

    '''
    if isinstance(func, types.MethodType):
        obj = six.get_method_self(func)
        func = six.get_method_function(func)
        _subscribers[func] = (weakref.ref(func), weakref.ref(obj))
    else:
        _subscribers[func] = (weakref.ref(func), None)


def unsubscribe(func):
    '''
    Remove a subscriber from option events

    Parameters
    ----------
    func : callable
        The callable used to subscribe to option events

    Returns
    -------
    None

    '''
    _subscribers.pop(func, None)


@contextlib.contextmanager
def option_context(*args, **kwargs):
    '''
    Create a context for setting option temporarily

    Parameters
    ----------
    *args : string / any pairs
        Name / value pairs in consecutive arguments (not tuples)
    **kwargs : dict
        Key / value pairs of options

    '''
    # Save old state and set new option values
    oldstate = {}
    for key, value in iteroptions(*args, **kwargs):
        key = key.lower()
        oldstate[key] = get_option(key)
        set_option(key, value)

    # Yield control
    yield

    # Set old state back
    for key, value in six.iteritems(oldstate):
        set_option(key, value)


def _get_option_leaf_node(key):
    '''
    Find full option name of given key

    Parameters
    ----------
    key : string
        Either a partial key or full key name of an option

    Returns
    -------
    string
        The full key name of the option

    Raises
    ------
    SWATOptionError
        If more than one option matches

    '''
    flatkeys = list(_config.flatkeys())
    key = key.lower()
    if key in flatkeys:
        return key
    keys = [k for k in flatkeys if k.endswith('.' + key)]
    if len(keys) > 1:
        raise SWATOptionError('There is more than one option with the name %s.' % key)
    if not keys:
        raise SWATOptionError('%s is not a valid option name.' % key)
    return keys[0]


def set_option(*args, **kwargs):
    '''
    Set the value of an option

    Parameters
    ----------
    *args : string / any pairs
        The name and value of an option in consecutive arguments (not tuples)
    **kwargs : dict
        Arbitrary keyword / value pairs

    Returns
    -------
    None

    '''
    for key, value in iteroptions(*args, **kwargs):
        key = _get_option_leaf_node(key)
        opt = _config[key]
        if not isinstance(opt, SWATOption):
            raise SWATOptionError('%s is not a valid option name' % key)
        opt.set(value)


set_options = set_option


def get_option(key):
    '''
    Get the value of an option

    Parameters
    ----------
    key : string
        The name of the option

    Returns
    -------
    any
        The value of the option

    '''
    key = _get_option_leaf_node(key)
    opt = _config[key]
    if not isinstance(opt, SWATOption):
        raise SWATOptionError('%s is not a valid option name' % key)
    return opt.get()


def get_suboptions(key):
    '''
    Get the dictionary of options at the level `key`

    Parameters
    ----------
    key : string
        The name of the option collection

    Returns
    -------
    dict
        The dictionary of options at level `key`

    '''
    if key not in _config:
        raise SWATOptionError('%s is not a valid option name' % key)
    opt = _config[key]
    if isinstance(opt, SWATOption):
        raise SWATOptionError('%s does not have sub-options' % key)
    return opt


def get_default(key):
    '''
    Get the default value of an option

    Parameters
    ----------
    key : string
        The name of the option

    Returns
    -------
    any
        The default value of the option

    '''
    key = _get_option_leaf_node(key)
    opt = _config[key]
    if not isinstance(opt, SWATOption):
        raise SWATOptionError('%s is not a valid option name' % key)
    return opt.get_default()


get_default_val = get_default


def describe_option(*keys, **kwargs):
    '''
    Print the description of one or more options

    Parameters
    ----------
    *keys : one or more strings
        Names of the options

    Returns
    -------
    None

    '''
    _print_desc = kwargs.get('_print_desc', True)

    out = []

    if not keys:
        keys = sorted(_config.flatkeys())
    else:
        newkeys = []
        for k in keys:
            try:
                newkeys.append(_get_option_leaf_node(k))
            except SWATOptionError:
                newkeys.append(k)

    for key in keys:

        if key not in _config:
            raise SWATOptionError('%s is not a valid option name' % key)

        opt = _config[key]
        if isinstance(opt, xdict):
            desc = describe_option(*['%s.%s' % (key, x)
                                   for x in opt.flatkeys()], _print_desc=_print_desc)
            if desc is not None:
                out.append(desc)
            continue

        if _print_desc:
            print(opt.__doc__)
            print('')
        else:
            out.append(opt.__doc__)

    if not _print_desc:
        return '\n'.join(out)


def reset_option(*keys):
    '''
    Reset one or more options back to their default value

    Parameters
    ----------
    *keys : one or more strings
        Names of options to reset

    Returns
    -------
    None

    '''
    if not keys:
        keys = sorted(_config.flatkeys())
    else:
        keys = [_get_option_leaf_node(k) for k in keys]

    for key in keys:

        if key not in _config:
            raise SWATOptionError('%s is not a valid option name' % key)

        opt = _config[key]
        if not isinstance(opt, SWATOption):
            raise SWATOptionError('%s is not a valid option name' % key)

        # Reset swat options
        set_option(key, get_default(key))


def check_int(value, minimum=None, maximum=None, exclusive_minimum=False,
              exclusive_maximum=False, multiple_of=None):
    '''
    Validate an integer value

    Parameters
    ----------
    value : int or float
        Value to validate
    minimum : int, optional
        The minimum value allowed
    maximum : int, optional
        The maximum value allowed
    exclusive_minimum : boolean, optional
        Should the minimum value be excluded as an endpoint?
    exclusive_maximum : boolean, optional
        Should the maximum value be excluded as an endpoint?
    multiple_of : int, optional
        If specified, the value must be a multple of it in order for
        the value to be considered valid.

    Returns
    -------
    int
        The validated integer value

    '''
    try:
        out = int(value)
    except Exception:
        raise SWATOptionError('Could not convert %s to an integer' % value)

    if minimum is not None:
        if out < minimum:
            raise SWATOptionError('%s is smaller than the minimum value of %s' %
                                  (out, minimum))
        if exclusive_minimum and out == minimum:
            raise SWATOptionError('%s is equal to the exclusive nimum value of %s' %
                                  (out, minimum))

    if maximum is not None:
        if out > maximum:
            raise SWATOptionError('%s is larger than the maximum value of %s' %
                                  (out, maximum))
        if exclusive_maximum and out == maximum:
            raise SWATOptionError('%s is equal to the exclusive maximum value of %s' %
                                  (out, maximum))

    if multiple_of is not None and (out % int(multiple_of)) != 0:
        raise SWATOptionError('%s is not a multiple of %s' % (out, multiple_of))

    return out


def check_float(value, minimum=None, maximum=None, exclusive_minimum=False,
                exclusive_maximum=False, multiple_of=None):
    '''
    Validate a floating point value

    Parameters
    ----------
    value : int or float
        Value to validate
    minimum : int or float, optional
        The minimum value allowed
    maximum : int or float, optional
        The maximum value allowed
    exclusive_minimum : boolean, optional
        Should the minimum value be excluded as an endpoint?
    exclusive_maximum : boolean, optional
        Should the maximum value be excluded as an endpoint?
    multiple_of : int or float, optional
        If specified, the value must be a multple of it in order for
        the value to be considered valid.

    Returns
    -------
    float
        The validated floating point value

    '''
    try:
        out = float(value)
    except Exception:
        raise SWATOptionError('Could not convert %s to a float' % value)

    if minimum is not None:
        if out < minimum:
            raise SWATOptionError('%s is smaller than the minimum value of %s' %
                                  (out, minimum))
        if exclusive_minimum and out == minimum:
            raise SWATOptionError('%s is equal to the exclusive nimum value of %s' %
                                  (out, minimum))

    if maximum is not None:
        if out > maximum:
            raise SWATOptionError('%s is larger than the maximum value of %s' %
                                  (out, maximum))
        if exclusive_maximum and out == maximum:
            raise SWATOptionError('%s is equal to the exclusive maximum value of %s' %
                                  (out, maximum))

    if multiple_of is not None and (out % int(multiple_of)) != 0:
        raise SWATOptionError('%s is not a multiple of %s' % (out, multiple_of))

    return out


def check_boolean(value):
    '''
    Validate a boolean value

    Parameters
    ----------
    value : int or boolean
        The value to validate.  If specified as an integer, it must
        be either 0 for False or 1 for True.

    Returns
    -------
    boolean
        The validated boolean

    '''
    if value is False or value is True:
        return value

    if isinstance(value, int_types):
        if value == 1:
            return True
        if value == 0:
            return False

    if isinstance(value, (text_types, binary_types)):
        if value.lower() in ['y', 'yes', 'on', 't', 'true', 'enable', 'enabled', '1']:
            return True
        if value.lower() in ['n', 'no', 'off', 'f', 'false', 'disable', 'disabled', '0']:
            return False

    raise SWATOptionError('%s is not a recognized boolean value')


def check_string(value, pattern=None, max_length=None, min_length=None,
                 valid_values=None):
    '''
    Validate a string value

    Parameters
    ----------
    value : string
        The value to validate
    pattern : regular expression string, optional
        A regular expression used to validate string values
    max_length : int, optional
        The maximum length of the string
    min_length : int, optional
        The minimum length of the string
    valid_values : list of strings, optional
        List of the only possible values

    Returns
    -------
    string
        The validated string value

    '''
    try:
        out = a2u(value)
    except Exception:
        raise SWATOptionError('Could not convert string value to unicode')

    if max_length is not None and len(out) > max_length:
        raise SWATOptionError('%s is longer than the maximum length of %s' %
                              (out, max_length))

    if min_length is not None and len(out) < min_length:
        raise SWATOptionError('%s is shorter than the minimum length of %s' %
                              (out, min_length))

    if pattern is not None and not re.search(pattern, out):
        raise SWATOptionError('%s does not match pattern %s' % (out, pattern))

    if valid_values is not None and out not in valid_values:
        raise SWATOptionError('%s is not one of the possible values: %s' %
                              (out, ', '.join(valid_values)))

    return out


def check_url(value, pattern=None, max_length=None, min_length=None, valid_values=None):
    '''
    Validate a URL value

    Parameters
    ----------
    value : any
        The value to validate.  This value will be cast to a string
        and converted to unicode.
    pattern : regular expression string, optional
        A regular expression used to validate string values
    max_length : int, optional
        The maximum length of the string
    min_length : int, optional
        The minimum length of the string
    valid_values : list of strings, optional
        List of the only possible values

    Returns
    -------
    string
        The validated URL value

    '''
    out = check_string(value, pattern=pattern, max_length=max_length,
                       min_length=min_length, valid_values=valid_values)
    try:
        urlparse(out)
    except Exception:
        raise SWATOptionError('%s is not a valid URL' % value)
    return out


class SWATOption(object):
    '''
    SWAT configuration option

    Parameters
    ----------
    name : string
        The name of the option
    typedesc : string
        Description of the option data type (e.g., int, float, string)
    validator : callable
        A callable object that validates the option value and returns
        the validated value.
    default : any
        The default value of the option
    doc : string
        The documentation string for the option

    environ : string or list-of-strings, optional
        If specified, the value should be specified in an environment
        variable of that name.

    Returns
    -------
    SWATOption object

    '''

    def __init__(self, name, typedesc, validator, default, doc, environ=None):
        self._name = name
        self._typedesc = typedesc
        self._validator = validator
        if environ is not None:
            self._default = validator(_getenv(environ, default))
        else:
            self._default = validator(default)
        self._environ = environ
        self._value = self._default
        self._doc = doc

    @property
    def __doc__(self):
        ''' Documentation string '''
        separator = ' '
        if isinstance(self._value, (text_types, binary_types)) and len(self._value) > 40:
            separator = '\n    '
        return '''%s : %s\n    %s\n    [default: %s]%s[currently: %s]\n''' % \
            (self._name, self._typedesc, self._doc.rstrip().replace('\n', '\n    '),
             self._default, separator, self._value)

    def set(self, value):
        '''
        Set the value of the option

        Parameters
        ----------
        value : any
           The value to set

        Returns
        -------
        None

        '''
        value = self._validator(value)
        _config[self._name]._value = value

        if self._environ is not None:
            if value is None:
                _delenv(self._environ)
            else:
                _setenv(self._environ, str(value))

        for func, obj in list(_subscribers.values()):
            if func is not None:
                if obj is None:
                    func = func()
                    if func is not None:
                        func(self._name, value)
                else:
                    func, obj = func(), obj()
                    if func is not None and obj is not None:
                        func(obj, self._name, value)

    def get(self):
        '''
        Get the value of the option

        Returns
        -------
        any
            The value of the option

        '''
        if self._environ is not None:
            try:
                _config[self._name]._value = self._validator(_getenv(self._environ))
            except KeyError:
                pass
        return _config[self._name]._value

    def get_default(self):
        '''
        Get the default value of the option

        Returns
        -------
        any
            The default value of the option

        '''
        return _config[self._name]._default


def register_option(key, typedesc, validator, default, doc, environ=None):
    '''
    Register a new option

    Parameters
    ----------
    key : string
        The name of the option
    typedesc : string
        Description of option data type (e.g., int, float, string)
    validator : callable
        A callable object that validates the value and returns
        a validated value.
    default : any
        The default value of the option
    doc : string
        The documentation string for the option
    environ : string or list-of-strings, optional
        If specified, the value should be specified in an environment
        variable of that name.

    Returns
    -------
    None

    '''
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        _config[key] = SWATOption(key, typedesc, validator, default, doc, environ=environ)


class AttrOption(object):
    '''
    Attribute-style access of SWAT options

    '''

    def __init__(self, name):
        object.__setattr__(self, '_name', name)

    def __dir__(self):
        if self._name in _config:
            return _config[self._name].flatkeys()
        return _config.flatkeys()

    @property
    def __doc__(self):
        if self._name:
            return describe_option(self._name, _print_desc=False)
        return describe_option(_print_desc=False)

    def __getattr__(self, name):
        name = name.lower()
        if self._name:
            fullname = self._name + '.' + name
        else:
            fullname = name
        if fullname not in _config:
            fullname = _get_option_leaf_node(fullname)
        out = _config[fullname]
        if not isinstance(out, SWATOption):
            return type(self)(fullname)
        return out.get()

    def __setattr__(self, name, value):
        name = name.lower()
        if self._name:
            fullname = self._name + '.' + name
        else:
            fullname = name
        if fullname not in _config:
            fullname = _get_option_leaf_node(fullname)
        out = _config[fullname]
        if not isinstance(out, SWATOption):
            return type(self)(fullname)
        _config[fullname].set(value)
        return

    def __call__(self, *args, **kwargs):
        ''' Shortcut for option context '''
        return option_context(*args, **kwargs)


# Object for setting and getting options using attribute syntax
options = AttrOption(None)
