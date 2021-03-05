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
Class for creating CAS sessions

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import contextlib
import copy
import inspect
import itertools
import json
import os
import random
import re
import six
import warnings
import weakref
from six.moves.urllib.parse import urlparse
from . import rest
from .. import clib
from .. import config as cf
from ..exceptions import SWATError, SWATCASActionError, SWATCASActionRetry
from ..logging import logger
from ..utils.config import subscribe, get_option
from ..clib import errorcheck
from ..utils.compat import (a2u, a2n, int32, int64, float64, text_types,
                            binary_types, items_types, int_types, dict_types)
from ..utils import getsoptions
from ..utils.args import iteroptions
from ..formatter import SASFormatter
from .actions import CASAction, CASActionSet
from .table import CASTable
from .transformers import py2cas
from .request import CASRequest
from .response import CASResponse
from .results import CASResults
from .utils.params import ParamManager, ActionParamManager
from .utils.misc import super_dir, any_file_exists

# pylint: disable=W0212

RETRY_ACTION_CODE = 0x280034
SESSION_ABORTED_CODE = 0x2D51AC


def _option_handler(key, value):
    ''' Handle option changes '''
    sessions = list(CAS.sessions.values())

    key = key.lower()
    if key == 'cas.print_messages':
        key = 'print_messages'
    elif key == 'cas.trace_actions':
        key = 'trace_actions'
    elif key == 'cas.trace_ui_actions':
        key = 'trace_ui_actions'
    else:
        return

    for ses in sessions:
        ses._set_option(**{key: value})


subscribe(_option_handler)


def _lower_actionset_keys(asinfo):
    '''
    Lowercase action set information keys

    Parameters
    ----------
    asinfo : dict
       Action set reflection information

    Returns
    -------
    dict
       Same dictionary with lower-cased action / param keys

    '''
    for act in asinfo.get('actions', []):
        act['name'] = act['name'].lower()
        for param in act.get('params', []):
            param['name'] = param['name'].lower()
            if 'parmList' in param:
                param['parmList'] = _lower_parmlist_keys(param['parmList'])
            if 'exemplar' in param:
                param['exemplar'] = _lower_parmlist_keys(param['exemplar'])
    return asinfo


def _lower_parmlist_keys(parmlist):
    '''
    Lowercase parmList/exemplar keys

    Parameters
    ----------
    parmlist : list
       parmList or exemplar reflection information

    Returns
    -------
    list
       Same list with lower-cased name keys

    '''
    for parm in parmlist:
        parm['name'] = parm['name'].lower()
        if 'parmList' in parm:
            parm['parmList'] = _lower_parmlist_keys(parm['parmList'])
        if 'exemplar' in parm:
            parm['exemplar'] = _lower_parmlist_keys(parm['exemplar'])
    return parmlist


@six.python_2_unicode_compatible
class CAS(object):
    '''
    Create a connection to a CAS server.

    Parameters
    ----------
    hostname : string or list-of-strings, optional
        Host or URL to connect to.  This parameter can also be specified
        by a ``CAS_URL`` or ``CAS_HOST`` environment variable.
    port : int or long, optional
        Port number.  If not specified, the value will come from the
        ``cas.port`` option or ``CAS_PORT`` environment variable.
        If a URL is specified in the first parameter, that port number
        will be used.
    username : string, optional
        Name of user on CAS host.  This parameter can also be specified
        in a ``CAS_USER`` environment variable.
    password : string, optional
        Password of user on CAS host or OAuth token.  If an OAuth token
        is specified, the `username` parameter should be None.
        This parameter can also be specified in a ``CAS_PASSWORD``
        or ``CAS_TOKEN`` environment variable.
    session : string, optional
        ID of existing session to reconnect to.
    locale : string, optional
        Name of locale used for the session.
    name : string, optional
        User-definable name for the session.
    nworkers : int or long, optional
        Number of worker nodes to use.
    authinfo : string or list-of-strings, optional
        The filename or list of filenames of authinfo/netrc files used
        for authentication.
    protocol : string, optional
        The protocol to use for communicating with the server.
        This protocol must match the protocol spoken by the specified
        server port.  If the first parameter is a URL, that protocol will
        be used.
    path : string, optional
        Base path of URL when using the REST protocol.
    ssl_ca_list : string, optional
        The path to the SSL certificates for the CAS server.
    **kwargs : any, optional
        Arbitrary keyword arguments used for internal purposes only.

    Raises
    ------
    IOError
        When a connection can not be established.

    Returns
    -------
    :class:`CAS` object

    Examples
    --------
    To create a connection to a CAS host, you simply supply a hostname
    (or list of hostnames), a port number, and user credentials.  Here is
    an example specifying a single hostname, and username and password as
    strings.

    >>> conn = swat.CAS('mycashost.com', 5570, 'username', 'password')

    If you use an authinfo file and it is in your home directory, you don't
    have to specify any username or password.  You can override the authinfo
    file location with the authinfo= parameter.  This form also works for
    Kerberos authentication.

    >>> conn = swat.CAS('mycashost.com', 5570)

    If you specify multiple hostnames, it will connect to the first available
    server in the list.

    >>> conn = swat.CAS(['mycashost1.com', 'mycashost2.com', 'mycashost3.com'],
                        5570, 'username', 'password')

    URLs can also be used for both binary and REST protocols.  Notice that
    you need to specify the username= and password= keywords since the
    port number is skipped.

    >>> conn = swat.CAS('cas://mycashost1.com:5570',
    ...                 username='username', password='password')
    >>> conn = swat.CAS('http://mycashost1.com:80',
    ...                 username='username', password='password')

    To connect to an existing CAS session, you specify the session identifier.

    >>> conn = swat.CAS('mycashost.com', 5570,
    ...                 session='ABCDEF12-ABCD-EFG1-2345-ABCDEF123456')

    If you wish to change the locale used on the server, you can use the
    locale= option.

    >>> conn = swat.CAS('mycashost.com', 5570, locale='es_US')

    To limit the number of worker nodes in a grid, you use the nworkers=
    parameter.

    >>> conn = swat.CAS('mycashost.com', 5570, nworkers=4)

    '''
    trait_names = None  # Block IPython's query for this
    sessions = weakref.WeakValueDictionary()
    _sessioncount = 1

    @classmethod
    def _expand_url(cls, url):
        ''' Expand [...] groups in URL to all linear combinations '''
        if not isinstance(url, items_types):
            url = [url]

        out = []

        for item in url:
            parts = [x for x in re.split(r'(?:\[|\])', item) if x]

            for i, part in enumerate(parts):
                if ',' in part:
                    parts[i] = re.split(r'\s*,\s*', part)
#               elif re.match(r'^\d+\-\d+$', part):
#                   start, end = part.split('-')
#                   width = len(start)
#                   start = int(start)
#                   end = int(end)
#                   parts[i] = [('%%0%sd' % width) % x for x in range(start, end+1)]
                else:
                    parts[i] = [part]

            out += list(''.join(x) for x in itertools.product(*parts))

        return out

    @classmethod
    def _get_connection_info(cls, hostname, port, username, password, protocol, path):
        ''' Distill connection information from parameters, config, and environment '''

        # Get defaults from config, if needed
        username = username or cf.get_option('cas.username')
        password = password or cf.get_option('cas.token')
        protocol = protocol or cf.get_option('cas.protocol')
        hostname = hostname or cf.get_option('cas.hostname')
        port = port or cf.get_option('cas.port')

        logger.debug('Connection info: hostname=%s port=%s protocol=%s '
                     'username=%s password=%s path=%s',
                     hostname, port, protocol, username, password, path)

        # Always make hostname a list
        if not isinstance(hostname, items_types):
            hostname = re.split(r'\s+', re.sub(r'\s*,\s*', r',', hostname.strip()))
        else:
            hostname = [re.sub(r'\s*,\s*', r',', x.strip()) for x in hostname]

        # Check hostname for other components
        new_hostname = []
        for name in hostname:
            if not re.match(r'^\w+://', hostname[0]):
                new_hostname.append('%s://%s' % (protocol, name))
            else:
                new_hostname.append(name)

        hostname = cls._expand_url(new_hostname)
        urlp = urlparse(hostname[0])
        protocol = urlp.scheme or protocol
        hostname = [urlparse(x).hostname for x in hostname]
        port = urlp.port or port
        username = urlp.username or username
        password = urlp.password or password
        path = urlp.path or path

        # Set port based on protocol, if port number is missing
        if not port:
            if protocol == 'http':
                port = 80
            elif protocol == 'https':
                port = 443
            elif protocol == 'cas':
                port = 5570
            else:
                raise SWATError('Port number was not specified')

        # Auto-detect protocol if still missing
        if protocol == 'auto':
            protocol = cls._detect_protocol(hostname, port, protocol=protocol)

        if protocol not in ['http', 'https', 'cas']:
            raise SWATError('Unrecognized protocol: %s' % protocol)

        # For http(s), construct URLs
        if protocol.startswith('http'):
            urls = []
            for name in hostname:
                url = '%s://%s:%s' % (protocol, name, port)
                if path:
                    url = '%s/%s' % (url, re.sub(r'^/+', r'', path))
                urls.append(url)
            hostname = ' '.join(urls)
            logger.debug('Distilled connection parameters: '
                         "url='%s' username=%s", urls, username)

        else:
            hostname = ' '.join(hostname)
            logger.debug('Distilled connection parameters: '
                         "hostname='%s' port=%s, username=%s, protocol=%s",
                         hostname, port, username, protocol)

        return a2n(hostname), int(port), a2n(username), a2n(password), a2n(protocol)

    def __init__(self, hostname=None, port=None, username=None, password=None,
                 session=None, locale=None, nworkers=None, name=None,
                 authinfo=None, protocol=None, path=None, ssl_ca_list=None,
                 **kwargs):

        # Filter session options allowed as parameters
        _kwargs = {}
        sess_opts = {}
        for k, v in kwargs.items():
            if k.lower() in ['caslib', 'metrics', 'timeout', 'timezone']:
                sess_opts[k] = v
            else:
                _kwargs[k] = v
        kwargs = _kwargs

        # Check for unknown connection parameters
        unknown_keys = [k for k in kwargs if k not in ['prototype']]
        if unknown_keys:
            warnings.warn('Unrecognized keys in connection parameters: %s' %
                          ', '.join(unknown_keys))

        # If a prototype exists, use it for the connection config
        prototype = kwargs.get('prototype')
        if prototype is not None:
            soptions = a2n(prototype._soptions)
            protocol = a2n(prototype._protocol)
        else:
            # Distill connection information from parameters, config, and environment
            hostname, port, username, password, protocol = \
                self._get_connection_info(hostname, port, username,
                                          password, protocol, path)
            soptions = a2n(getsoptions(session=session, locale=locale,
                                       nworkers=nworkers, protocol=protocol))

        # Check for SSL certificate
        if ssl_ca_list is None:
            ssl_ca_list = cf.get_option('cas.ssl_ca_list')
        if ssl_ca_list:
            logger.debug('Using certificate file: %s', ssl_ca_list)
            os.environ['CAS_CLIENT_SSL_CA_LIST'] = ssl_ca_list

        # Check for explicitly specified authinfo files
        if authinfo is not None:
            if not any_file_exists(authinfo):
                if not isinstance(authinfo, items_types):
                    authinfo = [authinfo]
                raise OSError('None of the specified authinfo files from'
                              'list exist: %s' % ', '.join(authinfo))

        # Create error handler
        try:
            if protocol in ['http', 'https']:
                self._sw_error = rest.REST_CASError(soptions)
            else:
                self._sw_error = clib.SW_CASError(soptions)
        except SystemError:
            raise SWATError('Could not create CAS error handler object. '
                            'Check your SAS TK path setting.')

        # Make the connection
        try:
            # Make a copy of the prototype connection
            if prototype is not None:
                self._sw_connection = errorcheck(prototype._sw_connection.copy(),
                                                 prototype._sw_connection)

            # Create a new connection
            else:
                # Set up authinfo paths
                if authinfo is not None and password is None:
                    password = ''
                    if not isinstance(authinfo, items_types):
                        authinfo = [authinfo]
                    for item in authinfo:
                        password += '{%s}' % item
                    password = 'authinfo={%s}' % password

                # Set up connection parameters
                params = (hostname, port, username, password, soptions, self._sw_error)
                if protocol in ['http', 'https']:
                    self._sw_connection = rest.REST_CASConnection(*params)
                else:
                    self._sw_connection = clib.SW_CASConnection(*params)

                # If we don't have a connection, bail out.
                if self._sw_connection is None:
                    raise SystemError

        except SystemError:
            raise SWATError(self._sw_error.getLastErrorMessage())

        # Set up index origin for error messages
        errorcheck(self._sw_connection.setZeroIndexedParameters(), self._sw_connection)

        # Get instance structure values from connection layer
        self._hostname = errorcheck(
            a2u(self._sw_connection.getHostname(), 'utf-8'), self._sw_connection)
        self._port = errorcheck(self._sw_connection.getPort(), self._sw_connection)
        self._username = errorcheck(
            a2u(self._sw_connection.getUsername(), 'utf-8'), self._sw_connection)
        self._session = errorcheck(
            a2u(self._sw_connection.getSession(), 'utf-8'), self._sw_connection)
        self._soptions = errorcheck(
            a2u(self._sw_connection.getSOptions(), 'utf-8'), self._sw_connection)
        self._protocol = protocol
        if name:
            self._name = a2u(name)
        else:
            self._name = 'py-session-%d' % type(self)._sessioncount
            type(self)._sessioncount = type(self)._sessioncount + 1

        # Caches for action classes and reflection information
        self._action_classes = {}
        self._action_info = {}
        self._actionset_classes = {}
        self._actionset_info = {}

        # Dictionary of result hook functions
        self._results_hooks = {}

        # Get server attributes
        (self.server_type,
         self.server_version,
         self.server_features) = self._get_server_features()

        # Preload __dir__ information.  It will be extended later with action names
        self._dir = set([x for x in super_dir(CAS, self)])

        # Pre-populate action set attributes
        for asname, value in self._raw_retrieve('builtins.help',
                                                showhidden=True,
                                                _messagelevel='error',
                                                _apptag='UI').items():
            self._actionset_classes[asname.lower()] = None
            if value is not None:
                for actname in value['name']:
                    self._action_classes[asname.lower() + '.' + actname.lower()] = None
                    self._action_classes[actname.lower()] = None

        # Populate CASTable documentation and method signatures
        CASTable._bootstrap(self)
        init = CASTable.__init__
        if hasattr(init, '__func__'):
            init = init.__func__
        self.CASTable.__func__.__doc__ = init.__doc__

        # Add loadactionset handler to populate actionset and action classes
        def handle_loadactionset(conn, results):
            ''' Force the creation of actionset and action classes '''
            if 'actionset' in results:
                conn.__getattr__(results['actionset'], atype='actionset')

        self.add_results_hook('builtins.loadactionset', handle_loadactionset)

        # Set the session name
        self._raw_retrieve('session.sessionname', name=self._name,
                           _messagelevel='error', _apptag='UI')

        # Set session options
        if sess_opts:
            self._raw_retrieve('sessionprop.setsessopt', _messagelevel='error',
                               _apptag='UI', **sess_opts)

        # Set options
        self._set_option(print_messages=cf.get_option('cas.print_messages'))
        self._set_option(trace_actions=cf.get_option('cas.trace_actions'))
        self._set_option(trace_ui_actions=cf.get_option('cas.trace_ui_actions'))

        # Add the connection to a global dictionary for use by IPython notebook
        type(self).sessions[self._session] = self
        type(self).sessions[self._name] = self

        def _id_generator():
            ''' Generate unique IDs within a connection '''
            num = 0
            while True:
                yield num
                num = num + 1
        self._id_generator = _id_generator()

    def _gen_id(self):
        ''' Generate an ID unique to the session '''
        import numpy
        return numpy.base_repr(next(self._id_generator), 36)

    def _get_server_features(self):
        '''
        Determine which features are available in the server

        Returns
        -------
        set-of-strings

        '''
        out = set()

        info = self._raw_retrieve('builtins.serverstatus', _messagelevel='error',
                                  _apptag='UI')
        version = tuple([int(x) for x in info['About']['Version'].split('.')][:2])
        stype = info['About']['System']['OS Name'].lower()

        # Check for reflection levels feature
        res = self._raw_retrieve('builtins.reflect', _messagelevel='error',
                                 _apptag='UI', action='builtins.reflect',
                                 showlabels=False)
        if [x for x in res[0]['actions'][0]['params'] if x['name'] == 'levels']:
            out.add('reflection-levels')

        return stype, version, out

    @classmethod
    def _detect_protocol(cls, hostname, port, protocol=None, timeout=3):
        '''
        Detect the protocol type for the given host and port

        Parameters
        ----------
        hostname : string or list
            The CAS host to connect to.
        port : int
            The CAS port to connect to.
        protocol : string, optional
            The protocol override value.
        timeout : int, optional
            Timeout (in seconds) for checking a protocol

        Returns
        -------
        string
            'cas', 'http', or 'https'

        '''
        if protocol is None:
            protocol = cf.get_option('cas.protocol')

        if isinstance(hostname, six.string_types):
            hostname = re.split(r'\s+', hostname.strip())

        if protocol != 'auto':
            logger.debug('Protocol specified explicitly: %s' % protocol)

        # Try to detect the proper protocol

        if protocol == 'auto':
            try:
                import queue
            except ImportError:
                import Queue as queue
            import socket
            import ssl
            import threading

            for host in hostname:

                out = queue.Queue()

                logger.debug('Attempting protocol auto-detect on %s:%s', host, port)

                def check_cas_protocol():
                    ''' Test port for CAS (binary) support '''
                    proto = None
                    try:
                        cas_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        cas_socket.settimeout(timeout)
                        cas_socket.connect((host, port))
                        cas_socket.sendall(bytearray([0, 0x53, 0x41, 0x43,
                                                      0x10, 0, 0, 0, 0, 0, 0, 0,
                                                      0x10, 0, 0, 0,
                                                      0, 0, 0, 0,
                                                      2, 0, 0, 0,
                                                      5, 0, 0, 0]))

                        if cas_socket.recv(4) == b'\x00SAC':
                            proto = 'cas'

                    except Exception:
                        pass

                    finally:
                        cas_socket.close()
                        out.put(proto)

                def check_https_protocol():
                    ''' Test port for HTTPS support '''
                    proto = None
                    try:
                        ssl_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        ssl_socket.settimeout(timeout)
                        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                        ssl_conn = ssl_context.wrap_socket(ssl_socket,
                                                           server_hostname=host)
                        ssl_conn.connect((host, port))
                        ssl_conn.write(('GET /cas HTTP/1.1\r\n'
                                        + ('Host: %s\r\n' % host)
                                        + 'Connection: close\r\n'
                                        + 'User-Agent: Python-SWAT\r\n'
                                        + 'Cache-Control: no-cache\r\n\r\n')
                                       .encode('utf8'))

                    except ssl.SSLError as exc:
                        if 'certificate verify failed' in str(exc):
                            proto = 'https'

                    except Exception:
                        pass

                    finally:
                        ssl_socket.close()
                        out.put(proto)

                def check_http_protocol():
                    ''' Test port for HTTP support '''
                    proto = None
                    try:
                        http_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        http_socket.settimeout(timeout)
                        http_socket.connect((host, port))

                        http_socket.send(('GET /cas HTTP/1.1\r\n'
                                          + ('Host: %s\r\n' % host)
                                          + 'Connection: close\r\n'
                                          + 'User-Agent: Python-SWAT\r\n'
                                          + 'Cache-Control: no-cache\r\n\r\n')
                                         .encode('utf8'))

                        txt = http_socket.recv(16).decode('utf-8').lower()
                        if txt.startswith('http') and txt.split()[1] != '400':
                            proto = 'http'

                    except Exception:
                        pass

                    finally:
                        http_socket.close()
                        out.put(proto)

                checkers = [check_cas_protocol, check_https_protocol, check_http_protocol]
                for item in checkers:
                    threading.Thread(target=item).start()

                try:
                    for i in range(len(checkers)):
                        proto = out.get(block=True, timeout=timeout)
                        if proto is not None:
                            protocol = proto
                            break
                except queue.Empty:
                    pass

                if protocol != 'auto':
                    logger.debug('Protocol detected: %s', protocol)
                    break

        # No protocol detected
        if protocol == 'auto':
            if port == 80:
                logger.debug('Protocol defaulted by port 80: http')
                protocol = 'http'
            elif port == 443:
                logger.debug('Protocol defaulted by port 443: http')
                protocol = 'https'
            else:
                logger.debug('No protocol detected: defaulting to \'cas\'')
                protocol = 'cas'

        return protocol

    def __enter__(self):
        ''' Enter a context '''
        return self

    def __exit__(self, type, value, traceback):
        ''' Exit the context '''
        self.retrieve('session.endsession', _apptag='UI', _messagelevel='error')
        self.close()

    @contextlib.contextmanager
    def session_context(self, *args, **kwargs):
        '''
        Create a context of session options

        This method is intended to be used in conjunction with Python's
        ``with`` statement.  It allows you to set CAS session options within
        that context, then revert them back to their previous state.

        For all possible session options, see the `sessionprop.getsessopt`
        CAS action documentation.

        Parameters
        ----------
        *args : string / any pairs
            Name / value pairs of options in consecutive arguments, name / value
            pairs in tuples, or dictionaries.
        **kwargs : string / any pairs
            Key / value pairs of session options

        Examples
        --------
        >>> conn = swat.CAS()
        >>> print(conn.getsessopt('locale').locale)
        en_US
        >>> with conn.session_context(locale='fr'):
        ...     print(conn.getsessopt('locale').locale)
        fr
        >>>  print(conn.getsessopt('locale').locale)
        en_US

        '''
        state = {}
        newkwargs = {}
        for key, value in iteroptions(*args, **kwargs):
            state[key.lower()] = list(self.retrieve('sessionprop.getsessopt',
                                                    _messagelevel='error',
                                                    _apptag='UI',
                                                    name=key.lower()).values())[0]
            newkwargs[key.lower()] = value
        self.retrieve('sessionprop.setsessopt', _messagelevel='error',
                      _apptag='UI', **newkwargs)
        yield
        self.retrieve('sessionprop.setsessopt', _messagelevel='error',
                      _apptag='UI', **state)

    def get_action_names(self):
        '''
        Return the list of action classes

        Returns
        -------
        list of strings

        '''
        return self._action_classes.keys()

    def get_actionset_names(self):
        '''
        Return the list of actionset classes

        Returns
        -------
        list of strings

        '''
        return self._actionset_classes.keys()

    def has_action(self, name):
        '''
        Does the given action name exist?

        Parameters
        ----------
        name : string
            The name of the CAS action to look for.

        Returns
        -------
        boolean

        '''
        return name.lower() in self._action_classes

    def has_actionset(self, name):
        '''
        Does the given actionset name exist?

        Parameters
        ----------
        name : string
            The name of the CAS action set to look for.

        Returns
        -------
        boolean

        '''
        return name.lower() in self._actionset_classes

    def get_action(self, name):
        '''
        Get the CAS action instance for the given action name

        Parameters
        ----------
        name : string
            The name of the CAS action to look for.

        Returns
        -------
        :class:`CASAction` object

        '''
        return self.__getattr__(name, atype='action')

    def get_action_class(self, name):
        '''
        Get the CAS action class for the given action name

        Parameters
        ----------
        name : string
            The name of the CAS action to look for.

        Returns
        -------
        :class:`CASAction`

        '''
        return self.__getattr__(name, atype='action_class')

    def get_actionset(self, name):
        '''
        Get the CAS action set instance for the given action set name

        Parameters
        ----------
        name : string
            The name of the CAS action set to look for.

        Returns
        -------
        :class:`CASActionSet` object

        '''
        return self.__getattr__(name, atype='actionset')

    def __dir__(self):
        # Short-circuit PyCharm's introspection
        if 'get_names' in [x[3] for x in inspect.stack()]:
            return list(self._dir)
        return list(sorted(list(self._dir) + list(self.get_action_names())))

    def __dir_actions__(self):
        return list(sorted(self.get_action_names()))

    def __dir_members__(self):
        return list(sorted(self._dir))

    def __str__(self):
        args = []
        args.append(repr(self._hostname))
        args.append(repr(self._port))
        if self._username:
            args.append(repr(self._username))
        return 'CAS(%s, protocol=%s, name=%s, session=%s)' % (', '.join(args),
                                                              repr(self._protocol),
                                                              repr(self._name),
                                                              repr(self._session))

    def __repr__(self):
        return str(self)

    def CASTable(self, name, **kwargs):
        '''
        Create a CASTable instance

        The primary difference between constructing a :class:`CASTable`
        object through this method rather than directly, is that the
        current session will be automatically registered with the
        :class:`CASTable` object so that CAS actions can be called on
        it directly.

        Parameters
        ----------
        name : string
           Name of the table in CAS.
        **kwargs : any, optional
           Arbitrary keyword arguments.  These keyword arguments are
           passed to the :class:`CASTable` constructor.

        Returns
        -------
        :class:`CASTable` object

        '''
        table = CASTable(name, **kwargs)
        table.set_connection(self)
        return table

    def SASFormatter(self):
        '''
        Create a SASFormatter instance

        :class:`SASFormatters` can be used to format Python values using
        SAS data formats.

        Returns
        -------
        :class:`SASFormatter` object

        '''
        return SASFormatter(soptions=self._soptions)

    def add_results_hook(self, name, func):
        '''
        Add a post-processing function for results

        The function will be called with two arguments: the CAS connection
        object and the :class:`CASResult` object.

        Parameters
        ----------
        name : string
           Full name of action (actionset.actionname)
        func : function
           Function to call for result set

        See Also
        --------
        :meth:`del_results_hook`
        :meth:`del_results_hooks`

        Examples
        --------
        To add a post-processing function for a particular action, you
        specify the fully-qualified action name and a function.

        >>> def myfunc(connection, results):
               if results and results.get('num'):
                  results['num'] = math.abs(results['num'])
               return results
        >>>
        >>> s.add_results_hook('myactionset.myaction', myfunc)

        '''
        name = name.lower()
        if name not in self._results_hooks:
            self._results_hooks[name] = []
        self._results_hooks[name].append(func)

    def del_results_hook(self, name, func):
        '''
        Delete a post-processing function for an action

        Parameters
        ----------
        name : string
           Full name of action (actionset.actionname)
        func : function
           The function to remove

        See Also
        --------
        :meth:`add_results_hook`
        :meth:`del_results_hooks`

        Examples
        --------
        To remove a post-processing hook from an action, you must specify the
        action name as well as the function to remove.  This is due to the fact
        that multiple functions can be registered to a particular action.

        >>> s.del_results_hook('myactionset.myaction', myfunc)

        '''
        name = name.lower()
        if name in self._results_hooks:
            self._results_hooks[name] = [x for x in self._results_hooks[name]
                                         if x is not func]

    def del_results_hooks(self, name):
        '''
        Delete all post-processing functions for an action

        Parameters
        ---------
        name : string
           Full name of action (actionset.actionname)

        See Also
        --------
        :meth:`add_results_hook`
        :meth:`del_results_hook`

        Examples
        --------
        The following code removes all post-processing functions registered to
        the `myactionset.myaction` action.

        >>> s.del_results_hooks('myactionset.myaction')

        '''
        name = name.lower()
        if name in self._results_hooks:
            del self._results_hooks[name]

    def close(self, close_session=False):
        ''' Close the CAS connection '''
        if close_session:
            self.retrieve('session.endsession', _messagelevel='error', _apptag='UI')
        errorcheck(self._sw_connection.close(), self._sw_connection)

    def terminate(self):
        ''' End the session and close the CAS connection '''
        self.close(close_session=True)

    def _set_option(self, **kwargs):
        '''
        Set connection options

        Parameters
        ---------
        **kwargs : any
           Arbitrary keyword arguments.  Each key/value pair will be
           set as a connection option.

        Returns
        -------
        True
            If all options were set successfully

        '''
        for name, value in six.iteritems(kwargs):
            name = str(name)
            typ = errorcheck(self._sw_connection.getOptionType(name),
                             self._sw_connection)
            try:
                if typ == 'boolean':
                    if value in [True, False, 1, 0]:
                        errorcheck(self._sw_connection.setBooleanOption(name,
                                                                        value and 1 or 0),
                                   self._sw_connection)
                    else:
                        raise SWATError('%s is not a valid boolean value' % value)
                elif typ == 'string':
                    if isinstance(value, (binary_types, text_types)):
                        errorcheck(self._sw_connection.setStringOption(name, a2n(value)),
                                   self._sw_connection)
                    else:
                        errorcheck(self._sw_connection.setStringOption(name, value),
                                   self._sw_connection)
                elif typ == 'int32':
                    errorcheck(self._sw_connection.setInt32Option(name, int32(value)),
                               self._sw_connection)
                elif typ == 'int64':
                    errorcheck(self._sw_connection.setInt64Option(name, int64(value)),
                               self._sw_connection)
                elif typ == 'double':
                    errorcheck(self._sw_connection.setDoubleOption(name, float64(value)),
                               self._sw_connection)
            except TypeError:
                raise SWATError('%s is not the correct type' % value)
        return True

    def copy(self):
        '''
        Create a copy of the connection

        The copy of the connection will use the same parameters as ``self``,
        but it will create a new session.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> print(conn)
        CAS(..., session='76dd2bbe-de65-554f-a94f-a5e0e1abfdc8')

        >>> conn2 = conn.copy()
        >>> print(conn2)
        CAS(..., session='19cef586-6997-ae40-b62c-036f44cb60fc')

        See Also
        --------
        :meth:`fork`

        Returns
        -------
        :class:`CAS` object

        '''
        return type(self)(None, None, prototype=self)

    def fork(self, num=2):
        '''
        Create multiple copies of a connection

        The copies of the connection will use the same parameters as ``self``,
        but each will create a new session.

        Notes
        -----
        The first element in the returned list is the same object that
        the method was called on.  You only get `num`-1 copies.

        Parameters
        ----------
        num : int, optional
           Number of returned connections.  The first element of the returned
           list is always the object that the fork method was called on.

        Examples
        --------
        The code below demonstrates how to get four unique connections.

        >>> conn = swat.CAS()
        >>> c1, c2, c3, c4 = conn.fork(4)
        >>> c1 is conn
        True
        >>> c2 is conn
        False

        See Also
        --------
        :meth:`copy`

        Returns
        -------
        list of :class:`CAS` objects

        '''
        output = [self]
        for i in range(1, num):
            output.append(self.copy())
        return output

    def _invoke_without_signature(self, _name_, **kwargs):
        '''
        Call an action on the server

        Parameters
        ----------
        _name_ : string
            Name of the action.

        **kwargs : any, optional
            Arbitrary keyword arguments.

        Returns
        -------
        :obj:`self`

        '''
        if isinstance(self._sw_connection, rest.REST_CASConnection):
            errorcheck(self._sw_connection.invoke(a2n(_name_), kwargs),
                       self._sw_connection)
        else:
            errorcheck(self._sw_connection.invoke(a2n(_name_),
                                                  py2cas(self._soptions,
                                                         self._sw_error, **kwargs)),
                       self._sw_connection)
        return self

    def _merge_param_args(self, parmlist, kwargs, action=None):
        '''
        Merge keyword arguments into a parmlist

        This method modifies the parmlist *in place*.

        Parameters
        ----------
        parmlist : list
            Parameter list.
        kwargs : dict
            Dictionary of keyword arguments.
        action : string
            Name of the action.

        '''
        if action is None:
            action = ''

        if isinstance(kwargs, ParamManager):
            kwargs = copy.deepcopy(kwargs.params)

        # Short circuit if we can
        if not isinstance(kwargs, dict):
            return

        # See if we have a caslib= parameter
        caslib = False
        for param in parmlist:
            if param['name'] == 'caslib':
                caslib = True
                break

        # kwargs preserving case
        casekeys = {k.lower(): k for k in kwargs.keys()}

        # Add support for CASTable objects
        inputs = None
        fetch = {}
        uses_inputs = False
        uses_fetchvars = False
        for param in parmlist:
            ptype = param['parmType']
            key = param['name']
            key = casekeys.get(key, key)
            key_lower = key.lower()

            # Check for inputs= / fetchvars= parameters
            if ptype == 'value_list':
                if key_lower == 'inputs':
                    uses_inputs = True
                elif key_lower == 'fetchvars':
                    uses_fetchvars = True

            # Get table object if it exists
            tbl = kwargs.get('__table__', None)

            # Convert table objects to the proper form based on the argument type
            if key in kwargs and isinstance(kwargs[key], CASTable):
                if param.get('isTableDef'):
                    inputs = kwargs[key].get_inputs_param()
                    fetch = kwargs[key].get_fetch_params()
                    kwargs[key] = kwargs[key].to_table_params()
                elif param.get('isTableName'):
                    inputs = kwargs[key].get_inputs_param()
                    fetch = kwargs[key].get_fetch_params()
                    # Fill in caslib= first
                    if caslib and 'caslib' not in kwargs and \
                            kwargs[key].has_param('caslib'):
                        kwargs['caslib'] = kwargs[key].get_param('caslib')
                    kwargs[key] = kwargs[key].to_table_name()
                elif param.get('isOutTableDef'):
                    kwargs[key] = kwargs[key].to_outtable_params()
                elif param.get('isCasLib') and kwargs[key].has_param('caslib'):
                    kwargs[key] = kwargs[key].get_param('caslib')

            # If a string is given for a table object, convert it to a table object
            elif key in kwargs and isinstance(kwargs[key], text_types) and \
                    param.get('isTableDef'):
                kwargs[key] = {'name': kwargs[key]}

            elif tbl is not None and param.get('isTableDef') and \
                    key_lower == 'table' and 'table' not in casekeys:
                inputs = tbl.get_inputs_param()
                fetch = tbl.get_fetch_params()
                kwargs[key] = tbl.to_table_params()

            elif tbl is not None and param.get('isTableName') and \
                    key_lower == 'name' and 'name' not in casekeys:
                inputs = tbl.get_inputs_param()
                fetch = tbl.get_fetch_params()
                if caslib and 'caslib' not in kwargs and tbl.has_param('caslib'):
                    kwargs['caslib'] = tbl.get_param('caslib')
                kwargs[key] = tbl.to_table_name()

            # Workaround for columninfo / update which doesn't define table= as
            # a table definition.
            elif tbl is not None and key_lower == 'table' and \
                    action.lower() in ['columninfo', 'table.columninfo',
                                       'update', 'table.update'] and \
                    'table' not in casekeys:
                inputs = tbl.get_inputs_param()
                kwargs[key] = tbl.to_table_params()
                if not uses_inputs:
                    if inputs and 'vars' not in kwargs:
                        kwargs[key]['vars'] = inputs
                    inputs = None

        # Apply input variables
        if uses_inputs and inputs and 'inputs' not in kwargs:
            kwargs['inputs'] = inputs
        elif uses_fetchvars and inputs and 'fetchvars' not in kwargs:
            kwargs['fetchvars'] = inputs

        # Apply fetch parameters
        if fetch and action.lower() in ['fetch', 'table.fetch']:
            for key, value in fetch.items():
                if key in kwargs:
                    continue
                if key == 'sortby' and ('orderby' in kwargs or 'orderBy' in kwargs):
                    continue
                kwargs[key] = value

        # Apply inputs= to specific actions that don't support it
        if 'table' in kwargs and not uses_inputs and inputs \
                and action.lower() in ['partition', 'table.partition',
                                       'save', 'table.save']:
            tbl = kwargs['table']
            if not isinstance(tbl, dict):
                tbl = dict(name=tbl)
            tbl['vars'] = inputs

        # Fix aggregate action when both inputs= and varspecs= are supplied
        if 'table' in kwargs and action.lower() in ['aggregate', 'aggregation.aggregate']:
            if 'inputs' in kwargs and 'varspecs' in kwargs:
                kwargs.pop('inputs', None)

        kwargs.pop('__table__', None)

        # Workaround for tableinfo which aliases table= to name=, but
        # the alias is hidden.
        if action.lower() in ['tableinfo', 'table.tableinfo'] and 'table' in kwargs:
            if isinstance(kwargs['table'], CASTable):
                kwargs['table'] = kwargs['table'].to_table_params()
            if isinstance(kwargs['table'], dict):
                if caslib and 'caslib' not in kwargs and kwargs['table'].get('caslib'):
                    kwargs['caslib'] = kwargs['table']['caslib']
                kwargs['table'] = kwargs['table']['name']

        # Add current value fields in the signature
        for param in parmlist:
            if param['name'] in kwargs:
                if 'parmList' in param:
                    self._merge_param_args(param['parmList'], kwargs[param['name']],
                                           action=action)
                else:
                    if isinstance(kwargs[param['name']], text_types):
                        param['value'] = kwargs[param['name']].replace('"', '\\u0022')
                    # TODO: This should only happen for binary inputs (i.e., never)
                    elif isinstance(kwargs[param['name']], binary_types):
                        # param['value'] = kwargs[param['name']].replace('"', '\\u0022')
                        pass
                    else:
                        param['value'] = kwargs[param['name']]

    def _get_action_params(self, name, kwargs):
        '''
        Get additional parameters associated with the given action

        Parameters
        ----------
        name : string
            Name of the action being executed.
        kwargs : dict
            Action parameter dictionary.

        Returns
        -------
        dict
            The new set of action parameters.

        '''
        newkwargs = kwargs.copy()
        for value in six.itervalues(kwargs):
            if isinstance(value, ActionParamManager):
                newkwargs.update(value.get_action_params(name, {}))
        return newkwargs

    def _invoke_with_signature(self, _name_, **kwargs):
        '''
        Call an action on the server

        Parameters
        ----------
        _name_ : string
            Name of the action.
        **kwargs : any, optional
            Arbitrary keyword arguments.

        Returns
        -------
        dict
            Signature of the action

        '''
        # Get the signature of the action
        signature = self._get_action_info(_name_)[-1]

        # Check for additional action parameters
        kwargs = self._get_action_params(_name_, kwargs)

        if signature:
            signature = copy.deepcopy(signature)
            kwargs = copy.deepcopy(kwargs)
            self._merge_param_args(signature.get('params', {}), kwargs, action=_name_)

        self._invoke_without_signature(_name_, **kwargs)

        return signature

    def _extract_dtypes(self, df):
        '''
        Extract importoptions= style data types from the DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame to get types from
        format : string, optional
            The output format: dict or list

        Returns
        -------
        OrderedDict

        '''
        out = collections.OrderedDict()

        for key, value in df.dtypes.iteritems():
            value = value.name

            if value == 'object':
                value = 'varchar'

            elif value.startswith('float'):
                value = 'double'

            elif value.endswith('int64'):
                if 'csv-ints' in self.server_features:
                    value = 'int64'
                else:
                    value = 'double'

            elif value.startswith('int'):
                if 'csv-ints' in self.server_features:
                    value = 'int32'
                else:
                    value = 'double'

            elif value.startswith('bool'):
                if 'csv-ints' in self.server_features:
                    value = 'int32'
                else:
                    value = 'double'

            elif value.startswith('datetime'):
                value = 'varchar'

            else:
                continue

            out['%s' % key] = dict(type=value)

        return out

    def _apply_importoptions_vars(self, importoptions, df_dtypes):
        '''
        Merge in vars= parameters to importoptions=

        Notes
        -----
        This method modifies the importoptions in-place.

        Parameters
        ----------
        importoptions : dict
            The importoptions= parameter
        df_dtypes : dict or list
            The DataFrame data types dictionary

        '''
        if 'vars' not in importoptions:
            importoptions['vars'] = df_dtypes
            return

        vars = importoptions['vars']

        # Merge options into dict vars
        if isinstance(vars, dict_types):
            for key, value in six.iteritems(df_dtypes):
                if key in vars:
                    for k, v in six.iteritems(value):
                        vars[key].setdefault(k, v)
                else:
                    vars[key] = value

        # Merge options into list vars
        else:
            df_dtypes_list = []
            for key, value in six.iteritems(df_dtypes):
                value = dict(value)
                value['name'] = key
                df_dtypes_list.append(value)

            for i, item in enumerate(df_dtypes_list):
                if i < len(vars):
                    if not vars[i]:
                        vars[i] = item
                    else:
                        for key, value in six.iteritems(item):
                            vars[i].setdefault(key, value)
                else:
                    vars.append(item)

    def upload(self, data, importoptions=None, casout=None, date_format=None, **kwargs):
        '''
        Upload data from a local file into a CAS table

        The primary difference between this data loader and the other data
        loaders on this class is that, in this case, the parsing of the data
        is done on the server.  This method simply uploads the file as
        binary data which is then parsed by `table.loadtable` on the server.

        While the server parsers may not be quite a flexible as Python, they
        are generally much faster.  Files such as CSV can be parsed on the
        server in multiple threads across many machines in the grid.

        Notes
        -----
        This method uses paths that are on the **client side**.  This means
        you need to use paths to files that are **on the same machine that Python
        is running on**.  If you want to load files from the CAS server side, you
        would use the `table.loadtable` action.

        Also, when uploading a :class:`pandas.DataFrame`, the data is exported to
        a CSV file, then the CSV file is uploaded.  This can cause a loss of
        metadata about the columns since the server parser will guess at the
        data types of the columns.  You can use `importoptions=` to specify more
        information about the data.

        Parameters
        ----------
        data : string or :class:`pandas.DataFrame`
            If the value is a string, it can be either a filename
            or a URL.  DataFrames will be converted to CSV before
            uploading.
        importoptions : dict, optional
            Import options for the ``table.loadtable`` action.
        casout : dict, optional
            Output table definition for the ``table.loadtable`` action.
        date_format : string, optional
            Format string for datetime objects.
        **kwargs : keyword arguments, optional
            Additional parameters to the ``table.loadtable`` action.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> out = conn.upload('data/iris.csv')
        >>> tbl = out.casTable
        >>> print(tbl.head())
           sepal_length  sepal_width  petal_length  petal_width species
        0           5.1          3.5           1.4          0.2  setosa
        1           4.9          3.0           1.4          0.2  setosa
        2           4.7          3.2           1.3          0.2  setosa
        3           4.6          3.1           1.5          0.2  setosa
        4           5.0          3.6           1.4          0.2  setosa

        Returns
        -------
        :class:`CASResults`

        '''
        delete = False
        name = None
        df_dtypes = None

        for key, value in list(kwargs.items()):
            if importoptions is None and key.lower() == 'importoptions':
                importoptions = value
                del kwargs[key]
            elif casout is None and key.lower() == 'casout':
                casout = value
                del kwargs[key]

        if importoptions is None:
            importoptions = {}

        import pandas as pd
        if isinstance(data, pd.DataFrame):
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
                delete = True
                filename = tmp.name
                name = os.path.splitext(os.path.basename(filename))[0]
                data.to_csv(filename, encoding='utf-8',
                            index=False, sep=a2n(',', 'utf-8'),
                            decimal=a2n('.', 'utf-8'),
                            date_format=a2n(date_format, 'utf-8'),
                            line_terminator=a2n('\r\n', 'utf-8'))
                df_dtypes = self._extract_dtypes(data)
                importoptions['locale'] = 'EN-us'

        elif data.startswith('http://') or \
                data.startswith('https://') or \
                data.startswith('ftp://'):
            import certifi
            import ssl
            import tempfile
            from six.moves.urllib.request import urlopen
            from six.moves.urllib.parse import urlparse
            parts = urlparse(data)
            ext = os.path.splitext(parts.path)[-1].lower()
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                delete = True
                tmp.write(
                    urlopen(
                        data,
                        context=ssl.create_default_context(cafile=certifi.where())
                    ).read())
                filename = tmp.name
                if parts.path:
                    name = os.path.splitext(parts.path.split('/')[-1])[0]
                else:
                    name = os.path.splitext(os.path.basename(filename))[0]

        else:
            filename = data
            name = os.path.splitext(os.path.basename(filename))[0]

        # TODO: Populate docstring with table.loadtable action help
        filetype = {
            'sav': 'spss',
            'xlsx': 'excel',
            'sashdat': 'hdat',
            'sas7bdat': 'basesas',
        }

        if isinstance(importoptions, (dict, ParamManager)) and \
                'filetype' not in [x.lower() for x in importoptions.keys()]:
            ext = os.path.splitext(filename)[-1][1:].lower()
            if ext in filetype:
                importoptions['filetype'] = filetype[ext]
            elif len(ext) == 3 and ext.endswith('sv'):
                importoptions['filetype'] = 'csv'

        kwargs['importoptions'] = importoptions

        if df_dtypes:
            self._apply_importoptions_vars(importoptions, df_dtypes)

        if casout is None:
            casout = {}
        if isinstance(casout, CASTable):
            casout = casout.to_outtable_params()
        if isinstance(casout, dict) and 'name' not in casout:
            casout['name'] = name
        kwargs['casout'] = casout

        if isinstance(self._sw_connection, rest.REST_CASConnection):
            resp = self._sw_connection.upload(a2n(filename), kwargs)
        else:
            resp = errorcheck(self._sw_connection.upload(a2n(filename),
                                                         py2cas(self._soptions,
                                                                self._sw_error,
                                                                **kwargs)),
                              self._sw_connection)

        # Remove temporary file as needed
        if delete:
            try:
                os.remove(filename)
            except Exception:
                pass

        return self._get_results([(CASResponse(resp, connection=self), self)])

    def upload_file(self, data, importoptions=None, casout=None, **kwargs):
        '''
        Upload a client-side data file to CAS and parse it into a CAS table

        Parameters
        ----------
        data : string
            Either a filename or URL.
            or a URL.  DataFrames will be converted to CSV before
            uploading.
        importoptions : dict, optional
            Import options for the ``table.loadtable`` action.
        casout : dict, optional
            Output table definition for the ``table.loadtable`` action.
        **kwargs : keyword arguments, optional
            Additional parameters to the ``table.loadtable`` action.

        Returns
        -------
        :class:`CASTable`

        '''
        for key, value in list(kwargs.items()):
            if importoptions is None and key.lower() == 'importoptions':
                importoptions = value
                del kwargs[key]
            elif casout is None and key.lower() == 'casout':
                casout = value
                del kwargs[key]

        out = self.upload(data, importoptions=importoptions,
                          casout=casout, **kwargs)

        if out.severity > 1:
            raise SWATError(out.status)

        return out['casTable']

    def upload_frame(self, data, importoptions=None, casout=None, **kwargs):
        '''
        Upload a client-side data file to CAS and parse it into a CAS table

        Parameters
        ----------
        data : :class:`pandas.DataFrame`
            DataFrames will be converted to CSV before uploading.
        importoptions : dict, optional
            Import options for the ``table.loadtable`` action.
        casout : dict, optional
            Output table definition for the ``table.loadtable`` action.
        **kwargs : keyword arguments, optional
            Additional parameters to the ``table.loadtable`` action.

        Returns
        -------
        :class:`CASTable`

        '''
        for key, value in list(kwargs.items()):
            if importoptions is None and key.lower() == 'importoptions':
                importoptions = value
                del kwargs[key]
            elif casout is None and key.lower() == 'casout':
                casout = value
                del kwargs[key]

        out = self.upload(data, importoptions=importoptions,
                          casout=casout, **kwargs)

        if out.severity > 1:
            raise SWATError(out.status)

        return out['casTable']

    def _raw_invoke(self, _name_, **kwargs):
        ''' Invoke a CAS action without any parameter checking '''
        self._invoke_without_signature(a2n(_name_), **kwargs)
        return self

    def _raw_retrieve(self, _name_, **kwargs):
        ''' Call a CAS action without parameter checking and return results '''
        try:
            # Call the action and compile the results
            self._invoke_without_signature(a2n(_name_), **kwargs)
            return self._get_results(getnext(self))
        except SWATCASActionRetry:
            self._invoke_without_signature(a2n(_name_), **kwargs)
            return self._get_results(getnext(self))

    def invoke(self, _name_, **kwargs):
        '''
        Call an action on the server

        The :meth:`invoke` method only calls the action on the server.  It
        does not retrieve the responses.  To get the responses, you iterate
        over the connection object.

        Parameters
        ----------
        _name_ : string
            Name of the action
        **kwargs : any, optional
            Arbitrary keyword arguments

        Returns
        -------
        `self`

        See Also
        --------
        :meth:`retrieve` : Calls action and retrieves results
        :meth:`__iter__` : Iterates over responses

        Examples
        --------
        The code below demonstrates how you invoke an action on the server and
        iterate through the results.

        >>> s.invoke('help')
        <swat.CAS object at 0x7fab0a9031d0>
        >>> for response in s:
        ...     for key, value in response:
        ...         print(key)
        ...         print(value)
        builtins
                      name                                        description
        0          addnode                           Add a node to the server
        1             help                        Lists the available actions
        .
        .
        .

        '''
        self._invoke_with_signature(a2n(_name_), **kwargs)
        return self

    def retrieve(self, _name_, **kwargs):
        '''
        Call the action and aggregate the results

        Parameters
        ----------
        _name_ : string
           Name of the action
        **kwargs : any, optional
           Arbitrary keyword arguments

        Returns
        -------
        :class:`CASResults` object

        See Also
        --------
        :meth:`invoke` : Calls action, but does not retrieve results

        Examples
        --------
        The code below demonstrates how you invoke an action on the server and
        retrieve the results.

        >>> out = s.retrieve('help')
        >>> print(out.keys())
        ['builtins', 'casidx', 'casmeta', 'espact', 'tkacon', 'table', 'tkcsessn',
         'tkcstate']
        >>> print(out['builtins'])
                      name                                        description
        0          addnode                           Add a node to the server
        1             help                        Lists the available actions
        2        listnodes                              List the server nodes
        .
        .
        .


        Status and performance information is also available on the returned object.
        Here is an example of an action call to an action that doesn't exist.

        >>> out = s.retrieve('foo')
        >>> print(out.status)
        'The specified action was not found.'
        >>> print(out.severity)
        2
        >>> print(out.messages)
        ["ERROR: Action 'foo' was not found.",
         'ERROR: The CAS server stopped processing this action because of errors.']

        Here is an example that demonstrates the performance metrics that are available.

        >>> out = s.retrieve('help')
        >>> print(out.performance)
        <swat.CASPerformance object at 0x33b1c50>

        Performance values are loaded lazily, but you can get a dictionary of
        all of them using the ``to_dict`` method.

        >>> print(out.performance.to_dict())
        {'system_cores': 1152L, 'memory_quota': 303759360L, 'cpu_user_time': 0.014995,
         'elapsed_time': 0.004200000000000001, 'system_nodes': 48L,
         'memory_system': 432093312L, 'cpu_system_time': 0.018999, 'memory': 150688L,
         'memory_os': 294322176L, 'system_total_memory': 4868538236928L}

        Rather than having the ``retrieve`` method compile all of the results into one
        object, you can control how the responses and results from the server are
        handled in your own functions using the ``responsefunc`` or ``resultfunc`` keyword
        arguments.

        The ``responsefunc`` argument allows you to specify a function that is called for
        each response from the server after the action is called.  The ``resultfunc``
        is called for each result in a response.  These functions can not be used at the
        same time though.  In the case where both are specified, only the resultfunc
        will be used.  Below is an example of using a responsefunc function.
        This function closely mimics what the `retrieve` method does by default.

        >>> def myfunc(response, connection, userdata):
        ...     if userdata is None:
        ...         userdata = {}
        ...     for key, value in response:
        ...         userdata[key] = value
        ...     return userdata
        >>> out = s.retrieve('help', responsefunc=myfunc)
        >>> print(out['builtins'])
                      name                                        description
        0          addnode                           Add a node to the server
        1             help                        Lists the available actions
        2        listnodes                              List the server nodes
        .
        .
        .

        The same result can be gotten using the ``resultfunc`` option as well.

        >>> def myfunc(key, value, response, connection, userdata):
        ...    if userdata is None:
        ...       userdata = {}
        ...    userdata[key] = value
        ...    return userdata
        >>> out = s.retrieve('help', resultfunc=myfunc)
        >>> print(out['builtins'])
                      name                                        description
        0          addnode                           Add a node to the server
        1             help                        Lists the available actions
        2        listnodes                              List the server nodes
        .
        .
        .

        '''
        kwargs = dict(kwargs)

        # Decode from JSON as needed
        if '_json' in kwargs:
            newargs = json.loads(kwargs['_json'])
            newargs.update(kwargs)
            del newargs['_json']
            kwargs = newargs

        datamsghandler = None
        if 'datamsghandler' in kwargs:
            datamsghandler = kwargs['datamsghandler']
            kwargs.pop('datamsghandler')
            if self._protocol.startswith('http'):
                raise SWATError('Data message handlers are not supported '
                                'in the REST interface.')

        # Response callback function
        responsefunc = None
        if 'responsefunc' in kwargs:
            responsefunc = kwargs['responsefunc']
            kwargs.pop('responsefunc')

        # Result callback function
        resultfunc = None
        if 'resultfunc' in kwargs:
            resultfunc = kwargs['resultfunc']
            kwargs.pop('resultfunc')

        try:
            # Call the action and compile the results
            signature = self._invoke_with_signature(a2n(_name_), **kwargs)
            results = self._get_results(getnext(self, datamsghandler=datamsghandler),
                                        responsefunc=responsefunc, resultfunc=resultfunc)
        except SWATCASActionRetry:
            signature = self._invoke_with_signature(a2n(_name_), **kwargs)
            results = self._get_results(getnext(self, datamsghandler=datamsghandler),
                                        responsefunc=responsefunc, resultfunc=resultfunc)

        # Return raw data if a function was supplied
        if responsefunc is not None or resultfunc is not None:
            return results

        results.signature = signature

        # run post-processing hooks
        if signature and signature.get('name') in self._results_hooks:
            for func in self._results_hooks[signature['name']]:
                func(self, results)

        return results

    def _get_results(self, riter, responsefunc=None, resultfunc=None):
        '''
        Walk through responses in ``riter`` and compile results

        Parameters
        ----------
        riter : iterable
            Typically a CAS object, but any iterable that returns a
            response / connection pair for each iteration can be used.
        responsefunc : callable, optional
            Callback function that is called for each response
        resultfunc : callable, optional
            Callback function that is called for each result

        Returns
        -------
        CASResults
            If no callback functions were supplied.
        any
            If a callback function is supplied, the result of that function
            is returned.

        '''
        results = CASResults()
        results.messages = messages = []
        results.updateflags = updateflags = set()
        results.session = self._session
        results.sessionname = self._name
        events = results.events
        idx = 0
        resultdata = None
        responsedata = None

        try:
            for response, conn in riter:

                if response.disposition.status_code == RETRY_ACTION_CODE:
                    raise SWATCASActionRetry(response.disposition.status)
                elif response.disposition.status_code == SESSION_ABORTED_CODE:
                    # Any new requests sent to the session will never return,
                    # so just close the connection now.
                    self.close()
                    raise SWATCASActionError(response.disposition.status, response, conn)

                if responsefunc is not None:
                    responsedata = responsefunc(response, conn, responsedata)
                    continue

                # Action was restarted by the server
                if 'action-restart' in response.updateflags:
                    results = CASResults()
                    results.messages = messages = []
                    results.updateflags = updateflags = set()
                    results.session = self._session
                    results.sessionname = self._name
                    events = results.events
                    idx = 0
                    continue

                # CASTable parameters
                caslib = None
                tablename = None
                castable = None

                for key, value in response:

                    if resultfunc is not None:
                        resultdata = resultfunc(key, value, response,
                                                conn, resultdata)
                        continue

                    if key is None or isinstance(key, int_types):
                        results[idx] = value
                        idx += 1
                    else:
                        lowerkey = key.lower()
                        if lowerkey == 'tablename':
                            tablename = value
                        elif lowerkey == 'caslib':
                            caslib = value
                        elif lowerkey == 'castable':
                            castable = True
                        # Event results start with '$'
                        if key.startswith('$'):
                            events[key] = value
                        else:
                            results[key] = value

                # Create a CASTable instance if all of the pieces are there
                if caslib and tablename and not castable:
                    results['casTable'] = self.CASTable(tablename, caslib=caslib)

                results.performance = response.performance
                for key, value in six.iteritems(response.disposition.to_dict()):
                    setattr(results, key, value)
                messages.extend(response.messages)
                updateflags.update(response.updateflags)

        except SWATCASActionError as err:
            if responsefunc:
                err.results = responsedata
            elif resultfunc:
                err.results = resultdata
            else:
                err.results = results
                err.events = events
            raise err

        if responsefunc is not None:
            return responsedata

        if resultfunc is not None:
            return resultdata

        return results

    def __getattr__(self, name, atype=None):
        '''
        Convenience method for getting a CASActionSet/CASAction as an attribute

        When an attribute that looks like an action name is accessed, CAS
        is queried to see if it is an action set or action name.  If so,
        the reflection information for the entire actionset is used to
        generate classes for the actionset and all actions.

        Parameters
        ----------
        name : string
           Action name or action set name
        atype : string, optional
           Type of item to search for exclusively ('actionset', 'action',
           or 'action_class')

        Returns
        -------
        CASAction
           If `name` is an action name
        CASActionSet
           If `name` is an action set name

        Raises
        ------
        AttributeError
           if `name` is neither an action name or action set name

        '''
        class_requested = False
        origname = name

        # Normalize name
        if re.match(r'^[A-Z]', name):
            class_requested = True

        if atype is not None and atype == 'action_class':
            class_requested = True
            atype = 'action'

        name = name.lower()

        # Check cache for actionset and action classes
        if (atype in [None, 'actionset'] and name in self._actionset_classes
                and self._actionset_classes[name] is not None):
            return self._actionset_classes[name]()

        if (atype in [None, 'action'] and name in self._action_classes
                and self._action_classes[name] is not None):
            if class_requested:
                return self._action_classes[name]
            return self._action_classes[name]()

        # See if the action/action set exists
        asname, actname, asinfo = self._get_actionset_info(name.lower(), atype=atype)

        # Generate a new actionset class
        ascls = CASActionSet.from_reflection(asinfo, self)

        # Add actionset and actions to the cache
        self._actionset_classes[asname.lower()] = ascls
        for key, value in six.iteritems(ascls.actions):
            self._action_classes[key] = value
            self._action_classes[asname.lower() + '.' + key] = value

        # Check cache for actionset and action classes
        if atype in [None, 'actionset'] and name in self._actionset_classes:
            return self._actionset_classes[name]()

        if atype in [None, 'action'] and name in self._action_classes:
            if class_requested:
                return self._action_classes[name]
            return self._action_classes[name]()

        # Look for actions that can't be reflected
        if asname and actname:
            enabled = ['yes', 'y', 'on', 't', 'true', '1']
            if os.environ.get('CAS_ACTION_TEST_MODE', '').lower() in enabled:
                if asname not in self._actionset_classes:
                    self._actionset_classes[asname.lower()] = ascls
                else:
                    ascls = self._actionset_classes[asname.lower()]
                if actname not in ascls.actions:
                    ascls.actions[actname.lower()] = None
                return getattr(ascls(), actname)

        raise AttributeError(origname)

    def _get_action_info(self, name, showhidden=True, levels=None):
        '''
        Get the reflection information for the given action name

        Parameters
        ----------
        name : string
            Name of the action
        showhidden : boolean
            Should hidden actions be shown?
        levels : int, optional
            Number of levels of reflection data to return. Default is all.

        Returns
        -------
        ( string, string, dict )
           Tuple containing action-set-name, action-name, and action-info-dict

        '''
        name = name.lower()
        if name in self._action_info:
            return self._action_info[name]

        asname, actname, asinfo = self._get_reflection_info(name,
                                                            showhidden=showhidden,
                                                            levels=levels)

        # If action name is None, it is the same as the action set name
        if actname is None:
            actname = asname

        # Populate action set info while we're here
        self._actionset_info[asname.lower()] = asname, None, asinfo

        # Populate action info
        actinfo = {}
        for item in asinfo.get('actions'):
            asname, aname = item['name'].split('.', 1)
            if aname == actname.lower():
                actinfo = item
            self._action_info[aname] = asname, aname, item
            self._action_info[item['name']] = asname, aname, item

        return asname, actname, actinfo

    def _get_actionset_info(self, name, atype=None, showhidden=True, levels=None):
        '''
        Get the reflection information for the given action set / action name

        If the name is an action set, the returned action name will be None.

        Parameters
        ----------
        name : string
            Name of the action set or action
        atype : string, optional
            Specifies the type of the name ('action' or 'actionset')
        showhidden : boolean, optional
            Should hidden actions be shown?
        levels : int, optional
            Number of levels of reflection data to return. Default is all.

        Returns
        -------
        ( string, string, dict )
           Tuple containing action-set-name, action-name, and action-set-info-dict

        '''
        name = name.lower()
        if atype in [None, 'actionset'] and name in self._actionset_info:
            return self._actionset_info[name]
        if atype in [None, 'action'] and name in self._action_info:
            asname, aname, actinfo = self._action_info[name]
            return asname, aname, self._actionset_info[asname.lower()][-1]

        asname, actname, asinfo = self._get_reflection_info(name, atype=atype,
                                                            showhidden=showhidden,
                                                            levels=levels)

        # Populate action set info
        self._actionset_info[asname.lower()] = asname, None, asinfo

        # Populate action info while we're here
        for item in asinfo.get('actions'):
            asname, aname = item['name'].split('.', 1)
            self._action_info[aname] = asname, aname, item
            self._action_info[item['name']] = asname, aname, item

        return asname, actname, asinfo

    def _get_reflection_info(self, name, atype=None, showhidden=True, levels=None):
        '''
        Get the full action name of the called action including the action set information

        Parameters
        ----------
        name : string
            Name of the argument
        atype : string, optional
            Specifies the type of the name ('action' or 'actionset')
        showhidden : boolean, optional
            Should hidden actions be shown?
        levels : int, optional
            Number of levels of reflection data to return. Default is all.

        Returns
        -------
        tuple
           ( action set name, action name, action set reflection info )

        '''
        asname = None
        actname = None

        # See if the name is an action set name, action name, or nothing
        if atype in [None, 'actionset']:
            for response in self._invoke_without_signature('builtins.queryactionset',
                                                           actionset=name,
                                                           _messagelevel='error',
                                                           _apptag='UI'):
                for key, value in response:
                    if value:
                        asname = name.lower()
                    break

        if asname is None:
            idx = 0
            out = {}
            for response in self._invoke_without_signature('builtins.queryname',
                                                           name=name,
                                                           _messagelevel='error',
                                                           _apptag='UI'):
                for key, value in response:
                    if key is None or isinstance(key, int_types):
                        out[idx] = value
                        idx += 1
                    else:
                        out[key] = value

            asname = out.get('actionSet')
            actname = out.get('action')

            # We can't have both in the same namespace, action set name wins
            if asname == actname:
                actname = None

        # If we have an action set name, reflect it
        if asname:
            asname = asname.lower()
            query = {'showhidden': showhidden, 'actionset': asname}

            if not get_option('interactive_mode'):
                query['showlabels'] = False

            if 'reflection-levels' in self.server_features:
                if levels is not None:
                    query['levels'] = levels
                else:
                    query['levels'] = get_option('cas.reflection_levels')

            idx = 0
            out = {}
            for response in self._invoke_without_signature('builtins.reflect',
                                                           _messagelevel='error',
                                                           _apptag='UI', **query):
                for key, value in response:
                    if key is None or isinstance(key, int_types):
                        out[idx] = value
                        idx += 1
                    else:
                        out[key] = value

            # Normalize the output
            asinfo = _lower_actionset_keys(out[0])
            for act in asinfo.get('actions'):
                act['name'] = (asname + '.' + act['name']).lower()

            return asname, actname, asinfo

        raise AttributeError(name)

    def __iter__(self):
        '''
        Iterate over responses from CAS

        If you used the :meth:`invoke` method to call a CAS action, the
        responses from the server are not automatically retrieved.  You
        will need to pull them down manually.  Iterating over the CAS
        connection object after calling :meth:`invoke` will pull responses
        down until they have been exhausted.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> conn.invoke('serverstatus')
        >>> for resp in conn:
        ...     for k, v in resp:
        ...        print(k, v)

        See Also
        --------
        :meth:`invoke` : Calls a CAS action without retrieving results
        :meth:`retrieve` : Calls a CAS action and retrieves results

        Yields
        ------
        :class:`CASResponse` object

        '''
        for response, conn in getnext(self, timeout=0):
            if conn is not None:
                yield response

    #
    # Top-level Pandas functions
    #

    def _get_table_args(self, **kwargs):
        ''' Extract table paramaters from function arguments '''
        out = {}
        kwargs = kwargs.copy()
        casout = kwargs.pop('casout', {})
        if isinstance(casout, CASTable):
            casout = casout.to_outtable_params()
        elif not isinstance(casout, dict):
            casout = dict(name=casout)
        out['table'] = casout.get('name', None)
        out['caslib'] = casout.get('caslib', None)
        out['replace'] = casout.get('replace', None)
        out['label'] = casout.get('label', None)
        out['promote'] = casout.get('promote', None)
        if not out['table']:
            out.pop('table')
        if not out['caslib']:
            out.pop('caslib')
        if out['replace'] is None:
            out.pop('replace')
        if out['label'] is None:
            out.pop('label')
        if out['promote'] is None:
            out.pop('promote')
        return out, kwargs

    def load_path(self, path=None, readahead=None, importoptions=None,
                  promote=None, ondemand=None, attrtable=None,
                  caslib=None, datasourceoptions=None, casout=None, singlepass=None,
                  where=None, vars=None, groupby=None, groupbyfmts=None,
                  groupbymode=None, orderby=None, nosource=None, returnwhereinfo=None,
                  **kwargs):
        '''
        Load a path from a CASLib

        The parameters for this are the same as for the ``builtins.loadtable``
        CAS action.  This method is simply a convenience method that loads a
        table and returns a :class:`CASTable` in one step.

        Notes
        -----
        The path specified must exist on the **server side**.  For loading
        data from the client side, see the ``read_*`` and :meth:`upload` methods.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.load_path('data/iris.csv')
        >>> print(tbl.head())

        See Also
        --------
        :meth:`read_csv`
        :meth:`upload_file`

        Returns
        -------
        :class:`CASTable`

        '''
        args = {k: v for k, v in dict(path=path, readahead=readahead,
                importoptions=importoptions, promote=promote,
                ondemand=ondemand, attrtable=attrtable, caslib=caslib,
                datasourceoptions=datasourceoptions, casout=casout,
                singlepass=singlepass, where=where, vars=vars, groupby=groupby,
                groupbyfmts=groupbyfmts, groupbymode=groupbymode,
                orderby=orderby, nosource=nosource,
                returnwhereinfo=returnwhereinfo).items() if v is not None}
        args.update(kwargs)
        out = self.retrieve('table.loadtable', _messagelevel='error', **args)
        try:
            return out['casTable']
        except KeyError:
            raise SWATError(out.status)

    def _importoptions_from_dframe(self, dframe):
        '''
        Derive importoptions= values from DataFrame

        '''
        use_options = False
        ivars = []
        importoptions = dict(filetype='csv', vars=ivars)
        for i, dtype in enumerate(dframe.dtypes.values):
            dtype = str(dtype)
            if 'int64' in dtype:
                ivars.append(dict(type='int64'))
                use_options = True
            elif 'int32' in dtype:
                ivars.append(dict(type='int32'))
                use_options = True
            else:
                ivars.append({})
        if use_options:
            return importoptions

    def _read_any(self, _method_, *args, **kwargs):
        '''
        Generic data file reader

        Parameters
        ----------
        _method_ : string
            The name of the pandas data reader function.
        *args : one or more arguments
            Arguments to pass to the data reader.
        **kwargs : keyword arguments
            Keyword arguments to pass to the data reader function.
            The keyword parameters 'table', 'caslib', 'promote', and
            'replace' will be stripped to use for the output CAS
            table parameters.

        Returns
        -------
        :class:`CASTable`

        '''
        import pandas as pd
        use_addtable = kwargs.pop('use_addtable', False)
        table, kwargs = self._get_table_args(**kwargs)
        dframe = getattr(pd, _method_)(*args, **kwargs)
        # REST doesn't support table.addtable
        if not use_addtable or self._protocol.startswith('http'):
            if 'table' in table:
                table['name'] = table.pop('table')
            return self.upload_frame(dframe, casout=table and table or None)
#                                    importoptions=self._importoptions_from_dframe(dframe)
        from swat import datamsghandlers as dmh
        table.update(dmh.PandasDataFrame(dframe).args.addtable)
        return self.retrieve('table.addtable', **table).casTable

    def read_pickle(self, path, casout=None, **kwargs):
        '''
        Load pickled pandas object from the specified path

        This method calls :func:`pandas.read_pickle` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        path : string
            Path to a local pickle file.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_pickle`.

        Notes
        -----
        Paths to specified files point to files on the **client machine**.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_pickle('dataframe.pkl')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_pickle`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_pickle', path, casout=casout, **kwargs)

    def read_table(self, filepath_or_buffer, casout=None, **kwargs):
        '''
        Read general delimited file into a CAS table

        This method calls :func:`pandas.read_table` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        filepath_or_buffer : str or any object with a read() method
            Path, URL, or buffer to read.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_table`.

        Notes
        -----
        Paths to specified files point to files on the client machine.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_table('iris.tsv')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_table`
        :meth:`upload_file`

        Returns
        -------
        :class:`CASTable`

        '''
        use_addtable = kwargs.pop('use_addtable', False)
        table, kwargs = self._get_table_args(casout=casout, **kwargs)
        # REST doesn't support table.addtable
        if not use_addtable or self._protocol.startswith('http'):
            import pandas as pd
            dframe = pd.read_table(filepath_or_buffer, **kwargs)
            if 'table' in table:
                table['name'] = table.pop('table')
            return self.upload_frame(dframe, casout=table and table or None)
#                                    importoptions=self._importoptions_from_dframe(dframe)
        from swat import datamsghandlers as dmh
        table.update(dmh.Text(filepath_or_buffer, **kwargs).args.addtable)
        return self.retrieve('table.addtable', **table).casTable

    def read_csv(self, filepath_or_buffer, casout=None, **kwargs):
        '''
        Read CSV file into a CAS table

        This method calls :func:`pandas.read_csv` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        filepath_or_buffer : str or any object with a read() method
            Path, URL, or buffer to read.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_csv`.

        Notes
        -----
        Paths to specified files point to files on the client machine.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_csv('iris.csv')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_table`
        :meth:`upload_file`

        Returns
        -------
        :class:`CASTable`

        '''
        use_addtable = kwargs.pop('use_addtable', False)
        table, kwargs = self._get_table_args(casout=casout, **kwargs)
        # REST doesn't support table.addtable
        if not use_addtable or self._protocol.startswith('http'):
            import pandas as pd
            dframe = pd.read_csv(filepath_or_buffer, **kwargs)
            if 'table' in table:
                table['name'] = table.pop('table')
            return self.upload_frame(dframe, casout=table and table or None)
#                                    importoptions=self._importoptions_from_dframe(dframe)
        from swat import datamsghandlers as dmh
        table.update(dmh.CSV(filepath_or_buffer, **kwargs).args.addtable)
        return self.retrieve('table.addtable', **table).casTable

    def read_frame(self, dframe, casout=None, **kwargs):
        '''
        Convert DataFrame to CAS table

        Parameters
        ----------
        dframe : DataFrame
            The DataFrame to read into CAS
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.

        Notes
        -----
        When `use_addtable=False` (the default) is specified, this method
        is equivalent to `upload_frame`.  If `use_addtable=True` is specified,
        the `table.addtable` CAS action is used and the DataFrame does not
        need to be written to disk first.  However, this mode can only be used
        with the binary (not REST) protocol.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_frame(pd.DataFrame(np.random.randn(100, 4),
        ...                       columns='ABCD'))
        >>> print(tbl.head())

        See Also
        --------
        :meth:`upload_frame`

        Returns
        -------
        :class:`CASTable`

        '''
        use_addtable = kwargs.pop('use_addtable', False)
        table, kwargs = self._get_table_args(casout=casout, **kwargs)
        # REST doesn't support table.addtable
        if not use_addtable or self._protocol.startswith('http'):
            if 'table' in table:
                table['name'] = table.pop('table')
            return self.upload_frame(dframe, casout=table and table or None)
#                                    importoptions=self._importoptions_from_dframe(dframe)
        from swat import datamsghandlers as dmh
        table.update(dmh.PandasDataFrame(dframe, **kwargs).args.addtable)
        return self.retrieve('table.addtable', **table).casTable

    def read_fwf(self, filepath_or_buffer, casout=None, **kwargs):
        '''
        Read a table of fixed-width formatted lines into a CAS table

        This method calls :func:`pandas.read_fwf` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        filepath_or_buffer : str or any object with a read() method
            Path, URL, or buffer to read.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_table`.

        Notes
        -----
        Paths to specified files point to files on the client machine.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_table('iris.dat')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_table`
        :meth:`upload_file`

        Returns
        -------
        :class:`CASTable`

        '''
        use_addtable = kwargs.pop('use_addtable', False)
        table, kwargs = self._get_table_args(casout=casout, **kwargs)
        # REST doesn't support table.addtable
        if not use_addtable or self._protocol.startswith('http'):
            import pandas as pd
            dframe = pd.read_fwf(filepath_or_buffer, **kwargs)
            if 'table' in table:
                table['name'] = table.pop('table')
            return self.upload_frame(dframe, casout=table and table or None)
#                                    importoptions=self._importoptions_from_dframe(dframe)
        from swat import datamsghandlers as dmh
        table.update(dmh.FWF(filepath_or_buffer, **kwargs).args.addtable)
        return self.retrieve('table.addtable', **table).casTable

    def read_clipboard(self, casout=None, **kwargs):
        '''
        Read text from clipboard and pass to :meth:`read_table`

        Parameters
        ----------
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_table`.

        See Also
        --------
        :func:`pandas.read_clipboard`
        :func:`pandas.read_table`
        :meth:`read_table`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_clipboard', casout=casout, **kwargs)

    def read_excel(self, io, casout=None, **kwargs):
        '''
        Read an Excel table into a CAS table

        This method calls :func:`pandas.read_excel` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        io : string or path object
            File-like object, URL, or pandas ExcelFile.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_table`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_excel('iris.xlsx')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_excel`
        :meth:`upload_file`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_excel', io, casout=casout, **kwargs)

    def read_json(self, path_or_buf=None, casout=None, **kwargs):
        '''
        Read a JSON string into a CAS table

        This method calls :func:`pandas.read_json` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        path_or_buf : string or file-like object
            The path, URL, or file object that contains the JSON data.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_table`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_json('iris.json')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_json`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_json', path_or_buf, casout=casout, **kwargs)

    def json_normalize(self, data, casout=None, **kwargs):
        '''
        "Normalize" semi-structured JSON data into a flat table and upload to a CAS table

        This method calls :func:`pandas.json_normalize` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        data : dict or list of dicts
            Unserialized JSON objects
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.json_normalize`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.json_normalize('iris.json')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.json_normalize`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('json_normalize', data, casout=casout, **kwargs)

    def read_html(self, io, casout=None, **kwargs):
        '''
        Read HTML tables into a list of CASTable objects

        This method calls :func:`pandas.read_html` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        io : string or file-like object
            The path, URL, or file object that contains the HTML data.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_html`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_html('iris.html')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_html`

        Returns
        -------
        :class:`CASTable`

        '''
        import pandas as pd
        from swat import datamsghandlers as dmh
        use_addtable = kwargs.pop('use_addtable', False)
        out = []
        table, kwargs = self._get_table_args(casout=casout, **kwargs)
        for i, dframe in enumerate(pd.read_html(io, **kwargs)):
            if i and table.get('table'):
                table['table'] += str(i)
            if not use_addtable or self._protocol.startswith('http'):
                out.append(self.upload_frame(dframe, casout=table and table or None))
#                                            importoptions=self._importoptions_from_dframe(dframe)
            else:
                table.update(dmh.PandasDataFrame(dframe).args.addtable)
                out.append(self.retrieve('table.addtable', **table).casTable)
        return out

    def read_hdf(self, path_or_buf, casout=None, **kwargs):
        '''
        Read from the HDF store and create a CAS table

        This method calls :func:`pandas.read_hdf` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        path_or_buf : string or file-like object
            The path, URL, or file object that contains the HDF data.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_hdf`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_hdf('iris.html')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_hdf`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_hdf', path_or_buf, casout=casout, **kwargs)

    def read_sas(self, filepath_or_buffer, casout=None, **kwargs):
        '''
        Read SAS files stored as XPORT or SAS7BDAT into a CAS table

        This method calls :func:`pandas.read_sas` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        filepath_or_buffer : string or file-like object
            The path, URL, or file object that contains the HDF data.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_sas`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_sas('iris.sas7bdat')
        >>> print(tbl.head())

        See Also
        --------
        :func:`pandas.read_sas`
        :meth:`upload_file`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_sas', filepath_or_buffer, casout=casout, **kwargs)

    def read_sql_table(self, table_name, con, casout=None, **kwargs):
        '''
        Read SQL database table into a CAS table

        This method calls :func:`pandas.read_sql_table` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        table_name : string
            Name of SQL table in database.
        con : SQLAlchemy connectable (or database string URI)
            Database connection.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_sql_table`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_sql_table('iris', dbcon)
        >>> print(tbl.head())

        Notes
        -----
        The data from the database will be pulled to the client machine
        in the form of a :class:`pandas.DataFrame` then uploaded to CAS.
        If you are moving large amounts of data, you may want to use
        a direct database connecter from CAS.

        See Also
        --------
        :func:`pandas.read_sql_table`
        :meth:`read_sql_query`
        :meth:`read_sql`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_sql_table', table_name, con, casout=casout, **kwargs)

    def read_sql_query(self, sql, con, casout=None, **kwargs):
        '''
        Read SQL query table into a CAS table

        This method calls :func:`pandas.read_sql_query` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        sql : string
            SQL to be executed.
        con : SQLAlchemy connectable (or database string URI)
            Database connection.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_sql_query`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_sql_query('select * from iris', dbcon)
        >>> print(tbl.head())

        Notes
        -----
        The data from the database will be pulled to the client machine
        in the form of a :class:`pandas.DataFrame` then uploaded to CAS.
        If you are moving large amounts of data, you may want to use
        a direct database connecter from CAS.

        See Also
        --------
        :func:`pandas.read_sql_query`
        :meth:`read_sql_table`
        :meth:`read_sql`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_sql_query', sql, con, casout=casout, **kwargs)

    def read_sql(self, sql, con, casout=None, **kwargs):
        '''
        Read SQL query or database table into a CAS table

        This method calls :func:`pandas.read_sql` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        sql : string
            SQL to be executed or table name.
        con : SQLAlchemy connectable (or database string URI)
            Database connection.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_sql`.

        Examples
        --------
        >>> conn = swat.CAS()
        >>> tbl = conn.read_sql('select * from iris', dbcon)
        >>> print(tbl.head())

        Notes
        -----
        The data from the database will be pulled to the client machine
        in the form of a :class:`pandas.DataFrame` then uploaded to CAS.
        If you are moving large amounts of data, you may want to use
        a direct database connecter from CAS.

        See Also
        --------
        :func:`pandas.read_sql`
        :meth:`read_sql_table`
        :meth:`read_sql_query`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_sql', sql, con, casout=casout, **kwargs)

    def read_gbq(self, query, casout=None, **kwargs):
        '''
        Load data from a Google BigQuery into a CAS table

        This method calls :func:`pandas.read_gbq` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        query : string
            SQL-like query to return data values.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_gbq`.

        See Also
        --------
        :func:`pandas.read_gbq`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_gbq', query, casout=casout, **kwargs)

    def read_stata(self, filepath_or_buffer, casout=None, **kwargs):
        '''
        Read Stata file into a CAS table

        This method calls :func:`pandas.read_stata` with the
        given arguments, then uploads the resulting :class:`pandas.DataFrame`
        to a CAS table.

        Parameters
        ----------
        filepath_or_buffer : string or file-like object
            Path to .dta file or file-like object containing data.
        casout : string or :class:`CASTable`, optional
            The output table specification.  This includes the following parameters.
                name : string, optional
                    Name of the output CAS table.
                caslib : string, optional
                    CASLib for the output CAS table.
                label : string, optional
                    The label to apply to the output CAS table.
                promote : boolean, optional
                    If True, the output CAS table will be visible in all sessions.
                replace : boolean, optional
                    If True, the output CAS table will replace any existing CAS.
                    table with the same name.
        **kwargs : any, optional
            Keyword arguments to :func:`pandas.read_stata`.

        See Also
        --------
        :func:`pandas.read_stata`

        Returns
        -------
        :class:`CASTable`

        '''
        return self._read_any('read_stata', filepath_or_buffer, casout=casout, **kwargs)

    def path_to_caslib(self, path, name=None, **kwargs):
        '''
        Return a caslib name for a given path

        If a caslib does not exist for the current path or for a parent
        path, a new caslib will be created.

        Parameters
        ----------
        path : string
            The absolute path to the desired caslib directory
        name : string, optional
            The name to give to the caslib, if a new one is created
        kwargs : keyword-parameter, optional
            Additional parameters to use when creating a new caslib

        Returns
        -------
        ( caslib-name, relative-path )
            The return value is a two-element tuple.  The first element
            is the name of the caslib.  The second element is the relative
            path to the requested directory from the caslib.  The second
            element will be blank if the given path matches a caslib,
            or a new caslib is created.

        '''
        if not name:
            name = 'Caslib_%x' % random.randint(0, 1e9)

        activeonadd_key = None
        subdirectories_key = None
        datasource_key = None

        for key, value in kwargs.items():
            if key.lower() == 'activeonadd':
                activeonadd_key = key
            elif key.lower() == 'subdirectories':
                subdirectories_key = key
            elif key.lower() == 'datasource':
                datasource_key = key

        if not activeonadd_key:
            activeonadd_key = 'activeonadd'
            kwargs[activeonadd_key] = False
        if not subdirectories_key:
            subdirectories_key = 'subdirectories'
            kwargs[subdirectories_key] = True
        if not datasource_key:
            datasource_key = 'datasource'
            kwargs[datasource_key] = dict(srctype='path')

        is_windows = self.server_type.startswith('win')

        if is_windows:
            sep = '\\'
            normpath = path.lower()
        else:
            sep = '/'
            normpath = path

        if normpath.endswith(sep):
            normpath = normpath[:-1]

        info = self.retrieve('table.caslibinfo',
                             _messagelevel='error')['CASLibInfo']

        for libname, item, subdirs in zip(info['Name'], info['Path'],
                                          info['Subdirs']):
            if item.endswith(sep):
                item = item[:-1]
            if is_windows:
                item = item.lower()
            if item == normpath:
                if bool(subdirs) != bool(kwargs[subdirectories_key]):
                    raise SWATError('caslib exists, but subdirectories flag differs')
                return libname, ''
            elif normpath.startswith(item):
                if bool(subdirs) != bool(kwargs[subdirectories_key]):
                    raise SWATError('caslib exists, but subdirectories flag differs')
                return libname, path[len(item) + 1:]

        out = self.retrieve('table.addcaslib', _messagelevel='error',
                            name=name, path=path, **kwargs)

        if out.severity > 1:
            raise SWATError(out.status)

        return name, ''


def getone(connection, datamsghandler=None):
    '''
    Get a single response from a connection

    Parameters
    ----------
    connection : :class:`CAS` object
        The connection/CASAction to get the response from.
    datamsghandler : :class:`CASDataMsgHandler` object, optional
        The object to use for data messages from the server.

    Examples
    --------
    >>> conn = swat.CAS()
    >>> conn.invoke('serverstatus')
    >>> print(getone(conn))

    See Also
    --------
    :meth:`CAS.invoke`

    Returns
    -------
    :class:`CASResponse` object

    '''
    output = None, connection

    # enable data messages as needed
    if datamsghandler is not None:
        errorcheck(connection._sw_connection.enableDataMessages(),
                   connection._sw_connection)

    _sw_message = errorcheck(connection._sw_connection.receive(),
                             connection._sw_connection)
    if _sw_message:
        mtype = _sw_message.getType()
        if mtype == 'response':
            _sw_response = errorcheck(_sw_message.toResponse(
                connection._sw_connection), _sw_message)
            if _sw_response is not None:
                output = CASResponse(_sw_response, connection=connection), connection
        elif mtype == 'request' and datamsghandler is not None:
            _sw_request = errorcheck(_sw_message.toRequest(
                connection._sw_connection), _sw_message)
            if _sw_request is not None:
                req = CASRequest(_sw_request)
                output = datamsghandler(req, connection)
        elif mtype == 'request':
            _sw_request = errorcheck(_sw_message.toRequest(
                connection._sw_connection), _sw_message)
            if _sw_request is not None:
                req = CASRequest(_sw_request)
                output = req, connection

    if datamsghandler is not None:
        errorcheck(connection._sw_connection.disableDataMessages(),
                   connection._sw_connection)

    # Raise exception as needed
    if isinstance(output[0], CASResponse):
        exception_on_severity = get_option('cas.exception_on_severity')
        if exception_on_severity is not None and \
                output[0].disposition.severity >= exception_on_severity:
            raise SWATCASActionError(output[0].disposition.status, output[0], output[1])

    return output


def getnext(*objs, **kwargs):
    '''
    Return responses as they appear from multiple connections

    Parameters
    ----------
    *objs : :class:`CAS` objects and/or :class:`CASAction` objects
        Connection/CASAction objects to watch for responses.
    timeout : int, optional
        Timeout for waiting for a response on each connection.
    datamsghandler : :class:`CASDataMsgHandler` object, optional
        The object to use for data messages from the server.

    Examples
    --------
    >>> conn1 = swat.CAS()
    >>> conn2 = swat.CAS()
    >>> conn1.invoke('serverstatus')
    >>> conn2.invoke('userinfo')
    >>> for resp in getnext(conn1, conn2):
    ...     for k, v in resp:
    ...         print(k, v)

    See Also
    --------
    :meth:`CAS.invoke`

    Returns
    -------
    :class:`CASResponse` object

    '''
    timeout = kwargs.get('timeout', 0)
    datamsghandler = kwargs.get('datamsghandler')

    if len(objs) == 1 and isinstance(objs[0], (list, tuple, set)):
        connections = list(objs[0])
    else:
        connections = list(objs)

    # if the item in a CASAction, use the connection
    for i, conn in enumerate(connections):
        if isinstance(conn, CASAction):
            conn.invoke()
            connections[i] = conn.get_connection()

    # TODO: Set timeouts; check for mixed connection types
    if isinstance(connections[0]._sw_connection, rest.REST_CASConnection):
        for item in connections:
            yield getone(item)
        return

    _sw_watcher = errorcheck(clib.SW_CASConnectionEventWatcher(len(connections), timeout,
                                                               a2n(connections[
                                                                   0]._soptions),
                                                               connections[0]._sw_error),
                             connections[0]._sw_error)

    for item in connections:
        errorcheck(_sw_watcher.addConnection(item._sw_connection), _sw_watcher)

    try:

        while True:
            i = errorcheck(_sw_watcher.wait(), _sw_watcher)

            # finished
            if i == -2:
                break

            # timeout / retry
            if i == -1:
                yield [], None

            yield getone(connections[i], datamsghandler=datamsghandler)

    except (KeyboardInterrupt, SystemExit):
        for conn in connections:
            errorcheck(conn._sw_connection.stopAction(), conn._sw_connection)
        raise


def dir_actions(obj):
    ''' Return list of CAS actionsets / actions associated with the object '''
    if hasattr(obj, '__dir_actions__'):
        return obj.__dir_actions__()
    return []


def dir_members(obj):
    ''' Return list of members not including associated CAS actionsets / actions '''
    if hasattr(obj, '__dir_members__'):
        return obj.__dir_members__()
    return dir(obj)
