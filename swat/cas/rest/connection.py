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
Class for creating REST CAS sessions

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import json
import os
import re
import requests
import six
import ssl
import sys
from six.moves import urllib
from .message import REST_CASMessage
from .response import REST_CASResponse
from ..types import blob
from ..table import CASTable
from ...config import options, get_option
from ...exceptions import SWATError
from ...utils.args import parsesoptions
from ...utils.keyword import keywordify
from ...utils.compat import (a2u, int_types, int32_types, int64_types, dict_types,
                             float64_types, items_types, int32, int64, float64)
from ...utils.authinfo import query_authinfo

# pylint: disable=C0330


def _print_request(rtype, url, headers, data=None):
    ''' Print debugging information of request '''
    sys.stderr.write('%s %s\n' % (rtype, url))
    if get_option('cas.debug.request_bodies'):
        for key, value in six.iteritems(headers):
            sys.stderr.write('%s: %s\n' % (key, value))
        if headers or data:
            sys.stderr.write('\n')
        if data:
            sys.stderr.write(a2u(data, 'utf-8'))
            sys.stderr.write('\n')
            sys.stderr.write('\n')


def _print_response(text):
    ''' Print the respnose for debugging '''
    sys.stderr.write(a2u(text, 'utf-8'))
    sys.stderr.write('\n')


def _print_params(params, prefix=''):
    ''' Print parameters for tracing actions '''
    for key, value in sorted(six.iteritems(params)):
        if isinstance(value, dict_types):
            _print_params(value, prefix='%s%s.' % (prefix, key))
        elif isinstance(value, items_types):
            _print_params_list(value, prefix='%s%s.' % (prefix, key))
        else:
            print('%s%s = %s' % (prefix, key, repr(value)))


def _print_params_list(plist, prefix=''):
    ''' Print parameter list for tracing actions '''
    if plist:
        for i, item in enumerate(plist):
            if isinstance(item, dict_types):
                _print_params(item, prefix='%s[%s].' % (prefix, i))
            elif isinstance(item, items_types):
                _print_params_list(item, prefix='%s[%s].' % (prefix, i))
    else:
        if prefix.endswith('.'):
            print('%s = []' % prefix[:-1])
        else:
            print('%s = []' % prefix)


def _normalize_params(params):
    '''
    Normalize action parameters

    Make sure that dictionaries only contain string keys and
    that other non-JSON serializable objects (like sets) are
    converted to standard structures.

    Parameters
    ----------
    params : dict
        Dictionary of action parameters

    Returns
    -------
    dict
        Normalized action parameters

    '''
    out = {}
    for key, value in params.items():
        key = keywordify(key)
        if value is None:
            continue
        elif value is True:
            pass
        elif value is False:
            pass
        elif isinstance(value, dict_types):
            numkeys = [x for x in value.keys() if isinstance(x, int_types)]
            if not numkeys:
                value = _normalize_params(value)
            else:
                value = _normalize_list(value.values())
        elif isinstance(value, items_types):
            value = _normalize_list(value)
        elif isinstance(value, CASTable):
            value = _normalize_params(value.to_params())
        elif isinstance(value, int64_types):
            value = int64(value)
        elif isinstance(value, int32_types):
            value = int32(value)
        elif isinstance(value, int32_types):
            value = int32(value)
        elif isinstance(value, float64_types):
            value = float64(value)
        elif isinstance(value, blob):
            b64data = base64.b64encode(value)
            value = dict(_blob=True, data=a2u(b64data), length=len(value))
        out[key] = value
    return out


def _normalize_list(items):
    ''' Normalize objects using standard python types '''
    newitems = []
    for item in items:
        if isinstance(item, dict_types):
            item = _normalize_params(item)
        elif isinstance(item, items_types):
            item = _normalize_list(item)
        elif isinstance(item, CASTable):
            item = _normalize_params(item.to_params())
        newitems.append(item)
    return newitems


class SSLContextAdapter(requests.adapters.HTTPAdapter):
    ''' HTTPAdapter that uses the default SSL context on the machine '''

    def init_poolmanager(self, connections, maxsize,
                         block=requests.adapters.DEFAULT_POOLBLOCK, **pool_kwargs):
        context = ssl.create_default_context()
        pool_kwargs['ssl_context'] = context
        return super(SSLContextAdapter, self).init_poolmanager(connections,
                                                               maxsize, block,
                                                               **pool_kwargs)


class REST_CASConnection(object):
    '''
    Create a REST CAS connection

    Parameters
    ----------
    hostname : string
        The REST CAS host
    port : int
        The REST CAS port
    username : string
        The CAS username
    password : string
        The CAS password
    soptions : string
        The string containing connection options
    error : REST_CASError
        The object to use for error messages

    Returns
    -------
    REST_CASConnection object

    '''

    def __init__(self, hostname, port, username, password, soptions, error):

        _soptions = parsesoptions(soptions)
        protocol = _soptions.get('protocol', 'http')
        session = _soptions.get('session')
        locale = _soptions.get('locale')

        self._orig_hostname = hostname
        self._orig_port = port

        if isinstance(hostname, six.string_types):
            hostname = re.split(r'\s+', hostname.strip())
        else:
            hostname = list(hostname)

        if hostname[0].startswith('http:') or hostname[0].startswith('https:'):
            protocol = hostname[0].split(':', 1)[0]
            self._baseurl = hostname
            self._hostname = []
            self._port = []
            for host in hostname:
                urlparts = urllib.parse.urlparse(host)
                self._hostname.append(urlparts.hostname)
                self._port.append(urlparts.port or port)

        else:
            self._baseurl = []
            self._hostname = []
            self._port = []
            for host in hostname:
                path = ''
                # If host contains a path, split them apart first
                if '/' in host:
                    host, path = host.split('/', 1)
                # Rebuild URL from pieces
                self._baseurl.append('%s://%s:%d' % (protocol, host, port))
                if path:
                    self._baseurl[-1] = self._baseurl[-1] + '/' + path
                self._hostname.append(host)
                self._port.append(port)

        self._host_index = 0
        self._current_hostname = self._hostname[self._host_index]
        self._current_baseurl = self._baseurl[self._host_index]
        self._current_port = self._port[self._host_index]

        authinfo = None
        if password and password.startswith('authinfo={'):
            authinfo = password[11:-2]
            authinfo = authinfo.split('}{')
            authinfo = query_authinfo(host=self._current_hostname, user=username,
                                      protocol=self._current_port, path=authinfo)
        elif not password:
            authinfo = query_authinfo(host=self._current_hostname, user=username,
                                      protocol=self._current_port)

        if authinfo is not None:
            hostname = authinfo.get('host', hostname)
            port = authinfo.get('protocol', port)
            username = authinfo.get('user', username)
            password = authinfo.get('password')

        self._username = username
        self._soptions = soptions
        self._error = error
        self._results = None

        self._auth = b'Basic ' + base64.b64encode(
            ('%s:%s' % (username, password)).encode('utf-8')).strip()

        self._req_sess = requests.Session()

        if 'SSLCALISTLOC' in os.environ:
            self._req_sess.verify = os.path.expanduser(os.environ['SSLCALISTLOC'])
        elif 'CAS_CLIENT_SSL_CA_LIST' in os.environ:
            self._req_sess.verify = os.path.expanduser(os.environ['CAS_CLIENT_SSL_CA_LIST'])
        elif 'REQUESTS_CA_BUNDLE' not in os.environ:
            self._req_sess.mount('https://', SSLContextAdapter())

        if os.environ.get('SSLREQCERT', 'y').lower().startswith('n'):
            self._req_sess.verify = False

        self._req_sess.headers.update({
            'Content-Type': 'application/json',
            'Content-Length': '0',
            'Authorization': self._auth,
        })

        while True:
            try:
                if session:
                    url = urllib.parse.urljoin(self._current_baseurl,
                                               'cas/sessions/%s' % session)

                    if get_option('cas.debug.requests'):
                        _print_request('GET', url, self._req_sess.headers)

                    res = self._req_sess.get(url, data=b'')

                    if get_option('cas.debug.responses'):
                        _print_response(res.text)

                    try:
                        txt = a2u(res.text, 'utf-8')
                        out = json.loads(txt, strict=False)
                    except Exception:
                        sys.stderr.write(txt)
                        sys.stderr.write('\n')
                        raise

                    if out.get('error', None):
                        if out.get('details', None):
                            raise SWATError('%s (%s)' % (out['error'], out['details']))
                        raise SWATError(out['error'])

                    self._session = out['uuid']
                    break

                else:
                    url = urllib.parse.urljoin(self._current_baseurl,
                                               'cas/sessions')

                    if get_option('cas.debug.requests'):
                        _print_request('PUT', url, self._req_sess.headers)

                    res = self._req_sess.put(url, data=b'')

                    if get_option('cas.debug.responses'):
                        _print_response(res.text)

                    try:
                        txt = a2u(res.text, 'utf-8')
                        out = json.loads(txt, strict=False)
                    except Exception:
                        sys.stderr.write(txt)
                        sys.stderr.write('\n')
                        raise

                    if out.get('error', None):
                        if out.get('details', None):
                            raise SWATError('%s (%s)' % (out['error'], out['details']))
                        raise SWATError(out['error'])

                    self._session = out['session']

                    if locale:
                        self.invoke('session.setlocale', dict(locale=locale))
                        if self._results.get('disposition', {})\
                                .get('severity', '') == 'Error':
                            raise SWATError(self._results.get('disposition', {})
                                            .get('formattedStatus',
                                                 'Invalid locale: %s' % locale))
                        self._results.clear()
                    break

            except requests.ConnectionError:
                self._set_next_connection()

            except Exception as exc:
                raise SWATError(str(exc))

    def _set_next_connection(self):
        ''' Iterate to the next available controller '''
        self._host_index += 1
        try:
            self._current_hostname = self._hostname[self._host_index]
            self._current_baseurl = self._baseurl[self._host_index]
            self._current_port = self._port[self._host_index]
        except IndexError:
            self._current_hostname = ''
            self._current_baseurl = ''
            self._current_port = -1
            raise SWATError('Unable to connect to any URL: %s' %
                            ', '.join(self._baseurl))

    def invoke(self, action_name, kwargs):
        '''
        Invoke an action

        Parameters
        ----------
        action_name : string
            The name of the action
        kwargs : dict
            The dictionary of action parameters

        Returns
        -------
        `self`

        '''
        is_ui = kwargs.get('_apptag', '') == 'UI'
        kwargs = json.dumps(_normalize_params(kwargs))

        if options.cas.trace_actions and \
                (not(is_ui) or (is_ui and options.cas.trace_ui_actions)):
            print('[%s]' % action_name)
            _print_params(json.loads(kwargs), prefix='    ')
            print('')

        post_data = a2u(kwargs).encode('utf-8')
        self._req_sess.headers.update({
            'Content-Type': 'application/json',
            'Content-Length': str(len(post_data)),
        })

        result_id = None

        txt = ''
        while True:
            try:
                url = urllib.parse.urljoin(self._current_baseurl,
                                           'cas/sessions/%s/actions/%s' %
                                           (self._session, action_name))

                if get_option('cas.debug.requests'):
                    _print_request('POST', url, self._req_sess.headers, post_data)

                res = self._req_sess.post(url, data=post_data)

                if get_option('cas.debug.responses'):
                    _print_response(res.text)

                res = res.text
                break

            except requests.ConnectionError:
                self._set_next_connection()

                # Get ID of results
                action_name = 'session.listresults'
                post_data = a2u('').encode('utf-8')
                self._req_sess.headers.update({
                    'Content-Type': 'application/json',
                    'Content-Length': str(len(post_data)),
                })

                url = urllib.parse.urljoin(self._current_baseurl,
                                           'cas/sessions/%s/actions/%s' %
                                           (self._session, action_name))

                if get_option('cas.debug.requests'):
                    _print_request('POST', url, self._req_sess.headers, post_data)

                res = self._req_sess.post(url, data=post_data)

                if get_option('cas.debug.responses'):
                    _print_response(res.text)

                try:
                    txt = a2u(res.text, 'utf-8')
                    out = json.loads(txt, strict=False)
                except Exception:
                    sys.stderr.write(txt)
                    sys.stderr.write('\n')
                    raise

                result_id = out['results']['Queued Results']['rows'][0][0]

                # Setup retrieval of results from ID
                action_name = 'session.fetchresult'
                post_data = a2u('{"id":%s}' % result_id).encode('utf-8')
                self._req_sess.headers.update({
                    'Content-Type': 'application/json',
                    'Content-Length': str(len(post_data)),
                })

            except Exception as exc:
                raise SWATError(str(exc))

        try:
            txt = a2u(res, 'utf-8')
            self._results = json.loads(txt, strict=False)
        except Exception:
            sys.stderr.write(txt)
            sys.stderr.write('\n')
            raise

        try:
            if self._results.get('disposition', None) is None:
                if self._results.get('error'):
                    raise SWATError(self._results['error'])
                else:
                    raise SWATError('Unknown error')
        except ValueError as exc:
            raise SWATError(str(exc))

        return self

    def receive(self):
        ''' Retrieve the next message from the server '''
        out = REST_CASMessage(self._results, connection=self)
        self._results = {}
        return out

    def getTypeName(self):
        ''' Get the object type '''
        return 'connection'

    def getSOptions(self):
        ''' Get the SOptions string '''
        return self._soptions

    def destroy(self):
        ''' Destroy the connection '''
        self.close()

    def isNULL(self):
        ''' Is this a NULL object? '''
        return False

    def isConnected(self):
        ''' Are we still connected? '''
        return True

    def hasPendingResponses(self):
        ''' Do we have pending responses? '''
        return False

    def setZeroIndexedParameters(self):
        ''' Declare the interface as a zero-indexed language '''
        return

    def copy(self):
        ''' Copy the connection object '''
        username, password = base64.b64decode(self._auth.split(b' ', 1)[-1]).split(b':', 1)
        return type(self)(self._orig_hostname, self._orig_port,
                          a2u(username), a2u(password),
                          self._soptions,
                          self._error)

    def getHostname(self):
        ''' Get the connection hostname '''
        return self._current_hostname

    def getUsername(self):
        ''' Get the connection username '''
        return self._username

    def getPort(self):
        ''' Get the connection port '''
        return self._current_port

    def getSession(self):
        ''' Get the connection session ID '''
        return self._session

    def close(self):
        ''' Close the connection '''
        if self._session and self._req_sess is not None:
            self._req_sess.headers.update({
                'Content-Type': 'application/json',
                'Content-Length': '0',
            })

            url = urllib.parse.urljoin(self._current_baseurl,
                                       'cas/sessions/%s' % self._session)

            if get_option('cas.debug.requests'):
                _print_request('DELETE', url, self._req_sess.headers)

            res = self._req_sess.delete(url, data=b'')

            self._session = None
            return res.status_code

    def upload(self, file_name, params):
        ''' Upload a data file '''
        with open(file_name, 'rb') as datafile:
            data = datafile.read()

        self._req_sess.headers.update({
            'Content-Type': 'application/octet-stream',
            'Content-Length': str(len(data)),
            'JSON-Parameters': json.dumps(_normalize_params(params))
        })

        while True:
            try:
                url = urllib.parse.urljoin(self._current_baseurl,
                                           'cas/sessions/%s/actions/table.upload' %
                                           self._session)

                if get_option('cas.debug.requests'):
                    _print_request('DELETE', url, self._req_sess.headers, data)

                res = self._req_sess.put(url, data=data)

                if get_option('cas.debug.responses'):
                    _print_response(res.text)

                res = res.text
                break

            except requests.ConnectionError:
                self._set_next_connection()

            except Exception as exc:
                raise SWATError(str(exc))

            finally:
                del self._req_sess.headers['JSON-Parameters']

        try:
            txt = a2u(res, 'utf-8')
            out = json.loads(txt, strict=False)
        except Exception:
            sys.stderr.write(txt)
            sys.stderr.write('\n')
            raise

        try:
            if out.get('disposition', None) is None:
                if out.get('error'):
                    raise SWATError(self._results['error'])
                else:
                    raise SWATError('Unknown error')
            return REST_CASResponse(out)
        except ValueError as exc:
            raise SWATError(str(exc))

    def stopAption(self):
        ''' Stop the current action '''
        return

    def getOptionType(self, option):
        ''' Get the option type '''
        return

    def getBooleanOption(self, option):
        ''' Get the value of a boolean option '''
        return

    def setBooleanOption(self, option, value):
        ''' Set the value of a boolean option '''
        return

    def getInt32Option(self, value):
        ''' Get the value of an int32 option '''
        return

    def setInt32Option(self, option, value):
        ''' Set the value of an int32 option '''
        return

    def getInt64Option(self, option):
        ''' Get the value of an int64 option '''
        return

    def setInt64Option(self, option, value):
        ''' Set the value of an int64 option '''
        return

    def getStringOption(self, option):
        ''' Get the value of a string option '''
        return

    def setStringOption(self, option, value):
        ''' Set the value of a string option '''
        return

    def getDoubleOption(self, option):
        ''' Get the value of a double option '''
        return

    def setDoubleOption(self, option, value):
        ''' Set the value of a double option '''
        return

    def enableDataMessages(self):
        ''' Enable data messages from the server '''
        return

    def disableDataMessages(self):
        ''' Disable data messages from the server '''
        return

    def getLastErrorMessage(self):
        ''' Retrieve the last generated error message '''
        return ''
