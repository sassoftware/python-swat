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
Mocks for testing

'''

from unittest import mock

import swat

SESSION_ABORTED_MESSAGE = "The Session is no longer active due to an unhandled exception."


def mock_getone_session_aborted(connection, datamsghandler=None):
    '''
    Mock getting a single "session aborted" response from a connection

    Parameters
    ----------
    connection : :class:`CAS` object
        The connection/CASAction to get the mock response from.
    datamsghandler : :class:`CASDataMsgHandler` object, optional
        The object to use for data messages from the server. This is not used in the mock.

    See Also
    --------
    :meth:`swat.cas.connection.getone`

    Returns
    -------
    :class:`CASResponse` object

    '''

    # Mock the CAS Response object
    with mock.patch('swat.cas.response.CASResponse', autospec=True):
        response = swat.cas.response.CASResponse(None)
        response.disposition.status = SESSION_ABORTED_MESSAGE
        response.disposition.status_code = swat.cas.connection.SESSION_ABORTED_CODE
        return response, connection
