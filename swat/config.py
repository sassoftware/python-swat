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
Initialization of SWAT options

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import datetime
import functools
import logging
import sys
import warnings
from . import logging as swat_logging
from .clib import InitializeTK
from .utils.config import (register_option, check_boolean, check_int, get_option,
                           set_option, reset_option, describe_option, check_url,
                           SWATOptionError, check_string, options, get_suboptions,
                           get_default, check_float, option_context)
from .utils.compat import a2n


class OptionWarning(UserWarning):
    ''' Warning class for all option warnings '''


#
# TK options
#


def set_tkpath(val):
    ''' Check and set the TK path '''
    if val is None:
        return
    path = check_string(val)
    InitializeTK(a2n(path, 'utf-8'))
    return path


def _initialize_tkpath():
    ''' Check for TKPATH locations '''
    import os
    # Ignore ';' as a path, it's most likely set by InitializeTK.
    if 'TKPATH' in os.environ and os.environ['TKPATH'].strip() != ';':
        return os.environ['TKPATH']

    import sys
    platform = 'linux'
    if sys.platform.lower().startswith('win'):
        platform = 'win'
    elif sys.platform.lower().startswith('darwin'):
        platform = 'mac'

    # See if the lib/<platform>/ directory has files in it.
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'lib', platform))
    if os.path.isdir(path) and len(os.listdir(path)) > 20:
        return path


def _is_interactive():
    ''' See if Python is running in interactive mode '''
    return bool(getattr(sys, 'ps1', sys.flags.interactive))


register_option('tkpath', 'string', set_tkpath, _initialize_tkpath(),
                'Displays the path for SAS TK components.  This is determined\n'
                'when SWAT is imported.  By default, it points to the platform\n'
                'directory under the swat.lib module.  It can be overridden by\n'
                'setting a TKPATH environment variable.')

#
# General options
#

register_option('encoding_errors', 'string', check_string, 'strict',
                'Specifies the error handler for encoding and decoding errors in\n'
                'handling strings.  Possible values are given in the Python\n'
                'documentation.  Typical values are strict, ignore, replace, or\n'
                'xmlcharrefreplace.')

register_option('interactive_mode', 'boolean', check_boolean, _is_interactive(),
                'Indicates whether all interactive mode features should be enabled.\n'
                'Interactive features include things like generating formatted help\n'
                'strings for objects automatically generated from information from\n'
                'the server.  This may give a performance improvement in batch jobs\n'
                'that don\'t need interactive features.',
                environ=['SWAT_INTERACTIVE_MODE',
                         'CAS_INTERACTIVE_MODE',
                         'SAS_INTERACTIVE_MODE'])


def check_tz(value):
    ''' Verify that the value is a tzinfo object '''
    if value is None:
        return
    if isinstance(value, datetime.tzinfo):
        return value
    try:
        import pytz
    except ImportError:
        raise SWATOptionError('The pytz package must be installed to convert '
                              'timezone names to tzinfo objects.')
    return pytz.timezone(value)


register_option('timezone', 'string or tzinfo', check_tz, None,
                'Specifies the timezone to use when computing dates and times.\n'
                'The default behavior is to treat dates and times as timezone-naive.',
                environ=['SWAT_TIMEZONE', 'CAS_TIMEZONE', 'SAS_TIMEZONE'])


#
# CAS connection options
#

register_option('cas.print_messages', 'boolean', check_boolean, True,
                'Indicates whether or not CAS response messages should be printed.',
                environ='CAS_PRINT_MESSAGES')

register_option('cas.trace_actions', 'boolean', check_boolean, False,
                'Indicates whether or not CAS action names and parameters should\n'
                'be printed.  This can be helpful in debugging incorrect action\n'
                'parameters.', environ='CAS_TRACE_ACTIONS')

register_option('cas.trace_ui_actions', 'boolean', check_boolean, False,
                'Indicates whether or not CAS action names and parameters from\n'
                'actions invoked by the interface itself should be printed.\n'
                'This option is only honored if cas.trace_actions is also enabled.',
                environ='CAS_TRACE_UI_ACTIONS')

register_option('cas.reflection_levels', 'int', functools.partial(check_int, minimum=1),
                get_option('interactive_mode') and 10 or 1,
                'Sets the level of reflection data returned when reflecting action sets\n'
                'and actions. This data is downloaded at the start of each connection\n'
                'as well as whenever a new action set is loaded. Reducing the number of\n'
                'levels downloaded can reduce the amount of data downloaded, but will\n'
                'also limit the amount of documentation shown in action doc strings.',
                environ='CAS_REFLECTION_LEVELS')

register_option('cas.hostname', 'string', check_string,
                'localhost',
                'Specifies the hostname or complete URL (including host, port,\n'
                'and protocol) for the CAS server.',
                environ=['CAS_URL', 'CAS_HOST', 'CAS_HOSTNAME'])

register_option('cas.username', 'string', check_string, None,
                'Specifies the username for the CAS server.',
                environ=['CAS_USER', 'CAS_USERNAME'])

register_option('cas.token', 'string', check_string, None,
                'Specifies the OAuth token / password for the CAS server.',
                environ=['CAS_TOKEN', 'CAS_PASSWORD'])

register_option('cas.port', 'int', check_int, 0,
                'Sets the port number for the CAS server.',
                environ='CAS_PORT')

register_option('cas.protocol', 'string',
                functools.partial(check_string,
                                  valid_values=['auto', 'cas', 'http', 'https']),
                'auto',
                'Communication protocol for talking to CAS server.\n'
                'The value of "auto" will try to auto-detect the type.\n'
                'Using "http" or "https" will use the REST interface.',
                environ='CAS_PROTOCOL')


def get_default_cafile():
    ''' Retrieve the default CA file in the ssl module '''
    import ssl
    get_paths = getattr(ssl, 'get_default_verify_paths', None)
    if get_paths:
        paths = get_paths()
        if hasattr(paths, 'openssl_cafile'):
            return paths.openssl_cafile
        if hasattr(paths, 'cafile'):
            return paths.cafile


register_option('cas.ssl_ca_list', 'string', check_string, get_default_cafile(),
                'Sets the path to the SSL certificates for the CAS server.',
                environ=['CAS_CLIENT_SSL_CA_LIST',
                         'SAS_TRUSTED_CA_CERTIFICATES_PEM_FILE',
                         'SSLCALISTLOC'])


def check_severity(sev):
    ''' Make sure the severity is None or an int '''
    if sev is None:
        return None
    return check_int(sev, maximum=2, minimum=0)


register_option('cas.exception_on_severity', 'int or None', check_severity, None,
                'Indicates the CAS action severity level at which an exception\n'
                'should be raised.  None means that no exception should be raised.\n'
                '1 would raise exceptions on warnings.  2 would raise exceptions\n'
                'on errors.')

#
# Integer missing value substitutions
#

register_option('cas.missing.int64', 'int', check_int, -2**(64 - 1),
                'Sets substitution value for int64 missing values.')

register_option('cas.missing.int32', 'int', check_int, -2**(32 - 1),
                'Sets substitution value for int32 missing values.')

register_option('cas.missing.date', 'int', check_int, -2**(32 - 1),
                'Sets substitution value for date missing values.')

register_option('cas.missing.time', 'int', check_int, -2**(64 - 1),
                'Sets substitution value for time missing values.')

register_option('cas.missing.datetime', 'int', check_int, -2**(64 - 1),
                'Sets substitution value for datetime missing values.')

#
# Tabular data options
#

register_option('cas.dataset.format', 'string',
                functools.partial(check_string,
                                  valid_values=['dataframe:sas', 'dataframe',
                                                'dict', 'dict:list',
                                                'dict:series', 'dict:split',
                                                'dict:records', 'tuple']),
                'dataframe:sas',
                'Data structure for tabular data returned from CAS.  The following\n'
                'formats are supported.\n'
                'dataframe:sas : Pandas Dataframe extended with SAS metadata such as\n'
                '    SAS data formats, titles, labels, etc.\n'
                'dataframe : Standard Pandas Dataframe\n'
                'dict : Dictionary like {column => {index => value}}\n'
                'dict:list : Dictionary like {column => [values]}\n'
                'dict:series : Dictionary like {column => pandas.Series(values)\n'
                'dict:split : Dictionary like {index => [index],\n'
                '                              columns => [columns],\n'
                '                              data => [values]}\n'
                'dict:records : List like [{column => value}, ... ,\n'
                '                          {column => value}]\n'
                'tuple : A tuple where each element is a tuple of the data values only.')

register_option('cas.dataset.auto_castable', 'boolean', check_boolean, True,
                'Should a column of CASTable objects be automatically\n'
                'created if a CASLib and CAS table name are columns in the data?\n'
                'NOTE: This applies to all except the \'tuples\' format.')


def check_string_list(val):
    ''' Verify that value is a string or list of strings '''
    if isinstance(val, (list, set, tuple)):
        for item in val:
            check_string(item)
        return val
    return check_string(val)


register_option('cas.dataset.date_formats', 'string or list of strings',
                check_string_list,
                ['B8601DA', 'DATE', 'DAY', 'DDMMYY.?', 'DOWNAME', 'E8601DA',
                 'EURDFDD', 'EURDFDE', 'EURDFDN', 'EURDFDWN', 'EURDFMN', 'EURDFMY',
                 'EURDFWDX', 'EURDFWKX', 'HDATE', 'HEBDATE', 'JULDAY', 'JULIAN',
                 'MINGUO', 'MMDDYY.?', 'MMYY.?', 'MONNAME', 'MONTH', 'MONYY',
                 'NENGO', 'NLDATE[A-Z]*', 'PDJULG', 'PDJULI', 'QTRR', 'QTR',
                 'WEEKDATE', 'WEEKDATX', 'WEEKDAY', 'WEEKU', 'WEEKV', 'WEEK',
                 'WORDDATE', 'WORDDATX', 'YEAR', 'YYMMDD.?', 'YYMM.?', 'YYMM.?',
                 'YYMON', 'YYQR.?', 'YYQZ', 'YYQ', 'YYQ.?', 'YYWEEKU', 'YYWEEKV',
                 'YYWEEKW'],
                'Format names used to indicate the column should be converted\n'
                'to a Python date object.')

register_option('cas.dataset.datetime_formats', 'string or list of strings',
                check_string_list,
                ['B8601DN', 'B8601DT', 'B8601DX', 'B8601DZ', 'B8601LX', 'B8601LZ',
                 'B8601TM', 'B8601TX', 'B8601TZ', 'DATEAMPM', 'DATETIME', 'DTDATE',
                 'DTMONYY', 'DTWEEKV', 'DTWKDATX', 'DTYEAR', 'DTYYQC', 'E8601DN',
                 'E8601DT', 'E8601DX', 'E8601DZ', 'E8601LX', 'E8601LZ', 'E8601TM',
                 'E8601TX', 'E8601TZ', 'EURDFDT', 'MDYAMPM', 'NLDATM[A-Z]*'],
                'Format names used to indicate the column should be converted\n'
                'to a Python datetime object.')

register_option('cas.dataset.time_formats', 'string or list of strings',
                check_string_list,
                ['HHMM', 'HOUR', 'MMSS', 'NLTIMAP', 'NLTIME', 'TIMEAMPM',
                 'TIME', 'TOD'],
                'Format names used to indicate the column should be converted\n'
                'to a Python time object.')

register_option('cas.dataset.index_name', 'string or list of strings',
                check_string_list, '_Index_',
                'The name or names of the columns to be automatically converted\n'
                'to the index.')

register_option('cas.dataset.drop_index_name', 'boolean', check_boolean, True,
                'If True, the name of the index is set to None.')

register_option('cas.dataset.index_adjustment', 'int', check_int, -1,
                'Adjustment to the index specified by cas.dataset.index.\n'
                'This can be used to adjust SAS 1-based index data sets to\n'
                '0-based Pandas DataFrames.')


def check_max_rows_fetched(val):
    ''' Check the max_rows_fetched value and print warning '''
    warnings.warn('max_rows_fetched does not affect explicit calls to the '
                  'table.fetch action, only hidden fetches in methods such '
                  'as head, tail, plot, etc.', OptionWarning)
    return check_int(val)


register_option('cas.dataset.max_rows_fetched', 'int', check_max_rows_fetched, 10000,
                'The maximum number of rows to fetch with methods that use\n'
                'the table.fetch action in the background (i.e. the head, tail,\n'
                'values, etc. of CASTable).\n'
                'NOTE: This does not affect explicit calls to the table.fetch action.\n'
                '      Using the maxrows=, to=, and from= action parameters will\n'
                '      return any number of rows, but in batches (e.g., Fetch1, \n'
                '      Fetch2, etc.).')

register_option('cas.dataset.bygroup_columns', 'string',
                functools.partial(check_string,
                                  valid_values=['none', 'raw', 'formatted', 'both']),
                'formatted',
                'CAS returns by grouping information as metadata on a table.\n'
                'This metadata can be used to construct columns in the output table.\n'
                'The possible values of this option are:\n'
                '    none : Do not convert metadata to columns\n'
                '    raw  : Use the raw (i.e., unformatted) values\n'
                '    formatted : Use the formatted value.  This is the actual value\n'
                '                used to do the grouping\n'
                '    both : Add columns for both raw and formatted')

register_option('cas.dataset.bygroup_formatted_suffix', 'string', check_string, '_f',
                'Suffix to use on the formatted column name when both raw and\n'
                'formatted by group colunms are added.')

register_option('cas.dataset.bygroup_collision_suffix', 'string', check_string, '_by',
                'Suffix to use on the By group column name when a By group column\n'
                'is also included as a data column.')

register_option('cas.dataset.bygroup_as_index', 'boolean', check_boolean, True,
                'If True, any by group columns are set as the DataFrame index.')

register_option('cas.dataset.bygroup_casout_threshold', 'int', check_int, 25000,
                'When using pandas DataFrame APIs for simple statistics (e.g. \n'
                'min, max, quantiles, etc.), if the number of By groupings is\n'
                'greater than this threshold, a CAS table of results is created\n'
                'rather than returning the results to the client.  Note that the\n'
                'number of by groups is only estimated based on the product of the\n'
                'cardinality of each by group variable.')


#
# Debugging options
#
register_option('cas.debug.requests', 'boolean', check_boolean, False,
                'Display requested URL when accessing REST interface.',
                environ='CAS_DEBUG_REQUESTS')
register_option('cas.debug.request_bodies', 'boolean', check_boolean, False,
                'Display body of request when accessing REST interface.',
                environ='CAS_DEBUG_REQUEST_BODIES')
register_option('cas.debug.responses', 'boolean', check_boolean, False,
                'Display raw responses from server.',
                environ='CAS_DEBUG_RESPONSES')


#
# Connection retry options
#
register_option('cas.connection_retries', 'int', check_int, 3,
                'Number of retries to attempt on a REST connection if a network\n'
                'error occurs.',
                environ='CAS_CONNECTION_RETRIES')
register_option('cas.connection_retry_interval', 'int', check_int, 10,
                'Number of seconds to wait before each REST connection retry.',
                environ='CAS_CONNECTION_RETRY_INTERVAL')


#
# Logging options
#
def check_log_level(val):
    ''' Check and set the log level '''
    val = check_string(val, valid_values=['critical', 'error',
                                          'warning', 'info', 'debug'])
    swat_logging.logger.setLevel(dict(
        debug=logging.DEBUG,
        info=logging.INFO,
        warning=logging.WARNING,
        error=logging.ERROR,
        critical=logging.CRITICAL,
    )[val])
    return val


register_option('log.level', 'string', check_log_level, swat_logging.default_level,
                'Set the level of displayed log messages.',
                environ=['SWAT_LOG_LEVEL', 'CAS_LOG_LEVEL', 'SAS_LOG_LEVEL'])


def check_log_format(val):
    ''' Check and set the log format '''
    val = check_string(val)
    swat_logging.handler.setFormatter(logging.Formatter(get_option('log.format')))
    return val


register_option('log.format', 'string', check_string, swat_logging.default_format,
                'Set the format of the displayed log messages.',
                environ=['SWAT_LOG_FORMAT', 'CAS_LOG_FORMAT', 'SAS_LOG_FORMAT'])


#
# Display options
#

register_option('display.apply_formats', 'boolean', check_boolean, False,
                'Format displayed values using SAS format. Full support for SAS\n'
                'formats is only available in SWAT installations that include the\n'
                'C extension (currently Linux and Windows).  Other platforms have\n'
                'limited format support.',
                environ=['SWAT_DISPLAY_APPLY_FORMATS',
                         'CAS_DISPLAY_APPLY_FORMATS',
                         'SAS_DISPLAY_APPLY_FORMATS'])


#
# IPython notebook options
#
#
# register_option('display.max_rows', 'int', check_int,
#                 pd.get_option('display.max_rows'),
#                 'Sets the maximum number of rows to be output for\n'
#                 'any of the rendered output types.')
#
#
# def check_show_dimensions(value):
#     ''' Check for True, False, or 'truncate' '''
#     if isinstance(value, text_types) or isinstance(value, binary_types):
#         if value.lower() == 'truncate':
#             return value.lower()
#         raise SWATOptionError('Invalid string value given')
#     return check_boolean(value)
#
#
# register_option('display.show_dimensions', 'boolean or \'truncate\'',
#                 check_show_dimensions, pd.get_option('display.show_dimensions'),
#                 'Whether to print the dimensions at the bottom of a\n'
#                 'SASDataFrame rendering.  If \'truncate\' is specified,\n'
#                 'it will only print the dimensions if not all rows are displayed.')
#
# register_option('display.notebook.repr_html', 'boolean', check_boolean,
#                 pd.get_option('display.notebook_repr_html'),
#                 'When True, IPython notebook will use HTML representation for\n'
#                 'for swat objects.  If display.notebook.repr_javascript is set,\n'
#                 'that will take precedence over this rendering.')
#
# register_option('display.notebook.repr_javascript', 'boolean', check_boolean, False,
#                 'When True, IPython notebook will use javascript representation for\n'
#                 'for swat objects.')
#
# register_option('display.notebook.css.datatables', 'URL', check_url,
#                 '//cdn.datatables.net/1.10.3/css/jquery.dataTables.min.css',
#                 'URL for the jQuery Datatables plugin CSS file')
#
# register_option('display.notebook.css.swat', 'URL', check_url,
#                 '//www.sas.com/cas/python/css/swat.css',
#                 'URL for the SWAT CSS file')
#
# register_option('display.notebook.css.font_awesome', 'URL', check_url,
#                 '//netdna.bootstrapcdn.com/font-awesome/4.3.0/css/font-awesome.min.css',
#                 'URL for the Font Awesome CSS file')
#
# register_option('display.notebook.js.datatables', 'URL', check_url,
#                 '//cdn.datatables.net/1.10.3/js/jquery.dataTables.min',
#                 'URL for the jQuery Datatables plugin Javascript file')
#
# register_option('display.notebook.js.swat', 'URL', check_url,
#                 '//www.sas.com/cas/python/js/swat',
#                 'URL for the SWAT Javascript file')
#
