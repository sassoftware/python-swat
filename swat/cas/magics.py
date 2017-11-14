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
Magic commands for IPython Notebook

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import re
import uuid
from IPython.core.magic import Magics, magics_class, line_cell_magic
from ..exceptions import SWATError


@magics_class
class CASMagics(Magics):
    '''
    Magic class for surfacing special CAS commands

    '''

    @line_cell_magic
    def casds(self, line, cell=None):
        '''
        Call datastep.runcode action with cell content as source

        %%casds [ options ] conn-object

        -q, --quiet : Don't display the result
        -o var, --output=var : Store output of action in Python variable `var`

        '''
        shell = self.shell
        opts, argsl = self.parse_options(line, 'qo:', 'quiet', 'output=')
        args = re.split(r'\s+', argsl, 1)

        if 'q' in opts and 'quiet' not in opts:
            opts['quiet'] = opts['q']
        if 'o' in opts and 'output' not in opts:
            opts['output'] = opts['o']

        # Get session variable
        try:
            session = shell.user_ns[args[0]]
        except KeyError:
            SWATError('No connection object was supplied')

        out = session.retrieve('builtins.loadactionset', actionset='datastep',
                               _messagelevel='error', _apptag='UI')
        if out.status:
            raise SWATError(out.status)

        code = ''
        if not cell and len(args) == 2:
            code = args[1]
        elif cell:
            code = cell

        out = session.retrieve('datastep.runcode', code=code, _apptag='UI',
                               _messagelevel='error')
        if out.status:
            raise SWATError(out.status)

        if 'quiet' in opts and 'output' not in opts:
            return

        if 'output' in opts:
            shell.user_ns[opts['output']] = out

        if 'quiet' not in opts:
            return out

    @line_cell_magic
    def cassql(self, line, cell=None):
        '''
        Call fedsql.execdirect action with cell content as source

        %%cassql [ options ] conn-object

        -q, --quiet : Don't display the result
        -o var, --output=var : Store output of action in Python variable `var`
        -k, --keep : Keep the temporary result table on the server?

        '''
        shell = self.shell
        opts, argsl = self.parse_options(line, 'qo:k', 'quiet', 'output=', 'keep')
        args = re.split(r'\s+', argsl, 1)

        if 'q' in opts and 'quiet' not in opts:
            opts['quiet'] = opts['q']
        if 'o' in opts and 'output' not in opts:
            opts['output'] = opts['o']
        if 'k' in opts and 'keep' not in opts:
            opts['keep'] = opts['k']

        # Get session variable
        try:
            session = shell.user_ns[args[0]]
        except KeyError:
            SWATError('No connection object was supplied')

        out = session.retrieve('builtins.loadactionset', actionset='fedsql',
                               _messagelevel='error', _apptag='UI')
        if out.status:
            raise SWATError(out.status)

        code = ''
        if not cell and len(args) == 2:
            code = args[1]
        elif cell:
            code = cell
        code = code.strip()

        outtable = '_PY_T_' + str(uuid.uuid4()).replace('-', '_')
        out = session.retrieve('fedsql.execdirect',
                               query=code, casout=outtable,
                               _apptag='UI', _messagelevel='error')
        if out.status:
            raise SWATError(out.status)

        if 'quiet' in opts and 'output' not in opts:
            return

        if 'output' in opts:
            out = session.fetch(table=outtable)['Fetch']
            out.label = None
            shell.user_ns[opts['output']] = out
            if 'keep' not in opts:
                session.retrieve('table.droptable', table=outtable,
                                 _apptag='UI', _messagelevel='error')

        if 'quiet' not in opts:
            return out


def load_ipython_extension(ipython):
    ''' Load extension in IPython '''
    ipython.register_magics(CASMagics)
