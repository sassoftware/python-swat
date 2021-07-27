#!/usr/bin/env python

'''
Return the WORKSPACE as a URL

Some downstream commands require the path to a Jenkins `WORKSPACE` variable
as a URL rather than a file path. This URL must be formatted differently for
Windows than for UNIX-like operating systems. This utility does the proper
formatting for the host type.

'''

import argparse
import itertools
import os
import re
import sys


def main(args):
    ''' Main routine '''
    cwd = os.getcwd()

    if sys.platform.lower().startswith('win'):
        workspace_url = 'file:///{}'.format(re.sub(r'^(/[A-Za-z])/', '\1:/',
                                            cwd.replace('\\', '/')))
    else:
        workspace_url = 'file://{}'.format(cwd)

    print(workspace_url)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip(),
                                     formatter_class=argparse.RawTextHelpFormatter)

    args = parser.parse_args()

    main(args)
