#!/usr/bin/env python

'''
Return the WORKSPACE as a URL

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
    parser = argparse.ArgumentParser(description=__doc__.strip())

    args = parser.parse_args()

    main(args)
