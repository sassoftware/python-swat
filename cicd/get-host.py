#!/usr/bin/env python

'''
Return a random hostname from the specified collection of names

This utility is used to randomly choose a hostname from a list
or names with a numeric range. It allows you to put bracketed
lists or ranges of values in one or more places in the hostname
to enumerate the possibilities. For example, if you had test
machines named `test01`, `test02`, and `test03`. You could
retrieve a random item from this list with the following call:

    get-host.py 'test[01,02,03]'

This would return one of the following:

    test01
    test02
    test03

You can also specify numeric ranges:

    get-host.py 'test[01-03]'

Which would return one of the above results as well.

'''

import argparse
import itertools
import os
import random
import re
import sys


def main(args):
    ''' Main routine '''
    out = []
    for arg in args.host_expr:
        parts = [x for x in re.split(r'(?:\[|\])', arg) if x]

        for i, part in enumerate(parts):
            if ',' in part:
                parts[i] = re.split(r'\s*,\s*', part)
            elif re.match(r'^\d+\-\d+$', part):
                start, end = part.split('-')
                width = len(start)
                start = int(start)
                end = int(end)
                parts[i] = [('%%0%sd' % width) % x for x in range(start, end + 1)]
            else:
                parts[i] = [part]

        out += list(''.join(x) for x in itertools.product(*parts))

    print(random.choice(out))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip(),
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('host_expr', type=str, metavar='hostname-expr', nargs='+',
                        help='hostname expression (ex. myhost[01-06].com)')

    args = parser.parse_args()

    main(args)
