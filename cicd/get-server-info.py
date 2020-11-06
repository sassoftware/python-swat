#!/usr/bin/env python

'''
get-server-info

Retrieve CAS server information using CAS log.  The CAS server command
is expected to have a -display option with a unique key for the invoked
server.  This is used to retrieve the PID of the server process.  The
basename of the CAS log file must match the value in the -display option.

'''

import argparse
import os
import re
import subprocess
import sys
import time


def print_err(*args, **kwargs):
    ''' Print a message to stderr '''
    sys.stderr.write(*args, **kwargs)
    sys.stderr.write('\n')


def main(args):
    '''
    Main routine

    Parameters
    ----------
    args : argparse arguments
        Arguments to the command-line

    '''
    if not os.path.isfile(args.log_file):
        print_err('ERROR: File not found: {}'.format(args.log_file))
        sys.exit(1)

    iters = 0
    for i in range(args.retries):
        iters += 1
        time.sleep(args.interval)
        if iters > args.retries:
            print_err('ERROR: Could not locate CAS log file.')
            sys.exit(1)

        with open(args.log_file, 'r') as logf:
            txt = logf.read()
            m = re.search(r'===\s+.+?(\S+):(\d+)\s+.+?\s+.+?:(\d+)\s+===', txt)
            if m:
                hostname = m.group(1)
                binary_port = m.group(2)
                http_port = m.group(3)

                sys.stdout.write('CASHOST={} '.format(hostname))
                sys.stdout.write('CAS_HOST={} '.format(hostname))
                sys.stdout.write('CAS_BINARY_PORT={} '.format(binary_port))
                sys.stdout.write('CAS_HTTP_PORT={} '.format(http_port))
                sys.stdout.write('CAS_BINARY_URL=cas://{}:{} '.format(hostname,
                                                                      binary_port))
                sys.stdout.write('CAS_HTTP_URL=http://{}:{} '.format(hostname,
                                                                     http_port))

                if 'CASPROTOCOL' in os.environ or 'CAS_PROTOCOL' in os.environ:
                    protocol = os.environ.get('CASPROTOCOL',
                                              os.environ.get('CAS_PROTOCOL', 'http'))
                    if protocol == 'cas':
                        sys.stdout.write('CASPROTOCOL=cas ')
                        sys.stdout.write('CAS_PROTOCOL=cas ')
                        sys.stdout.write('CASPORT={} '.format(binary_port))
                        sys.stdout.write('CAS_PORT={} '.format(binary_port))
                        sys.stdout.write('CASURL=cas://{}:{} '.format(hostname,
                                                                      binary_port))
                        sys.stdout.write('CAS_URL=cas://{}:{} '.format(hostname,
                                                                       binary_port))
                    else:
                        sys.stdout.write('CASPROTOCOL={} '.format(protocol))
                        sys.stdout.write('CAS_PROTOCOL={} '.format(protocol))
                        sys.stdout.write('CASPORT={} '.format(http_port))
                        sys.stdout.write('CAS_PORT={} '.format(http_port))
                        sys.stdout.write('CASURL={}://{}:{} '.format(protocol, hostname,
                                                                     http_port))
                        sys.stdout.write('CAS_URL={}://{}:{} '.format(protocol, hostname,
                                                                      http_port))

                elif 'REQUIRES_TK' in os.environ:
                    if os.environ.get('REQUIRES_TK', '') == 'true':
                        sys.stdout.write('CASPROTOCOL=cas ')
                        sys.stdout.write('CAS_PROTOCOL=cas ')
                        sys.stdout.write('CASPORT={} '.format(binary_port))
                        sys.stdout.write('CAS_PORT={} '.format(binary_port))
                        sys.stdout.write('CASURL=cas://{}:{} '.format(hostname,
                                                                      binary_port))
                        sys.stdout.write('CAS_URL=cas://{}:{} '.format(hostname,
                                                                       binary_port))
                    else:
                        sys.stdout.write('CASPROTOCOL=http ')
                        sys.stdout.write('CAS_PROTOCOL=http ')
                        sys.stdout.write('CASPORT={} '.format(http_port))
                        sys.stdout.write('CAS_PORT={} '.format(http_port))
                        sys.stdout.write('CASURL=http://{}:{} '.format(hostname,
                                                                       http_port))
                        sys.stdout.write('CAS_URL=http://{}:{} '.format(hostname,
                                                                        http_port))

                # Get CAS server pid
                cmd = ('ssh -x -o StrictHostKeyChecking=no {} '
                       'ps ax | grep {} | grep -v grep | head -1'
                       ).format(hostname, '.'.join(args.log_file.split('.')[:-1]))
                pid = subprocess.check_output(cmd, shell=True) \
                                .decode('utf-8').strip().split(' ', 1)[0]
                sys.stdout.write('CAS_PID={} '.format(pid))

                break


if __name__ == '__main__':

    opts = argparse.ArgumentParser(description=__doc__.strip())

    opts.add_argument('log_file', type=str, metavar='log-file',
                      help='path to CAS server log')

    opts.add_argument('--retries', '-r', default=5, type=int, metavar='#',
                      help='number of retries in attempting to locate the log file')
    opts.add_argument('--interval', '-i', default=3, type=int, metavar='#',
                      help='number of seconds between each retry')

    args = opts.parse_args()

    main(args)
