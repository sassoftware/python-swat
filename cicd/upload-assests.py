#!/usr/bin/env python

'''
Upload assets to a given Github release

This utility uploads a set of assets to an existing Github release
which is specified by the tag for that release.

'''

import argparse
import glob
import os
import re
import requests
import shutil
import subprocess
import sys
import tarfile
from urllib.parse import quote


if '--help' not in sys.argv and '-h' not in sys.argv:
    try:
        GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
    except KeyError:
        sys.stderr.write('ERROR: This utility requires a Github '
                         'token for accessing the Github release API.\n')
        sys.stderr.write('       The variable should be held in an '
                         'environment variable named GITHUB_TOKEN.\n')
        sys.exit(1)


def print_err(*args, **kwargs):
    ''' Print a message to stderr '''
    sys.stderr.write(*args, **kwargs)
    sys.stderr.write('\n')


def get_repo():
    ''' Retrieve the repo part of the git URL '''
    cmd = ['git', 'remote', 'get-url', 'origin']
    repo = subprocess.check_output(cmd).decode('utf-8').strip()
    repo = re.search(r'github.com/(.+?)\.git$', repo).group(1)
    return repo


def get_release(tag_name):
    ''' Retrieve the upload URL for the given tag '''
    res = requests.get(
        'https://api.github.com/repos/{}/releases/tags/{}'.format(get_repo(), tag_name),
        headers=dict(Authorization='token {}'.format(GITHUB_TOKEN),
                     Accept='application/vnd.github.v3+json'))

    if res.status_code < 400:
        return res.json()

    raise RuntimeError('Could not locate tag name: {}'.format(tag_name))


def upload_asset(url, filename):
    '''
    Upload an asset to a release

    POST :server/repos/:owner/:repo/releases/:release_id/assets?name=:asset_filename

    '''
    upload_url = url + '?name={}'.format(quote(os.path.split(filename)[-1]))
    with open(filename, 'rb') as asset_file:
        requests.post(
            upload_url,
            headers={'Authorization': 'token {}'.format(GITHUB_TOKEN),
                     'Accept': 'application/vnd.github.v3+json',
                     'Content-Type': 'application/octet-stream'},
            data=asset_file
        )


def delete_asset(asset_id):
    ''' Delete the resource at the given ID '''
    requests.delete(
        'https://api.github.com/repos/{}/releases/assets/{}'.format(get_repo(), asset_id),
        headers=dict(Authorization='token {}'.format(GITHUB_TOKEN),
                     Accept='application/vnd.github.v3+json'),
        json=dict(asset_id=asset_id))


def main(args):
    ''' Main routine '''
    release = get_release(args.tag)

    upload_url = release['upload_url'].split('{')[0]
    assets = {x['name']: x for x in release['assets']}

    for asset in args.assets:
        print(asset)
        filename = os.path.split(asset)[-1]
        if filename in assets:
            if args.force:
                delete_asset(assets[filename]['id'])
            else:
                print_err('WARNING: Asset already exists: {}'.format(asset))
                continue
        upload_asset(upload_url, asset)

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip(),
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--tag', '-t', type=str, metavar='tag_name', required=True,
                        help='tag of release to upload the asset to')
    parser.add_argument('--force', '-f', action='store_true',
                        help='force upload even if asset of the same name exists')
    parser.add_argument('assets', type=str, metavar='filename', nargs='+',
                        help='assets to upload')

    args = parser.parse_args()

    try:
        sys.exit(main(args))
    except argparse.ArgumentTypeError as exc:
        print_err('ERROR: {}'.format(exc))
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)
