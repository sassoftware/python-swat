#!/usr/bin/env python

''' Utitily for moving a release candidate to a full release '''

import argparse
import datetime
import glob
import io
import os
import re
import requests
import shutil
import sys
import subprocess
import tarfile
import tempfile
from urllib.parse import quote
from urllib.request import urlopen, urlretrieve


GITHUB_TOKEN = os.environ['GITHUB_TOKEN']


def get_repo():
    ''' Retrieve the repo part of the git URL '''
    cmd = ['git', 'remote', 'get-url', 'origin']
    repo = subprocess.check_output(cmd).decode('utf-8').strip()
    repo = re.search(r'github.com/(.+?)\.git$', repo).group(1)
    return repo


def create_release(tag_name, target_commitish, rc_release):
    '''
    Create new release on Github

    POST /repos/:owner/:repo/releases

    '''
    res = requests.post(
        'https://api.github.com/repos/{}/releases'.format(get_repo()),
        headers=dict(Authorization='token {}'.format(GITHUB_TOKEN),
                     Accept='application/vnd.github.v3+json'),
        json=dict(tag_name=tag_name,
                  target_commitish=target_commitish,
                  name=re.sub(r'(v\d+\.\d+\.\d+)-rc', r'\1', rc_release['name']),
                  body=re.sub(r'(v\d+\.\d+\.\d+)-rc', r'\1', rc_release['body']),
                  draft=False, prerelease=False)
    )

    if res.status_code >= 400:
        raise RuntimeError(res.json())

    res = res.json()

    copy_assets(res['upload_url'].split('{')[0], rc_release['assets'])


def copy_assets(url, assets):
    ''' Copy assets from other release to the upload url '''
    for asset in assets:
        asset_url = asset['browser_download_url']
        print(' > {}'.format(asset['name']))
        with urlopen(asset_url) as asset_file:
            requests.post(
                '{}?name={}'.format(url, quote(asset['name'])),
                headers={'Authorization': 'token {}'.format(GITHUB_TOKEN),
                         'Accept': 'application/vnd.github.v3+json',
                         'Content-Type': 'application/octet-stream'},
                data=asset_file.read()
            )


def git_tag(tag, sha=None):
    ''' Add a tag '''
    cmd = ['git', 'tag', tag]
    if sha:
        cmd.append(sha)
    subprocess.check_call(cmd)


def git_push(tag=None):
    ''' Push updates '''
    cmd = ['git', 'push']
    subprocess.check_call(cmd)
    if tag:
        cmd = ['git', 'push', 'origin', tag]
        subprocess.check_call(cmd)


def git_fetch():
    ''' Make sure we have all commits and tags '''
    cmd = ['git', 'fetch', '--tags']
    subprocess.check_call(cmd)


def delete_release(tag_name):
    ''' Remove local and remote tags for the given release '''
    # Delete release
    res = requests.get(
        'https://api.github.com/repos/{}/releases/tags/{}'.format(get_repo(), tag_name),
        headers=dict(Authorization='token {}'.format(GITHUB_TOKEN),
                     Accept='application/vnd.github.v3+json'))

    if res.status_code < 400:
        release_url = res.json()['url']
        res = requests.delete(
            release_url,
            headers=dict(Authorization='token {}'.format(GITHUB_TOKEN),
                         Accept='application/vnd.github.v3+json'))

    # Delete tags
    del_tags = [tag_name, tag_name.replace('-rc', '-snapshot')]
    cmd = ['git', 'show-ref', '--tags']
    for line in subprocess.check_output(cmd).decode('utf-8').strip().split('\n'):
        sha, tag = re.split(r'\s+', line.strip())
        tag = tag.split('/')[-1]
        if tag in del_tags:
            cmd = ['git', 'tag', '-d', tag]
            subprocess.check_call(cmd)
            cmd = ['git', 'push', 'origin', ':refs/tags/{}'.format(tag)]
            subprocess.check_call(cmd)
            break


def checkout_main(tag=None):
    ''' Make sure we're on the main branch '''
    cmd = ['git', 'checkout', 'main']
    subprocess.check_call(cmd)


def rotate_doc(tag_name):
    ''' Rotate documentation in the gh-pages branch '''
    cmd = ['git', 'checkout', 'gh-pages']
    subprocess.check_call(cmd)

    release = get_release(tag_name)
    assets = release['assets']

    doc_url = None
    for asset in assets:
        asset_url = asset['browser_download_url']
        if asset_url.endswith('-doc.tar.gz'):
            doc_url = asset_url
            break

    if not doc_url:
        raise RuntimeError('Could not locate documentation file')

    # Change to production tag
    tag_name = tag_name.replace('-rc', '')

    # Remove existing directory if it exists
    shutil.rmtree(tag_name, ignore_errors=True)

    with urlopen(doc_url) as doc_file:
        try:
            tar_file = tarfile.open(fileobj=doc_file, mode='r|gz')
            tar_file.extractall()
        finally:
            tar_file.close()

    for f in glob.glob('python-swat-*-doc'):
        shutil.move(f, tag_name)

    # Clear out existing top-level doc
    for f in (glob.glob('*.html') + glob.glob('*.js')
              + glob.glob('*.inv') + ['.buildinfo']):
        os.remove(f)
    for f in ['_images', '_sources', '_static', 'generated']:
        shutil.rmtree(f)

    # Copy new doc to the top-level
    from distutils.dir_util import copy_tree
    copy_tree(tag_name, '.')

    cmd = ['git', 'add', '*.html', '*.js', '*.inv', '_images',
           '_sources', '_static', 'generated', '.buildinfo', tag_name]
    subprocess.check_call(cmd)

    subprocess.check_call(['git', 'status'])

    # See if anything needs to be committed
    cmd = ['git', 'status', '--untracked-files=no', '--porcelain']
    txt = subprocess.check_output(cmd)

    if txt:
        cmd = ['git', 'commit', '-m', 'Rotate documentation to {}'.format(tag_name)]
        subprocess.check_call(cmd)
        git_push()


def get_release(tag_name):
    ''' Retrieve the upload URL for the given tag '''
    res = requests.get(
        'https://api.github.com/repos/{}/releases/tags/{}'.format(get_repo(), tag_name),
        headers=dict(Authorization='token {}'.format(GITHUB_TOKEN),
                     Accept='application/vnd.github.v3+json'))

    if res.status_code < 400:
        return res.json()

    raise RuntimeError('Could not locate tag name: {}'.format(tag_name))


def get_release_sha(tag_name):
    ''' Get the sha of the tag '''
    cmd = ['git', 'rev-list', '-n', '1', tag_name]
    return subprocess.check_output(cmd).decode('utf-8').strip()


def tag_type(value):
    ''' Check version syntax '''
    if re.match(r'^v\d+\.\d+\.\d+-rc$', value):
        return value
    raise argparse.ArgumentTypeError(value)


def main(args):
    ''' Main routine '''
    # Make sure local repo is up-to-date
    git_fetch()
    checkout_main()

    release_tag = args.tag.replace('-rc', '')
    release_sha = get_release_sha(args.tag)

    # Retrieve rc release info
    rc_release = get_release(args.tag)

    # Rotate documentation
    try:
        rotate_doc(args.tag)
    finally:
        checkout_main()

    # Push release
    git_tag(release_tag, sha=release_sha)
    git_push(tag=release_tag)
    create_release(release_tag, release_sha, rc_release)

    # Delete rc release and snapshots
    delete_release(args.tag)
    delete_release(args.tag.replace('-rc', '-snapshot'))

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='stage-release-candidate')

    parser.add_argument('tag', type=tag_type, metavar='tag',
                        help='tag of the release to promote')

    args = parser.parse_args()

    try:
        sys.exit(main(args))
    except KeyboardInterrupt:
        sys.exit(1)
