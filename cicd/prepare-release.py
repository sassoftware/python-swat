#!/usr/bin/env python

''' Utitily for preparing a new release '''

import argparse
import datetime
import os
import re
import subprocess
import tempfile


def next_tag(version):
    ''' Determine the next release tag for the given version '''
    cmd = 'git tag -l'.split()

    tags = []
    for line in subprocess.check_output(cmd).decode('utf-8').split('\n'):
        tag = line.strip()
        if not tag:
            continue
        if version in tag:
            tags.append(tag)
    if not tags:
        return 'v{}-rc1'.format(version)

    tag = sorted(tags, key=lambda x: int(re.search(r'(\d+)$', x).group(1)))[-1]
    if version == tag:
        raise RuntimeError('tag already exists: ' + tag)

    rc_num = int(re.search(r'-rc(\d+)$', tag).group(1)) + 1

    return 'v{}-rc{}'.format(version, rc_num)


def next_version(version):
    ''' Return the next dev version number '''
    def increment_patch(m):
        return '{}{}-dev'.format(m.group(1), int(m.group(2)) + 1)
    return re.sub(r'^(\d+\.\d+\.)(\d+)$', increment_patch, version)


def edit_changelog(version):
    ''' Open editor on CHANGELOG.md '''
    filename = 'CHANGELOG.md'

    with open(filename, 'r') as in_file:
        txt = in_file.read()

    if version not in txt:
        date = datetime.date.today().strftime('%Y-%m-%d')
        txt = re.sub(r'(#\s+Change\s+Log\s+)',
                     r'\1## {} - {}\n\n- \n\n'.format(version, date),
                     txt, flags=re.I)
        with open(filename, 'w') as out_file:
            out_file.write(txt)

    editor = os.environ.get('EDITOR', 'vim')
    cmd = [editor, filename]
    subprocess.check_call(cmd)

    with open(filename, 'r') as in_file:
        txt = in_file.read().strip()
        if not txt:
            raise RuntimeError('{} is empty'.format(filename))

    git_add(filename)


def set_version(version):
    ''' Set the version number in source files '''
    files = ['setup.py', 'swat/__init__.py']

    if '-dev' not in version:
        files.append('conda.recipe/meta.yaml')

    for f in files:
        with open(f, 'r') as in_file:
            txt = in_file.read()
        with open(f, 'w') as out_file:
            out_file.write(
                re.sub(r'^(\s*(?:__version__|version)\s*=\s*[\'"])[^\'"]+([\'"])',
                       r'\g<1>{}\g<2>'.format(version), txt, flags=re.M))


def set_tag(tag):
    ''' Tag the repository '''
    cmd = ['git', 'tag', tag]
    subprocess.check_call(cmd)
    return tag


def generate_whatsnew(md, rst):
    ''' Convert markdown changelog to rst '''

    def change_section_headings(m):
        version = m.group(1)
        date = datetime.datetime.strptime(m.group(2), '%Y-%m-%d')
        date = date.strftime('%B %d, %Y').replace(' 0', ' ')
        return '{} ({})'.format(version, date)

    with open(md, 'r') as md_file:
        md_txt = md_file.read()
        md_txt = re.sub(
            r'^#\s+Change\s+Log',
            r"# What's New\n\nThis document outlines features and "
            + r"improvements from each release.", md_txt, flags=re.M)
        md_txt = re.sub(r'^(##\s+\d+\.\d+\.\d+)\s+\-\s+(\d+\-\d+\-\d+)',
                        change_section_headings, md_txt, flags=re.M|re.I) 

    with tempfile.TemporaryDirectory() as tmp_dir:
        changelog = os.path.join(tmp_dir, 'CHANGELOG.md')
        header = os.path.join(tmp_dir, 'header.rst')

        with open(changelog, 'w') as ch_file:
            ch_file.write(md_txt)

        with open(header, 'w') as h_file:
            h_file.write('\n')
            h_file.write('.. Copyright SAS Institute\n\n')
            h_file.write('.. _whatsnew:\n\n')

        cmd = ['pandoc', changelog, '--from', 'markdown',
               '--to', 'rst', '-s', '-H', header, '-o', rst]
        subprocess.check_call(cmd)

    git_add(rst)


def version_type(value):
    ''' Check version syntax '''
    if re.match(r'^\d+\.\d+\.\d+(-dev)?', value):
        return value
    raise argparse.ArgumentTypeError


def git_add(filename):
    ''' Add file to staging '''
    cmd = ['git', 'add', filename]
    subprocess.check_call(cmd)


def git_push(message, tag=None):
    ''' Commit and push updates '''
    cmd = ['git', 'commit', '-m', message]
    subprocess.check_call(cmd)
    cmd = ['git', 'push']
    subprocess.check_call(cmd)
    if tag is not None:
        cmd = ['git', 'push', 'origin', tag]
        subprocess.check_call(cmd)


def git_fetch():
    ''' Make sure we have all commits and tags '''
    cmd = ['git', 'fetch', '--tags']
    subprocess.check_call(cmd)


def main(args):
    ''' Main routine '''
    git_fetch()

    edit_changelog(args.version)
    set_version(args.version)
    tag = set_tag(next_tag(args.version))
    generate_whatsnew('CHANGELOG.md', 'doc/source/whatsnew.rst')
    git_push('Create v{} release candidate'.format(args.version), tag=tag)

    set_version(next_version(args.version))
    git_push('Increment development version')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='prepare-release')
    parser.add_argument('version', type=str, metavar='version',
                        help='version of package')
    args = parser.parse_args()
    main(args)
