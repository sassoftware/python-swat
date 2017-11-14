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
Utilities for Zeppelin Notebook integration

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import cgi
import pandas as pd
import pprint
import six
import sys
from ..utils.compat import a2b


def img2tag(img, fmt='png', **kwargs):
    '''
    Convert image data into HTML tag with data URL

    Parameters
    ----------
    img : bytes
        The image data
    **kwargs : keyword arguments
        CSS attributes as keyword arguments

    Returns
    -------
    HTML string

    '''
    img = b'data:image/' + a2b(fmt) + b';base64,' + base64.b64encode(img.strip())
    css = []
    for key, value in kwargs.items():
        css.append('%s:%s' % (key, value))
    css = css and ("style='%s' " % '; '.join(css)) or ''
    return "<img src='%s' %s/>" % (img.decode('ascii'), css)


def show(obj, **kwargs):
    ''' Display object using the Zeppelin Display System '''
    if hasattr(obj, '_z_show_'):
        obj._z_show_(**kwargs)

    elif hasattr(obj, 'head') and callable(obj.head):
        show_dataframe(obj, **kwargs)

    elif hasattr(obj, 'savefig') and callable(obj.savefig):
        show_matplotlib(obj, **kwargs)

    elif hasattr(obj, '_repr_png_'):
        show_image(obj, fmt='png', **kwargs)

    elif hasattr(obj, '_repr_jpeg_'):
        show_image(obj, fmt='jpeg', **kwargs)

    elif hasattr(obj, '_repr_svg_'):
        show_svg(obj, **kwargs)

    else:
        print('%%html <pre>%s</pre>' % cgi.escape(pprint.pformat(obj)))


def show_image(img, fmt='png', width='auto', height='auto'):
    ''' Display an Image object '''
    if fmt == 'png':
        img = img2tag(img._repr_png_())

    elif fmt in ['jpeg', 'jpg']:
        img = img2tag(img._repr_jpeg_())

    else:
        raise ValueError("Image format must be 'png' or 'jpeg'.")

    out = "%html <div style='width:{width}; height:{height}'>{img}</div>"

    print(out.format(width=width, height=height, img=img))


def show_svg(img, width='auto', height='auto'):
    ''' Display an SVG object '''
    img = img._repr_svg_()

    out = "%html <div style='width:{width}; height:{height}'>{img}</div>"

    print(out.format(width=width, height=height, img=img))


def show_matplotlib(plt, fmt='png', width='auto', height='auto'):
    ''' Display a Matplotlib plot '''
    if fmt in ['png', 'jpeg', 'jpg']:
        io = six.BytesIO()
        plt.savefig(io, format=fmt)
        img = img2tag(io.getvalue(), width=width, height=height)
        io.close()

    elif fmt == 'svg':
        io = six.StringIO()
        plt.savefig(io, format=fmt)
        img = io.getvalue()
        io.close()

    else:
        raise ValueError("Image format must be 'png', 'jpeg', or 'svg'.")

    out = "%html <div style='width:{width}; height:{height}'>{img}</div>"

    print(out.format(width=width, height=height, img=img))


def show_dataframe(df, show_index=None, max_result=None, **kwargs):
    '''
    Display a DataFrame-like object in a Zeppelin notebook

    Parameters
    ----------
    show_index : bool, optional
       Should the index be displayed?  By default, If the index appears to
       simply be a row number (name is None, type is int), the index is
       not displayed.  Otherwise, it is displayed.
    max_result : int, optional
       The maximum number of rows to display.  Defaults to the Pandas option
       ``display.max_rows``.

    '''
    title = getattr(df, 'title', getattr(df, 'label', None))
    if title:
        sys.stdout.write('%%html <div>%s</div>\n\n' % title)

    sys.stdout.write('%table ')

    rows = df.head(n=max_result or pd.get_option('display.max_rows'))
    index = rows.index

    if show_index is None:
        show_index = True
        if index.names == [None] and str(index.dtype).startswith('int'):
            show_index = False

    if show_index and index.names:
        sys.stdout.write('\t'.join([x or '' for x in index.names]))
        sys.stdout.write('\t')

    sys.stdout.write('\t'.join(rows.columns))
    sys.stdout.write('\n')

    for idx, row in zip(index.values, rows.values):
        if show_index:
            if isinstance(idx, (list, tuple)):
                sys.stdout.write('\t'.join(['%s' % item for item in idx]))
            else:
                sys.stdout.write('%s' % idx)
            sys.stdout.write('\t')
        sys.stdout.write('\t'.join(['%s' % item for item in row]))
        sys.stdout.write('\n')
