#!/usr/bin/env python
# encoding: utf-8

'''
Utilities for IPython Notebook integration

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import json
from .. import config

# _CSS = config.get_suboptions('display.notebook.css')
# _JS = config.get_suboptions('display.notebook.js')
_CSS = _JS = {}


# Javascript used to include CSS and Javascript for IPython notebook
def bootstrap(code):
    '''
    Return a string that bootstraps the Javascript requirements in IPython notebook

    Parameters
    ----------
    code : string
        The Javascript code to append

    Returns
    -------
    string
        The bootstrap code with the given code appended

    '''
    return (r'''
    (function ($) {
       var custom = $('link[rel="stylesheet"][href^="/static/custom/custom.css"]');
       var head = $('head');
       $.each(%s, function (index, value) {
          if ( $('link[rel="stylesheet"][href="' + value + '"]').length == 0 ) {
             var e = $('<link />', {type:'text/css', href:value, rel:'stylesheet'});
             if ( custom.length > 0 ) {
                custom.before(e);
             } else {
                head.prepend(e);
             }
          }
       });
    })($);

    require(%s, function () { }, function (err) {
       var ids = err.requireModules;
       if ( ids && ids.length ) {
          var configpaths = %s;
          for ( var i = 0; i < ids.length; i++ ) {
             var id = ids[i];
             var paths = {};
             paths[id] = configpaths[id];
             requirejs.undef(id);
             requirejs.config({paths:paths});
             require([id], function () {});
          }
       }
    });
    ''' % (json.dumps(list([config.get_option('display.notebook.css.' + x)
                            for x in sorted(_CSS.keys())])),
           json.dumps(list(sorted(_JS.keys()))),
           json.dumps({k: config.get_option('display.notebook.js.' + k) for k in _JS}))) \
        + code
