
*****************************************
SWAT (SAS Wrapper for Analytics Transfer)
*****************************************

The **SWAT** package is a Python interface to the **SAS Cloud Analytic 
Services (CAS)** engine (the centerpiece of the 
`SAS Viya <http://www.sas.com/en_us/software/viya.html>`__ framework).
With this package, you can load and analyze data sets of any size on your
desktop or in the cloud.  Since **CAS** can be used on a local desktop
or in a hosted cloud environment, you can analyze extremely large data 
sets using as much processing power as you need, while still retaining 
the ease-of-use of Python on the client side.

Using **SWAT**, you can execute workflows of **CAS** statistical actions,
then pull down the summarized data to further process on the client side
in Python, or to merge with data from other sources using familiar
`Pandas <http://pandas.pydata.org>`__ data structures.  In fact, the 
**SWAT** package mimics much of the API of the Pandas package so that
using CAS should feel familiar to current Pandas users.

With the best-of-breed **SAS** statistics in the cloud and the use of
Python and its large collection of open source packages, the **SWAT**
package gives you access to the best of both worlds.


Requirements
------------

To access the CAS binary protocol (recommended), you need the following:

1. **64-bit** Python 2.7+/3.4+ on Linux
2. Pandas 0.16.0+

The binary protocol requires pre-compiled components found in the
``pip`` installer only.  These pieces are not available as source code and
are under a separate license (see documentation on SAS TK).  The binary protocol
offers better performance than REST, especially when transferring larger
amounts of data.  It also offers more advanced data loading from the client
and data formatting features.

To access the CAS REST interface only, you can use the pure Python code
which runs in Python 2.7+/3.4+.  While not as fast as the binary protocol,
the pure Python interface is more portable.


Installation
------------

SWAT can be installed from `<https://github.com/sassoftware/python-swat/releases>`_.
Simply locate the file for your platform and install it using ``pip`` as 
follows::

    pip install https://github.com/sassoftware/python-swat/releases/download/vX.X.X/swat-X.X.X-platform.tar.gz

Where ``X.X.X`` is the release you want to install, and ``platform`` is the 
platform you are installing on.  You can also use the source code distribution
if you only want to use the CAS REST interface.  It does not contain support
for the binary protocol.


Getting Started
---------------

Once you have SWAT installed and you have a CAS server to connect to,
you can import swat and create a connection::

    >>> import swat
    >>> conn = swat.CAS(host, port, username, password)

If that is successful, you should be able to run an action on the
CAS server::

    >>> out = conn.serverstatus()
    NOTE: Grid node action status report: 1 nodes, 6 total actions executed.
    >>> print(out)
    [About]
    
     {'CAS': 'Cloud Analytic Services',
      'Copyright': 'Copyright © 2014-2016 SAS Institute Inc. All Rights Reserved.',
      'System': {'Hostname': 'cas01',
       'Model Number': 'x86_64',
       'OS Family': 'LIN X64',
       'OS Name': 'Linux',
       'OS Release': '2.6.32-504.12.2.el6.x86_64',
       'OS Version': '#1 SMP Sun Feb 1 12:14:02 EST 2015'},
      'Version': '3.01',
      'VersionLong': 'V.03.01M0D08232016',
      'license': {'expires': '20Oct2016:00:00:00',
       'gracePeriod': 62,
       'site': 'SAS Institute Inc.',
       'siteNum': 1,
       'warningPeriod': 31}}
    
    [server]
    
     Server Status
    
        nodes  actions
     0      1        6
    
    [nodestatus]
    
     Node Status
    
         name        role  uptime  running  stalled
     0  cas01  controller   4.836        0        0
    
    + Elapsed: 0.0168s, user: 0.016s, sys: 0.001s, mem: 0.287mb

    >>> conn.close()


Resources
---------

`SWAT <http://github.com/sassoftware/python-swat/>`__

`Python <http://www.python.org/>`__

`SAS Viya <http://www.sas.com/en_us/software/viya.html>`__

.. Copyright SAS Institute
