
# SAS Scripting Wrapper for Analytics Transfer (SWAT)
## Overview

The **SAS SWAT** package is a Python interface to the **SAS Cloud Analytic 
Services (CAS)** engine (the centerpiece of the 
[SAS Viya](http://www.sas.com/en_us/software/viya.html) framework).
With this package, you can load and analyze data sets of any size on your
desktop or in the cloud.  Since **CAS** can be used on a local desktop
or in a hosted cloud environment, you can analyze extremely large data 
sets using as much processing power as you need, while still retaining 
the ease-of-use of Python on the client side.

Using **SWAT**, you can execute workflows of **CAS** analytic actions,
then pull down the summarized data to further process on the client side
in Python, or to merge with data from other sources using familiar
[Pandas](http://pandas.pydata.org) data structures.  In fact, the 
**SWAT** package mimics much of the API of the Pandas package so that
using CAS should feel familiar to current Pandas users.

With the best-of-breed **SAS** analytics in the cloud and the use of
Python and its large collection of open source packages, the **SWAT**
package gives you access to the best of both worlds.

To view updates to this project see the [Change Log](https://github.com/sassoftware/python-swat/blob/master/CHANGELOG.md).

# Prerequisites

To access the CAS binary protocol (recommended), you need the following:

* **64-bit** Python 2.7.x or 3.4+ on Windows or Linux (see shared library notes below)

The binary protocol requires pre-compiled components found in the
`pip` installer only.  These pieces are not available as source code and
are under a separate license (see documentation on SAS TK).  The binary protocol
offers better performance than REST, especially when transferring larger
amounts of data.  It also offers more advanced data loading from the client
and data formatting features.

To access the CAS REST interface only, you can use the pure Python code
which runs in Python 2.7/3.4+ on all platforms.  While not as fast as the
binary protocol, the pure Python interface is more portable.

## Linux Library Dependencies

Some Linux distributions may not install all of the needed shared libraries
by default.  Most notably, the shared library `libnuma.so.1` is required to
make binary protocol connections to CAS.  If you do not have this library on
your machine you can install the `numactl` package for your distribution
to make it available to SWAT.

## Python Dependencies

The SWAT package uses many features of the Pandas Python package and other
dependencies of Pandas.  If you do not already have version 0.16.0 or greater
of Pandas installed, `pip` will install or update it for you when you
install SWAT.


# Installation

SWAT can be installed using `pip` or `conda` (if you are using Anaconda)::

    pip install swat
    
    conda install -c sas-institute swat
    
You can also install from the files on the 
[SWAT project releases page](https://github.com/sassoftware/python-swat/releases).
Simply locate the file for your platform and install it using `pip` as 
follows:

    pip install https://github.com/sassoftware/python-swat/releases/download/vX.X.X/python-swat-X.X.X-platform.tar.gz

Where `X.X.X` is the release you want to install, and `platform` is the 
platform you are installing on.  You can also use the source code distribution
if you only want to use the CAS REST interface.  It does not contain support
for the binary protocol.


# Getting Started

For the full documentation go to 
[sassoftware.github.io/python-swat](https://sassoftware.github.io/python-swat/).
A simple example is shown below.

Once you have SWAT installed and you have a CAS server to connect to,
you can import swat and create a connection::

    >>> import swat
    >>> conn = swat.CAS(host, port, username, password)

Note the default port for the Python SWAT connection is 5570.

If you get an error message about the **TCP/IP negClientSSL support routine**, you 
likely have an issue with your SSL certificate configuration.  See the 
[Encryption](https://sassoftware.github.io/python-swat/encryption.html) documentation
for more information.

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

# Contributing 
The [Contributor Agreement](https://github.com/sassoftware/python-swat/blob/master/ContributorAgreement.txt) details on how contributions can be made to the project. The [Contributing](https://github.com/sassoftware/python-swat/blob/master/CONTRIBUTING.md) includes instructions and rules as it relates to making contributions on the project. 

# Licensing 
The [LICENSE.md](https://github.com/sassoftware/python-swat/blob/master/LICENSE.md) states how this package is released and licensed. 

# Additional Resources
- [Python-SWAT API Documentation](https://sassoftware.github.io/python-swat/)
- [SAS Viya Documentation](https://support.sas.com/documentation/onlinedoc/viya/index.html)
- [Q&A Support](https://communities.sas.com/t5/SAS-Viya-Coders-Library/tkb-p/cas%40tkb)
- [Python](http://www.python.org/)
- [SAS Viya](http://www.sas.com/en_us/software/viya.html)

