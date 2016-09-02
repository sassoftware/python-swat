
.. Copyright SAS Institute

Installation
============

The SWAT package can be installed using either ``pip`` or just by running
``python setup.py install`` in the expanded tar.gz file.  Before installing,
you should make sure that you have fulfilled the following requirements.
These requirements are for the CAS binary protocol (recommended).

* **64-bit** Python 2.7+/3.4+ on Linux
* Pandas 0.16.0+

**The binary protocol requires pre-compiled components found in the Pip Wheel
files and Conda installers only.  These pieces are not available as source code and
are under a separate license.**  The binary protocol offers better performance
than REST, especially when transferring larger amounts of data.  It also offers
more advanced data loading from the client and data formatting features.

To access the CAS REST interface only, you can use the pure Python code which
runs in Python 2.7+/3.4+.  You will still need Pandas installed.  While not as
fast as the binary protocol, the pure Python interface is more portable.

Note tha this package is merely a client to a CAS server.  It has no utility unless
you have a licensed CAS server to connect to.

Pip
---

SWAT can be installed from `<https://github.com/sassoftware/python-swat/releases>`_.
Simply locate the file for your platform and install it using ``pip`` as
follows::

    pip install https://github.com/sassoftware/python-swat/releases/download/vX.X.X/swat-X.X.X-platform.tar.gz

Where ``X.X.X`` is the release you want to install, and ``platform`` is the
platform you are installing on.  You can also use the source code distribution
if you only want to use the CAS REST interface.  It does not contain support
for the binary protocol.
