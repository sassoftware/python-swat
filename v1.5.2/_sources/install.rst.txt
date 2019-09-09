
.. Copyright SAS Institute

Installation
============

The SWAT package is installed using the ``pip`` command.  The requirements
for using the binary protocol of CAS (recommended) are as follows.

* **64-bit** Python 2.7 or 3.4+ on Linux or Windows

See additional shared library notes below.

The binary protocol requires pre-compiled components found in the ``pip``
installer only.  These pieces are not available as source code and
are under a separate license (see :ref:`SAS TK <sastk>`).  The binary protocol
offers better performance than REST, especially when transferring larger
amounts of data.  It also offers more advanced data loading from the client
and data formatting features.

To access the CAS REST interface only, you can use the pure Python code which
runs in Python 2.7/3.4+.  You will still need Pandas installed.  While not as
fast as the binary protocol, the pure Python interface is more portable.
For more information, see :ref:`Binary vs. REST <binaryvsrest>`.

Note that this package is merely a client to a CAS server.  It has no utility unless
you have a licensed CAS server to connect to.

If you do not already have a modern Python installation, it is highly recommended
that you install the `Anaconda Python distribution <https://www.continuum.io/downloads>`_
from Continuum Analytics.  This distribution includes dozens of commonly used Python
packages, and can even be installed without administrator privileges.

If you do not have ``pip`` installed, you can use ``easy_install pip`` to add
it to your current Python installation.


Additional Linux Library Dependencies
-------------------------------------

Some Linux distributions may not install all of the needed shared libraries
by default.  Most notably, the shared library ``libnuma.so.1`` is required to
make binary protocol connections to CAS.  If you do not have this library on
your machine you can install the ``numactl`` package for your distribution
to make it available to SWAT.


Python Dependencies
-------------------

The SWAT package uses many features of the Pandas Python package and other
dependencies of Pandas.  If you do not already have version 0.16.0 or greater
of Pandas installed, ``pip`` will install or update it for you when you
install SWAT.


Pip
---

SWAT can be installed from `<https://github.com/sassoftware/python-swat/releases>`_.
Simply locate the file for your platform and install it using ``pip`` as
follows::

    pip install https://github.com/sassoftware/python-swat/releases/download/vX.X.X/python-swat-X.X.X-platform.tar.gz

Where ``X.X.X`` is the release you want to install, and ``platform`` is the
platform you are installing on.  You can also use the source code distribution
if you only want to use the CAS REST interface.  It does not contain support
for the binary protocol::

    pip install https://github.com/sassoftware/python-swat/archive/vX.X.X.tar.gz
