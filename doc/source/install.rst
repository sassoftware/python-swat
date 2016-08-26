
.. Copyright SAS Institute

Installation
============

The SWAT package can be installed using standard Python tools.  This includes both ``pip`` 
and ``conda``.  The packages work with Python 2.7 and Python 3.4+.

Before installing, you should make sure that you have fulfilled the following
requirements.  These requirements are for the CAS binary protocol (recommended).

* **64-bit** Python 2.7+/3.4+ on Linux
* Pandas 0.16.0+

**The binary protocol requires pre-compiled components found in the Pip Wheel
files and Conda installers only.  These pieces are not available as source code and
are under a separate license.**  The binary protocol offers better performance
than REST, especially when transferring larger amounts of data.  It also offers
more advanced data loading from the client and data formatting features.

To access the CAS REST interface only, you can use the pure Python code which
runs in Python 2.7+/3.4+.  While not as fast as the binary protocol, the pure Python
interface is more portable.

Note tha this package is merely a client to a CAS server.  It has no utility unless
you have a licensed CAS server to connect to.

Conda
-----

The easiest way to fulfill the requirements and also install many other Python packages
that you may want to use in the near future is to install the Anaconda Python distribution
from `Continuum Analytics <https://www.continuum.io/downloads>`_.

If you've chosen to install Anaconda, you will have all of the needed prerequisites and
you can install the SWAT package with the following command.  The ``conda`` command will 
automatically choose the appropriate download for your platform.

.. code-block:: bash

   conda install -c conda-forge swat

Pip
---

If you are installing with ``pip``, it will download and install the appropriate Wheel
file for your platform.  This package has the appropriate 
prerequesites specified, so ``pip`` should install those if they aren't installed already.

.. code-block:: bash

   pip install swat

