
.. Copyright SAS Institute

Installation
============

The SWAT package can be installed using standard Python tools.  This includes both ``pip`` 
and ``conda``.  The packages work with Python 2.7 and Python 3.4+.

Before installing, you should make sure that you have fulfilled the following
requirements:

* **64-bit** Python 2.7 or **64-bit** Python 3.4+
* Pandas 0.16.0+

Also, this package is merely a client to a CAS server.  It has no utility unless you 
have a licensed CAS server to connect to.

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

If you are installing with ``pip``, you need to choose the appropriate download for your platform.
This is because the SWAT downloads include precompiled binaries specific to that platform.
The commands for each supported platform are shown below.  This package has the appropriate 
prerequesites specified, so ``pip`` should install those if they aren't installed already.

.. code-block:: bash

   # Linux 64
   pip install http://github.com/sassoftware/python-swat/releases/0.9.0/swat-0.9.0-linux64.tar.gz

