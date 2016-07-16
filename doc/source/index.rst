:tocdepth: 4

*****************************************
SWAT (SAS Wrapper for Analytics Transfer)
*****************************************

.. module:: swat

**Date**: |today| **Version**: |version|

**Binary Installers:** http://github.com/sassoftware/python-swat/releases

**Source Repository:** http://github.com/sassoftware/python-swat

**Issues & Ideas:** https://github.com/sassoftware/python-swat/issues

**Q&A Support:** http://communities.sas.com/sas-viya-programming


The **SWAT** package is a Python interface to the **SAS Cloud Analytic Services (CAS)** engine
(the centerpiece of the `SAS Viya <http://www.sas.com/en_us/software/viya.html>`__ framework).
With this package, you can load and analyze 
data sets of any size on your desktop or in the cloud.  Since **CAS** can be used on a local
desktop or in a hosted cloud environment, you can analyze extremely large data sets using
as much processing power as you need, while still retaining the ease-of-use of Python
on the client side.

Using **SWAT**, you can execute workflows of **CAS** statistical actions, then pull down
the summarized data to further process on the client side in Python, or to merge with data
from other sources using familiar `Pandas <http://pandas.pydata.org>`__ data structures.  In fact,
the **SWAT** package mimics much of the API of the Pandas package so that using CAS should
feel familiar to current Pandas users.

With the best-of-breed **SAS** statistics in the cloud and the use of Python and 
its large collection of open source packages, the **SWAT** package gives you access
to the best of both worlds.

.. toctree::
   :maxdepth: 3

   install
   getting-started
   workflows
   api
   about


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

