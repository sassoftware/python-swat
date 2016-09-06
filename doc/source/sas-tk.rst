
.. Copyright SAS Institute

.. _sastk:

******
SAS TK
******

The SAS TK subsytem is a set of components used for various operations in
SAS products.  It can be thought of as SAS' standard library.  The binary
communication used by CAS is included in the functionality of these components.

In order to support binary communications with CAS in SWAT, a subset of
the shared objects (or DLLs) is included in the SWAT install file for 
supported platforms along with a Python extension module that interfaces with
them.

In addition to communication with the CAS server, the TK components also 
contain functionality for applying SAS data formats to Python values for 
rendering purposes.  This includes only the standard SAS data formats, not
user-defined formats.

If you plan to use the binary protocol of CAS, the SAS TK components are 
required.  The REST interface can be accessed using SWAT's Python code.
For more information on protocol comparisons, see the
:ref:`Binary vs. REST <binaryvsrest>` section.

The SAS TK components are also released under a separate license from the
Python source.  See :ref:`Licenses <licenses>` for more information.
