
.. Copyright SAS Institute

.. currentmodule:: swat
.. _gotchas:

*******************
Programming Gotchas
*******************

There are a few areas in working with SWAT where you can get tripped up.  We've
tried to outline some of the common issues to watch out for.

Return Values
=============

.. currentmodule:: swat.cas.results

Since the SWAT API tries to blend the world of CAS and Pandas into a single world,
you have to be aware of whether you are calling a CAS action or a method from
the Pandas API.  CAS actions will always return a :class:`CASResults` object
(which is a subclass of Python's dictionary). 

.. ipython:: python
   :suppress:

   import os
   import swat
   hostname = os.environ['CASHOST']
   port = os.environ['CASPORT']
   username = os.environ.get('CASUSER', None)
   password = os.environ.get('CASPASSWORD', None)
   conn = swat.CAS(hostname, port, username, password)
   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv')

.. ipython:: python

   out = tbl.summary()
   type(out)

   out = tbl.serverstatus()
   type(out)
    
Methods from the Pandas API will
typically return a :class:`CASTable`, :class:`CASColumn`, :class:`pandas.DataFrame`,
or :class:`pandas.Series` object.

.. ipython:: python

   out = tbl.head()
   type(out)

   out = tbl.mean()
   type(out)

   out = tbl.Make
   type(out)



Name Collisions
===============

.. currentmodule:: swat.cas.table

Much like the way :class:`pandas.DataFrames` allows you to access column names
as attributes as well as keys, some objects in the SWAT package also have 
multiple namespaces mapped to their attributes.  This is especially true with
:class:`CASTable` objects.

:class:`CASTable` objects have attributes that can come from various sources.
These include real object attributes and methods, CAS action names, CAS 
table parameter names, and CAS table column names.  Mapping all of these 
attributes into one namespace can create collisions.  The most notable
collisions on :class:`CASTable` objects are ``groupby`` (method, CAS action,
table parameter) and ``promote`` (CAS action, table parameter).

These collisions can manifest themselves in ways that seem confusing.  Here
is an example.

.. ipython:: python
   
   tbl.groupby

   tbl.groupby = ['Origin']

   tbl.groupby

   tbl.params

As you can see, the ``groupby`` method is returned when getting the attribute,
but the table ``groupby`` parameter is set when setting the attribute.  The
reason for this is that the dynamic look-thru for CAS actions and table parameters
only happens if there isn't a real Python attribute or method defined.  In the case
of :class:`CASTable` objects, the ``groupby`` method is defined to match the
:meth:`pandas.DataFrame.groupby` method.  

When setting attributes, the name of 
the attribute is checked against the valid parameter names for a table.  If it matches
the name of a table attribute, it is set as a CAS table parameter, otherwise it
is just set on the object as a standard Python attribute.

While this attribute syntax can be convenient for interactive programming, because
of the possibility of collisions, it's generally useful to use the explicit namespace
for table parameters when writing programs.

.. ipython:: python

   tbl.params.groupby = ['Origin']
   tbl.params

Here is the ``simple.groupby`` CAS action explicitly accessed using the ``simple`` 
action set name.

.. ipython:: python

   @suppress
   del tbl.params.groupby

   tbl[['Origin', 'Cylinders']].simple.groupby()

When getting the ``groupby`` attribute, it will always return the real Python ``groupby`` 
method, which corresponds to the :meth:`pandas.DataFrame.groupby` method.

.. ipython:: python

   tbl.groupby('Origin')


Case-Sensitivity
================

While Python programming is case-sensitive, CAS is not.  This means that CAS action names
and parameters can be specified in any sort of mixed case.  This can cause problems on 
clients where case-sensitivity matters though.  For example, it's possible to make a CAS
action call as follows::

    conn.summary(subset=['Max', 'Min']
                 subSet=['N'])

While you would never actually type code in like that, you may build parameters up 
programmatically and mistakenly put in mixed case keys.  When you send this action call to
CAS the action will fail because they are considered duplicate keys.

The SWAT client automatically lower-cases all of the action and parameter names in the 
help content to encourage you to always use lower-case as well.  It also uses all lower-cased
parameter names in operations done behind the scenes.  This convention gets as
close to the Python convention of lower-cased, underscore-delimited names as possible
even though it does cause some longer names to be a bit more difficult to read.

The one case where case-sensitivity does make a difference is in CAS action names.
If the first letter of the CAS action name is capitalized (e.g., ``conn.Summary()``),
This causes an instance of a CAS action object to be returned rather than calling the action.
This allows you to build action parameters in a more object oriented way and call the same
action multiple times.


SASDataFrame vs pandas.DataFrame
================================

.. currentmodule:: swat.dataframe

For the most part, you don't need to worry about the difference between a :class:`SASDataFrame`
and a :class:`panda.DataFrame`.  They work exactly the same way.  The only difference is that
a :class:`SASDataFrame` contains extra attributes to store the SAS metadata such as
``title``, ``label``, and ``name``, as well as ``colinfo`` for the column metadata.
The only time you need to be concerned about the difference is if you are doing operations
on a :class:`SASDataFrame` such as :func:`pandas.concat` that end up returning a 
:class:`pandas.DataFrame`.  In cases such as that, you will lose the SAS metadata on 
the result.


Weak CAS Object References
==========================

.. currentmodule:: swat.cas.table

:class:`CASTable` objects can only call CAS actions on :class:`CAS` connections that still
exist in the Python namespace (meaning, they haven't been deleted or overwritten with another object).
If you delete the :class:`CAS` object that a :class:`CASTable` object is associated
with then try to call a CAS action on that :class:`CASTable`, the action call will fail.
This is due to the fact that :class:`CASTable` objects only keep a weak reference to the 
:class:`CAS` connection.  However, you can re-associate a connection with the :class:`CASTable`
using the :meth:`CASTable.set_connection` method.


.. ipython:: python
   :suppress:
   
   conn.close()
