
.. Copyright SAS Institute

.. currentmodule:: swat.cas.results
.. _bygroups:

******************
Handling By Groups
******************

If By groups are specified when running a CAS action, the result are returned
with the following behaviors.

1. A result key named 'ByGroupInfo' is returned with all of the By group 
   variable values.
2. Each By group table is returned in a separate result key with a prefix of
   'ByGroup#.'.

These behaviors can help when you have a large number of By groups and you
want to process them as they arrive at the client rather than trying to hold
the entire set of results in memory.  However, when your result sets are 
smaller, you may want to combine all of the By group tables into a single
:class:`pandas.DataFrame`.  To help in these situations, the :class:`CASResults`
class defines some helper methods for you.

.. ipython:: python
   :suppress:

   import os
   import swat
   hostname = os.environ['CASHOST']
   port = os.environ['CASPORT']
   userid = os.environ.get('CASUSER', None)
   password = os.environ.get('CASPASSWORD', None)
   conn = swat.CAS(hostname, port, userid, password)
   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv')

Here is what it looks like to run a standard ``summary`` action, and a ``summary``
action with a By group specified.  We will use this output to demonstrate the
By group processing methods.

.. ipython:: python

   tbl = tbl[['MSRP', 'Horsepower']]

   tbl.summary(subset=['Min', 'Max'])

   tbl.groupby(['Origin', 'Cylinders']).summary(subset=['Min', 'Max'])


Selecting Tables by Name Across By Groups
=========================================

The :meth:`CASResults.get_tables` method will return all tables with a given
name across By groups in a list.  If the results do not contain By groups,
the single table with that name will be returned in a list.  This makes it
possible to use :meth:`CASResults.get_tables` the same way whether By
group variables are specified or not.

.. ipython:: python

   tbl.summary(subset=['Min', 'Max']).get_tables('Summary')

   tbl.groupby(['Origin', 'Cylinders']).summary(subset=['Min', 'Max']).get_tables('Summary')

The reason that the table name is required is that many CAS actions can
have multiple tables with different names in each By group.


Concatenating By Group Tables
=============================

While you can use the :meth:`CASResults.get_tables` method to retrieve tables
of a specified name then use the :func:`concat` function to concatenate them
together, there is also a :meth:`CASResults.concat_bygroups` method that you can
use.  This method will concatenate all tables with the same name across all
By groups and set the concatenated table under the table's key.

.. ipython:: python

   tbl.groupby(['Origin', 'Cylinders']).summary(subset=['Min', 'Max']).concat_bygroups()

By default, this method returns a new :class:`CASResults` object with the 
concatenated tables.  If you want to modify the :class:`CASResults` object
in place, you can add a ``inplace=True`` option.


Selecting a Specific By Group
=============================

In addition to selecting tables by name, you can also select a specific
By group in the result by specifying the By variable values.  This is
done with the :meth:`CASResults.get_group` method.

.. ipython:: python

   tbl.groupby(['Origin', 'Cylinders']).summary(subset=['Min', 'Max']).get_group(['Asia', 4])

The values given for the By group variable values can be either the raw
value or the formatted value.  

You can also specify the grouping variables as keyword arguments.

.. ipython:: python

   tbl.groupby(['Origin', 'Cylinders']).summary(subset=['Min', 'Max']).get_group(Origin='Asia', Cylinders=4)

Multiple Sets of By Groups
==========================

Some CAS actions like ``simple.mdsummary`` allow you to specify multiple By group sets.  In cases
like this, the keys for each By group set are prefixed with "`ByGroupSet#.`".  To select a By group
set, you can use the :meth:`CASResults.get_set` method.  This takes a numeric index indicating which
By group set to select.  The return value is a new :class:`CASResults` object that contains just 
the selected By group set.  You can then use the methods above to select tables or concatenate
tables together.

.. ipython:: python

   tbl.mdsummary(sets=[dict(groupby=["Origin"]),
                       dict(groupby=["Cylinders"])])

   tbl.mdsummary(sets=[dict(groupby=["Origin"]),
                       dict(groupby=["Cylinders"])]).get_set(1).get_group('Asia')

.. ipython:: python
   :suppress:

   conn.close()
