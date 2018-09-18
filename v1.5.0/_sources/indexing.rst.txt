
.. Copyright SAS Institute

.. currentmodule:: swat.cas.table
.. _indexing:

***************************
Indexing and Data Selection
***************************

Indexing of :class:`CASTable` objects works much in the same way as they do
in :class:`pandas.DataFrame` objects.  You can select one or more columns based on 
column names or indexes, and you can select slices of columns.  However, data 
selection does have some limitations.  CAS tables can be distributed across
a grid of computers and they do not have a specified order.  Because of this,
indexing based on a row index is not possible at this time.  However, it is
possible to apply `where` clauses to a the table parameters to filter rows
based on that.

There are a few properties that allow indexing a :class:`CASTable` object in
various ways.  These properties work just like they :class:`pandas.DataFrame`
counterparts (with the limitations described above).

======================  ====================================================
Property / Method       Description
======================  ====================================================
o[`columns`]            Subset table based on column names
o.loc[:, `columns`]     Subset table based on column names
o.iloc[:, `columns`]    Subset table based on column indexes
o.ix[:, `columns`]      Subset table based on mixed column names and indexes
o.xs(`column`, axis=1)  Select a cross-section of the table
o[`boolean-column`]     Filter data rows based on boolean column values
o.query('`expr`')       Apply a filter to the data values
======================  ====================================================

.. ipython:: python
   :suppress:

   import os
   import swat
   hostname = os.environ['CASHOST'] 
   port = os.environ['CASPORT'] 
   username = os.environ.get('CASUSER', None)
   password = os.environ.get('CASPASSWORD', None)
   conn = swat.CAS(hostname, port, username, password)


The Basics
----------

Just as with :class:`pandas.DataFrames`, :class:`CASTable` objects implement
Python's ``__getitem__`` method to allow indexing using ``[ ]``.  This allows
you to subset the columns that are visible in the table.

.. ipython:: python

   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv')

   tbl.head()

Here we are selecting a single column from the table.  This will return a 
:class:`CASColumn` object.

.. ipython:: python

   tbl['Make'].head()

Selecting multiple columns returns a new :class:`CASTable` object.

.. ipython:: python

   tbl[['Make', 'Model', 'Horsepower']].head()

You can also access individual columns using attribute syntax.

.. ipython:: python

   tbl.Make.head()

Caution should be used when using attribute syntax because it depends on the
fact that there are no existing attributes, methods, or CAS actions with that
same name on the :class:`CASTable`.  It also requires that the column name
contains a valid Python identifier.  Since CAS actions can be added dynamically,
attribute access should generally only be used in interactive programming.
For programs that will be reused, it is safer to use the ``[ ]`` syntax.


Selecting by Name
-----------------

The ``loc`` property is used to select columns based on the column names.
Column names can be specified as a string, a list of strings, or a slice.
If a string is given, a :class:`CASColumn` is returned.  If a list of strings
or a slice is specified, a :class:`CASTable` is returned.

A single string selects a column.  Since row selection is not supported at
this time, this is equivalent to ``tbl.loc['Make']``.

.. ipython:: python

   tbl.loc[:, 'Make'].head()

Using a list of strings selects those columns and returns a new :class:`CASTable`
object. Again, this is equivalent to ``tbl[['Make', 'Model']]``.

.. ipython:: python

   tbl.loc[:, ['Make', 'Model']].head()

Slicing using column names allows you to select a range of columns.

.. ipython:: python

   tbl.loc[:, 'Model':'Invoice'].head()

You can even specify a step size.

.. ipython:: python

   tbl.loc[:, 'Model':'Invoice':2].head()

Note that when using columns names in slices, both endpoints are included
in the slice.  This is not the same behavior for numeric indexes, but is 
consistent with the way that slicing works in :class:`pandas.DataFrame`
objects.


Selecting by Position
---------------------

The ``iloc`` property is used to select columns based on column indices.
Just like with ``loc``, the column indices can be specified as a single
integer, a list of integers, or a slice.

.. ipython:: python

   tbl.iloc[:, 1].head()

Using a list of integers returns a new :class:`CASTable` object.

.. ipython:: python

   tbl.iloc[:, [1, 5, 3]].head()

Of course, ranges work here as well, with or without a step size.

.. ipython:: python

   tbl.iloc[:, 2:6].head()

.. ipython:: python

   tbl.iloc[:, 6:2:-2].head()


Mixing Names and Position
-------------------------

The ``ix`` property works just like the ``loc`` and ``iloc`` properties
except that it takes a mix of column names and indexes.

.. ipython:: python

   tbl.ix[:, 'Model'].head()

   tbl.ix[:, 3].head()

.. ipython:: python

   tbl.ix[:, ['Model', 4, 3]].head()

.. ipython:: python

   tbl.ix[:, 'Model':6:2].head()


Selecting a Cross Section
-------------------------

The ``xs`` method currently only supports column selection (i.e., axis=1).
It is primarily here for future development.

.. ipython:: python

   tbl.xs('Model', axis=1).head()


Boolean Indexing
----------------

It is possible to use a :class:`CASColumn` as a way to select rows in a
CAS table.  The :class:`CASColumn` should contain values that are valid
booleans to CAS (typically integer values where 0 is `false` and non-zero
is `true`).

Here is a basic example that selects all cars with an MSRP value over 80,000.

.. ipython:: python

   tbl[tbl.MSRP > 80000].head()

Conditions can be combined with ``|`` for `or`, ``&`` for `and`, and ``~`` 
for `not`.  However, due to the order of precedence in Python, you must put
your comparisons operations in parentheses before combining them with
these operators.

.. ipython:: python

   tbl[(tbl.MSRP > 80000) & (tbl.Horsepower > 400)].head()

Since each mask of a :class:`CASTable` object returns a new :class:`CASTable`
object, you can split operations across multiple steps.

.. ipython:: python

   expensive = tbl[tbl.MSRP > 80000]

   expensive[expensive.Horsepower > 400].head()

.. warning::
   You can only use columns from within the same CAS table in boolean 
   operations.  If you want to combine operations across tables, you
   should create a view that contains all of the data, then use
   the filtering features outlined above on that view.


The ``query`` Method
--------------------

Rather than using the boolean data selection described above, you can 
write a CAS `where` expression and apply it to a :class:`CASTable` object
directly using the :meth:`CASTable.query` method.  This can often result
in more readable code when using longer expressions.

.. ipython:: python

   tbl.query('MSRP > 80000 and Horsepower > 400').head()

Of course, queries can be combined across multiple steps as well.

.. ipython:: python

   expensive = tbl.query('MSRP > 80000')

   expensive.query('Horsepower > 400').head()


.. ipython:: python
   :suppress:

   conn.close()
