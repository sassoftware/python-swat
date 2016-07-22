.. currentmodule:: swat
.. _tblvsdf:

**********************
CASTable vs. DataFrame
**********************

:class:`CASTable` objects and DataFrame object (either :class:`pandas.DataFrame` or 
:class:`SASDataFrame`) act very similar in many ways, but they are extremely different
constructs.  :class:`CASTable` objects do not contain actual data.  They are simply
a client-side view of the data in a CAS table on a CAS server.  DataFrames contain
data in-memory on the client machine.

Even though they are very different architectures, :class:`CASTable` objects support
much of the :class:`pandas.DataFrame` API.  However, since CAS tables can contain
enormous amounts of data that wouldn't fit into the memory of a single machine, there
are some differences in the way the APIs work.  The basic rules to remember 
about :class:`CASTable` data access are as follows.

    * If a method returns observation-level data, it will be returned as a new
      :class:`CASTable` object.
    * If a method returns summarized data, it will return data as a :class:`SASDataFrame`.

In other words, if the method is going to return a new data set with potentially 
huge amounts of data, you will get a new :class:`CASTable` object that is a view of that
data.  If the method is going to compute a result of some analysis that is a summary
of the data, you will get a local copy of that result in a :class:`SASDataFrame`.
Most actions allow you to specify a ``casout=`` parameter that allows you to send the
summarized data to a table in the server as well.

A sampling of a number of methods common to both :class:`CASTable` and
DataFrame are shown below with the result type of that method.

============  ==========   =========
Method        CASTable     DataFrame
============  ==========   =========
o.head()      DataFrame    DataFrame
o.describe()  DataFrame    DataFrame
o['col']      CASColumn    Series
o[['col']]    CASTable     DataFrame
o.summary()   CASResults   N/A
============  ==========   =========

In the table above, the :class:`CASColumn` is a subclass of :class:`CASTable`
that only references a single column of the CAS table.

The last entry in the table above is a call to a CAS action called ``summary``.
All CAS actions return a :class:`CASResults` object (which is a subclass of 
Python's ordered dictionary).  DataFrame's can not call CAS actions, although
you can upload a DataFrame to a CAS table using the :meth:`CAS.upload` method.

It is possible to convert all of the data from a CAS table into a DataFrame by
using the :meth:`CASTable.to_frame` method, however, you do need to be careful.
It will attempt to pull all of the data down regardless of size.

:class:`CASColumn` objects work much in the same was as :class:`CASTable` objects
except that they operate on a single column of data like a :class:`pandas.Series`.

============  =========  =========
Method        CASColumn  Series
============  =========  =========
c.head()      Series     Series
c[c.col > 1]  CASColumn  Series
============  =========  =========
