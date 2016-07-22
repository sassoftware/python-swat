************
Loading Data
************

There are various ways of loading data into CAS.  They range from parsing client-side
files from various formats into :class:`pandas.DataFrame` objects to loading large data
files that are stored on the server.  Which method you choose depends on your needs
and how large the data is.  For small data sets or data that you want to use a custom
parser for, you can use client-side data.  For large data sets, you would use 
server-side data files.


Client-Side Data Files and Sources
==================================

Using Client-Side Parsers
-------------------------

The easiest way to get data into CAS is using the data loading methods on the 
:class:`CAS` object that parallel data reading operations in the :mod:`pandas`
module.  This includes :func:`pandas.read_csv`, :func:`pandas.read_table`,
:func:`pandas.read_excel`, etc.  The same methods exist on :class:`CAS` objects
as well and, in fact, use the Pandas functions in the background to do the parsing.
The only difference is that the result of the method is a :class:`CASTable` object
rather than a :class:`pandas.DataFrame`.  Let's look at an example.

.. ipython:: python
   :suppress:

   import os
   import swat
   host = os.environ['CASHOST']
   port = os.environ['CASPORT']
   username = None
   password = None

In this example, we are pointing to a URL that references CSV data.  You could
just as easily point to a local file.  Just keep in mind that when using a URL,
the data is downloaded from wherever it is to the client machine for parsing
before it is uploaded to CAS.

.. ipython:: python

   conn = swat.CAS()
   cars = conn.read_csv('https://raw.githubusercontent.com/'
                        'sassoftware/sas-viya-programming/master/data/cars.csv')
   cars.head()

If all went well, you should now have the cars data set in your CAS session.

As we mentioned earlier, these methods on the :class:`CAS` object use the Pandas
functions under-the-covers to do the parsing.  This means that you can also use
all of the Pandas parsing options.

.. ipython:: python

   cars = conn.read_csv('https://raw.githubusercontent.com/'
                        'sassoftware/sas-viya-programming/master/data/cars.csv',
                        usecols=[0, 1, 2, 3], names=['A', 'B', 'C', 'D'], skiprows=1)
   cars.head()


Parsing Client-Side Data on the Server
--------------------------------------

If you don't need the full power of Pandas' parsers, you may be better off uploading
the file to CAS and parse it there.  This offers some advantages as well.  The server
parsers will likely be faster than a client-side parser (especially in MPP mode where
parsing of some file types can happen in parallel).  Doing server-side parsing is also
more efficient since it doesn't require the data to be converted to Python objects
before creating the data buffer to be sent to the server.

The :meth:`CAS.upload_file` method uploads a data file as-is to CAS and invokes the 
``table.loadtable`` action in the background to parse it.  Let's use the same data
file as the previous examples, but let the server do the parsing.  Just as before,
if a URL is specified, it must be downloaded to the client then uploaded CAS.

.. ipython:: python

   cars = conn.upload_file('https://raw.githubusercontent.com/'
                           'sassoftware/sas-viya-programming/master/data/cars.csv')
   cars.head()


Creating Custom Data Loaders
----------------------------


Server-Side Data Files and Sources
==================================

Using Server-Side Parsers
-------------------------

If you have data files on the server, you can load them directly from the CASLib that 
they are in.  Paths to files in a CASLib are always relative paths.  This is the 
recommended method for large data files.

.. ipython:: python

   cars = conn.read_cas_path('data/cars.csv', caslib='casuser')
   cars.head()


Loading Data from Other Sources
-------------------------------


.. ipython:: python
   :suppress:

   conn.close()
