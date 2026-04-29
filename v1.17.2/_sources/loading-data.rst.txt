
.. Copyright SAS Institute

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
   username = os.environ.get('CASUSER', None)
   password = os.environ.get('CASPASSWORD', None)

In this example, we are pointing to a URL that references CSV data.  You could
just as easily point to a local file.  Just keep in mind that when using a URL,
the data is downloaded from wherever it is to the client machine for parsing
before it is uploaded to CAS.

.. ipython:: python

   conn = swat.CAS(host, port, username, password)
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

.. note:: Loading data from the client-side will be slower than loading data
          files from the server.  Client-side data loading is intended for 
          smaller data sets.


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

.. note:: Loading data from the client-side will be slower than loading data
          files from the server.  Client-side data loading is intended for 
          smaller data sets.


Creating Custom Data Loaders
----------------------------

In addition to the pre-configured data loaders described above, it's possible to write
custom data loaders that can upload data to a CAS table from any type of file or stream
of data that Python can handle.  These are called "data message handlers" and are implemented
in the :class:`CASDataMsgHandler` class.  The :class:`CASDataMsgHandler` class can not be
used directly, but is the base class for all data message handlers classes.

.. note:: Data message handlers are not supported in the REST interface.

To implement your own data message handler, you only need to implement two things in
the subclass.

1. The variable definitions required in the ``vars=`` parameter of the ``table.addtable`` CAS action.

2. The :meth:`getrow` method to return each row of data.

The variable definitions should be stored in the :attr:`vars` attribute of the subclass.
The :meth:`getrow` method returns a row of values (as defined by :attr:`vars`) for each requested
row index.  When there is not more data to return, ``None`` should be returned.

How you generate the :attr:`vars` attribute is really dependent on your situation.  It can be
inferred from a sample of the data or simply hard-coded.  We'll do a simple example using hard-coded
variable definitions below.

.. ipython:: python

    import swat.cas.datamsghandlers as dmh

    class MyDMH(dmh.CASDataMsgHandler):
        def __init__(self, data):
            self.data = data
            vars = [
                dict(name='Name', type='varchar'),
                dict(name='Age', type='int32'),
                dict(name='Height', type='double'),
                dict(name='Weight', type='double'),
            ]
            dmh.CASDataMsgHandler.__init__(self, vars)
        def getrow(self, index):
            try:
                return self.data[index]
            except IndexError:
                pass

    mydmh = MyDMH([
        ['Alfred', 13, 69, 112.5],
        ['Judy', 14, 64.3, 90],
        ['Robert', 12, 64.8, 128]
    ])

The ``table.addtable`` CAS action call below uses a Python shortcut to pass a dictionary as keyword
parameters.  If you print ``**mydmh.arg.addtable``, you'll see the parameters that are
getting passed to ``table.addtable``.  You don't have to use this shortcut mode; you could
construct the variable definitions and pass them in manually.  However, you still need
to pass the data message handler instance to the ``datamsghandler=`` argument.
    
.. ipython:: python

    out = conn.addtable(table='Students', **mydmh.args.addtable)
    students = out.casTable
    students.columninfo()
    students.head()

While this example uses an explicit list of data and passes that to the data message handler
class to index into, it doesn't have to be done this way.  In fact, there is a :class:`DBAPI`
data message handler in the :mod:`swat.cas.datamsghandlers` module that takes a Python database
connection that queries for the data that is returned by :meth:`getrow`.

.. note:: Loading data from the client-side will be slower than loading data
          files from the server.  Client-side data loading is intended for 
          smaller data sets.


Server-Side Data Files and Sources
==================================

Using Server-Side Parsers
-------------------------

If you have data files on the server, you can load them directly from the CASLib that 
they are in.  Paths to files in a CASLib are always relative paths.  This is the 
recommended method for large data files.

.. ipython:: python
   :verbatim:

   cars = conn.load_path('data/cars.csv', caslib='casuser')


Loading Data from Other Sources
-------------------------------

In addition to files, CAS has many other data loaders available to connect to sources 
such as databases.  These other data sources require you to configure a CASLib that can
connect and retrieve the data as a CAS table.  For these other data sources, you would
still use the :class:`CAS.load_path` method, but rather than specifying a file
path, you would specify the name of a resource in that data loader (such as a database
table).  This topic is beyond the scope of this document, but we are pointing it out
in case you require this type of data access.


.. ipython:: python
   :suppress:

   conn.close()
