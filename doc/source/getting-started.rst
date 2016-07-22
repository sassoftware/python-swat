.. currentmodule:: swat

Getting Started
===============

Before you can use the SWAT package, you will need a running CAS server.  The SWAT package
can connect to either the binary port or the HTTP port.  If you have the option of either,
the binary protocol will give you better performance. 

Other than the CAS host and port, you just need a user name and password to connect. 
User names and passwords can be implemented in various ways, so you may need to see your
system administrator on how to acquire an account.

To connect to a CAS server, you simply import SWAT and use the :class:`swat.CAS` class
to create a connection.

.. ipython:: python
   :suppress:

   import os 
   host = os.environ['CASHOST']
   port = os.environ['CASPORT']
   userid = None
   password = None

.. ipython:: python

   import swat
   conn = swat.CAS(host, port, userid, password)

Now that we have a connection to CAS, we can run some actions on it.

Running CAS Actions
-------------------

To test your connection, you can run the ``serverstatus`` action.

.. ipython:: python

   out = conn.serverstatus()
   out


Handling the Output
-------------------

All CAS actions return a :class:`CASResults` object.  This is simply an ordered
Python dictionary with a few extra methods and attributes added.  In the output 
above, you'll see the keys of the dictionary surrounded in square brackets.  They
are `About`, `server`, and `nodestatus`.  Since this is a dictionary, you can just
use the standard way of accessing keys.

.. ipython:: python

   out['nodestatus'] 

In addition, you can access the keys as attributes.  This convenience was added
to keep your code looking a bit cleaner.  However, be aware that if the name of a
key collides with a standard Python attribute or method, you'll get that attribute
or method instead.  So this form is fine for interactive programming, but you may
want to use the syntax above for actual programs.

.. ipython:: python

    out.nodestatus

The types of the result keys can vary as well.  In this case, the `About` key holds
a dictionary.  The `server` and `nodestatus` keys hold :class:`SASDataFrame` objects
(a subclass of :class:`pandas.DataFrame`).

.. ipython:: python

   for key, value in out.items():
       print(key, type(value))

Since the values in the result are standard Python (and pandas) objects, you can
work with them as you normally do.

.. ipython:: python

   out.nodestatus.role
   out.About['Version']

Simple Statistics
-----------------

We can't have a getting started section without doing some sort of statistical
analysis.  First, we need to see what CAS action sets are loaded.  We can get a 
listing of all of the action sets and actions using the `help` CAS action.  If you
run `help` without any arguments, it will display all of the loaded actions and
their descriptions.  Rather than printing that large listing, we'll specifically
ask for the `simple` action set since we already know that's the one we want.

.. ipython:: python

   conn.help(actionset='simple');

Let's start with the `summary` action.  Of course, we first need to load some data.
The simplest way to load data is to do it from the client side.  Note that while this
is the simplest way, it's probably not the best way for large data sets.  Those should
be loaded from the server side if possible.

The :meth:`CAS.read_csv` method works just like the :meth:`pandas.read_csv` function.
In fact, :meth:`CAS.read_csv` usesing :meth:`pandas.read_csv` in the background.
When :meth:`pandas.read_csv` finishes parsing the CSV file into a :class:`pandas.DataFrame`,
it gets uploaded to a CAS table by :meth:`CAS.read_csv`.  The returned object is
a :class:`CASTable` object.

.. ipython:: python

   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv')

:class:`CASTable` objects are essentially client-side views of the table of data
in the CAS server.  You can interact with them using CAS actions as well as many
of the :class:`pandas.DataFrame` methods and attributes.  The :class:`pandas.DataFrame`
API is mirrored as much as possible, the only difference is that behind-the-scenes 
the real work is being done by CAS.

Since we started down this path with the intent to use the ``summary`` action, let's 
do that first.

.. ipython:: python

   out = conn.summary(table=tbl)
   out

In addition, you can also call the ``summary`` action directly on the :class:`CASTable`
object.  It will automatically populate the ``table=`` parameter.

.. ipython:: python

   out = tbl.summary()
   out

Again, the output is a :class:`CASResults` object (a subclass of a Python dictionary),
so we can pull off the keys we want (there is only one in this case).  This key contains
a :class:`SASDataFrame`, but since it's a subclass of :class:`pandas.DataFrame`, 
you can do all of the standard DataFrame operations on it.

.. ipython:: python

   summ = out.Summary
   summ = summ.set_index('Column')
   summ.loc['Cylinders', 'Max']

CAS Tables as DataFrames
------------------------

As we mentioned previously, :class:`CASTable` objects implement many of the 
:class:`pandas.DataFrame` methods and properties.  This means that you can use the 
familiar :class:`pandas.DataFrame` API, but use it on data that is far too large
for pandas to handle.  Here are a few simple examples.

.. ipython:: python

   tbl.head()

.. ipython:: python

   tbl.describe()

.. ipython:: python

   tbl[['MSRP', 'Invoice']].describe(percentiles=[0.3, 0.7])

For more information about :class:`CASTable`, see the :ref:`API Reference <api>`.

Closing the Connection
----------------------

When you are finished with the connection, it's always a good idea to
close it.

.. ipython:: python

   conn.close()
