
.. Copyright SAS Institute

.. currentmodule:: swat

***************
Getting Started
***************

Before you can use the SWAT package, you will need a running CAS server.  The SWAT package
can connect to either the binary port or the HTTP port.  If you have the option of either,
the binary protocol will give you better performance. 

Other than the CAS host and port, you just need a user name and password to connect. 
User names and passwords can be implemented in various ways, so you may need to see your
system administrator on how to acquire an account.

To connect to a CAS server, you simply import SWAT and use the :class:`swat.CAS` class
to create a connection.  This has a couple of different forms.  The most
basic is to pass the hostname, port, userid, and password.

.. ipython:: python
   :suppress:

   import os 
   host = os.environ['CASHOST']
   port = os.environ['CASPORT']
   userid = os.environ.get('CASUSER', None)
   password = os.environ.get('CASPASSWORD', None)

.. ipython:: python

   import swat
   conn = swat.CAS(host, port, userid, password)

However, if you are using a REST connection to CAS, a URL is the more natural
way to specify a host, port, and protocol.

.. ipython:: python
   :verbatim:

   conn = swat.CAS('https://my-cas-host.com:443/cas-shared-default-http/',
                   userid='...', password='...')

Notice that in the URL case, ``userid`` and ``password``, must be specified
as keyword parameters since the ``port`` parameter is being skipped.  Also,
in this case we are using a proxy server that requires the base path
of 'cas-shared-default-http'.  If you are connecting directly to a CAS
server, this is typically not required.

Now that we have a connection to CAS, we can run some actions on it.

Running CAS Actions
===================

To test your connection, you can run the ``serverstatus`` action.

.. ipython:: python

   out = conn.serverstatus()
   out


Handling the Output
===================

All CAS actions return a :class:`CASResults` object.  This is simply an ordered
Python dictionary with a few extra methods and attributes added.  In the output 
above, you'll see the keys of the dictionary surrounded in square brackets.  They
are 'About', 'server', and 'nodestatus'.  Since this is a dictionary, you can just
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

The types of the result keys can vary as well.  In this case, the 'About' key holds
a dictionary.  The 'server' and 'nodestatus' keys hold :class:`SASDataFrame` objects
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
=================

We can't have a getting started section without doing some sort of statistical
analysis.  First, we need to see what CAS action sets are loaded.  We can get a 
listing of all of the action sets and actions using the ``help`` CAS action.  If you
run ``help`` without any arguments, it will display all of the loaded actions and
their descriptions.  Rather than printing that large listing, we'll specifically
ask for the `simple` action set since we already know that's the one we want.

.. ipython:: python

   conn.help(actionset='simple');

Let's start with the ``summary`` action.  Of course, we first need to load some data.
The simplest way to load data is to do it from the client side.  Note that while this
is the simplest way, it's probably not the best way for large data sets.  Those should
be loaded from the server side if possible.

The :meth:`CAS.read_csv` method works just like the :meth:`pandas.read_csv` function.
In fact, :meth:`CAS.read_csv` uses :meth:`pandas.read_csv` in the background.
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

If you don't want the difficult-to-read generated name for a table, you can specify one
using the ``casout=`` parameter.

.. ipython:: python

   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv',
                       casout='cars')

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

Loading CAS Action Sets
=======================

While CAS comes with a few pre-loaded action sets, you will likely want to load
action sets with other capabilities such as percentiles, Data step, SQL, or even
machine learning.  Most action sets will require a license to run them, so you'll
have to take care of those issues before you can load them.

The action used to load action sets is called ``loadactionset``.

.. ipython:: python

   conn.loadactionset('percentile')

Once you load an action set, its actions will be automatically added as methods
to the :class:`CAS` connection and any :class:`CASTable` objects associated with
that connection.

.. ipython:: python

   tbl.percentile()

Note that the ``percentile`` action set has an action called ``percentile`` in it.
you can call the action either as ``tbl.percentile`` or ``tbl.percentile.percentile``.


CAS Tables as DataFrames
========================

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
======================

When you are finished with the connection, it's always a good idea to
close it.

.. ipython:: python
   :verbatim:

   conn.close()


Authentication
==============

While it is possible to put your username and password in the :class:`CAS` constructor,
it's generally not a good idea to have a password in your code.  To get around this issue,
the :class:`CAS` class supports authinfo files.  Authinfo files are a a file used to
store username and password information for specified hostname and port.  They are 
protected by file permissions so that only you can read them.  This allows you to set
and protect your passwords in one place and have them used by all of your programs.

The format of the file is as follows::

     host HOST user USERNAME password PASSWORD port PORT

``machine`` is a synonym for ``host``, ``login`` and ``account`` are
synonyms for ``user``, and ``protocol`` is a synonym for ``port``.

You can specify as many of the ``host`` lines as possible.  The ``port`` field
is optional.  If it is left off, all ports will use the same password.
Hostnames much match the hostname used in the :class:`CAS` constructor exactly.  It does
not do any DNS expanding of the names.  So 'host1' and 'host1.my-company.com' are
considered two different hosts.

Here is an exmaple for a user named 'user01' and password '!s3cret' on host 
'cas.my-company.com' and port 12354::

    host cas.my-company.com port 12354 user user01 password !s3cret

By default, the authinfo files are looked for in your home directory under the name
``.authinfo``.  You can also use the name ``.netrc`` which is the name of an older
specification that authinfo was based on.

The permissions on the file must be readable and writable by the owner only.  This 
is done with the following command::

    chmod 0600 ~/.authinfo

If you don't want to use an authinfo in your home directory, you can specify the name
of a file explicitly using the ``authinfo=`` parameter.

.. ipython:: python
   :verbatim:

   conn = swat.CAS('cas.my-company.com', 12354, authinfo='/path/to/authinfo.txt')


.. ipython:: python
   :suppress:

   conn.close()
