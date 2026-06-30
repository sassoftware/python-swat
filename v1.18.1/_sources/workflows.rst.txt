
.. Copyright SAS Institute

.. currentmodule:: swat

*********
Workflows
*********

The SWAT package includes the ability to call CAS actions and process the 
output in various ways.  These range from simple (calling CAS actions as Python
methods and getting a dictionary of results back) to complex (invoking CAS
actions in multiple sessions and handling server responses from them directly).
The various workflows are described in the following sections to give you an idea
of how the classes in SWAT interact with each other.

The Easy Way
============

In the most basic form of calling CAS actions, the actions are called directly
on the connection object or a :class:`CASTable` object.  When you load a CAS
action set (using ``builtins.loadactionset``), the CAS actions in that action set
are added to the connection object as Python methods.  For example, when the 
``simple`` action set is loaded, the CAS connection object is extended with 
methods for ``summary``, ``freq``, ``crosstab``, etc.  These can then be called on 
the CAS connection object directly.

In addition, any :class:`CASTable` objects that are registered with that connection
also get methods for those actions.  When you call a CAS action on a :class:`CASTable`
object, the ``table=`` (or ``name=`` and ``caslib=`` in some cases) parameter get populated
automatically with that table object before it is sent to the CAS server.

Here is a simple example.  We are reading a CSV file using the :meth:`CAS.read_csv`
method.  This loads the CSV data into a CAS table and returns a :class:`CASTable`
object.  You can then use many of the standard :class:`pandas.DataFrame` methods
such as :meth:`pandas.DataFrame.head` to interact with the CAS table on the server.

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

   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv')
   tbl.head()

You can call CAS actions directly on the :class:`CAS` connection object and 
pass in the table argument, or you can call the CAS action directly on the 
:class:`CASTable` object from :meth:`CAS.read_csv`.

.. ipython:: python

   conn.summary(table=tbl)

   tbl.summary()

   conn.close()

The output from the CAS action methods is always a :class:`CASResults` object.
This is simply an ordered Python dictionary with a few extra attributes and 
methods added.  You can see the keys printed in the output above (surround by 
square brackets).  This output only contains a single key: **Summary**.

Here is a diagram showing the process of calling and action and consuming
the responses into the :class:`CASResults` object.

.. image:: _static/easyway-workflow.png

Using Response and Result Callbacks
===================================

The next workflow is to use callback functions to handle either the responses
from the CAS server, or the individual result keys in the responses.  You
still use the CAS action methods on the :class:`CAS` connection object, but this
time you add either a ``responsefunc=`` or ``resultfunc=`` function argument.

The result callback function takes five arguments: ``key``, ``value``, ``response``,
``connection``, and ``userdata``.  Those are the result key and value, the response the result
belongs to, the connection that the result belongs to, and an arbitrary user data
structure.

The response callback just takes three arguments for the response, connection,
and user data structure.

The result callback and response callback are called for each result and response,
respectively.  Keep in mind that you can only specify one or the other (a 
response callback would override a result callback).  If you want to keep any sort
of state information between calls, you can store it in the ``userdata`` argument
and return it.  The returned value will get passed in as the ``userdata`` argument
on the next call.

Here is an example demonstrating both styles of callbacks.

.. ipython:: python

   import swat

   conn = swat.CAS(host, port, userid, password)

   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv')
   tbl.head()

   def result_cb(key, value, response, connection, userdata):
       print('>>> RESULT', key, value)
       return userdata

   tbl.summary(resultfunc=result_cb) 

   def response_cb(response, connection, userdata):
       for k, v in response:
           print('>>> RESPONSE', k, v)
       return userdata

   tbl.summary(responsefunc=response_cb) 

   conn.close()

Here is the flow diagram for using callbacks.  Note that only the result **or**
response callback is called.  It will not call both.
   
.. image:: _static/callbacks-workflow.png

Handling Multiple Actions Simultaneously
========================================

The final workflow is the most hands-on, but also offers the most flexibility.
For most cases, you can use the callback form to handle any size of data coming
back from the CAS server.  However, in that mode, you can only run one action
at a time.  If you want to run simultaneous actions and handle the responses
as they come back from either connection, you need to use the :meth:`CAS.invoke`
method.

The :meth:`CAS.invoke` method calls a CAS action, but doesn't automatically retrieve
the responses.  You must iterate over the connection object to get the responses.
To iterate over the responses for multiple connections, you can use the 
:func:`getnext` function.  Using this technique, you can take advantage of 
running multiple CAS actions simultaneously while still just running in a single
thread on the client.

.. ipython:: python

   import swat

   conn1 = swat.CAS(host, port, userid, password)
   conn2 = swat.CAS(host, port, userid, password)

   tbl1 = conn1.read_csv('https://raw.githubusercontent.com/'
                         'sassoftware/sas-viya-programming/master/data/class.csv')
   tbl1.head()

   tbl2 = conn2.read_csv('https://raw.githubusercontent.com/'
                         'sassoftware/sas-viya-programming/master/data/cars.csv')
   tbl2.head()

Now that we have the tables loaded, we can invoke the ``summary`` action on each
one and retrieve the responses from both connections.

.. ipython:: python

   tbl1.invoke('summary');
   tbl2.invoke('summary');

   for resp, conn in swat.getnext(conn1, conn2):
       for k, v in resp:
           print('>>> RESULT', k, v)

   conn1.close()
   conn2.close()

The flow diagram for handling multiple connections simultaneously is shown below.

.. image:: _static/simultaneous-workflow.png
