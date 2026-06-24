
.. Copyright SAS Institute

*******
Sorting
*******

Since data in CAS can be spread across many machines and may even be redistributed
depending on events that occur, the data is not stored in an ordered form.
In general, when using statistical actions, this doesn't make much difference
since the CAS actions doing the work will handle the data regardless of the
order that it is in.  However, when you are bringing a table of data back to
the client from CAS using the ``fetch`` action, you may want to have it come
back in a sorted form.

.. ipython:: python
   :suppress:

   import os
   import swat
   host = os.environ['CASHOST']
   port = os.environ['CASPORT']
   username = os.environ.get('CASUSER', None)
   password = os.environ.get('CASPASSWORD', None)

.. ipython:: python

   conn = swat.CAS(host, port, username, password)

   tbl = conn.read_csv('https://raw.githubusercontent.com/'
                       'sassoftware/sas-viya-programming/master/data/cars.csv')
   tbl.fetch(to=5)

   tbl.fetch(to=5, sortby=['MSRP'])

Of course, it is possible to set the direction of the sorting as well.

.. ipython:: python

   tbl.fetch(to=5, sortby=[{'name':'MSRP', 'order':'descending'}])

If you are using the :class:`pandas.DataFrame` style API for :class:`CASTable`,
you can also use the :meth:`sort_values` method on :class:`CASTable` objects.

.. ipython:: python

   sorttbl = tbl.sort_values(['MSRP'])
   sorttbl.head()

   sorttbl = tbl.sort_values(['MSRP'], ascending=False)
   sorttbl.head()

As previously mentioned, this doesn't affect anything in the table on the
CAS server itself, it merely stores away the sort keys and applies them 
when data is fetched (either through ``fetch`` directly, or any method that
calls ``fetch`` in the background).
