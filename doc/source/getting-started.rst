Getting Started
===============

Before you can use the SWAT package, you will need a running CAS server.  The SWAT package can connect
to either the binary port or the HTTP port.  If you have the option of either, the binary protocol
will give you higher performance. 

Other than the CAS host and port, you just need a user name and password to connect.  User names and
passwords can be implemented in various ways, so you may need to see your system administrator
on how to acquire an account.

To connect to a CAS server, you simply import SWAT and use the `swat.CAS` class to create a connection.

.. ipython:: python
   :verbatim:

   import swat
   conn = swat.CAS(host, port, userid, password)

To test your connection, you can run the `serverstatus` action.

.. ipython:: python
   :verbatim:

   conn.serverstatus()

::

   {hi = htere}
