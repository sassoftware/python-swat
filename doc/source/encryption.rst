
.. Copyright SAS Institute

.. _encryption:

Encryption (SSL)
================

If your CAS server is configured to use SSL for communication, you must 
configure your certificates on the client side as well.  To
do this, you simply set the following environment variable in the environment
where you are running Python::

   CAS_CLIENT_SSL_CA_LIST='/path/to/certificates.pem'

The path indicated here is a client-side path, so the certificates are
typically copied to a local directory from the server.

.. note:: Beginning in SAS Viya 3.3, encrypted communication is enabled
             in the server by default.
