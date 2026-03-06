
.. Copyright SAS Institute

.. _encryption:

****************
Encryption (SSL)
****************

If your CAS server is configured to use SSL for communication, you must 
configure your certificates on the client side.

.. note:: Beginning in SAS Viya 3.3, encrypted communication is enabled
             in the server by default.

.. note:: The hostname used when connecting to CAS must match the hostname
          in the certificate.


Linux, Mac, and REST Protocol on Windows
========================================

Set the following environment variable in the environment
where you are running Python::

   CAS_CLIENT_SSL_CA_LIST='/path/to/certificates.pem'

The path indicated here is a client-side path, so the certificates are
typically copied to a local directory from the server.


Windows Binary CAS Protocol
===========================

If you are using the binary protocol on Windows, you must import the
certificate into the Windows Certificate Store.  Documentation on this
process is located at:

    https://go.documentation.sas.com/?docsetId=secref&docsetTarget=n12036intelplatform00install.htm&docsetVersion=9.4&locale=en


Troubleshooting
===============

There are various issues that you may run into when using encryption.  The
most common situations are described below.


No Certificate is Being Used
----------------------------

If you are trying to connect to a server with SSL enabled, but you haven't
configured the certificate on your client, you will get the following
**(note the status code at the end)**::

    ERROR: The TCP/IP negClientSSL support routine failed with status 807ff013.

This error should be alleviated by setting the path to the correct
certificate in the CAS_CLIENT_SSL_CA_LIST environment variable as shown
in the section above.


Incorrect Certificate is Being Used
-----------------------------------

If you are using the wrong certificate for your server, you will get an
error like the following **(note the status code at the end)**::

    ERROR: The TCP/IP negClientSSL support routine failed with status 807ff008.

This error should be alleviated by setting the path to the correct
certificate in the CAS_CLIENT_SSL_CA_LIST environment variable as shown
in the section above.


Unable to Locate libssl on the Client Side
------------------------------------------

The following error indicates that the client can not locate the shared library
libssl on your machine **(note the status code at the end)**::

    ERROR: The TCP/IP negClientSSL support routine failed with status 803fd068

This error seems to be most common on Ubuntu Linux systems.  To alleviate the
situation, you can either point the client directly to your libssl file using
an environment variable as follows (substituting in your libssl version)::

    TKESSL_OPENSSL_LIB=/path/to/libs/libssl.so.1.0.0

Or, you can use the more general ld library path environment variable::

    LD_LIBRARY_PATH=/path/to/libs


Server-Side Issues
------------------

If you are still getting SSL errors, you should check your server logs and make
sure that it has been correctly configured and can find the SSL libraries as well.
