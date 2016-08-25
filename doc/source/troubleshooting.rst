
.. Copyright SAS Institute

***************
Troubleshooting
***************

Just as with any autenticated network service, you may run into problems
from time to time while connecting to CAS.  These are some of the more
common problems you may run across.

Incorrect SAS TK Configuration
==============================

The following error can occur if you do not have the SAS TK system properly 
configured.  Normally, the correct TK settings are applied when SWAT is loaded.
However, if you are using an installation of the libraries in a separate
directory, you may get errors if the correct files are not in that directory.

.. code-block:: python

   In [1]: conn = swat.CAS(hostname, port)
   tkBoot failed:  Cannot Locate [tkmk] in [/usr/local/lib/swat-tk:/opt/sas/viya/SASFoundation/sasexe/:
   Could not get TK handle
   ---------------------------------------------------------------------------

   . . .

   SWATError                                 Traceback (most recent call last)
   <ipython-input-2-d18c0dfd66ee> in <module>()
   ----> 1 s = swat.CAS('my-cas', 12345)
   
   connection.py in __init__(self, hostname, port, username, password, session,
                             locale, nworkers, name, authinfo, protocol, **kwargs)
       225                 self._sw_error = clib.SW_CASError(a2n(soptions))
       226         except SystemError:
   --> 227             raise SWATError('Could not create CAS object. Check your TK path setting.')
       228 
       229         # Make the connection
   
   SWATError: Could not create CAS object. Check your TK path setting.

To fix this issue, make sure that ``swat.options.tkpath`` is set to the location of the
SAS TK libraries.


Unable to Import _pyXXswat
==========================

If you are creating a CAS connection and you get the following error, it 
generally means that you don't have the SAS TK libraries or the Python
extension installed.  This can happen if you only installed the Python
source version of SWAT (not the installer that bundles the SAS TK 
libraries) and you tried to connect to CAS using the binary protocol.

The source-only Python installer can only connect to the REST interface of
CAS, so you must point :class:`CAS` to a REST port rather than a binary
port.

.. code-block:: python

   In [1]: conn = swat.CAS(hostname, port)
   
   . . .
   
   During handling of the above exception, another exception occurred:
   
   ValueError                                Traceback (most recent call last)
   <ipython-input-2-cb8d2ab92685> in <module>()
   ----> 1 swat.CAS('my-cas', 12345)
   
   connection.py in __init__(self, hostname, port, username, password, session,
                             locale, nworkers, name, authinfo, protocol, **kwargs)
       223                 self._sw_error = rest.REST_CASError(a2n(soptions))
       224             else:
   --> 225                 self._sw_error = clib.SW_CASError(a2n(soptions))
       226         except SystemError:
       227             raise SWATError('Could not create CAS object. Check your TK path setting.')
   
   clib.py in SW_CASError(*args, **kwargs)
        84     ''' Return a CASError (importing _pyswat as needed) '''
        85     if _pyswat is None:
   ---> 86         _import_pyswat()
        87     return _pyswat.SW_CASError(*args, **kwargs)
        88 
   
   clib.py in _import_pyswat()
        43         raise ValueError(('Could not import import %s.  This is likely due to an '
        44                           'incorrect TK path or an error while loading the TK subsystem. '
   ---> 45                           'Try using the HTTP port for the REST interface.') % libname)
        46 
        47 
   
   ValueError: Could not import import _py34swat.  This is likely due to an incorrect TK
               path or an error while loading the TK subsystem. Try using the HTTP port 
               for the REST interface.


Refused Connection
==================

If you get an error saying that the connection was refused, you probably do not 
a CAS server running on that host or port, or you are behind a firewall that is
preventing your from accessing that server.

.. code-block:: python

   In [1]: conn = swat.CAS('my-cas', 12345)
   ERROR: The TCP/IP tcpSockConnect support routine failed with error 61 (The connection was refused.).
   ERROR: Failed to connect to host 'my-cas', port 12345.
   
   . . .
   
   During handling of the above exception, another exception occurred:
   
   SWATError                                 Traceback (most recent call last)
   <ipython-input-3-404a7919d58a> in <module>()
   ----> 1 conn = swat.CAS('my-cas', 12345)
   
   cas/connection.py in __init__(self, hostname, port, username, password, session,
                                 locale, nworkers, name, authinfo, protocol, **kwargs)
       259                     raise SystemError
       260         except SystemError:
   --> 261             raise SWATError(self._sw_error.getLastErrorMessage())
       262 
       263         errorcheck(self._sw_connection.setZeroIndexedParameters(), self._sw_connection)
   
   SWATError: Could not connect to 'my-cas' on port 12345.


Authentication Issues
=====================

Authentication problems can occur for many reasons.  The examples below show 
two possible issues.  The first one is an issue with the Authinfo file that
contains the passwords.  It requires the file permissions to be readable by
the owner ownly.  In this case, the Authinfo file was readable by others, 
which is invalid.

Other issues that can occur with an Authinfo file include not having a 
hostname and port that matches the one used in the :class:`CAS` constructor
or simply having the incorrect (possibly outdated) password in the file.
The second code sample below shows the error for an incorrect password.

.. code-block:: python

   In [1]: conn = swat.CAS('my-cas', 12345)
   WARNING: Incorrect permissions on netrc/authinfo file.
   ERROR: Kerberos initialization failed. Your credential cache is either expired or missing.
   ---------------------------------------------------------------------------
   SystemError                               Traceback (most recent call last)
   connection.py in__init__(self, hostname, port, username, password, session,
                            locale, nworkers, name, authinfo, protocol, **kwargs)
       256                                                                 a2n(soptions),
   --> 257                                                                 self._sw_error)
       258                 if self._sw_connection is None:
   
   . . .
   
   SWATError: Could not connect to 'my-cas' on port 12345.

.. code-block:: python

   In [1]: conn = swat.CAS('my-cas', 12345)
   ERROR: Connection failed. Server returned: Authentication failed: Access denied.
   ---------------------------------------------------------------------------
   SystemError                               Traceback (most recent call last)
   connection.py in __init__(self, hostname, port, username, password, session,
                             locale, nworkers, name, authinfo, protocol, **kwargs)
       256                                                                 a2n(soptions),
   --> 257                                                                 self._sw_error)
       258                 if self._sw_connection is None:
   
   . . .
       
   SWATError: Could not connect to 'my-cas' on port 12345.
