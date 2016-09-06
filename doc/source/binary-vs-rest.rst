
.. Copyright SAS Institute

.. _binaryvsrest:
.. currentmodule:: swat

***************
Binary vs. REST
***************

As we have mentioned in other sections of documentation on SWAT, there
are two methods of connecting to CAS: binary and REST.  The binary 
protocol is implemented in the SAS TK subsystem which is bundled as part
of the SWAT installation (on platforms where it is supported).  This 
protocol packs all data as efficient binary transmissions.

The REST interface, on the other hand, uses standard HTTP or HTTPS 
communication.  All requests and responses are done using HTTP 
mechanisms and JSON data.  This interface does not require any extra
Python extension modules or the SAS TK subsystem; it is all done in
pure Python.  This does have the advantage of being more open, but 
a performance penalty is incurred because of the extra layer of JSON
processing.

The pros and cons of each interface are discussed more below.


Binary (CAS) Protocol
---------------------

The diagram below shows the overall process for a CAS action request
from invoking the action to getting the results back.  The places where
conversion from one data format to another occurs is highlighted in red.
As you can see, the only conversions that take place are in the client-side
to convert Python objects to and from binary CAS constructs.  All of 
these conversions are done in a Python extension written in C in order
to make them as fast and efficient as possible.

.. image:: _static/binary-workflow.png

The default protocol for connecting to CAS is the binary form, but the 
SWAT client does try to auto-detect the type by sending test packets over
of different types to see which one succeeds.  You can specify the binary
communication specifically by using ``protocol="cas"`` in the 
connection constructor.

.. ipython:: python
   :verbatim:
    
   conn = swat.CAS(cashost, casport, protocol='cas')

The REST procotol requires more conversions due to the fact that 
communication is primarily done using JSON.  Let's look at that in the
next section.


REST Protocol
-------------

The next diagram shows the process for a CAS action request using the
REST interface.  Again, the conversion processes are highlighted in red.
In this case, you'll see that more steps are involved in calling the 
action.  The Python objects must be converted to and from JSON on the 
client, and the server must also parse the JSON before calling the CAS
action.  While the JSON parsing one the client is done in a Python
extension method, there is more processing overhead than with the 
binary interface.

.. image:: _static/rest-workflow.png

To specify the REST protocol explicitly, you can use the ``protocol='http'``
option on the connection constructor.

.. ipython:: python
   :verbatim:
 
   conn = swat.CAS(cashost, casport, protocol='http')

While there is more processing in the REST interface, there are some
advantages that can make it a better choice.  We'll cover the pros and
cons of each protocol in the last section.


Synopsis
--------

Binary (CAS) Procotol
~~~~~~~~~~~~~~~~~~~~~

Pros
++++

* Fast and efficient; Fewer conversions
* More authentication mechanisms supported
* Supports custom data loaders using data message handlers
* Addition of SAS TK subsystem also includes support for SAS data formats

Cons
++++

* Platform support is more limited because SAS TK subsystem is a requirement
* Download / installation size is larger due to addition of SAS TK subsystem


REST Protocol
~~~~~~~~~~~~~ 

Pros
++++

* Uses standard HTTP / HTTPS communication
* Code is pure Python, so it can be used on any platform that Pandas runs on
* Smaller download / installation size

Cons
++++

* Conversion of objects to and from JSON is slower than binary
* Less efficient communication
* Data message handlers are not supported
* Extra data formatting features are not available (unless SAS TK is also installed)

