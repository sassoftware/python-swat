
.. Copyright SAS Institute

.. _whatsnew:

What’s New
==========

This document outlines features and improvements from each release.

1.12.1 (January 9, 2023)
------------------------

-  Update Authentication documentation

1.12.0 (November 11, 2022)
--------------------------

-  New Image CASDataMsgHandler to allow easy uploading of client-side
   images to a CAS table
-  Update TK subsystem

1.11.0 (July 5, 2022)
---------------------

-  Update TK subsystem

1.10.0 (June 10, 2022)
----------------------

-  Add Python 3.10 support
-  Update TK subsystem

1.9.3 (August 6, 2021)
----------------------

-  Fix showlabels issue in Viya deployment before version 3.5

1.9.2 (June 18, 2021)
---------------------

-  Add authorization code as authentication method

1.9.1 (June 11, 2021)
---------------------

-  Add Python 3.9 support

1.9.0 (May 24, 2021)
--------------------

-  Add additional plotting method parameters for correct data fetches
-  Add ``date_format=`` to ``CAS.upload`` method for formatting CSV
   exported data to a specific format
-  Update TK subsystem

1.8.1 (January 21, 2021)
------------------------

-  Fix compatibility with pandas 1.2 DataFrame rendering
-  Fix TKECERT error by locating libcrypto automatically

1.8.0 (January 12, 2021)
------------------------

-  Use ssl module’s CA list as default
-  Improve initial connection performance
-  Fix OAuth authentication in REST connections
-  Fix log truncation with messages >1000 characters

1.7.1 (September 29, 2020)
--------------------------

-  Add ability to apply SAS formats to columns in a ``SASDataFrame``
-  Support timezones in data uploaded and downloaded from CAS tables
-  Fix issue with TK path on Windows when using flask

1.7.0 (August 19, 2020)
-----------------------

-  Add Python 3.8 support
-  Improve connection parameter handling (add CAS_URL= connection
   variable)
-  Improve connection protocol auto-detection

1.6.1 (February 10, 2020)
-------------------------

-  Add pandas v1.0.0 support

1.6.0 (November 21, 2019)
-------------------------

-  Fix letter-casing in ``has_action`` and ``has_actionset`` methods
-  Remove usage of deprecated ``ix`` accessor
-  Explicitly specify column and line delimiters and locale in uploaded
   CSV files
-  Fix TKPATH issues in Windows and PPC

1.5.2 (September 9, 2019)
-------------------------

-  Fix issue with nulls in REST parameters
-  Add fallback default configuration for SSL
-  Add ``CASTable.get`` method

1.5.1 (March 1, 2019)
---------------------

-  Fix Authinfo matching when using base URLs in REST interface
-  Fix compatibility with pandas 0.24
-  Fix blob parameters in REST interface
-  Add ``CASTable.last_modified_date``, ``CASTable.last_accessed_date``,
   and ``CASTable.created_date`` properties
-  Add reverse operator methods to ``CASColumn``

1.5.0 (September 18, 2018)
--------------------------

-  Windows support for binary CAS protocol
-  Added ``with_params`` method to ``CASTable`` to create one-off
   parameter object
-  Set appropriate column data types when uploading a ``DataFrame``

1.4.0 (July 25, 2018)
---------------------

-  Automatic CAS table creation when large number of By groups are
   generated in some DataFrame methods
-  Debugging options for REST interface
-  Python 3.7 support

1.3.1 (June 4, 2018)
--------------------

-  Increase compatibility with older and newer versions of pandas
-  Automatically convert columns with SAS date/time formats to Python
   date/time objects
-  Improve ``CASTable.merge`` algorithm
-  Fix autocompletion on ``CAS`` and ``CASTable`` objects

1.3.0 (December 12, 2017)
-------------------------

-  Add new summary statistics for new version of CAS
-  Improve missing value support in ``CASTable`` ``describe`` method
-  Add controller failover support
-  Improve encrypted communication support
-  Add ``add``, ``any``, ``all``, ``merge``, and ``append`` methods to
   ``CASTable``
-  Add ``merge`` and ``concat`` functions with ``CASTable`` support

1.2.1 (September 13, 2017)
--------------------------

-  Better support for binary data in table uploads and parameters
-  Add integer missing value support
-  Allow list parameters to also be sets
-  Improve connection protocol detection
-  Add ``eval`` method to ``CASTable``

1.2.0 (May 2, 2017)
-------------------

-  Use ``upload`` action rather than ``addtable`` for ``read_*``
   methods.
-  Add basic Zeppelin notebook support
   (``from swat.notebook.zeppelin import show``)

1.1.0 (March 21, 2017)
----------------------

-  Add support for Python 3.6 (Linux extension)
-  Implement ``sample`` method on ``CASTable``
-  Add sampling support to plotting methods
-  ``cas.dataset.max_rows_fetched`` increased to 10,000
-  Add ``terminate`` method to ``CAS`` object to end session and close
   connection
-  Implement ``fillna``, ``replace``, and ``dropna`` methods on
   ``CASTable``
-  Add ``apply_labels`` method on ``SASDataFrame`` to set column labels
   as column names

1.0.0 (September 27, 2016)
--------------------------

-  Initial Release
