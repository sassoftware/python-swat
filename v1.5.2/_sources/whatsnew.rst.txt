
.. Copyright SAS Institute

.. _whatsnew:

**********
What's New
**********

This document outlines features and improvements from each release.

1.5.2 (September 9, 2019)
=========================

- Fix issue with nulls in REST parameters
- Add fallback default configuration for SSL
- Add ``CASTable.get`` method


1.5.1 (March 1, 2019)
=====================

- Fix Authinfo matching when using base URLs in REST interface
- Fix compatibility with pandas 0.24
- Fix blob parameters in REST interface
- Add ``CASTable.last_modified_date``, ``CASTable.last_accessed_date``, and ``CASTable.created_date`` prop
erties
- Add reverse operator methods to `CASColumn`


v1.5.0 (September 18, 2018)
===========================

- Windows support for binary CAS protocol
- Added ``with_params`` method to ``CASTable`` to create one-off parameter object
- Set appropriate column data types when uploading a ``DataFrame``


v1.4.0 (July 25, 2018)
======================

- Automatic CAS table creation when large number of By groups are generated in some DataFrame methods
- Debugging options for REST interface
- Python 3.7 support


v1.3.1 (June 4, 2018)
=====================

- Increase compatibility with older and newer versions of pandas
- Automatically convert columns with SAS date/time formats to Python date/time objects
- Improve ``CASTable.merge`` algorithm
- Fix autocompletion on ``CAS`` and ``CASTable`` objects


v1.3.0 (December 12, 2017)
==========================

- Add new summary statistics for new version of CAS
- Improve missing value support in ``CASTable`` ``describe`` method
- Add controller failover support
- Improve encrypted communication support
- Add ``add``, ``any``, ``all``, ``merge``, and ``append`` methods to ``CASTable``
- Add ``merge`` and ``concat`` functions with ``CASTable`` support


v1.2.1 (September 13, 2017)
===========================

- Better support for binary data in table uploads and parameters
- Add integer missing value support
- Allow list parameters to also be sets
- Improve connection protocol detection
- Add ``eval`` method to ``CASTable``

v1.2.0 (May 2, 2017)
====================

- Use ``upload`` action rather than ``addtable`` for ``read_*`` methods.
- Add basic Zeppelin notebook support (``from swat.notebook.zeppelin import show``)

v1.1.0 (March 21, 2017)
=======================

- Add support for Python 3.6 (Linux extension)
- Implement ``sample`` method on ``CASTable``
- Add sampling support to plotting methods
- ``cas.dataset.max_rows_fetched`` increased to 10,000
- Add ``terminate`` method to ``CAS`` object to end session and close connection
- Implement ``fillna``, ``replace``, and ``dropna`` methods on ``CASTable``
- Add ``apply_labels`` method on ``SASDataFrame`` to set column labels as column names

v1.0.0 (September 9, 2016)
==========================

- Initial Release
