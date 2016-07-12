.. currentmodule:: swat
.. _api:

*************
API Reference
*************

.. _api.functions:


CAS
---

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.connection

.. autosummary::
   :toctree: generated/

   CAS

Reading Data
~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CAS.read_pickle
   CAS.read_table
   CAS.read_csv
   CAS.read_fwf
   CAS.read_clipboard
   CAS.read_excel
   CAS.read_html
   CAS.read_hdf
   CAS.read_sas
   CAS.read_sql_table
   CAS.read_sql_query
   CAS.read_sql
   CAS.read_gbq
   CAS.read_stata

Running Actions
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CAS.retrieve
   CAS.invoke
   CAS.__iter__


CASResults
----------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.results

.. autosummary::
   :toctree: generated/

   CASResults


SASDataFrame
------------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.dataframe

.. autosummary::
   :toctree: generated/

   SASDataFrame


CASTable
--------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.table

.. autosummary::
   :toctree: generated/

   CASTable

CAS Connections
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.get_connection
   CASTable.set_connection

Setters and Getters
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.__setattr__
   CASTable.__getattr__
   CASTable.__delattr__

Attributes and Underlying Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.as_matrix
   CASTable.dtypes
   CASTable.ftypes
   CASTable.get_dtype_counts
   CASTable.get_ftype_counts
   CASTable.select_dtypes
   CASTable.values
   CASTable.axes
   CASTable.ndim
   CASTable.size
   CASTable.shape
   CASTable.memory_usage

Indexing, Iteration
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.head
   CASTable.at
   CASTable.iat
   CASTable.ix
   CASTable.loc
   CASTable.iloc
   CASTable.__iter__
   CASTable.iteritems
   CASTable.iterrows
   CASTable.itertuples
   CASTable.lookup
   CASTable.tail
   CASTable.query

For more information on ``.at``, ``.iat``, ``.ix``, ``.loc``, and
``.iloc``,  see the :ref:`indexing documentation <indexing>`.

.. _api.dataframe.stats:

Computations / Descriptive Stats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: generated/

   CASTable.abs
   CASTable.all
   CASTable.any
   CASTable.clip
   CASTable.clip_lower
   CASTable.clip_upper
   CASTable.corr
   CASTable.corrwith
   CASTable.count
   CASTable.cov
   CASTable.cummax
   CASTable.cummin
   CASTable.cumprod
   CASTable.cumsum
   CASTable.describe
   CASTable.diff
   CASTable.eval
   CASTable.kurt
   CASTable.mad
   CASTable.max
   CASTable.mean
   CASTable.median
   CASTable.min
   CASTable.mode
   CASTable.pct_change
   CASTable.prod
   CASTable.quantile
   CASTable.rank
   CASTable.round
   CASTable.sem
   CASTable.skew
   CASTable.sum
   CASTable.std
   CASTable.var

Reindexing / Selection / Label manipulation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: generated/

   CASTable.filter
   CASTable.first
   CASTable.head
   CASTable.idxmax
   CASTable.idxmin
   CASTable.last
   CASTable.sample
   CASTable.select
   CASTable.tail
   CASTable.take


CASColumn
--------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.table

.. autosummary::
   :toctree: generated/

   CASColumn


CASResponse
-----------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.response

.. autosummary::
   :toctree: generated/

   CASResponse
