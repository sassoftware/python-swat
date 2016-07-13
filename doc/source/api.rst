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
   CAS.upload

Running Actions
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CAS.retrieve
   CAS.invoke
   CAS.__iter__

Utilities
~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CAS.copy
   CAS.fork

Session Options
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CAS.session_context


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

   * **columns**: column labels

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

   CASTable.corr
   CASTable.count
   CASTable.describe
   CASTable.max
   CASTable.mean
   CASTable.median
   CASTable.min
   CASTable.mode
   CASTable.quantile
   CASTable.sum
   CASTable.std
   CASTable.var
   CASTable.nmiss
   CASTable.stderr
   CASTable.uss
   CASTable.css
   CASTable.cv
   CASTable.tvalue
   CASTable.probt

Reindexing / Selection / Label manipulation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.head
   CASTable.tail

Sorting
~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.sort_values
   CASTable.nlargest
   CASTable.nsmallest
   CASTable.to_xarray

Plotting
~~~~~~~

:meth:`CASTable.plot` is both a callable method and a namespace attribute for
specific plotting methods of the form `CASTable.plot.<kind>`.

.. note:: In all of the plotting methods, the rendering is done completely 
          on the client side.  This means that all of the data is fetched
          in the background prior to doing the plotting.

.. autosummary::
   :toctree: generated/

   CASTable.plot

.. autosummary::
   :toctree: generated/

   CASTable.plot.area
   CASTable.plot.bar
   CASTable.plot.barh
   CASTable.plot.box
   CASTable.plot.density
   CASTable.plot.hexbin
   CASTable.plot.hist
   CASTable.plot.kde
   CASTable.plot.line
   CASTable.plot.pie
   CASTable.plot.scatter

.. autosummary::
   :toctree: generated/

   CASTable.boxplot
   CASTable.hist

Serialization / IO / Conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: generated/

   CASTable.from_csv
   CASTable.from_dict
   CASTable.from_items
   CASTable.from_records
   CASTable.info
   CASTable.to_pickle
   CASTable.to_csv
   CASTable.to_hdf
   CASTable.to_sql
   CASTable.to_dict
   CASTable.to_excel
   CASTable.to_json
   CASTable.to_html
   CASTable.to_latex
   CASTable.to_stata
   CASTable.to_msgpack
   CASTable.to_gbq
   CASTable.to_records
   CASTable.to_sparse
   CASTable.to_dense
   CASTable.to_string
   CASTable.to_clipboard


CASColumn
---------

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
