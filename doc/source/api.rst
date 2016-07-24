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

Session Management
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CAS.close
   CAS.session_context
   CAS.copy
   CAS.fork

Reading Data
~~~~~~~~~~~~

Server-Side Files
.................

.. autosummary::
   :toctree: generated/

   CAS.read_cas_path

Client-Side Files
.................

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
   CAS.upload_file

Client-Side DataFrames
......................
   
.. autosummary::
   :toctree: generated/

   CAS.upload_frame

Running Actions
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CAS.retrieve
   CAS.invoke
   CAS.__iter__
   getone
   getnext

CASResults
----------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.results

.. autosummary::
   :toctree: generated/

   CASResults

By Group Processing
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASResults.get_tables
   CASResults.get_group
   CASResults.get_set
   CASResults.concat_bygroups


SASDataFrame
------------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.dataframe

.. autosummary::
   :toctree: generated/

   SASDataFrame

Column Metadata
~~~~~~~~~~~~~~~

.. currentmodule:: swat.dataframe

.. autosummary::
   :toctree: generated/

   SASColumnSpec

Utilities
~~~~~~~~~

.. autosummary::
   :toctree: generated/

   concat
   reshape_bygroups


SASFormatter
------------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.formatter

.. autosummary::
   :toctree: generated/

   SASFormatter

Formatting Data
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   SASFormatter.format


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


GroupBy
~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.groupby


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
~~~~~~~~

:meth:`CASTable.plot` is both a callable method and a namespace attribute for
specific plotting methods of the form ``CASTable.plot.<kind>``.

.. note:: In all of the plotting methods, the rendering is done completely 
          on the client side.  This means that all of the data is fetched
          in the background prior to doing the plotting.

.. autosummary::
   :toctree: generated/

   CASTable.plot

.. autosummary::
   :toctree: generated/

   CASTablePlotter.area
   CASTablePlotter.bar
   CASTablePlotter.barh
   CASTablePlotter.box
   CASTablePlotter.density
   CASTablePlotter.hexbin
   CASTablePlotter.hist
   CASTablePlotter.kde
   CASTablePlotter.line
   CASTablePlotter.pie
   CASTablePlotter.scatter

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

Attributes
~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASColumn.values
   CASColumn.dtype
   CASColumn.ftype
   CASColumn.shape
   CASColumn.ndim
   CASColumn.size

Indexing, Iteration
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASColumn.get
   CASColumn.at
   CASColumn.iat
   CASColumn.ix
   CASColumn.loc
   CASColumn.iloc
   CASColumn.__iter__
   CASColumn.iteritems

For more information on ``.at``, ``.iat``, ``.ix``, ``.loc``, and
``.iloc``,  see the :ref:`indexing documentation <indexing>`.

Binary Operator Functions
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASColumn.add
   CASColumn.sub
   CASColumn.mul
   CASColumn.div
   CASColumn.truediv
   CASColumn.floordiv
   CASColumn.mod
   CASColumn.pow
   CASColumn.radd
   CASColumn.rsub
   CASColumn.rmul
   CASColumn.rdiv
   CASColumn.rtruediv
   CASColumn.rfloordiv
   CASColumn.rmod
   CASColumn.rpow
   CASColumn.round
   CASColumn.lt
   CASColumn.gt
   CASColumn.le
   CASColumn.ge
   CASColumn.ne
   CASColumn.eq

GroupBy
~~~~~~~

.. autosummary::
   :toctree: generated/

   CASColumn.groupby

Computations / Descriptive Stats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASColumn.abs
   CASColumn.all
   CASColumn.any
   CASColumn.between
   CASColumn.clip
   CASColumn.clip_lower
   CASColumn.clip_upper
   CASColumn.count
   CASColumn.describe
   CASColumn.max
   CASColumn.mean
   CASColumn.median
   CASColumn.min
   CASColumn.mode
   CASColumn.nlargest
   CASColumn.nsmallest
   CASColumn.quantile
   CASColumn.std
   CASColumn.sum
   CASColumn.var
   CASColumn.nmiss
   CASColumn.stderr
   CASColumn.uss
   CASColumn.css
   CASColumn.cv
   CASColumn.tvalue
   CASColumn.probt
   CASColumn.unique
   CASColumn.nunique
   CASColumn.is_unique
   CASColumn.value_counts

Selection
~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASColumn.head
   CASColumn.isin
   CASColumn.tail

Sorting
~~~~~~~

.. autosummary::
   :toctree: generated/

   CASColumn.sort_values

Datetime Properties
~~~~~~~~~~~~~~~~~~~

:attr:`CASColumn.dt` can be used to access the values of a CAS table 
column as datetime-like properties.  They are accessed as
``CASColumn.dt.<property>``.

.. autosummary::
   :toctree: generated/

   DatetimeColumnMethods.year
   DatetimeColumnMethods.month
   DatetimeColumnMethods.day
   DatetimeColumnMethods.hour
   DatetimeColumnMethods.minute
   DatetimeColumnMethods.second
   DatetimeColumnMethods.microsecond
   DatetimeColumnMethods.nanosecond
   DatetimeColumnMethods.week
   DatetimeColumnMethods.weekofyear
   DatetimeColumnMethods.dayofweek
   DatetimeColumnMethods.weekday
   DatetimeColumnMethods.dayofyear
   DatetimeColumnMethods.quarter
   DatetimeColumnMethods.is_month_start
   DatetimeColumnMethods.is_month_end
   DatetimeColumnMethods.is_quarter_start
   DatetimeColumnMethods.is_quarter_end
   DatetimeColumnMethods.is_year_start
   DatetimeColumnMethods.is_year_end
   DatetimeColumnMethods.daysinmonth
   DatetimeColumnMethods.days_in_month


String Handling
~~~~~~~~~~~~~~~

:attr:`CASColumn.str` can be used to access the values of a CAS table 
column as stings and apply operations. They are accessed as
``CASColumn.str.<method/property>``.

.. autosummary::
   :toctree: generated/

   CharacterColumnMethods.capitalize
   CharacterColumnMethods.contains
   CharacterColumnMethods.count
   CharacterColumnMethods.endswith
   CharacterColumnMethods.find
   CharacterColumnMethods.index
   CharacterColumnMethods.len
   CharacterColumnMethods.lower
   CharacterColumnMethods.lstrip
   CharacterColumnMethods.repeat
   CharacterColumnMethods.replace
   CharacterColumnMethods.rfind
   CharacterColumnMethods.rindex
   CharacterColumnMethods.rstrip
   CharacterColumnMethods.startswith
   CharacterColumnMethods.strip
   CharacterColumnMethods.title
   CharacterColumnMethods.upper
   CharacterColumnMethods.isalpha
   CharacterColumnMethods.isdigit
   CharacterColumnMethods.isspace
   CharacterColumnMethods.islower
   CharacterColumnMethods.isupper
   CharacterColumnMethods.istitle
   CharacterColumnMethods.isnumeric
   CharacterColumnMethods.isdecimal


CASResponse
-----------

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.response

.. autosummary::
   :toctree: generated/

   CASResponse

Response Properties
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASDisposition
   CASPerformance


Data Message Handlers
---------------------

.. currentmodule:: swat.cas.datamsghandlers

.. autosummary::
   :toctree: generated/

   CASDataMsgHandler
   PandasDataFrame
   SAS7BDAT
   CSV
   Text
   FWF
   JSON
   HTML
   SQLTable
   SQLQuery
   Excel
   Clipboard
   DBAPI


Date and Time Functions
-----------------------

.. currentmodule:: swat.cas.utils.datetime

CAS Dates and Times
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   cas2python_timestamp
   cas2python_datetime
   cas2python_date
   cas2python_time
   python2cas_timestamp
   python2cas_datetime
   python2cas_date
   python2cas_time
   str2cas_timestamp
   str2cas_datetime
   str2cas_date
   str2cas_time
   cas2sas_timestamp
   cas2sas_datetime
   cas2sas_date
   cas2sas_time

SAS Dates and Times
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   sas2python_timestamp
   sas2python_datetime
   sas2python_date
   sas2python_time
   python2sas_timestamp
   python2sas_datetime
   python2sas_date
   python2sas_time
   str2sas_timestamp
   str2sas_datetime
   str2sas_date
   str2sas_time
   sas2cas_timestamp
   sas2cas_datetime
   sas2cas_date
   sas2cas_time
