
.. Copyright SAS Institute

.. currentmodule:: swat
.. _api:

*************
API Reference
*************

.. _api.functions:

Utility Functions
-----------------

.. currentmodule:: swat.functions

.. autosummary::
   :toctree: generated/

   concat
   merge


CAS
---

The :class:`CAS` object is the connection to the CAS server.  CAS actions
can be called on this object.  It also incorporates many of the data 
reader functions of the Pandas package.

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
   CAS.terminate
   CAS.copy
   CAS.fork
   CAS.session_context

Reading Data
~~~~~~~~~~~~

There are various ways of loading data into CAS: server-side parsed and loaded,
client-side parsed, and client-side files uploaded and parsed on the server.
They follow the a naming convention to prevent confusion.

    * load_* : Loads server-side paths
    * read_* : Uses client-side parsers, then uploads the result
    * upload_* : Uploads client-side files as-is which are parsed on the server

Server-Side Files
.................

.. autosummary::
   :toctree: generated/

   CAS.load_path

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

The :class:`CASResults` object is a subclass of Python's ordered
dictionary.  CAS actions can return any number of result objects
which are accessible by the dictionary keys.  This class also
defines several methods for handling tables in By groups.

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

The :class:`SASDataFrame` object is a simple subclass of :class:`pandas.DataFrame`.
It merely adds attributes to hold SAS metadata such as titles, labels, column
metadata, etc.  It also adds a few utility methods for handling By group
representations.

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

   reshape_bygroups


SASFormatter
------------

The :class:`SASFormatter` object can be used to apply SAS data formats to 
Python values.  It will only work with builtin SAS data formats; not
user-defined formats.  If you need user-defined formats, the ``fetch``
action can be configured to bring back formatted values rather than
raw values.

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

The :class:`CASTable` is essentially a client-side view of a table
in the CAS server.  CAS actions can be called on it directly just like
a CAS connection object, and it also supports much of the Pandas
:class:`pandas.DataFrame` API.

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

   CASTable.drop
   CASTable.head
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

For more information on ``.ix``, ``.loc``, and ``.iloc``,
see the :ref:`indexing documentation <indexing>`.


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

   CASTable.abs
   CASTable.all
   CASTable.any
   CASTable.clip
   CASTable.clip_lower
   CASTable.clip_upper
   CASTable.corr
   CASTable.count
   CASTable.css
   CASTable.cv
   CASTable.describe
   CASTable.eval
   CASTable.kurt
   CASTable.max
   CASTable.mean
   CASTable.median
   CASTable.min
   CASTable.mode
   CASTable.nmiss
   CASTable.probt
   CASTable.quantile
   CASTable.skew
   CASTable.stderr
   CASTable.sum
   CASTable.std
   CASTable.tvalue
   CASTable.uss
   CASTable.var

Reindexing / Selection / Label manipulation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.head
   CASTable.sample
   CASTable.tail

Sorting
~~~~~~~

.. note:: There is no concept of a sorted table in the server.
          The ``sort_values`` merely stores sorting information that
          is applied when fetching data.

.. autosummary::
   :toctree: generated/

   CASTable.sort_values
   CASTable.nlargest
   CASTable.nsmallest
   CASTable.to_xarray

Combining / Merging
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTable.append
   CASTable.merge

Plotting
~~~~~~~~

:meth:`CASTable.plot` is both a callable method and a namespace attribute for
specific plotting methods of the form ``CASTable.plot.<kind>``.

.. note:: In all of the plotting methods, the rendering is done completely 
          on the client side.  This means that all of the data is fetched
          in the background prior to doing the plotting.

Since plotting is done on the client-side, data must be downloaded to create
the graphs.  By default, the amount of data pulled down is limited by the 
``cas.dataset.max_rows_fetched`` option.  Sampling is used to randomize the 
data that is plotted.  You can control the sampling with the following options:

``sample_pct=float`` 
    The percentage of the rows of the original table to return given as a float
    value between 0 and 1.  Using this option disables the 
    ``cas.dataset.max_rows_fetched`` option row limits.

``sample_seed=int``
    The seed for the random number generator given an as integer.  This can be 
    set to create deterministic sampling.

``stratify_by='var-name'``
    Specifies the variable to do stratified sampling by.

``sample=bool``
    A boolean used to indicate that the values fetched should be sampled.
    This is used in conjunction with the ``cas.dataset.max_rows_fetched`` option
    to return random samples up to that limit.  It is assumed to be true 
    when ``sample_pct=`` is specified.

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

Utilities
~~~~~~~~~
.. autosummary::
   :toctree: generated/

   CASTable.copy
   CASTable.with_params


CASColumn
---------

While CAS does not have a true concept of a standalone column, the
:class:`CASColumn` object emulates one by creating a client-side view
of the CAS table using just a single column. :class:`CASColumn` objects
are used much in the same way as :class:`pandas.Series` objects.
They support many of the :class:`pandas.Series` methods, and can
also be used in indexing operations to filter data in a CAS table.

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

   CASColumn.ix
   CASColumn.loc
   CASColumn.iloc
   CASColumn.__iter__
   CASColumn.iteritems

For more information on ``.ix``, ``.loc``, and
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
   CASColumn.skew
   CASColumn.kurt
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
   CASColumn.sample
   CASColumn.tail

Sorting
~~~~~~~

.. note:: There is no concept of a sorted table in the server.
          The ``sort_values`` merely stores sorting information that
          is applied when fetching data.

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
column as strings and apply operations. They are accessed as
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

SAS Functions
~~~~~~~~~~~~~

:attr:`CASColumn.sas` can be used to apply SAS functions to
values in a table column. They are accessed as
``CASColumn.sas.<method>``.  Documentation for SAS functions
can be seen at
`support.sas.com <https://support.sas.com/documentation/cdl/en/lefunctionsref/67960/HTML/default/viewer.htm#p0w6napahk6x0an0z2dzozh2ouzm.htm>`_.

.. autosummary::
   :toctree: generated/

   SASColumnMethods.abs
   SASColumnMethods.airy
   SASColumnMethods.beta
   SASColumnMethods.cnonct
   SASColumnMethods.constant
   SASColumnMethods.dairy
   SASColumnMethods.digamma
   SASColumnMethods.erf
   SASColumnMethods.erfc
   SASColumnMethods.exp
   SASColumnMethods.fact
   SASColumnMethods.fnonct
   SASColumnMethods.gamma
   SASColumnMethods.lgamma
   SASColumnMethods.log
   SASColumnMethods.log1px
   SASColumnMethods.log10
   SASColumnMethods.log2
   SASColumnMethods.logbeta
   SASColumnMethods.mod
   SASColumnMethods.modz
   SASColumnMethods.sign
   SASColumnMethods.sqrt
   SASColumnMethods.tnonct
   SASColumnMethods.trigamma


CASTableGroupBy
---------------

:class:`CASTableGroupBy` objects are returned by :meth:`CASTable.grouppby`
and :meth:`CASColumn.groupby`.

Constructor
~~~~~~~~~~~

.. currentmodule:: swat.cas.table

.. autosummary::
   :toctree: generated/

   CASTableGroupBy 

Indexing and Iteration
~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTableGroupBy.__iter__
   CASTableGroupBy.get_group
   CASTableGroupBy.query

Conversion
~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTableGroupBy.to_frame
   CASTableGroupBy.to_series

Computations / Descriptive Statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   CASTableGroupBy.css
   CASTableGroupBy.cv
   CASTableGroupBy.describe
   CASTableGroupBy.head
   CASTableGroupBy.max
   CASTableGroupBy.mean
   CASTableGroupBy.median
   CASTableGroupBy.min
   CASTableGroupBy.mode
   CASTableGroupBy.nth
   CASTableGroupBy.nmiss
   CASTableGroupBy.nlargest
   CASTableGroupBy.nsmallest
   CASTableGroupBy.nunique
   CASTableGroupBy.probt
   CASTableGroupBy.quantile
   CASTableGroupBy.std
   CASTableGroupBy.stderr
   CASTableGroupBy.sum
   CASTableGroupBy.tvalue
   CASTableGroupBy.skew
   CASTableGroupBy.kurt
   CASTableGroupBy.unique
   CASTableGroupBy.uss
   CASTableGroupBy.value_counts
   CASTableGroupBy.var

CASResponse
-----------

:class:`CASResponse` objects are primarily used internally, but they
can be used in more advanced workflows.  They are never instantiated
directly, they will always be created by the :class:`CAS` connection
object and returned by an iterator.


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

Data message handlers are used to create custom data loaders.
They construct the parameters to the ``addtable`` CAS action and
handle the piece-wise loading of data into the server.

.. note:: Data message handlers are not currently supported in the
          REST interface.

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

The following date / time / datetime functions can be used to convert
dates to and from Python, CAS, and SAS date values.

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
