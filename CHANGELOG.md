
# Change Log

## 1.9.0 - 2021-05-24

- Add additional plotting method parameters for correct data fetches
- Add `date_format=` to `CAS.upload` method for formatting CSV exported data to a specific format
- Update TK subsystem

## 1.8.1 - 2021-01-21

- Fix compatibility with pandas 1.2 DataFrame rendering
- Fix TKECERT error by locating libcrypto automatically

## 1.8.0 - 2021-01-12

- Use ssl module's CA list as default
- Improve initial connection performance
- Fix OAuth authentication in REST connections
- Fix log truncation with messages >1000 characters

## 1.7.1 - 2020-09-29

- Add ability to apply SAS formats to columns in a `SASDataFrame`
- Support timezones in data uploaded and downloaded from CAS tables
- Fix issue with TK path on Windows when using flask

## 1.7.0 - 2020-08-19

- Add Python 3.8 support
- Improve connection parameter handling (add CAS_URL= connection variable)
- Improve connection protocol auto-detection

## 1.6.1 - 2020-02-10

- Add pandas v1.0.0 support

## 1.6.0 - 2019-11-21

- Fix letter-casing in `has_action` and `has_actionset` methods
- Remove usage of deprecated `ix` accessor
- Explicitly specify column and line delimiters and locale in uploaded CSV files
- Fix TKPATH issues in Windows and PPC

## 1.5.2 - 2019-09-09

- Fix issue with nulls in REST parameters
- Add fallback default configuration for SSL
- Add `CASTable.get` method

## 1.5.1 - 2019-03-01

- Fix Authinfo matching when using base URLs in REST interface
- Fix compatibility with pandas 0.24
- Fix blob parameters in REST interface
- Add `CASTable.last_modified_date`, `CASTable.last_accessed_date`, and `CASTable.created_date` properties
- Add reverse operator methods to `CASColumn`

## 1.5.0 - 2018-09-18

- Windows support for binary CAS protocol
- Added `with_params` method to `CASTable` to create one-off parameter object
- Set appropriate column data types when uploading a `DataFrame`

## 1.4.0 - 2018-07-25

- Automatic CAS table creation when large number of By groups are generated in some DataFrame methods
- Debugging options for REST interface
- Python 3.7 support

## 1.3.1 - 2018-06-04

- Increase compatibility with older and newer versions of pandas
- Automatically convert columns with SAS date/time formats to Python date/time objects
- Improve `CASTable.merge` algorithm
- Fix autocompletion on `CAS` and `CASTable` objects

## 1.3.0 - 2017-12-12

- Add new summary statistics for new version of CAS
- Improve missing value support in `CASTable` `describe` method
- Add controller failover support
- Improve encrypted communication support
- Add `add`, `any`, `all`, `merge`, and `append` methods to `CASTable`
- Add `merge` and `concat` functions with `CASTable` support

## 1.2.1 - 2017-09-13

- Better support for binary data in table uploads and parameters
- Add integer missing value support
- Allow list parameters to also be sets
- Improve connection protocol detection
- Add `eval` method to `CASTable`

## 1.2.0 - 2017-05-02

- Use `upload` action rather than `addtable` for `read_*` methods.
- Add basic Zeppelin notebook support (`from swat.notebook.zeppelin import show`)

## 1.1.0 - 2017-03-21

- Add support for Python 3.6 (Linux extension)
- Implement `sample` method on `CASTable`
- Add sampling support to plotting methods
- `cas.dataset.max_rows_fetched` increased to 10,000
- Add `terminate` method to `CAS` object to end session and close connection
- Implement `fillna`, `replace`, and `dropna` methods on `CASTable`
- Add `apply_labels` method on `SASDataFrame` to set column labels as column names

## 1.0.0 - 2016-09-27

- Initial Release
