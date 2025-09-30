
# Change Log

## 1.17.0 - 2025-09-30

- Add Python 3.13 support
- Update TK subsystem
- Add Openssl 3 support

## 1.16.0 - 2025-08-15

- Update TK subsystem
- PowerPC is no longer supported

## 1.15.0 - 2024-12-05

- Add Python 3.12 support
- Update TK subsystem
- Conda packages will no longer be generated

## 1.14.0 - 2024-06-07
 
- Add support for Proof Key for Code Exchange (PKCE) 
- Update TK subsystem
- Wheel and Conda files for Python releases < 3.7 are no longer created

## 1.13.3 - 2023-08-31

- add wheel files for macosx_11_0_arm64

## 1.13.2 - 2023-07-31

- Add CASTable.rename() to rename the columns of a table
- Add biomedical image filetypes to the Image Data Message Handler 

## 1.13.1 - 2023-07-14

- Add nunique method for CASTable
- Add drop_duplicates method for CASTable
- Add new testcases to swat/tests/test_dataframe.py and swat/tests/cas/test_table.py 

## 1.13.0 - 2023-04-20

- Add Python 3.11 support 
- Update TK subsystem

## 1.12.2 - 2023-04-14

- updates to swat/tests/cas/test_imstat.py for benchmark changes 
- updates to swat/tests/cas/test_builtins.py for benchmark changes
- cleanup deprecation warning messages
- improve error message when on, onleft, onright merge parameters contain a list rather than a string

## 1.12.1 - 2023-01-09

- Update Authentication documentation

## 1.12.0 - 2022-11-11

- New Image CASDataMsgHandler to allow easy uploading of client-side images to a CAS table
- Update TK subsystem

## 1.11.0 - 2022-07-05

- Update TK subsystem

## 1.10.0 - 2022-06-10

- Add Python 3.10 support
- Update TK subsystem

## 1.9.3 - 2021-08-06

- Fix showlabels issue in Viya deployment before version 3.5

## 1.9.2 - 2021-06-18

- Add authorization code as authentication method

## 1.9.1 - 2021-06-11

- Add Python 3.9 support

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
