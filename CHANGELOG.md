
# Change Log

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
