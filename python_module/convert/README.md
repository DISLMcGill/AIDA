# Conversion Module
## Description 
This conversion module is a C extension module using Python’s extension APIs. It aims to facilitate the process of converting the row-based data returned from the [plpy.execute(...)  ](https://www.postgresql.org/docs/12/plpython-database.html) function to a column-based format that is compliant with AIDA framework.
## Setup
Run the following command from this directory.
```shell
python3 setup.py install 
```

## Usage
The convert function returns a dictionary with each key as a column name. The value under a key is a NumPy array holding the data in that column.
```python3
import convert from convert

# data(list): A list of dictionaries that each dictionary holds data from a row.
# col_names(list): A list that contains column names.
# row_num(int): The number of rows.
# col_num(int): The number of columns.

data_cols = convert(data, col_names, row_num, col_num)

```
## Uninstall
Run the following command from this directory.
```shell
pip3 uninstall convert
```
