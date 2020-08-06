# AIDA DB Adapter for PostgreSQL
## Description
Since AIDA mainly works with data in column-format for statistical functionalities, the row-by-row resultset provided by PostgreSQL’s embedded Python APIs cannot be directly applied to AIDA. Therefore, this requires the database adapter to perform such transformation. 

---
## Conversion Approaches

This database adapter has following conversion approaches.
<br>
1. Generate a NumPy array for each column by iterating through the rows naively. 
2. Pass the row-based resultset to a C extension module and perform the conversion in C.
3. Modify the PostgreSQL embedded Python API to produce a column-based resultset.

<br>
<br>

* To change the conversion option on the server end, set the [CONVERSIONOPTION](https://github.com/joedsilva/AIDA/blob/postgresqlAdapter/misc/aidaconfig.ini) variable to the corresponding number.


* The extension module for conversion can be found [here](https://github.com/joedsilva/AIDA/tree/postgresqlAdapter/python_module)

* The modified PostgreSQL source code can be found in this [branch](https://github.com/H-Cao/postgres/tree/REL_12_STABLE)
