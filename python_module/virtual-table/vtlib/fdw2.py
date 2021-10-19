from multicorn import ForeignDataWrapper
import logging
from . import foreign_tables, postgres_type_len_lookup

class FDW2(ForeignDataWrapper): 
    def __init__(self, fdw_options, fdw_columns): 
        super(FDW2, self).__init__(fdw_options, fdw_columns) 
        self._fdw_columns = fdw_columns
        self._prepareData(fdw_options['tblname'])

    def execute(self, quals, columns):
        if len(self._fdw_columns) == 1:
            for i in range( self._numRows ):
                yield self._data[i]
        else:
            for i in range( self._numRows ):
                yield {col : self._data[i][col] for col in columns}

    def get_rel_size(self, quals, columns):
        expected_width = 0
        for col in columns:
            col_type_oid = self._fdw_columns[col].type_oid
            if col_type_oid in postgres_type_len_lookup.keys():
                expected_width += postgres_type_len_lookup[col_type_oid]
            else:
                # assume the variable-length type column has an average length of 32 bytes
                expected_width += 32

        return (self._numRows, expected_width) 

    def _prepareData(self, tblname):
        try: 
            data = foreign_tables[tblname].rows
            self._numRows = foreign_tables[tblname].numRows

            if len(self._fdw_columns) == 1:
                self._data = [ [e] for e in data[next(iter(self._fdw_columns))]  ]
            else:
                self._data = [dict(zip(data,t)) for t in zip(*data.values())]

        except:
            data = foreign_tables[tblname]
            self._numRows = len(data.index)

            if len(self._fdw_columns) == 1:
                self._data = [ [e] for e in data.iloc[:,0] ]
            else:
                self._data = data.to_dict('records')

    def __del__(self):
        del self._data

