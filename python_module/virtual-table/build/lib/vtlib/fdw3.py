from multicorn import ForeignDataWrapper
import logging
import threading
from . import foreign_tables, postgres_type_len_lookup

class FDW3(ForeignDataWrapper): 
    def __init__(self, fdw_options, fdw_columns): 
        super(FDW3, self).__init__(fdw_options, fdw_columns) 
        self._fdw_columns = fdw_columns
        self._prepareData(fdw_options['tblname'])
        self._converted_columns = set()
        self.__lock__ = threading.Lock()

    def execute(self, quals, columns):
        if len(self._fdw_columns) == 1:
            for i in range( self._numRows ):
                yield self._data[i]
        else:
            with self.__lock__:
                self._convert( set(columns) -  self._converted_columns )

            for i in range( self._numRows ):
                # yield the a row with required columns only
                yield {col : self._data_in_rows[i][col] for col in columns}

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
                self._data = data

        except:
            data = foreign_tables[tblname]
            self._numRows = len(data.index)

            if len(self._fdw_columns) == 1:
                self._data = [ [e] for e in data.iloc[:,0] ]
            else:
                self._data = data
        
        self._data_in_rows = [ {} for _ in range(self._numRows) ]

    def _convert(self, cols):
        # for the columns that have not been converted, add its values into the result cache
        try: 
            new_data = self._data[cols].to_dict('records')
            for i in range( self._numRows ):
                self._data_in_rows[i].update( new_data[i] )

        except:
            for i in range( self._numRows ):
                for col in cols:
                    self._data_in_rows[i][col] = self._data[col][i]

        self._converted_columns.update(cols)

    def __del__(self):
        del self._data
        del self._self._converted_columns
        del self._data_in_rows
        del self.__lock__
