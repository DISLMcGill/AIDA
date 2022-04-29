from typing import Iterator, Dict, Any, Optional
import io

import numpy as np

from aidac.data_source.DataSource import DataSource
from aidac.data_source.QueryLoader import QueryLoader
import psycopg

from aidac.data_source.ResultSet import ResultSet

DS = 'postgres'
ql = QueryLoader(DS)

typeConverter = {np.int8: 'TINYINT', np.int16: 'SMALLINT', np.int32: 'INTEGER', np.int64: 'BIGINT'
    , np.float32: 'FLOAT', np.float64: 'FLOAT', np.object: 'STRING', np.object_: 'STRING', bytearray: 'BLOB'
    , 'date': 'DATE', 'time': 'TIME', 'timestamp': 'TIMESTAMP'};


class PostgreDataSource(DataSource):
    def connect(self):
        self.port = 5432 if self.port is None else self.port

        self.__conn = psycopg.connect(
            f'''host={self.host} 
            dbname={self.dbname} 
            user={self.username} 
            password={self.password}'''
        )
        self.__cursor = self.__conn.cursor()

    def ls_tables(self):
        qry = ql.load_query('list_tables').format(self.dbname)
        return self._execute(qry).get_result_ls()

    def import_table(self, table: str, data):
        # todo: data type should be changed, maybe also create a col object for cols

        with self.__cursor.copy(ql.load_query('copy_data')) as copy:
            copy.write(data)

    def table_meta_data(self, table):
        qry = ql.load_query('table_meta_data').format(table)
        self._execute(qry)

    def cardinality(self, table):
        """

        @param table:
        @return:
        """
        qry = ql.load_query('row_number').format(table)
        rows = self._execute(qry).get_value()
        qry = ql.load_query('column_number').format(table)
        columns = self._execute(qry).get_value()
        return rows, columns

    def create_table(self, table_name, cols):
        col_def = []
        for cname, ctype in cols:
            db_type = typeConverter[ctype]
            col_def.append(str(cname)+': '+db_type)
        col_def = ', '.join(col_def)

        qry = ql.load_query('create_table').format(table_name, col_def)
        self._execute(qry)

    def _execute(self, qry) -> ResultSet:
        self.__cursor.execute(qry)
        rs = ResultSet(self.__cursor.description, self.__cursor.fetchall())
        return rs

