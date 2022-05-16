from __future__ import annotations

import numpy as np

from aidac.common.column import Column
from aidac.data_source.DataSource import DataSource
from aidac.data_source.QueryLoader import QueryLoader
import psycopg

from aidac.data_source.ResultSet import ResultSet

DS = 'postgres'
ql = QueryLoader(DS)

typeConverter = {np.int8: 'TINYINT', np.int16: 'SMALLINT', np.int32: 'INTEGER', np.int64: 'BIGINT'
    , np.float32: 'FLOAT', np.float64: 'FLOAT', np.object: 'STRING', np.object_: 'STRING', bytearray: 'BLOB'
    , 'date': 'DATE', 'time': 'TIME', 'timestamp': 'TIMESTAMP'};

typeConverter_rev = {'integer': np.int32, 'character varying': np.object, 'double precision': np.float64, 'boolean': bool}

constant_converter = {'YES': True, 'NO': False}


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
        qry = ql.list_tables()
        return self._execute(qry).get_result_ls()

    def import_table(self, table: str, cols: dict, data):
        # todo: allow to specify the columns to be inserted, maybe also create a col object for cols
        # todo: right now data iterate rows, rooms for optimization later
        column_name = ', '.join(list(cols.keys()))
        with self.__cursor.copy(ql.copy_data(table, column_name)) as copy:
            for row in data:
                copy.write_row(row)

    def table_meta_data(self, table: str):
        qry = ql.table_meta_data(table)
        rs = self._execute(qry)
        # expected return value from pd:
        # schemaname, tablename, columnname, columntype, columnsize, columnpos, nullable
        cols = [Column(x[2], typeConverter_rev[x[3]], x[1], x[0], constant_converter[x[-1]]) for x in rs.data]
        return cols

    def cardinality(self, table: str):
        """

        @param table:
        @return:
        """
        qry = ql.row_card(table)
        rows = self._execute(qry).get_value()
        qry = ql.column_card(table)
        columns = self._execute(qry).get_value()
        return rows, columns

    def create_table(self, table_name: str, cols: dict):
        """
        create a temporary table inside the db
        @param table_name:
        @param cols: data column definition
        @return: in db column definition
        """
        col_def = []
        for cname, ctype in cols.items():
            db_type = typeConverter[ctype]
            col_def.append(str(cname)+' '+db_type)
        col_def = ', '.join(col_def)

        qry = ql.create_table(table_name, col_def)
        self._execute(qry)
        return col_def

    def _execute(self, qry) -> ResultSet | None:
        self.__cursor.execute(qry)
        if self.__cursor.description is not None:
            # as no record returned for insert, update queries
            rs = ResultSet(self.__cursor.description, self.__cursor.fetchall())
            return rs
        return None
