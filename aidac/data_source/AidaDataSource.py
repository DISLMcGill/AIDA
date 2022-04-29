from aida.aida import AIDA
from aidac.data_source.DataSource import DataSource


class AidaDataSource(DataSource):
    def connect(self):
        self.__conn = AIDA.connect(self.host, self.port, self.username, self.job_name)

    def ls_tables(self):
        return self.__conn.tables
