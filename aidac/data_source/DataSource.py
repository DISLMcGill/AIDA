# from aidac.data_source.AidaDataSource import AidaDataSource


class DataSource:
    def __init__(self, host, username, password, port=None, dbname=None, job_name=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
        self.job_name = job_name
        self.__conn = None
        self.__cursor = None
        self.mapping = {}

    def connect(self):
        pass

    def ls_tables(self):
        """
        List all user tables in current database
        @return: sql query
        """
        pass

    def import_table(self, table_name, data):
        """
        send data to the data source as a table
        @param table_name: insert to table
        @param data: data to be inserted
        @return:
        """
        pass

    def create_table(self, table_name, cols):
        """
        create a table using the table name and columns specified
        @param table_name: table name
        @param kwargs: column name and type pair as dictionary
        @return:
        """
        pass

    def _execute(self, query):
        pass

    def table_meta_data(self, table):
        """
        Retrieve the column metadata of a table
        @param table:
        @return:
        """
        pass


class DataSourceFactory:
    def __init__(self):
        pass

    # def create_data_source(self, source: str, host: str, port: str, user: str, password: str, db: str, job_name: str):
    #     if source == 'aida':
    #         return AidaDataSource(host, port, user, password, db, job_name)


data_source_factory = DataSourceFactory()

