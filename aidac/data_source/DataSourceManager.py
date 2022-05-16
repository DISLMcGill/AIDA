import uuid

from aidac.data_source.DataSource import DataSource
from aidac.data_source.PostgreDataSource import PostgreDataSource
from aidac.data_source.exceptions import DataSourceException


class DataSourceManager:
    def __init__(self):
        self.sources = {}

    def add_data_source(self, source: str, host: str, user: str, password: str, db: str, job_name: str, port: str):
        """
        Create a data source of specified source type
        @param source: data source type
        @param host: host address
        @param port: port number
        @param user: username
        @param password:
        @param db: database name
        @param job_name: user given job name which uniquely identify the data source
        @return:
        """
        if job_name in self.sources:
            raise DataSourceException("Job {} already exists".format(job_name))

        self.sources[job_name] = data_source_factory.create_data_source(source, host, user, password, db, job_name, port)
        self.sources[job_name].connect()

    def tables(self) -> dict:
        """
        List all tables in all data sources in string
        @return: the dict representation of each data source and their tables
        """
        table_ls = {}
        for job, source in self.sources.items():
            table_ls[job] = self.source.ls_tables()
        return table_ls

    def get_data_source(self, job) -> DataSource:
        try:
            return self.sources[job]
        except KeyError:
            raise DataSourceException('Data source with the job name {} does not exist.'.format(job))

    def send_data(self, origin: str, dest: str):
        pass


class DataSourceFactory:
    def __init__(self):
        pass

    def create_data_source(self, source: str, host: str, user: str, password: str, db: str, job_name: str = None,
                           port: str = None):
        if job_name is None:
            job_name = source+'_'+str(uuid.uuid4())
        if source == 'postgres':
            return PostgreDataSource(host, user, password, db, job_name, port)


data_source_factory = DataSourceFactory()
