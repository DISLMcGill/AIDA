from aidac.data_source.DataSourceManager import DataSourceManager


class Scheduler:
    def __init__(self):
        self.sources = {}
        self.source_manager = DataSourceManager()

    def transfer(self, src, dest):
        scols = src.columns
        # todo: check for duplicate names
        dest.datasource.create_table(src.table_name, scols)
        dest.datasource.import_table(src.table_name, src.data)

    def schedule(self, src, dest):
        self.transfer(src, dest)

