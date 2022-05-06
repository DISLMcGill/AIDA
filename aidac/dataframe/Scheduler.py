from aidac.dataframe import DataFrame
from aidac.data_source.DataSourceManager import DataSourceManager
from aidac.exec.Executable import Executable


class Scheduler:
    def __init__(self):
        self.sources = {}
        self.source_manager = DataSourceManager()


    def transfer(self, src: DataFrame, dest: DataFrame):
        """
        Transfer data from one datasource to another
        @param src: source table to be transferred
        @param dest: dataframe whose datasource would be the destination
        @return: local stub points to the temporary table?
        """
        scols = src.columns
        # todo: check for duplicate names
        dest.datasource.create_table(src.table_name, scols)
        dest.datasource.import_table(src.table_name, src.data)
        # todo: decide if a local stub should be created


    def schedule(self, df: DataFrame) -> Executable:
        """
        Schedule and build execution pipes for the given dataframe
        @param df:
        @return:
        """
        pass



