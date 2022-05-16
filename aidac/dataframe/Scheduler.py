from __future__ import annotations

from aidac.dataframe import DataFrame
from aidac.dataframe import DataFrame
from aidac.data_source.DataSourceManager import DataSourceManager
from aidac.exec.Executable import Executable


def _link_tb(df: DataFrame.RemoteTable) -> str | None:
    """
    check if the corresponding table exists in the remote data source
    @param df:
    @return: None if no matching table is found, otherwise return the name of the table
    """
    ds = df.source
    if df.tbl_name is not None:
        return df.tbl_name
    else:
        tid = str(df.__tid__)
        if tid not in ds.ls_tables():
            return None
        else:
            return tid


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

    def retrieve_meta_data(self, df: DataFrame.RemoteTable):
        # todo: solve multiple ds
        tb_name = _link_tb(df)
        if tb_name is None:
            assert df.transform is not None, "A table without a remote linked to it must have a transform"
            df.transform.column
            return None
        else:
            return df.source.table_meta_data(tb_name)



