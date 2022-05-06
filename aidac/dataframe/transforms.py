from __future__ import annotations

import collections
import copy
import weakref

from aidac import DataFrame
from aidac.common.column import Column


class Transform:
    def transform_name(self):
        pass


class TableTransform(Transform):
    def __init__(self, tableTransformFunc):
        self.tableTransformFunc = tableTransformFunc

    def applyTransformation(self, data):
        return self.tableTransformFunc(data)


# ------------------------------------------------

# Base class for all SQL Transformations.

def _col_in_source(source: DataFrame, col: Column):
    if source.has_transform():
        scols = source._transform_.columns
    else:
        scols = source.columns
    if col in scols:
        return True
    return False

def infer_col_type(cols: Column):
    pass


class SQLTransform(Transform):
    def __init__(self, source):
        self._columns_ = None
        self._source_ = weakref.proxy(source) if source else None

    @property
    # The columns that will be produced once this transform is applied on its source.
    def columns(self):
        if (not self._columns_):
            self._columns_ = copy.deepcopy(self._source_.columns)
        return self._columns_

    # The SQL equivalent of applying this transformation in the database.
    @property
    def genSQL(self): pass


class SQLProjectionTransform(SQLTransform):
    def __init__(self, source, projcols):
        super().__init__(source)
        self._projcols_ = projcols

    def _gen_column(self, source):
        if not self._columns_:
            colcount = 0

            def _get_proj_col_info(c: dict|str):
                nonlocal colcount
                colcount += 1
                if isinstance(c, dict):  # check if the projected column is given an alias name
                    sc1 = list(c.keys())[0]  # get the source column name / function
                    pc1 = c.get(sc1)  # and the alias name for projection.
                else:
                    sc1 = pc1 = c  # otherwise projected column name / function is the same as the source column.
                # we only consider one possible source column as the renaming is one-one
                # todo: may need to extend this to use F class
                srccol = sc1
                # projected column alias, use the one given, else take it from the expression if it has one, or else generate one.
                projcol = pc1 if (isinstance(pc1, str)) else (
                    sc1.columnExprAlias if (hasattr(sc1, 'columnExprAlias')) else 'col_'.format(colcount))
                # coltransform = sc1 if (isinstance(sc1, F)) else None
                # todo: extend this to use F class
                coltransform = None
                return srccol, projcol, coltransform

            src_cols = source.columns
            # columns = {};
            columns = collections.OrderedDict();
            for col in self._projcols_:
                srccol, projcoln, coltransform = _get_proj_col_info(col)

                sdbtables = []
                srccols = []
                scol = src_cols.get(srccol)
                if not scol:
                    raise AttributeError("Cannot locate column {} from {}".format(scol, source))
                else:
                    srccols += (scol.column_name if (isinstance(scol.column_name, list)) else [scol.column_name])
                    sdbtables += (scol.db_tbl if (isinstance(scol.db_tbl, list)) else [scol.db_tbl])

                column = Column(projcoln, scol.dtype)
                column.column_name = projcoln
                column.src_col_name = srccols
                column.db_tbl = sdbtables
                column.colTransform = coltransform
                columns[projcoln] = column
            self._columns_ = columns

    @property
    def columns(self):
        if not self._columns_:
            self._gen_column(self._source_)
        return self._columns_

    @property
    def genSQL(self):
        projcoltxt = None
        for c in self.columns:  # Prepare the list of columns going into the select statement.
            col = self.columns[c];
            projcoltxt = ((projcoltxt + ', ') if (projcoltxt) else '') + ((col.colTransform.columnExpr if (
                col.colTransform) else col.source_col_name[0]) + ' AS ' + col.column_name);

        sql_text = ('SELECT ' + projcoltxt + ' FROM '
                   + '(' + self._source_.genSQL.sqlText + ') ' + self._source_.tableName  # Source table transform SQL.
                   )

        return sql_text
