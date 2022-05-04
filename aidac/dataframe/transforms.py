import weakref


class Transform:
    def transform_name(self):
        pass
    def applyTransformation(self, data):
        pass


class TableTransform(Transform):
    def __init__(self, tableTransformFunc):
        self.tableTransformFunc = tableTransformFunc

    def applyTransformation(self, data):
        return self.tableTransformFunc(data)
#------------------------------------------------

#Base class for all SQL Transformations.
class SQLTransform(Transform):
    def __init__(self, source):
        self._source_    = weakref.proxy(source) if(source) else None
        self.__columns__ = None
        self._real_source_ = self._source_

    @property
    #The columns that will be produced once this transform is applied on its source.
    def columns(self):
        if(not self.__columns__):
            self.__columns__ = copy.deepcopy(self._source_.columns)
        return self.__columns__

    #The SQL equivalent of applying this transformation in the database.
    @property
    def genSQL(self):pass


class SQLSelectTransform(SQLTransform):
    def __init__(self, source, *selcols):
        super().__init__(source);
        self.__selcols__  = selcols

    def transform_name(self):
        return 'select'

    @property
    def genSQL(self):
        selCondition = '';
        for sc in self.__selcols__: #iterate through the filter conditions.
            srccollist = sc.srcColList; #get the source columns list involved in each one.
            for s in srccollist:
                c=self._source_.columns.get(s) #check if the columns are present in the source
                if(not c): #column not found in source.
                    raise AttributeError(c);
            #Append it to the select SQL condition.
            selCondition = ( (selCondition + ' AND ') if(selCondition) else ''  ) + sc.columnExpr;

        cols = None
        for c in self.columns: #Form list of columns for select
            cols = (cols + ' ,' + c) if(cols) else c;
        sqlText =   ( 'SELECT ' + cols + ' FROM ' +
                          # This is the SQL for the source table
                         '(' + (self._source_.genSQL.sqlText) + ') ' + self._source_.tableName +
                      ' WHERE ' + selCondition
                    );
        return SQLQuery(sqlText);
