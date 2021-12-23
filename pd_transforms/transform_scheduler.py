import logging
from enum import Enum

from aidacommon.dborm import TabularData
from aidas.dborm import DBTable, SQLSelectTransform

strategy_pd = 'use_pandas'
strategy_db = 'use_db'
condition_source = 'condition_on'


class LineageNode:
    def __init__(self, node: TabularData, meta=None, sources={}):
        self.node = node
        self.meta = meta
        self.sources = sources
        self.strategy = 'use_pandas'

    def set_strategy(self, strategy):
        self.strategy = strategy

    def add_source(self, link, source):
        assert isinstance(source, LineageNode)
        self.sources[link] = source

    def set_meta(self, meta):
        self.meta = meta

    def __str__(self):
        s = '*' + str(self.node) + '*\n'
        s += '  meta: ' + str(self.meta) + '\n'
        s += '  sources: ' + '\n'
        for link, node in self.sources.items():
            s += '\t' + link + ': \n'
            for line in str(node).split('\n'):
              s += '\t  ' + line + '\n'
        s += '  strategy: ' + self.strategy + '\n'
        return s


def get_meta_data(tb: TabularData):
    meta_clc = {'col': None, 'row': None, 'col_attributes': None}
    if tb.__data__ is not None:
        meta_clc['col'] = tb.shape[0]
        meta_clc['row'] = tb.shape[1]
        meta_clc['col_attributes'] = tb.columns
    else:
        """
        If no data is inside the memory, send query to db to retrieve the column and row numbers
        """
        from aidas.dborm import DBTable
        if isinstance(tb, DBTable):
            """retrieved value is a tuple with form ({'L1', array[]}, 1)"""
            rowNum = tb.dbc._getTableRowCount(tb.__tableName__)
            colNum = tb.dbc._getTableColumnCount(tb.__tableName__)
            strNum = tb.dbc._getTableStrCount(tb.__tableName__)
            meta_clc['col'] = colNum
            meta_clc['row'] = rowNum
    return meta_clc


class TransformScheduler:
    def __init__(self):
        pass

    def build_lineage(self, root: TabularData):
        pass

    def materialize(self, root: LineageNode):
        # materialize with pandas first since pandas needs to be executed bottom to top
        # execute in reverse order
        stack = [root]
        visited = set()
        pd_stack = []
        # BFS
        while stack:
            cur = stack.pop(0)
            # may have multiple tables have the same source
            # we skip it in this case
            if cur in visited:
                continue
            visited.add(cur)
            # add all nodes with pd strategy for later materialization
            if cur.strategy == strategy_pd:
                pd_stack.append(cur)
            for source in cur.sources.values():
                stack.append(source)

        logging.info(f'materializing: \n all_node = {visited}, \n pd_stack={pd_stack}')

        # materialize all pandas node, in reverse order
        while pd_stack:
            cur = pd_stack.pop()
            cur.node.run_query(with_pd=True)

        # materialize db nodes, DFS
        stack = [root]
        while stack:
            cur = stack.pop()
            # don't need to check further as the db will convert in-memory data and handle all upstream operations
            if cur.strategy == strategy_db:
                cur.node.run_query(with_pd=False)
                continue
            for source in cur.sources.values():
                stack.append(source)


class HeuristicScheduler(TransformScheduler):
    """
    simply check if the upstream data in pandas or db
    """
    def build_lineage(self, root: TabularData):
        # case join, has 2 sources
        meta = get_meta_data(root)
        lineage = LineageNode(root, meta, {})

        if root.__data__ is not None:
            lineage.set_strategy(strategy_pd)
        elif isinstance(root, DBTable):
            lineage.set_strategy(strategy_db)
        else:
            # follow the source's strategy if there is any
            if isinstance(root.__source__, tuple):
                lineage.add_source('join0', self.build_lineage(root.__source__[0]))
                lineage.add_source('join1', self.build_lineage(root.__source__[1]))
            else:
                ts = root.__transform__
                # handle the problem where a table is in the condition of a query
                if isinstance(ts, SQLSelectTransform):
                    for sc in ts.__selcols__:
                        if isinstance(sc._col2_, TabularData):
                            lineage.add_source(condition_source, self.build_lineage(sc._col2_))
                if ts is not None:
                    lineage.add_source(ts.transform_name(),  self.build_lineage(ts._source_))
            if len(lineage.sources) == 1:
                lineage.set_strategy(list(lineage.sources.values())[0].strategy)
            else:
            # when join 2 table, as long as one table is db, we use db
                for source in lineage.sources.values():
                    if source.strategy == strategy_db:
                        lineage.set_strategy(strategy_db)
                        return lineage
                lineage.set_strategy(strategy_pd)

        return lineage





