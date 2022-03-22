import logging
from enum import Enum

from aidacommon.dborm import TabularData, COL
from aidas.dborm import DBTable, SQLSelectTransform, SQLProjectionTransform, SQLJoinTransform, DataFrame, \
    SQLAggregateTransform

STRATEGY_PD = 'use_pandas'
STRATEGY_DB = 'use_db'
CONDITION_SOURCE = 'condition_on'


class LineageNode:
    def __init__(self, node: TabularData,  meta=None):
        # create a new lineage on top of the old one
        self.node = node
        self.meta = meta
        self.sources = {}
        self.strategy = None

    # call after all sources added
    def update_node(self):
        if hasattr(self.node, 'relink_source'):
            self.node.relink_source()

    def set_strategy(self, strategy):
        self.strategy = strategy

    def add_source(self, link, source):
        assert isinstance(source, LineageNode)
        self.sources[link] = source

    def set_node_source(self, val):
        setattr(self.node, '__source__', val)

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
        else:
            """ask the query planner to give an estimation of the cardinality"""
            row, width = tb.dbc._get_estimated_card(tb._genSQL_())
            meta_clc['col'] = row
            meta_clc['row'] = width

    return meta_clc


class TransformScheduler:
    def __init__(self):
        pass

    def build_lineage(self, root: TabularData):
        pass

    def has_more_rows(self, tbs):
        largest = tbs[0]
        for tb in tbs:
            if tb.meta['row']>largest.meta['row']:
                largest = tb
        return largest

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
            if cur.strategy == STRATEGY_PD:
                pd_stack.append(cur)
            for source in cur.sources.values():
                stack.append(source)

        logging.info('materializing: \n all_node = {}, \n pd_stack={}'.format(visited, pd_stack))

        # materialize all pandas node, in reverse order
        while pd_stack:
            cur = pd_stack.pop()
            logging.info('execute pandas for {}'.format(cur.node.__tableName__))
            cur.node.run_query(with_pd=True)

        # materialize db nodes, DFS
        stack = [root]
        logging.info('--------------after finish pandas, lineage-------------: \n{}'.format(root.node.preview_lineage()))
        while stack:
            cur = stack.pop()
            # don't need to check further as the db will convert in-memory data and handle all upstream operations
            if cur.strategy == STRATEGY_DB:
                cur.node.run_query(with_pd=False)
                continue
            for source in cur.sources.values():
                stack.append(source)
        return root.node.__data__


def pushdown_filters(root: TabularData):
    """
    push down (to leave nodes/database tables) the filters transform to lower layer if possible

    @param root:
    @return: the new node at the original location
    """
    stack = [root]
    prev = None
    # store each filter node and its downstream/child node
    filters = []
    # DFS find all filters and their downstream/child nodes
    while stack:
        cur = stack.pop()
        if hasattr(cur, '__transform__') and isinstance(cur.__transform__, SQLSelectTransform):
            # filter can have at most one source, so no need to worry about tuples
            # multiple nodes might have a same source, need to check it
            if (cur, prev) not in filters:
                filters.append((cur, prev))
        if hasattr(cur, '__source__') and cur.__source__ is not None:
            if isinstance(cur.__source__, tuple):
                for source in cur.__source__:
                    stack.append(source)
            else:
                stack.append(cur.__source__)
        prev = cur

    # by default, if no filters involved, we return the original node
    prev = root
    # pushdown nodes with smaller height for each branch
    while filters:
        to_push, parent = filters.pop()
        # collect all condition function
        conditions = set()
        for sc in to_push.__transform__.__selcols__:
            conditions.add(sc._col1_)
        source = to_push.__source__
        # true if we push down current node
        if pushdown(to_push, conditions, source):
            # if current node does not have a parent, which is true only when it's the root node,
            # we return its child/source to be the new root
            if parent is None:
                prev = source
            # if current node has a parent, then the parent's child/source should be to_push node's child/source
            else:
                parent.update_source(source)

    return prev


"""
retrieve the projected names for renaming as a set 
"""
def get_rename_ls(node):
    # todo: pass down the condition using the name before renaming
    trans = node.__transform__
    if trans is not None:
        renames = set()
        # if the condition is the renamed project columns, then the filter can not pass this
        if isinstance(node, SQLProjectionTransform) or isinstance(node, SQLAggregateTransform):
            for old_name, new_name in trans.__projcols__:
                renames.add(new_name)
    return set()


def columns_names(columns):
    cols = set()
    for col in columns:
        cols.add(col)
    return cols


def check_can_pass(condition, child):
    renames = get_rename_ls(child)
    return condition.isdisjoint(renames)


def check_condition(condition, child, gc):
    des_cols = columns_names(gc.columns)
    return check_can_pass(condition, child) and condition.issubset(des_cols)


def check_join_condition(condition, gp1, gp2):
    # print(f'check condition for {gp1.__tableName__} and {gp2.__tableName__}')
    des_cols1 = columns_names(gp1.columns)
    des_cols2 = columns_names(gp2.columns)
    # subset
    issub1 = condition.issubset(des_cols1)
    issub2 = condition.issubset(des_cols2)
    # check if any condition column in the grandparent columns
    intersects1 = condition.intersection(des_cols1)
    intersects2 = condition.intersection(des_cols2)
    # print(f'gp1{gp1.columns}, gp2 {gp2.columns}, cond {condition}')
    # print(f'issub1={issub1}, issub2={issub2}')
    # todo: split the filter conditions
    return (issub1 and issub2) or (issub1 and not intersects2) or (issub2 and not intersects1)


def pushdown(to_push: TabularData, condition: set, child: TabularData):
    """
    place to_push to child's source position if possible
    @param to_push: node to be pushed down
    @param condition: filter conditions on to_push
    @param child: to_push's source/child
    @return:
    """
    # child should not be tuple because we push down the filter for each branch
    assert not isinstance(child, tuple)
    # flag indicating if this node is pushed down or not
    pushed = False
    if child is not None:
        if hasattr(child, '__source__') and child.__source__ is not None:
            if isinstance(child.__source__, tuple):
                if check_can_pass(condition, child) and check_join_condition(condition, child.__source__[0], child.__source__[1]):
                    # try to recursively push down each branch
                    pushed1 = pushdown(to_push, condition, child.__source__[0])
                    # if can not push pass the grandchildren, insert filter at current level if condition satisfied
                    if not pushed1 and check_condition(condition, child, child.__source__[0]):
                        # copy lineage tree from the to push node
                        push_copy = to_push.partial_copy()
                        push_copy.update_source(child.__source__[0])
                        child.update_source((push_copy, child.__source__[1]))
                        pushed1 = True
                    pushed2 = pushdown(to_push, condition, child.__source__[1])
                    if not pushed2 and check_condition(condition, child, child.__source__[1]):
                        push_copy = to_push.partial_copy()
                        push_copy.update_source(child.__source__[1])
                        child.update_source((child.__source__[0], push_copy))
                        pushed2 = True
                    # todo: double check the condition here, should not affect though as it will be dumpy filter
                    pushed = pushed1 and pushed2
            else:
                if check_condition(condition, child, child.__source__):
                    pushed = pushdown(to_push, condition, child.__source__)
                    # if can not push pass the grandchildren, try insert filter at current level
                    if not pushed:
                        push_copy = to_push.partial_copy()
                        push_copy.update_source(child.__source__)
                        child.update_source((push_copy, child.__source__))
                        pushed = True
    return pushed


def build_single_node(root, strategy, source1, source2=None):
    """
    Build a single lineage node with the source1 and source2 added to the source
    if two sources are provided, then assume it's join
    @param root: current tabularData object
    @param strategy: strategy to use
    @param source1:
    @param source2:
    @return: lineage node
    """
    meta = get_meta_data(root)
    lineage = LineageNode(root, meta)

    if source2 is None:
        lineage.add_source(root.__transform__.transform_name(), source1)
    else:
        lineage.add_source('join0', source1)
        lineage.add_source('join1', source2)
    lineage.set_strategy(strategy)
    return lineage


class SplitJoinScheduler(TransformScheduler):

    # do_semi is just for purpose of the test
    def build_lineage(self, root: TabularData):
        # we need to push down the filters to lower levels if there is any
        new_root = pushdown_filters(root)
        # case join, has 2 sources
        return self._build_lineage(new_root)

    def _build_lineage(self, root):
        meta = get_meta_data(root)
        lineage = LineageNode(root, meta)
        if root.__data__ is not None:
            # if parent already materialized, use pandas
            lineage.set_strategy(STRATEGY_PD)
        elif isinstance(root, DBTable):
            # if parent is database table, use db
            lineage.set_strategy(STRATEGY_DB)
        else:
            # follow the source's strategy if there is any
            # need to update the reference to the new aida tabularData
            if isinstance(root.__source__, tuple):
                # perform semi join if the two sources from different locations
                s1 = self.build_lineage(root.__source__[0])
                s2 = self.build_lineage(root.__source__[1])
                # logging.info('join table1 = {}, table2 = {}'.format(s1.node.__tableName__, s2.node.__tableName__))
                # logging.info('schedule for join, s1={}, s2={}'.format(s1.strategy, s2.strategy))
                if s1.strategy != s2.strategy:
                    self.semijoin(root.__transform__, 1, s1, s2, lineage)
                else:
                    lineage.add_source('join0', s1)
                    lineage.add_source('join1', s2)
            else:
                ts = root.__transform__
                # handle the problem where a table is in the condition of a query
                if isinstance(ts, SQLSelectTransform):
                    for sc in ts.__selcols__:
                        if isinstance(sc._col2_, TabularData):
                            s = self.build_lineage(sc._col2_)
                            lineage.add_source(CONDITION_SOURCE, s)
                if ts is not None:
                    s = self.build_lineage(ts._source_)
                    lineage.add_source(ts.transform_name(), s)
                lineage.set_strategy(list(lineage.sources.values())[0].strategy)

        return lineage

    """
    implement semijoin by creating intermediate transforms
    handle the node creations for the intermediate nodes
    return a new node that holds the root after semijoin

    step1: temp1 = projection of intersection of join columns on table1
    step2: temp2 = join table2 join temp1
    step3: result = temp2 join table1
    """
    def semijoin(self, join_transform, source_tbl, source1, source2, lineage):
        cur = lineage.node

        tbl1 = source1.node if source_tbl == 1 else source2.node
        col1 = join_transform._src1joincols_ if source_tbl == 1 else join_transform._src2joincols_
        projcol1 = join_transform._src1projcols_ if source_tbl == 1 else join_transform._src2projcols_
        tbl2 = source2.node if source_tbl == 1 else source1.node
        col2 = join_transform._src2joincols_ if source_tbl == 1 else join_transform._src1joincols_
        projcol2 = join_transform._src2projcols_ if source_tbl == 1 else join_transform._src1projcols_

        # step1
        temp1 = DataFrame(tbl1, SQLProjectionTransform(tbl1, col1), 'step1_project')
        temp1_node = build_single_node(temp1, source1.strategy, source1)

        # step2
        temp2 = DataFrame((temp1, tbl2), SQLJoinTransform(temp1, tbl2, col1, col2, COL.ALL, projcol2), 'step2_join')
        temp2_node = build_single_node(temp2, source2.strategy, temp1_node, source2)

        # step3
        rs = SQLJoinTransform(tbl1, temp2, col1, col1, projcol1, COL.ALL)

        # reset the lineage
        cur.__source__ = (tbl1, temp2)
        cur.__transform__ = rs
        lineage.set_strategy(source1.strategy)
        lineage.add_source('join0', source1)
        lineage.add_source('join1', temp2_node)


class HeuristicScheduler(TransformScheduler):
    def build_lineage(self, root: TabularData):
        # we need to push down the filters to lower levels if there is any
        new_root = pushdown_filters(root)
        # case join, has 2 sources
        return self._build_lineage(new_root)

    """
    simply check if the upstream data in pandas or db
    """
    def _build_lineage(self, root: TabularData):
        # case join, has 2 sources
        lineage = LineageNode(root)

        if root.__data__ is not None:
            # if parent already materialized, use pandas
            lineage.set_strategy(STRATEGY_PD)
        elif isinstance(root, DBTable):
            # if parent is database table, use db
            lineage.set_strategy(STRATEGY_DB)
        else:
            # follow the source's strategy if there is any
            # need to update the reference to the new aida tabularData
            if isinstance(root.__source__, tuple):
                s1 = self.build_lineage(root.__source__[0])
                lineage.add_source('join0', s1)
                s2 = self.build_lineage(root.__source__[1])
                lineage.add_source('join1', s2)
                # lineage.set_node_source((s1.node, s2.node))
            else:
                ts = root.__transform__
                # handle the problem where a table is in the condition of a query
                if isinstance(ts, SQLSelectTransform):
                    for sc in ts.__selcols__:
                        if isinstance(sc._col2_, TabularData):
                            s = self.build_lineage(sc._col2_)
                            sc._col2_ = s.node
                            lineage.add_source(CONDITION_SOURCE, s)
                if ts is not None:
                    s =  self.build_lineage(ts._source_)
                    lineage.add_source(ts.transform_name(), s)
                    # lineage.set_node_source(s.node)
            if len(lineage.sources) == 1:
                strategy = list(lineage.sources.values())[0].strategy
                lineage.set_strategy(strategy)
            elif len(lineage.sources) > 1:
            # when join 2 table, we set the strategy the same as the parent table with more rows
                dom_table = self.has_more_rows(lineage.sources.values())
                lineage.set_strategy(dom_table.strategy)
            # materialize with pandas to get meta info
            if lineage.strategy == STRATEGY_PD:
                lineage.node.run_query(with_pd=True)

            lineage.set_meta(get_meta_data(root))

        # lineage.update_node()
        return lineage

    def materialize(self, root: LineageNode):
        # if the root is pd, should have already been materialized
        if root.strategy == STRATEGY_PD:
            return root.node.__data__
        return root.node.run_query(with_pd=False)




