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
        self.strategy = 'use_pandas'

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
            if cur.strategy == STRATEGY_PD:
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
            if cur.strategy == STRATEGY_DB:
                cur.node.run_query(with_pd=False)
                continue
            for source in cur.sources.values():
                stack.append(source)


"""
push down the filter transform to lower layer if possible
return the new node to the original location
"""
def pushdown_filter(to_push):
    trans = to_push.__transform__
    assert trans is not None and isinstance(trans, SQLSelectTransform), "Only a filter transformation can be pushed down"
    next_node = to_push.__source__

    # collect all condition function
    conditions = set()
    for sc in trans.__selcols__:
        conditions.add(sc._col1_)

    if next_node is not None:
        nns = next_node.__source__
        if nns is not None:
            if isinstance(nns, tuple):
                nnext1, nnext2 = nns
                can_push1, ns1, p1 = pushdown(to_push, conditions, next_node, nnext1)
                can_push2, ns2, p2 = pushdown(to_push, conditions, next_node, nnext2)
                if can_push1 and can_push2:
                    # resolve updating, after finish, return None
                    if ns1 is not None and ns2 is not None:
                        # p1 and p2 can only be same when p1=p2=grandparent
                        if p1 == p2:
                            p1.update_source((ns1, ns2))
                        else:
                            p1.update_source(ns1)
                            p2.update_source(ns2)
                    return next_node
            else:
                state, ns, p = pushdown(to_push, conditions, next_node, nns)
                if state:
                    if ns is not None:
                        p.update_source(ns)
                    return next_node
        return to_push

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

def check_can_pass(condition, parent):
    renames = get_rename_ls(parent)
    return condition.isdisjoint(renames)

def check_condition(condition, parent, gp):
    des_cols = columns_names(gp)
    return check_can_pass(condition, parent) and condition.issubset(des_cols)


def check_join_condition(condition, gp1, gp2):
    des_cols1 = columns_names(gp1)
    des_cols2 = columns_names(gp2)
    # subset
    issub1 = condition.issubset(des_cols1)
    issub2 = condition.issubset(des_cols2)
    # check if any condition column in the grandparent columns
    intersects1 = condition.intersection(des_cols1)
    intersects2 = condition.intersection(des_cols2)
    return (issub1 and issub2) or (issub1 and not intersects2) or (issub2 and not intersects1)

def pushdown(to_push, condition:set, parent, grandparent):
    assert not isinstance(parent, tuple)
    pushed = False
    if parent is not None:
        if parent.__source__ is not None:
            if isinstance(parent.__source__, tuple):
                if check_can_pass(condition, parent) and check_join_condition(condition, parent.__source__[0], parent.__source__[1]):
                    # try to recursively push down each branch
                    pushed1 = pushdown(to_push, parent.__source__[0])
                    # if grandparent and further down can not push, try insert filter at current level
                    if not pushed1 and check_condition(condition, parent, parent.__source__[0]):
                        to_push.update_source(parent.__source__[0])
                        parent.update_source((to_push, parent.__source__[1]))
                        pushed1 = True
                    pushed2 = pushdown(to_push, parent.__source__[1])
                    if not pushed2 and check_condition(condition, parent, parent.__source__[1]):
                        to_push.update_source(parent.__source__[1])
                        parent.update_source(( parent.__source__[1], to_push))
                        pushed2 = True
                    pushed = pushed1 or pushed2
            else:
                if check_condition(condition, parent, parent.__source__):
                    pushed = pushdown(to_push, parent.__source__)
                    # if grandparent and further down can not push, try insert filter at current level
                    if not pushed:
                        to_push.update_source(parent.__source__)
                        parent.update_source((to_push, parent.__source__))
                        pushed = True
    return False


            if to_push.__source__[0].__source__ is not None:
    des_cols = columns_names(grandparent.columns)
    renames = get_rename_ls()
    # todo: check table name as well
    # if the condition columns are not renamed ones and present in grandparent's columns
    if condition.isdisjoint(renames) and condition.issubset(des_cols):
        gs = grandparent.__source__
        if gs is None:
            new_filter = to_push.copy_with_source(grandparent)
            # if joinid != -1:
            #     ps = parent.__source__
            #     assert isinstance(ps, tuple),  "if joinid > -1 is passed, expecting the parent to have 2 sources"
            #     # create new source tuple
            #     new_source = (ps[0], new_filter) if joinid else (new_filter, ps[1])
            #     parent.update_source(ns)
            # else:
            #     new_source = new_filter
            #     parent.update_source(new_filter)
            return True, new_filter, parent
        else:
            if isinstance(gs, tuple):
                state1, ns1, p1 = pushdown(to_push, condition, grandparent, gs[0])
                state2, ns2, p2 = pushdown(to_push, condition, grandparent, gs[1])
                if state1 and state2:
                    # resolve updating, after finish, return None
                    if ns1 is not None and ns2 is not None:
                        # p1 and p2 can only be same when p1=p2=grandparent
                        if p1 == p2:
                            p1.update_source((ns1, ns2))
                        else:
                            p1.update_source(ns1)
                            p2.update_source(ns2)
                    return state1, None, None
            else:
                state, ns, p = pushdown(to_push, condition, grandparent, gs)
                if state:
                    if ns is not None:
                        p.update_source(ns)
                    return state, None, None
    return False, None, None


def build_single_node(root, strategy, source1, source2=None):
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
    """
    implement semijoin by creating intermediate transforms
    handle the node creations for the intermediate nodes
    return a new node that holds the root after semijoin

    step1: temp1 = projection of intersection of join columns on table1
    step2: temp2 = join table2 join temp1
    step3: result = temp2 join table1
    """
    def semijoin(self, join_transform, source_tbl, source1, source2):
        tbl1 = join_transform._source1_ if source_tbl == 1 else join_transform._source2_
        col1 = join_transform._src1joincols_ if source_tbl == 1 else join_transform._src2joincols_
        projcol1 = join_transform._src1projcols_ if source_tbl == 1 else join_transform._src2projcols_
        tbl2 = join_transform._source2_ if source_tbl == 1 else join_transform._source1_
        col2 = join_transform._src2joincols_ if source_tbl == 1 else join_transform._src1joincols_
        projcol2 = join_transform._src2projcols_ if source_tbl == 1 else join_transform._src1projcols_

        # step1
        temp1 = DataFrame(tbl1, SQLProjectionTransform(tbl1, col1))
        temp1_node = build_single_node(source1.strategy, source1)

        # step2
        temp2 = DataFrame(temp1, tbl2, SQLJoinTransform(temp1, tbl2, col1, col2, COL.ALL, projcol2))
        temp2_node = build_single_node(source2.strategy, temp1_node, source2)

        # step3
        rs = SQLJoinTransform(tbl1, temp2, col1, col1, projcol1, COL.ALL)
        rs_node = build_single_node(source1.strategy, source1, temp2_node)

        return rs_node

    def build_lineage(self, root: TabularData):
        # case join, has 2 sources
        root_copy = HeuristicScheduler().build_lineage(root)
        lineage = LineageNode(root)
        if root.__data__ is not None:
            # if parent already materialized, use pandas
            lineage.set_strategy(STRATEGY_PD)
        elif isinstance(root, DBTable):
            # if parent is database table, use db
            lineage.set_strategy(STRATEGY_DB)
        else:
            # follow the source's strategy if there is any
            if isinstance(root.__source__, tuple):
                source1 = self.build_lineage(root.__source__[0])
                source2 = self.build_lineage(root.__source__[0])
                # if tables are in 2 different location, perform semijoin
                if source1.strategy != source2.strategy:
                    new_source = self.semijoin(root.__transform__, lineage)
            else:
                ts = root.__transform__
                # handle the problem where a table is in the condition of a query
                if isinstance(ts, SQLSelectTransform):
                    for sc in ts.__selcols__:
                        if isinstance(sc._col2_, TabularData):
                            lineage.add_source(CONDITION_SOURCE, self.build_lineage(sc._col2_))
                if ts is not None:
                    lineage.add_source(ts.transform_name(), self.build_lineage(ts._source_))
            if len(lineage.sources) == 1:
                lineage.set_strategy(list(lineage.sources.values())[0].strategy)
            else:
                # when join 2 table, as long as one table is db, we use db
                for source in lineage.sources.values():
                    if source.strategy == STRATEGY_DB:
                        lineage.set_strategy(STRATEGY_DB)
                        return lineage
                lineage.set_strategy(STRATEGY_PD)

        return lineage


class HeuristicScheduler(TransformScheduler):
    """
    simply check if the upstream data in pandas or db
    """
    def build_lineage(self, root: TabularData):
        # case join, has 2 sources
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
                s1 = self.build_lineage(root.__source__[0])
                lineage.add_source('join1', s1)
                s2 = self.build_lineage(root.__source__[1])
                lineage.add_source('join2', s2)
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
                lineage.set_strategy(list(lineage.sources.values())[0].strategy)
            else:
            # when join 2 table, as long as one table is db, we use db
                for source in lineage.sources.values():
                    if source.strategy == STRATEGY_DB:
                        lineage.set_strategy(STRATEGY_DB)
                        return lineage
                lineage.set_strategy(STRATEGY_PD)

        # lineage.update_node()
        return lineage





