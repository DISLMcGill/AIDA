import pickle
import sys
import os
import random
from time import time
import re
import csv
import argparse

#from memory_profiler import profile

config = __import__('TPCHconfig-AIDA')
tpchqueries = __import__('tb_sampling2')
#table_list = getattr(tpchqueries, 'TABLE_INVOLVED')

parser = argparse.ArgumentParser()
parser.add_argument("--seed", type=int, default=101)
parser.add_argument("qry", type=int, metavar='N', nargs='+')
args = parser.parse_args()

assert len(sys.argv) > 1, 'Usage: python3 runTPCH-AIDA.py (<query_number>)...'
assert all(int(e) < 23 and int(e) >= 1 for e in args.qry), 'Query numbers must be integers between 1 and 22'
queries = ['0' + str(int(e)) if int(e) < 10 else str(int(e)) for e in args.qry]
seed = args.seed

db = config.getDBC(config.jobName);

os.system('mkdir -p {}'.format(config.outputDir))

cols =  {'region': ('r_regionkey', 'r_name', 'r_comment'),
         'nation': ('n_nationkey', 'n_name', 'n_regionkey', 'n_comment'),
         'part': ('p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment'),
         'supplier': ('s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'),
         'partsupp': ('ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'),
         'customer': ('c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'),
         'orders': ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
         'lineitem': ('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
        }

class Database:
    def __init__(self, db):
        self.db = db

    def __getattr__(self, name):
        t = getattr(self.db, name)
        try:
            t = t.project(cols[name]) * 1
            self.__setattr__(name, t)
        except KeyError:
            self.__setattr__(name, t)
        return t


def load_tables(query):
     tables = table_list[query]
     print('Tables used in query: {}'.format(tables))
     ttl = 0
     for name in tables:
         if chosen():
             print('Loading table {}'.format(name))
             t0 = time()
             tb = getattr(db, name)
             tb.loadData()
             ttl += time() - t0
         else:
             tb = getattr(db, name)
             tb.loadData()
     return ttl


def chosen():
    return True if random.random() > .5 else False


#PATH = "/mnt/local/xwang223/monet/dbfarm/aidas.log"
#PATH = "/home/monet/dbfarm/aidas.log"
PATH = "/mnt/local/xwang223/postgres_build/pg_storeddata/aidas.log"
PATTERN_FV = r".*Feature vector = \[(.*)\].*"
PATTERN_FV = r".*Feature vector = \[(.*)\].*"
PATTERN_LNG = r".*Lineage = (.*).*"
prog_fv = re.compile(PATTERN_FV)
prog_lng = re.compile(PATTERN_LNG)


def retrieve_fv(path):
    for line in reversed(open(path).readlines()):
        print(line)
        m = prog_fv.match(line.strip())
        if m:
            sv = m.group(1).split()
            fv = [float(x) for x in sv]
            return fv
    return None


def retrieve_lng(path):
    for line in reversed(open(path).readlines()):
        print(line)
        m = prog_lng.match(line.strip())
        if m:
            lineage = m.group(1)
            return lineage
    return None


#@profile
def run_test():
    getattr(tpchqueries, 'update_seed')(seed)
    for q in queries:
        os.system('>{}'.format(PATH))
        print('----------[ Query {0} ]----------'.format(q))
        r = getattr(tpchqueries, 'q' + q)(db)
        print('timer starts')
        t0 = time()
        if(hasattr(r, '_genSQL_')):
            r.loadData()
        t1 = time()
        print('timer stops')
        print('Executing query took {}s'.format(t1 - t0))

        # retrieve vector from the log file and empty it
        fv = retrieve_fv(PATH)
        lng = retrieve_lng(PATH)

        row = [seed, q]
        row.extend(fv)
        row.append(lng)
        row.append(t1-t0)

        with open('output/po2_random_load_pg.csv', 'a') as f:
            wr = csv.writer(f)
            wr.writerow(row)
run_test()
