import pickle
import sys
import os
import argparse
from time import time
import time as tm

#from memory_profiler import profile

config = __import__('bixi-config')
queries = __import__('bixi_queries')

assert len(sys.argv) > 1, 'Usage: python3 runTPCH-AIDA.py (<query_number>)...'

parser = argparse.ArgumentParser()
parser.add_argument('qrys', metavar='N', type=int, nargs='+')
parser.add_argument('--output', type=str, default=None)
args = parser.parse_args()

assert all(int(e) < 23 and int(e) >= 1 for e in args.qrys), 'Query numbers must be integers between 1 and 22'
queries = ['0' + str(int(e)) if int(e) < 10 else str(int(e)) for e in args.qrys]
output_file = args.output

db = config.getDBC(config.jobName);

os.system('mkdir -p {}'.format(config.outputDir))

cols =  {'gmdata2017': ('stscode', 'endscode', 'gdistm', 'gduration'),
        'stations2017': ('scode', 'sname', 'slatitude', 'slongitude', 'sispublic'),
         'tripdata2017': ('id', 'starttm', 'stscode', 'endtm', 'endcode', 'duration', 'ismember')
        }

tbs = ['gmdata2017', 'stations2017', 'tripdata2017']

class Database:

    def __init__(self, db):
        self.db = db

    def __getattr__(self, name):
        t = getattr(self.db, name)
        try:
            print('Get attr gets called, name={}'.format(name))
            if(name in tbs):
                t = t.project(cols[name]) * 1
            self.__setattr__(name, t)
        except KeyError:
            self.__setattr__(name, t)
            print("keyerror while loading {}", name);
        return t

if config.udfVSvtable:
    print('ctime: {}'.format(tm.ctime()))
    print('----------[ Preloading ]----------')
    t0 = time()
    db2 = Database(db)
    for t in tbs:
        t = getattr(db2, t)
        t.loadData()
    t1 = time()
    print('Preloading all tables took {}s'.format(t1 - t0))
else:
    db2 = db


#@profile
def run_test():
    for q in queries:
        print('ctime: {}'.format(tm.ctime()))
        print('----------[ Query {0} ]----------'.format(q))
        t0 = time()
        r = getattr(queries, 'q' + q)(db2)
        print(type(r))
        if(hasattr(r, '_genSQL_')):
            r.loadData()
        t1 = time()
        print('Execution time: {}'.format(t1-t0))
        if output_file:
            with open('{}/{}.csv'.format(config.outputDir, output_file), 'a') as f:
            # dump data to expected result file
            # with open('{}/expected_{}'.format(config.outputDir, q), 'wb') as file:
            #     if(hasattr(r, '_genSQL_')):
            #         print(r.rows)
            #         pickle.dump(r.rows, file)
            #     else:
            #         print(r)
            #         pickle.dump(r, file)
                f.write('{0},{1}\n'.format(int(q), t1 - t0))

run_test()
