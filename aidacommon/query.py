
import time
from aida.aida import *;
host = 'tfNewServer'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);


logging.basicConfig(level=logging.INFO, filename='query.log')
f = open('query_time.txt','a')

while True:
    t1=time.time()
    dw.tripdata2017.filter(Q('stscode', 'endscode', CMP.NE)).aggregate(('stscode', 'endscode', {COUNT('*'): 'numtrips'}), ('stscode', 'endscode'))
    f.write("start:{}:elapsed:{}".format(t1,time.time()-t1))
    f.write('\n')

