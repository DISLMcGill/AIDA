import logging
import time
from aida.aida import *;
host = 'tfNewServer'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);


logging.basicConfig(level=logging.INFO, filename='query.log')

t1=time.time()
dw.tripdata2017.filter(Q('stscode', 'endscode', CMP.NE))
logging.info("start:{}:elapsed:{}".format(t1,time.time()-t1))

