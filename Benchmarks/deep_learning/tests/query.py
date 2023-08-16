import logging
import time
import psycopg2
logging.basicConfig(level=logging.INFO, filename='query.log')
import numpy as np
import psutil
connection=psycopg2.connect(user='sf01',password='sf01',host='localhost',database='sf01')
cursor=connection.cursor()
number = 100
lengthArr = []
cpuArr = []
index = 1
while True:
    cpu = float(psutil.cpu_percent())
    t1=time.time()
    cursor.execute("SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = '[BRAND]' AND p_container = '[CONTAINER]' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey);")
    length = float(time.time() - t1)
    lengthArr.append(length)
    cpuArr.append(cpu)
    index  += 1
    if index == number:
        index = 0

        # logging.info("start:{}:elapsed:{}".format(t1,np.mean(lengthArr)))
        with open('result.csv', 'a') as f:
            f.write(str(t1)+','+str(np.mean(lengthArr)) +','+ str(np.mean(cpuArr))+'\n')
        lengthArr = []
        cpuArr = []