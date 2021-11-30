import logging
import time
import psycopg2
logging.basicConfig(level=logging.INFO, filename='query.log')
connection=psycopg2.connect(user='sf01',password='sf01',host='localhost',database='sf01')
cursor=connection.cursor()
while True:
    t1=time.time()
    cursor.execute("SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = '[BRAND]' AND p_container = '[CONTAINER]' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey);")
    logging.info("start:{}:elapsed:{}".format(t1,time.time()-t1))

