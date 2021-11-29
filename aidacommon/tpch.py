import logging
import time
import psycopg2
logging.basicConfig(level=logging.INFO, filename='query.log')
connection=psycopg2.connect(user='bixi',password='bixi',host='tfNewServer',database='bixi')
cursor=connection.cursor()
while True:
    t1=time.time()
    cursor.execute("SELECT * FROM gmdata2017;")
    logging.info("start:{}:elapsed:{}".format(t1,time.time()-t1))