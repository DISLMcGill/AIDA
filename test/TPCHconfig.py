import os
from aida.aida import *

host = 'localhost'
dbname = 'sf01'
user = 'sf01'
passwd = 'sf01'
jobName = 'test'
port = 44660


outputDir = 'output'

def getDBC(jobName):
    return AIDA.connect(host, dbname, user, passwd, jobName, port);
