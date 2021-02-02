import os
from aida.aida import *

host = 'localhost'
dbname = 'sf01'
user = 'sf01'
passwd = 'sf01'
jobName = 'test'
port = 55660

SF = 1 #used by query 11. indicate the scale factor of the tpch database.

udfVSvtable = True

outputDir = 'output'

def thisJobName(filename):
    return os.path.basename(filename);

def getDBC(jobName):
    return AIDA.connect(host, dbname, user, passwd, jobName, port);
