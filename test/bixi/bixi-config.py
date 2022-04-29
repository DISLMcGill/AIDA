import os
from aida.aida import *

host = 'localhost'
dbname = 'bixi'
user = 'bixi'
passwd = 'bixi'
jobName = 'tests'
port = 44660

SF = 1 #used by query 11. indicate the scale factor of the tpch database.

udfVSvtable = False
#udfVSvtable = False

outputDir = 'output'

def thisJobName(filename):
    return os.path.basename(filename);

def getDBC(jobName):
    return AIDA.connect(host, dbname, user, passwd, jobName, port);
