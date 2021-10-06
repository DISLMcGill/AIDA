import os
from aida.aida import *

host = 'server-v2'
dbname = 'sf01'
user = 'sf01'
passwd = 'sf01'
jobName = 'explorer'
port = 55660


outputDir = 'output'

def getDBC(jobName):
    return AIDA.connect(host, dbname, user, passwd, jobName, port);
