import os
from aida.aida import *

host = 'localhost'
dbname = 'sf00'
user = 'sf00'
passwd = 'sf00'
jobName = 'tests'
port = 44660


outputDir = 'output'

def getDBC(jobName):
    return AIDA.connect(host, dbname, user, passwd, jobName, port);
