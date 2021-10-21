from aida.aida import *;
host = 'Server3'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def helloWorld(dw):
    print("Hello world")


dw._X_torch(helloWorld)
