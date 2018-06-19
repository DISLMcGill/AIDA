from abc import ABCMeta
import socket;

import pickle;
from tblib import pickling_support;
pickling_support.install();
import dill as custompickle;

import aidacommon.aidaConfig;

from aidacommon.dborm import *;

#import logging;
#logging.basicConfig(filename='aida.log', level=logging.INFO);

class AIDA(metaclass=ABCMeta):
    @staticmethod
    def connect (host, dbname, user, passwd, jobName=None, port=55660):

        aidacommon.aidaConfig.loadConfig(topic='AIDACLIENT');

        # Make network connection.
        sock = socket.socket();
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1);
        sock.connect((host, port));

        #File handles to work directly with the custompickle functions.
        wf = sock.makefile('wb'); rf = sock.makefile('rb');

        #send authentication info to the connection manager running at the database.
        custompickle.dump((dbname, user, passwd, jobName),wf); wf.flush();

        ret = custompickle.load(rf);
        #Handsake to let the other side know that we have established the stubs.
        custompickle.dump(None,wf); wf.flush();

        #TODO check ret and throw it as an exception in case of some error.
        return ret;

