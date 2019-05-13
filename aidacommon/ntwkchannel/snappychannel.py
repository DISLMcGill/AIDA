import io
import json_tricks as json
import snappy
from collections import OrderedDict
import logging
import time
import pickle
from numpy import *
import struct

def transmit(result, sock):


    #logging.debug("Ntwk channel json-snappy: to transmit : ");

    numArray = len(result) #number of numpy arrays in the dict
    pickler = pickle.Pickler(sock)
    pickler.dump(numArray)
    items = list(result.items())

    for i in range(numArray):
        key = items[i][0]
        pickler.dump(key)

    for i in range(numArray):
        val = items[i][1]
        y = (json.dumps(val, ensure_ascii=False)).encode('utf-8')
        data = snappy.compress(y)
        sock.write(struct.pack("!I", len(data)))
        sock.write(data)

    #logging.debug("Ntwk channel json-snappy: transmission completed : ");


def receive(sock):

    #logging.debug("Ntwk channel json-snappy: waiting to receieve : ");


    unpickler = pickle.Unpickler(sock)

    numArray = unpickler.load()
    result = OrderedDict([])
    keylist = []

    for i in range(numArray):
        key = unpickler.load()
        keylist.append(key)



    for i in range(numArray):
        (length,) = struct.unpack("!I", sock.read(4))
        data = snappy.decompress(sock.read(length)).decode('utf-8')
        val = json.loads(data)
        result.update({keylist[i]:val})

    #logging.debug("Ntwk channel json-lz-pipe: data received : ");
    return result;

