from collections import OrderedDict
import lz4framed
import time
import logging
from numpy import *
import pickle
import json_tricks as json

def transmit(result, sock):


    logging.debug("Ntwk channel json-lz-pipe: to transmit : ");

    numArray = len(result) #number of numpy arrays in the dict
    pickler = pickle.Pickler(sock)
    pickler.dump(numArray)
    items = list(result.items())

    for i in range(numArray):
        key = items[i][0]
        pickler.dump(key)

    for i in range(numArray):
        val = items[i][1]
        y = (json.dumps(val)).encode('utf-8')
        with lz4framed.Compressor(sock, block_size_id=6) as compressor:
            try:
                compressor.update(y)
            except lz4framed.lz4FramedNoDataError:
                logging.debug("lz4framed no data error")
            except EOFError:
                logging.debug("eoferror")
        sft = time.time() ###

    logging.debug("Ntwk channel json-lz-pipe: transmission completed : ");


def receive(sock):

    logging.debug("Ntwk channel json-lz-pipe: waiting to receieve : ");

    unpickler = pickle.Unpickler(sock)
    numArray = unpickler.load()
    result = OrderedDict([])
    keylist = []

    for i in range(numArray):
        key = unpickler.load()
        keylist.append(key)

    for i in range(numArray):
        data = ''
        try:
            for chunk in lz4framed.Decompressor(sock):
                data += chunk.decode('utf-8')
        except lz4framed.Lz4FramedNoDataError:
            print("nde")
        except EOFError:
            print("eof")

        val = json.loads(data)
        result.update({keylist[i]:val})

    logging.debug("Ntwk channel json-lz-pipe: data received : ");
    return result;

