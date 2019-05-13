from collections import OrderedDict
import zstandard as zstd
import time
import logging
from numpy import *
import pickle
import json_tricks as json

def transmit(result, sock):


    #logging.debug("Ntwk channel json-zstd: to transmit : ");

    numArray = len(result) #number of numpy arrays in the dict
    pickler = pickle.Pickler(sock)
    pickler.dump(numArray)
    items = list(result.items())
    
    for i in range(numArray):
        key = items[i][0]
        pickler.dump(key)
    
    for i in range(numArray):
        val = items[i][1]
        y = json.dumps(val).encode('utf-8')
        cctx = zstd.ZstdCompressor()
        with cctx.stream_writer(sock) as compressor:
            compressor.write(y)


    #logging.debug("Ntwk channel json-zstd: transmission completed : ");


def receive(sock):

    #logging.debug("Ntwk channel json-zstd: waiting to receieve : ");

    unpickler = pickle.Unpickler(sock)

    numArray = unpickler.load()
    result = OrderedDict([])
    keylist = []

    for i in range(numArray):
        key = unpickler.load()
        keylist.append(key)

    for i in range(numArray):
        data = ''
        dctx = zstd.ZstdDecompressor()
        
        for chunk in dctx.read_to_iter(sock, read_size=1):
            data += chunk.decode('utf-8')


        val = json.loads(data)
        result.update({key[i]:val})



    #logging.debug("Ntwk channel json-zstd: data received : ");
    return result;

