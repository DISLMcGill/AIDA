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

    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    pickler.dump(cols);

    for col in cols:
        y = (json.dumps(result[col], ensure_ascii=False)).encode('utf-8')
        data = snappy.compress(y)
        sock.write(struct.pack("!I", len(data)))
        sock.write(data)


def receive(sock):

    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();

    for col in keylist:
        (length,) = struct.unpack("!I", sock.read(4))
        data = snappy.decompress(sock.read(length)).decode('utf-8')
        result[col] = json.loads(data)

    return result;

