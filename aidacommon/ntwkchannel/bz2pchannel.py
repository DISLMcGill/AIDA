import pickle;
from collections import OrderedDict;

import numpy as np;

import bz2;

def transmit(result, sock):
    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    pickler.dump(cols);

    for col in cols:
        if(result[col].dtype == object):
            colz = bz2.compress(pickle.dumps(result[col]))
        else:
            colz = bz2.compress(result[col]);
        pickler.dump(result[col].dtype);
        pickler.dump(colz);



def receive(sock):
    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();

    for col in keylist:
        dt = unpickler.load();
        if(dt == object):
            result[col] = pickle.loads(bz2.decompress(unpickler.load()));
        else:
            result[col] = np.frombuffer(bz2.decompress(unpickler.load()), dtype=dt)

    return result;