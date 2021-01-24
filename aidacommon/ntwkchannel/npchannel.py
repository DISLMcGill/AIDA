import pickle;
from collections import OrderedDict;

import numpy as np;
from io import BytesIO

def transmit(result, sock):

    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    pickler.dump(cols);

    for col in cols:
        bi = BytesIO()
        np.savez_compressed(bi, x=result[col]);
        bi.seek(0);
        pickler.dump(bi)


def receive(sock):

    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();

    for col in keylist:
        bi = unpickler.load()
        result[col] = np.load(bi)['x']

    return result;