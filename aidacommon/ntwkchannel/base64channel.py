import pickle;
from collections import OrderedDict;

import numpy as np;
import base64


def transmit(result, sock):

    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    pickler.dump(cols);

    for col in cols:
        if(result[col].dtype == object):
            colz = base64.b64encode(pickle.dumps(result[col]))
        else:
            colz = base64.b64encode(result[col]);
        pickler.dump(result[col].dtype);
        pickler.dump(colz);


def receive(sock):

    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();

    for col in keylist:
        dt = unpickler.load();
        if(dt == object):
            result[col] = pickle.loads(base64.b64decode(unpickler.load()));
        else:
            result[col] = np.frombuffer(base64.b64decode(unpickler.load()), dtype=dt)
    return result;