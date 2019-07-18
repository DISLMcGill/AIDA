import logging;

import pickle;
from collections import OrderedDict;

import bz2;

import numpy as np;


def transmit(result, sock):

    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    logging.info("Transmitting columns {}".format(cols))
    pickler.dump(cols);

    for col in cols:
        logging.info("Data type {}".format(result[col].dtype))
        if(result[col].dtype == object):
            colz = bz2.compress(pickle.dumps(result[col]))
        else:
            colz = bz2.compress(result[col]);
        pickler.dump(result[col].dtype);
        pickler.dump(colz);

    logging.info("transmission complete")




def receive(sock):

    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();
    print("col list -> {}".format(keylist));#

    for col in keylist:
        print("reading data for {}".format(col));
        dt = unpickler.load();
        print("data type ".format(dt))
        if(dt == object):
            result[col] = pickle.loads(bz2.decompress(unpickler.load()));
        else:
            result[col] = np.frombuffer(bz2.decompress(unpickler.load()), dtype=dt)
        print("read data");

    logging.info("receive complete")
    return result;

