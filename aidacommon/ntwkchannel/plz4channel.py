import logging;

import pickle;
from collections import OrderedDict;

import lz4framed;

def transmit(result, sock):

    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    pickler.dump(cols);

    for col in cols:
        with lz4framed.Compressor(sock, block_size_id=6) as compressor:
            #pickler.dump(result[col].dtype);
            try:
                compressor.update(pickle.dumps(result[col]))
            except lz4framed.lz4FramedNoDataError:
                pass
            except EOFError:
                pass;


def receive(sock):

    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();

    for col in keylist:
        colz = b'';
        for chunk in lz4framed.Decompressor(sock):
            try:
                colz += chunk;
            except lz4framed.Lz4FramedNoDataError:
                pass;
            except EOFError:
                pass;
        result[col] = pickle.loads(colz);

    return result;