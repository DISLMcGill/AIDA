from collections import OrderedDict
import lz4framed
import pickle
import json_tricks as json

def transmit(result, sock):

    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    pickler.dump(cols);

    for col in cols:
        y = (json.dumps(result[col])).encode('utf-8')
        with lz4framed.Compressor(sock, block_size_id=6) as compressor:
            try:
                compressor.update(y)
            except lz4framed.lz4FramedNoDataError:
                pass
            except EOFError:
                pass;


def receive(sock):

    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();

    for col in keylist:
        data = ''
        try:
            for chunk in lz4framed.Decompressor(sock):
                data += chunk.decode('utf-8')
        except lz4framed.Lz4FramedNoDataError:
            pass;
        except EOFError:
            pass;
        result[col] = json.loads(data)

    return result;

