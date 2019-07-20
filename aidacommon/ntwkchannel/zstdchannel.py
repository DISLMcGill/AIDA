from collections import OrderedDict
import zstandard as zstd
import pickle
import json_tricks as json

def transmit(result, sock):

    #logging.debug("Ntwk channel json-zstd: to transmit : ");

    pickler = pickle.Pickler(sock);
    cols = list(result.keys());
    pickler.dump(cols);

    for col in cols:
        y = json.dumps(result[col]).encode('utf-8')
        cctx = zstd.ZstdCompressor()
        with cctx.stream_writer(sock) as compressor:
            compressor.write(y)

    #logging.debug("Ntwk channel json-zstd: transmission completed : ");


def receive(sock):

    #logging.debug("Ntwk channel json-zstd: waiting to receieve : ");

    unpickler = pickle.Unpickler(sock);
    result = OrderedDict([]);
    keylist = unpickler.load();

    for col in keylist:
        data = ''
        dctx = zstd.ZstdDecompressor()
        
        for chunk in dctx.read_to_iter(sock, read_size=1):
            data += chunk.decode('utf-8')

        result[col] = json.loads(data)


    #logging.debug("Ntwk channel json-zstd: data received : ");
    return result;

