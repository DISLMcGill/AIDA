import dill as custompickle;
import logging;

def transmit(result, sock):
    #logging.debug("Ntwk channel pickle: to transmit : ");
    custompickle.dump(result, sock);
    sock.flush();
    #logging.debug("Ntwk channel pickle: transmission completed : ");


def receive(sock):
    #logging.debug("Ntwk channel pickle: waiting to receieve : ");
    result = custompickle.load(sock);
    #logging.debug("Ntwk channel pickle: data received : ");
    return result;

