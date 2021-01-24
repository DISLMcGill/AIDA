import logging;
import os;
import importlib;

import aidacommon.aidaConfig;
from aidacommon.aidaConfig import AConfig;
from aidacommon import rop;
import aidas.dmro as dmro;
import aidas.aidas as aidas;
from configparser import ConfigParser
import aidacommon.gbackend as gbackend;

def bootstrap():

    aidacommon.aidaConfig.loadConfig('AIDASERVER');

    # Initialize the DMRO repository.
    try:
        dmro.DMROrepository('aidasys');
        import aidasys;
    except Exception as e:
        logging.exception(e);
        raise;

    # Startup the remote object manager for RMI.
    robjMgr = rop.ROMgr.getROMgr('', AConfig.MONETRMIPORT, True);
    aidasys.robjMgr = robjMgr;

    # Start the connection manager.
    # Get the module and class name separated out for the database adapter that we need to load.
    dbAdapterModule, dbAdapterClass = os.path.splitext(AConfig.MONETDATABASEADAPTER);
    dbAdapterClass = dbAdapterClass[1:];
    dmod = importlib.import_module(dbAdapterModule);
    dadapt = getattr(dmod, dbAdapterClass);
    logging.info('AIDA: Loading database adapter {} for connection manager'.format(dadapt))
    conMgr = aidas.ConnectionManager.getConnectionManager(dadapt);
    aidasys.conMgr = conMgr;

    #Visualization
    import builtins;
    import matplotlib;
    matplotlib.use('Agg');
    builtins.matplotlib = matplotlib;
    import matplotlib.pyplot as plt;
    builtins.plt = plt;

    gBApp = gbackend.GBackendApp(AConfig.DASHPORT)
    aidasys.gBApp = gBApp;
    gBApp.start();
