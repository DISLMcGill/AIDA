import logging;
import os;
import importlib;

import aidacommon.aidaConfig;
from aidacommon.aidaConfig import AConfig;
from aidacommon import rop;
import aidas.dmro as dmro;
import aidas.aidas as aidas;
import aidacommon.gbackend as gbackend;

def bootstrap():

##    try:
##        configfile = os.environ['AIDACONFIG'];
##    except KeyError:
##        raise EnvironmentError('Environment variable AIDACONFIG is not set.');
##
##        # Check if the config file exists.
##    if (not os.path.isfile(configfile)):
##        raise FileNotFoundError("Error configuration file {} not found.".format(configfile));
##
##        # Load the configuration settings.
##    config = configparser.ConfigParser();
##    config.read(configfile);
##    defaultConfig = config['DEFAULT'];
##    serverConfig = config['AIDASERVER'];
##    AConfig.DATABASEPORT = serverConfig.getint('DATABASEPORT', defaultConfig['DATABASEPORT']);
##    AConfig.DATABASEADAPTER = serverConfig.get('DATABASEADAPTER', defaultConfig['DATABASEADAPTER']);
##    AConfig.LOGLEVEL = serverConfig.get('LOGLEVEL', defaultConfig['LOGLEVEL']);
##    AConfig.LOGFILE = serverConfig.get('LOGFILE', defaultConfig['LOGFILE']);
##    AConfig.RMIPORT = serverConfig.getint('RMIPORT', defaultConfig['RMIPORT']);
##    AConfig.CONNECTIONMANAGERPORT = serverConfig.getint('CONNECTIONMANAGERPORT', defaultConfig['CONNECTIONMANAGERPORT']);
##    udfType = serverConfig.get('UDFTYPE', defaultConfig['UDFTYPE']);
##    AConfig.UDFTYPE = UDFTYPE.TABLEUDF if (udfType == 'TABLEUDF') else UDFTYPE.VIRTUALTABLE;
##
##    # Setup the logging mechanism.
##    if (AConfig.LOGLEVEL == 'DEBUG'):
##        logl = logging.DEBUG;
##    elif (AConfig.LOGLEVEL == 'WARNING'):
##        logl = logging.WARNING;
##    elif (AConfig.LOGLEVEL == 'ERROR'):
##        logl = logging.ERROR;
##    else:
##        logl = logging.INFO;
##    logging.basicConfig(filename=AConfig.LOGFILE, level=logl);
##    logging.info('AIDA: Bootstrap procedure aidas_bootstrap starting...');

    aidacommon.aidaConfig.loadConfig('AIDASERVER');

    # Initialize the DMRO repository.
    try:
        dmro.DMROrepository('aidasys');
        import aidasys;
    except Exception as e:
        logging.exception(e);
        raise;

    # Startup the remote object manager for RMI.
    robjMgr = rop.ROMgr.getROMgr('', AConfig.RMIPORT, True);
    aidasys.robjMgr = robjMgr;

    # Start the connection manager.
    # Get the module and class name separated out for the database adapter that we need to load.
    dbAdapterModule, dbAdapterClass = os.path.splitext(AConfig.DATABASEADAPTER);
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
    import torch
    builtins.torch = torch;
    import torch.nn as nn;
    builtins.nn = nn;
    from sklearn import datasets;
    import time;
    builtins.time = time;
    builtins.logging = logging;
    from aidacommon.dbAdapter import DataConversion;
    import numpy as np;
    import geopy.distance;
    builtins.geopy.distance = geopy.distance;
    builtins.np = np;
    import sys;
    sys.argv = ['']
    # import tensorflow as tf;
    import tensorflow.compat.v1 as tf;
    tf.disable_v2_behavior()

    builtins.tf = tf;
    builtins.DataConversion = DataConversion;
    builtins.datasets = datasets;
    from torch.autograd import Variable
    builtins.Variable = Variable

    gBApp = gbackend.GBackendApp(AConfig.DASHPORT)
    aidasys.gBApp = gBApp;
    gBApp.start();
