import logging;
import os;
import importlib;

import aidacommon.aidaConfig;
from aidacommon.aidaConfig import AConfig;
from aidacommon import rop;
import aidas.dmro as dmro;
import aidas.aidas as aidas;

import aidacommon.gbackend as gbackend;

GPU_FUNC = ['_GPU']

def gpu_not_available_error(func_name):
    def wrapper(dw):
        raise AssertionError(f'CUDA is not present. CUDA is required for {func_name}')
    return wrapper

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

    # substitute all functions that requires GPU to the error function in the dbadapter
    def sub_gpu_funcs():
        import torch
        if not torch.cuda.is_available():
            for func_name in GPU_FUNC:
                setattr(dadapt, func_name, gpu_not_available_error(func_name))
        logging.info('AIDA: Loading database adapter {} for connection manager'.format(dadapt))

    sub_gpu_funcs()
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
