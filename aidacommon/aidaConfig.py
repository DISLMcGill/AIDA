from enum import Enum;

import logging;
import os;
import configparser;
import importlib;

class UDFTYPE(Enum):
    TABLEUDF=1; VIRTUALTABLE=2;


class AConfig:
    UDFTYPE=UDFTYPE.TABLEUDF;
    #UDFTYPE=UDFTYPE.VIRTUALTABLE;



def loadConfig(topic='AIDASERVER'):

    try:
        configfile = os.environ['AIDACONFIG'];
    except KeyError:
        raise EnvironmentError('Environment variable AIDACONFIG is not set.');

    # Check if the config file exists.
    if (not os.path.isfile(configfile)):
        raise FileNotFoundError("Error configuration file {} not found.".format(configfile));

    # Load the configuration settings.
    config = configparser.ConfigParser();
    config.read(configfile);
    defaultConfig = config['DEFAULT'];

    if(topic == 'AIDASERVER'):
        config_ = config['AIDASERVER'];
        AConfig.DATABASEPORT = config_.getint('DATABASEPORT', defaultConfig['DATABASEPORT']);
        AConfig.DATABASEADAPTER = config_.get('DATABASEADAPTER', defaultConfig['DATABASEADAPTER']);
        udfType = config_.get('UDFTYPE', defaultConfig['UDFTYPE']);
        AConfig.UDFTYPE = UDFTYPE.TABLEUDF if (udfType == 'TABLEUDF') else UDFTYPE.VIRTUALTABLE;
    else:
        config_ = config['AIDACLIENT'];

    #AConfig.NTWKCHANNEL =  config_.get('NTWKCHANNEL', defaultConfig['NTWKCHANNEL']);
    AConfig.LOGLEVEL = config_.get('LOGLEVEL', defaultConfig['LOGLEVEL']);
    AConfig.LOGFILE = config_.get('LOGFILE', defaultConfig['LOGFILE']);
    AConfig.CONNECTIONMANAGERPORT = config_.getint('CONNECTIONMANAGERPORT', defaultConfig['CONNECTIONMANAGERPORT']);
    AConfig.RMIPORT = config_.getint('RMIPORT', defaultConfig['RMIPORT']);

    # Setup the logging mechanism.
    if (AConfig.LOGLEVEL == 'DEBUG'):
        logl = logging.DEBUG;
    elif (AConfig.LOGLEVEL == 'WARNING'):
        logl = logging.WARNING;
    elif (AConfig.LOGLEVEL == 'ERROR'):
        logl = logging.ERROR;
    else:
        logl = logging.INFO;
    logging.basicConfig(filename=AConfig.LOGFILE, level=logl);

    AConfig.NTWKCHANNEL = importlib.import_module(config_.get('NTWKCHANNEL', defaultConfig['NTWKCHANNEL']));

#    if(topic == 'AIDASERVER'):
#        # Get the module and class name separated out for the database adapter that we need to load.
#        dbAdapterModule, dbAdapterClass = os.path.splitext(AConfig.DATABASEADAPTER);
#        dbAdapterClass = dbAdapterClass[1:];
#        dmod = importlib.import_module(dbAdapterModule);
#        dadapt = getattr(dmod, dbAdapterClass);
#        logging.info('AIDA: Loading database adapter {} for connection manager'.format(dadapt))

