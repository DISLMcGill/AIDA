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
        AConfig.DASHPORT = config_.getint('DASHPORT', defaultConfig['DASHPORT']);
        AConfig.MONETDATABASEADAPTER = config_.get('MONETDATABASEADAPTER', defaultConfig['MONETDATABASEADAPTER']);
        AConfig.POSTGRESDATABASEADAPTER = config_.get('POSTGRESDATABASEADAPTER', defaultConfig['POSTGRESDATABASEADAPTER']);
        udfType = config_.get('UDFTYPE', defaultConfig['UDFTYPE']);
        AConfig.UDFTYPE = UDFTYPE.TABLEUDF if (udfType == 'TABLEUDF') else UDFTYPE.VIRTUALTABLE;
        AConfig.SERVERTYPE = config_.get('SERVERTYPE');
        AConfig.AIDAM = config_.get('AIDAM');
        AConfig.AIDAMPORT = config_.getint('AIDAMPORT');
        AConfig.MAPBOXTOKEN = config_.get('MAPBOXTOKEN', defaultConfig['MAPBOXTOKEN']);
        AConfig.PAGETUNNEL = config_.get('PAGETUNNEL', None);
        AConfig.CONVERSIONOPTION = config_.getint('CONVERSIONOPTION', 1);
        if(not AConfig.PAGETUNNEL is None and AConfig.PAGETUNNEL == 'None'):
            AConfig.PAGETUNNEL = None;
        if(AConfig.SERVERTYPE=='monet'):
            AConfig.CONNECTIONMANAGERPORT = config_.getint('MONETCONNECTIONMANAGERPORT');
        elif(AConfig.SERVERTYPE=='postgres'):
            AConfig.CONNECTIONMANAGERPORT = config_.getint('POSTGRESCONNECTIONMANAGERPORT');
        if(AConfig.SERVERTYPE=='monet' or AConfig.AIDAM is None or AConfig.AIDAM == 'OFF' or AConfig.AIDAMPORT is None):
            AConfig.AIDAM = None;
    else:
        config_ = config['AIDACLIENT'];
        try:
            AConfig.PORTMAPFILE = config_.get('PORTMAPFILE');   #ADVANCED - for tunnelling ports.
            pmaps = configparser.ConfigParser();
            pmaps.read(AConfig.PORTMAPFILE);
            pmaps = pmaps['OVERRIDE']
            maps = {}
            for hp in pmaps.keys():
                mp = pmaps[hp]; hp_ = hp.split('^'); mp_ = mp.split('^');
                maps[(hp_[0],int(hp_[1]))] = (mp_[0],int(mp_[1]))
            AConfig.PORTMAPS = maps;
        except KeyError:
            AConfig.PORTMAPS = {};
            pass
        except TypeError:
            AConfig.PORTMAPS = {};
            pass

    #AConfig.NTWKCHANNEL =  config_.get('NTWKCHANNEL', defaultConfig['NTWKCHANNEL']);
    AConfig.LOGLEVEL = config_.get('LOGLEVEL', defaultConfig['LOGLEVEL']);
    AConfig.LOGFILE = config_.get('LOGFILE', defaultConfig['LOGFILE']);
    AConfig.MONETRMIPORT = config_.getint('MONETRMIPORT', defaultConfig['MONETRMIPORT']);
    AConfig.POSTGRESRMIPORT = config_.getint('POSTGRESRMIPORT', defaultConfig['POSTGRESRMIPORT']);


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


def portMapper(host,port):
    try:
        return AConfig.PORTMAPS[(host,port)];
    except:
        return (host,port);
