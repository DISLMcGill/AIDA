DROP FUNCTION aidas_bootstrap;
CREATE FUNCTION aidas_bootstrap() RETURNS TABLE(module STRING) LANGUAGE PYTHON
{
  import aidas.bootstrap;
  aidas.bootstrap.bootstrap();
  return 'OK';
};
-- SELECT * FROM aidas_bootstrap();

--#--DROP FUNCTION aidas_ctDMROrep;
--#--CREATE FUNCTION aidas_ctDMROrep(name STRING) RETURNS TABLE(module STRING) LANGUAGE PYTHON
--#--{
--#--  import logging;
--#--  import aidas.dmro as dmro;
--#--  try:
--#--    dmro.DMROrepository(name);
--#--  except Exception as e:
--#--     logging.exception(e);
--#--     return  e.__str__();
--#--  return 'created DMRO:' + name;
--#--};
--#----SELECT * from aidas_ctDMROrep('aidasys');
--#--
--#--
--#--DROP FUNCTION aidas_setrobjmgr;
--#--CREATE FUNCTION aidas_setrobjmgr(robjmgrname STRING) RETURNS TABLE(status STRING) LANGUAGE PYTHON
--#--{
--#--  from aidacommon import rop;
--#--  import aidasys;
--#--  robjMgr = rop.ROMgr.getROMgr('', 55668, True);
--#--  #TODO: get the name of the variable from the argument passed to the function.
--#--  aidasys.robjMgr = robjMgr;
--#--  return 'OK';
--#--};
--#----SELECT * FROM aidas_setrobjmgr('robjMgr');
--#--
--#--DROP FUNCTION aidas_setconmgr;
--#--CREATE FUNCTION aidas_setconmgr(conmgrname STRING) RETURNS TABLE(status STRING) LANGUAGE PYTHON
--#--{
--#--  try:
--#--    import logging;
--#--    import aidasys;
--#--    import aidas.aidas;
--#--    import aidaMonetDB.dbAdapter;
--#--
--#--    logging.debug('aidas_setconmgr called for {}'.format(conmgrname));
--#--    conMgr = aidas.aidas.ConnectionManager.getConnectionManager(aidaMonetDB.dbAdapter.DBCMonetDB);
--#--    #TODO: get the name of the variable from the argument passed to the function.
--#--    aidasys.conMgr = conMgr;
--#--    return 'OK';
--#--  except Exception as e:
--#--    logging.exception(e);
--#--    return 'Error';
--#--};
--#----SELECT * FROM aidas_setconmgr('conMgr');

DROP FUNCTION aidas_setdbccon;
CREATE FUNCTION aidas_setdbccon(jobname STRING) RETURNS TABLE(status STRING) LANGUAGE PYTHON
{
  import aidas.aidas;
  import logging;
  coMgr = aidas.aidas.ConnectionManager.getConnectionManager();
  dbcObj = coMgr.get(jobname);
  dbcObj._setConnection(_conn);
  logging.debug('aidas_setdbccon called for {}'.format(jobname));
  return 'OK';
};
--SELECT * FROM aidas_setdbccon('jobName_01');

DROP FUNCTION aidas_startlogger;
CREATE FUNCTION aidas_startlogger(logfile STRING, loglevel STRING) RETURNS TABLE(status STRING) LANGUAGE PYTHON
{
  import logging;
  if(loglevel == 'DEBUG'):
    logl = logging.DEBUG;
  elif(loglevel == 'WARNING'):
    logl = logging.WARNING;
  elif(loglevel == 'ERROR'):
    logl = logging.ERROR;
  else:
    logl = logging.INFO;
  logging.basicConfig(filename=logfile, level=logl);
  logging.info('Started logging...');
  return 'OK';
};
--SELECT * FROM aidas_startlogger('aidas.log');


DROP FUNCTION aidas_listpyinfo;
CREATE FUNCTION aidas_listpyinfo() RETURNS TABLE(name STRING, val STRING) LANGUAGE PYTHON
{
  import sys;
  import gc;
  import threading;
  import os;
  import psutil;

  name = []; val=[];

  name.append('python version'); val.append(sys.version_info[0]);
  name.append('__name__'); val.append(__name__);
  name.append('gc enabled'); val.append(gc.isenabled());
  name.append('gc threshold'); val.append(gc.get_threshold().__str__());
  name.append('gc count'); val.append(gc.get_count().__str__());
  name.append('thread id'); val.append(threading.get_ident().__str__());
  name.append('active threads'); val.append(threading.active_count());
  name.append('process id'); val.append(os.getpid().__str__());
  name.append('psutil'); val.append(psutil.Process(os.getpid()).memory_info().rss.__str__());

  return {'name':name, 'val':val};
};
--SELECT * FROM aidas_listpyinfo();

DROP FUNCTION aidas_list_pymodulecontents;
CREATE FUNCTION aidas_list_pymodulecontents(module STRING) RETURNS TABLE(contents STRING) LANGUAGE PYTHON
{
  import sys;
  return dir(sys.modules.get(module));
};
--SELECT * FROM  aidas_list_pymodulecontents();


DROP FUNCTION aidas_pygc;
CREATE FUNCTION aidas_pygc() RETURNS TABLE(reslt STRING) LANGUAGE PYTHON
{
  import gc, time;
  status = gc.isenabled();
  glens = len(gc.garbage);
  bf = gc.get_count();
  st = time.time();
  cnt = gc.collect();
  et = time.time();
  af = gc.get_count();
  glene = len(gc.garbage);

  reslt = 'GC enabled is {}, collection {}/{} . collection duration {:0.20f}  unreach = {} garbage = {}/{}'.format(status, bf,af, (et-st), cnt, glens, glene);
  return {'reslt':reslt};
};
--SELECT * FROM aidas_pygc();


DROP FUNCTION aidas_tmp_pygc;
CREATE FUNCTION aidas_tmp_pygc() RETURNS TABLE(reslt STRING) LANGUAGE PYTHON
{
  import gc;
  gc.set_debug(gc.DEBUG_UNCOLLECTABLE);
  return {'reslt': 'debug uncollectable enabled'};
};
--SELECT * FROM aidas_tmp_pygc();


DROP FUNCTION aidas_tmp_pygc_garbage;
CREATE FUNCTION aidas_tmp_pygc_garbage() RETURNS TABLE(objid STRING, objtype STRING) LANGUAGE PYTHON
{
  import gc;
  objid=[]; objtype=[];
  for obj in gc.garbage:
    objid.append(str(id(obj)));
    objtype.append(str(type(obj)));

  return {'objid':objid, 'objtype':objtype};
};
--SELECT * FROM aidas_tmp_pygc_garbage();
