DROP FUNCTION aidas_bootstrap;
CREATE FUNCTION aidas_bootstrap() RETURNS TABLE(module STRING) LANGUAGE PYTHON
{
  import aidas.monet_bootstrap;
  aidas.monet_bootstrap.bootstrap();
  return 'OK';
};
-- SELECT * FROM aidas_bootstrap();

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
