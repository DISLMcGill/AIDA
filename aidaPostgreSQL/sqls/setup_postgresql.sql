CREATE OR REPLACE FUNCTION aidas_bootstrap()
  RETURNS TABLE(module text)
AS $$
  import logging
  from datetime import datetime
  import aidas.bootstrap;
  aidas.bootstrap.bootstrap();
  import aidasys;
  conMgr = aidasys.conMgr;
  GD['conMgr'] = conMgr;
  conMgr._currentJobName = None;
  
  while True:
    while conMgr._currentJobName is None:
      pass;
    
    dbcObj = conMgr.get(conMgr._currentJobName);

    import plpy
    dbcObj._setConnection(plpy);
    logging.info("went back from pre exe")
    logging.info(datetime.now())
    del plpy
  return ['OK'];
$$ LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION prepare_execution(jobname text) 
  RETURNS TABLE(status text)
AS $$
  from datetime import datetime
  import aidas.aidas;
  import logging;
  coMgr = GD['conMgr']
  dbcObj = coMgr.get(jobname);
  dbcObj._preparePlpy(plpy);
  logging.info('after execution')
  logging.info(datetime.now())
  return ['OK'];
$$ LANGUAGE plpython3u;
--SELECT * FROM aidas_setdbccon('jobName_01');


CREATE OR REPLACE FUNCTION aidas_listpyinfo() 
  RETURNS TABLE(name text, val text)
AS $$
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

  return ( (name[i],val[i]) for i in range( 0, len(name) ) );
$$ LANGUAGE plpython3u;
--SELECT * FROM aidas_listpyinfo();

CREATE OR REPLACE FUNCTION aidas_list_pymodulecontents(module text) 
  RETURNS TABLE(contents text)
AS $$
  import sys;
  return dir(sys.modules.get(module));
$$ LANGUAGE plpython3u;
--SELECT * FROM  aidas_list_pymodulecontents();

CREATE OR REPLACE FUNCTION aidas_pygc() 
  RETURNS TABLE(reslt text)
AS $$
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
  return [reslt];
$$ LANGUAGE plpython3u;
--SELECT * FROM aidas_pygc();

CREATE OR REPLACE FUNCTION aidas_tmp_pygc() 
  RETURNS TABLE(reslt text)
AS $$
  import gc;
  gc.set_debug(gc.DEBUG_UNCOLLECTABLE);
  return ['debug uncollectable enabled']
$$ LANGUAGE plpython3u;
--SELECT * FROM aidas_tmp_pygc();

CREATE OR REPLACE FUNCTION aidas_tmp_pygc_garbage() 
  RETURNS TABLE(objid text, objtype text)
AS $$
  import gc;
  objid=[]; objtype=[];
  for obj in gc.garbage:
    objid.append(str(id(obj)));
    objtype.append(str(type(obj)));

  return ( (objid[i],objtype[i]) for i in range(0,len(objid) ) );
$$ LANGUAGE plpython3u;
--SELECT * FROM aidas_tmp_pygc_garbage();

