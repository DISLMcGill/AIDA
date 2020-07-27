CREATE OR REPLACE FUNCTION aidas_bootstrap()
  RETURNS TABLE(module text)
AS $$
  import aidas.bootstrap;
  aidas.bootstrap.bootstrap();
  
  import queue;
  requestQueue = queue.Queue();
  resultQueue = queue.Queue();

  import aidasys;
  aidasys.requestQueue = requestQueue;
  aidasys.resultQueue = resultQueue;
  conMgr = aidasys.conMgr
  
  while True:
    (jobName, request) = requestQueue.get();
    dbcObj = conMgr.get(jobName);
    dbcObj._executeRequest(plpy,request);
    requestQueue.task_done()
  return ['OK'];
$$ LANGUAGE plpython3u;
--SELECT * FROM aidas_bootstrap();


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


--There is no built-in aggregate Median in Postgresql
--The following snippet is on Postgres wiki and also part of the ulib_agg user-defined library
CREATE OR REPLACE FUNCTION _final_median(anyarray) RETURNS float8 AS $$
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) as c FROM q
  )
  SELECT AVG(val)::float8
  FROM
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)
  ) q2;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE AGGREGATE median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);
