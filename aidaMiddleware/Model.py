from abc import ABCMeta
from aida.aida import *
import threading

class agent(threading.Thread):

    dbname = 'db'
    user = 'monetdb'
    passwd = 'monetdb'
    port = 55660
    shutdown_flag = threading.Event()
    jobName = 'algo'

    def __init__(self, host):
        threading.Thread.__init__(self)
        self.daemon = True
        self.host = host
        self.db = None
        self.connect()

    def connect(self):
        self.db = AIDA.connect(self.host, agent.dbname, agent.user, agent.passwd, agent.jobName, agent.port)

class synch_agent(agent):

    barrier = None
    iter_time = 0

    def run(self):
        agent.preprocessing(self.db)
        while not agent.shutdown_flag.is_set():
            delta_m, validation_error, validation_batch_size, iter_time = self.db._XP('exec_iter', agent.algo.gparams[agent.algo.t]);
            t = agent.algo.update_param(delta_m, validation_error, validation_batch_size, self.host) # potential to raise exception
            synch_agent.barrier.wait()
            agent.algo.progress_check(self.db, t) # potential to raise exception

class Model(ABCMeta):
    def __init__(self, hosts):
        self.params = None
        self.hosts = hosts
        self.threads = []

    @abstractmethod
    def update_params(self, delta_params):
        pass

    @abstractmethod
    def exec_iter(self, params):
        pass

    @abstractmethod
    def get_data(self, data):
        pass

    def run(self):
        for h in self.hosts:
            agent = synch_agent(h)
            self.threads.append(agent)
        synch_agent.barrier = threading.Barrier(
            parties=len(self.hosts),
        )
        for a in self.threads:
            a.start()
        for a in self.threads:
            a.join()