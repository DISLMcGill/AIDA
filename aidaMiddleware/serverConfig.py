import sqlite3

class ServerConfig:
   db_name = "/home/build/AIDA/aidaMiddleware/servers.db"

   def __init__(self):
      self.connection = sqlite3.connect(ServerConfig.db_name)
      self.cursor = self.connection.cursor()
      self.names = None
      self.table_partitions = {}

   def get_server_names(self):
      if self.names is not None:
         return self.names
      else:
         self.names = []
         for row in self.cursor.execute("SELECT hostname FROM servers"):
            self.names += [row[0]]
      return self.names

   def get_servers(self, table_name):
      if table_name in self.table_partitions:
         return self.table_partitions[table_name]
      else:
         hosts = []
         for row in self.cursor.execute(f"SELECT hosts FROM tables WHERE table = '{table_name}'"):
            hosts += [row[0]]
         self.table_partitions[table_name] = hosts
      return self.table_partitions[table_name]