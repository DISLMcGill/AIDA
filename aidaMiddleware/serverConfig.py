import sqlite3
import logging

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

         for row in self.cursor.execute("SELECT * FROM tables"):
            if row[0] in self.table_partitions:
               self.table_partitions[row[0]].append(row[1])
            else:
               self.table_partitions[row[0]] = [row[1]]
      return self.names

   def get_servers(self, table_name):
      if table_name in self.table_partitions:
         return self.table_partitions[table_name]
      else:
         logging.error(
            "ERROR: cannot find table {} in configuration".format(table_name))
         raise KeyError(
            "ERROR: cannot find table {} in configuration".format(table_name))