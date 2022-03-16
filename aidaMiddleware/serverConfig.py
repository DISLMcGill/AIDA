import sqlite3

class ServerConfig:
   db_name = "servers.db"

   def __init__(self):
      self.connection = sqlite3.connect(ServerConfig.db_name)
      self.cursor = self.connection.cursor()
      self.names = None;


   def get_server_names(self):
      if self.names is not None:
         return self.names
      else:
         self.names = []
         for row in self.cursor.execute("SELECT hostname FROM servers"):
            self.names += [row[0]]
      return self.names
