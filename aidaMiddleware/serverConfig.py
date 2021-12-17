import sqlite3

class ServerConfig:
   db_name = "servers.db"

   def __init__(self):
      self.connection = sqlite3.connect(ServerConfig.db_name)
      self.cursor = self.connection.cursor()


   def get_server_names(self):
      names = []
      for row in self.cursor.execute("SELECT hostname FROM servers"):
         names += [row[0]]
      return names
