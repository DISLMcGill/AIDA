import sqlite3

class ServerConfig:
   db_name = "servers.db"

   def __init__(self):
      self.connection = sqlite3.connect(db_name)
      self.cursor = connection.cursor()


   def get_server_names():
      names = []
      for row in cursor.execute("SELECT hostname FROM servers"):
         names += [row[0]]
      return names
