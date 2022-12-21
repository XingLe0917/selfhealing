import time
from dao.dbconnectionfactory import wbxcache, dbconnectionFactory

if  __name__ == "__main__":
    factory = dbconnectionFactory()
    factory.reloadServerInfo()