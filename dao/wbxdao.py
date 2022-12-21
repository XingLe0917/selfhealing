import logging

from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker
import threading
import cx_Oracle

from common.config import Config
from common.wbxexception import wbxexception
from common.singleton import Singleton
from dao.dbconnectionfactory import dbconnectionFactory

threadlocal = threading.local()
threadlocal.isTransactionStart = False

logger = logging.getLogger("SELFHEALING")

class wbxdao(object):

    def setDaoManager(self, daoManager):
        self._wbxdaomanager = daoManager

    def getLocalSession(self):
        return self._wbxdaomanager.getLocalSession()



