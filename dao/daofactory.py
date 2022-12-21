import logging
import threading
import os
import sys
import json

from common.wbxexception import wbxexception
from common.singleton import Singleton
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event
from sqlalchemy.pool import NullPool

from dao.depotdbdao import DepotDBDao
from dao.configdbdao import ConfigDBDao
from dao.auditdbdao import AuditDBDao
from dao.dbconnectionfactory import dbconnectionFactory


threadlocal = threading.local()
threadlocal.isTransactionStart = False

logger = logging.getLogger("SELFHEALING")

class wbxdaomanagerfactory:
    _lock = threading.Lock()
    daomanagerfactory = None
    daoManagerCache = {}

    def __init__(self):
        raise wbxexception("Can not instance a wbxdaomanagerfactory, please call getDaoManagerFactory()")

    @staticmethod
    def getDaoManagerFactory():
        if wbxdaomanagerfactory.daomanagerfactory is None:
            wbxdaomanagerfactory.daomanagerfactory = object.__new__(wbxdaomanagerfactory)
        return wbxdaomanagerfactory.daomanagerfactory

    def getDeafultDaoManager(self):
        return self.getDaoManager("DEFAULT", "depot", False, True, 10)

    def getDaoManager(self, dbid, username = "system", expire_on_commit= False, poolengine=False, poolsize=2):
        if dbid in wbxdaomanagerfactory.daoManagerCache:
            return wbxdaomanagerfactory.daoManagerCache[dbid]
        else:
            with wbxdaomanagerfactory._lock:
                # logger.info("%s:%s@%s" % (schemaname, schemapwd, connectionurl))
                if poolengine:
                    _engine = create_engine('oracle+cx_oracle://%s:@' % (username),
                                           pool_recycle=600, pool_size=poolsize, max_overflow=10, pool_timeout=10,
                                           echo_pool=False, echo=False)
                else:
                    _engine = create_engine('oracle+cx_oracle://%s:@' % (username),
                                           poolclass=NullPool, echo=True)

                daomanager = wbxdaomanager(_engine, expire_on_commit)
                wbxdaomanagerfactory.daoManagerCache[dbid] = daomanager

                @event.listens_for(_engine, "do_connect")
                def receive_do_connect(dialect, conn_rec, cargs, cparams):
                    factory = dbconnectionFactory()
                    username = cparams.get("user", "system")
                    return factory.getConnection(dbid, username)

                return daomanager

class wbxdaomanager:
    _lock = threading.Lock()

    def __init__(self, engine, expire_on_commit=False):
        self.daoMap={}
        self.loadDaoMap()
        self._engine = engine
        self._sessionclz = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)
        # self.setLocalSession()

    def loadDaoMap(self):
        for daoName, daoClz in DaoKeys.daodict.items():
            idxsize = daoClz.rfind(".")
            modulepath=daoClz[0:idxsize]
            clzname=daoClz[idxsize + 1:]
            amodule = sys.modules[modulepath]
            daocls = getattr(amodule, clzname)
            wbxdao = daocls()
            self.daoMap[daoName] = wbxdao

    def getLocalSession(self):
        session = threadlocal.current_session
        return session

    # Session is not thread-safe. so each thread we create a new Session
    # http://docs.sqlalchemy.org/en/latest/orm/session_basics.html
    # Session is not appropriate to cache object; All cached data in Session should be removed after commit;
    # But because we do not introduce Cache Module in this project, so Session is also used as a cache, it is workaround method
    def setLocalSession(self):
        with wbxdaomanager._lock:
            session = self._sessionclz()
            threadlocal.current_session = session
            if not hasattr(threadlocal, "isTransactionStart"):
                threadlocal.isTransactionStart = True
            elif threadlocal.isTransactionStart:
                raise wbxexception("Transction already started, please close previous transaction at first")
            else:
                threadlocal.isTransactionStart = True

    def startTransaction(self):
        self.setLocalSession()
        # threadlocal.isTransactionStart = True
        # session = self.getLocalSession()
        # return session

    def commit(self):
        if not threadlocal.isTransactionStart:
            raise wbxexception("No transaction, please call startTransaction() at first")
        try:
            threadlocal.isTransactionStart = False
            session = threadlocal.current_session
            if session is not None:
                session.commit()
        except Exception as e:
            logger.error("Error occurred at session.commit with %s" % e)


    def rollback(self):
        if not threadlocal.isTransactionStart:
            raise wbxexception("No transaction, please call startTransaction() at first")
        try:
            threadlocal.isTransactionStart = False
            session = self.getLocalSession()
            if session is not None:
                session.rollback()
        except Exception as e:
            pass

    def getDao(self, daoKey):
        wbxdao = self.daoMap[daoKey]
        wbxdao.setDaoManager(self)
        return wbxdao

    def flush(self):
        session = self.getLocalSession()
        session.dirty()

    def close(self):
        session = self.getLocalSession()
        session.close()

class DaoKeys():
    DAO_DEPOTDBDAO = "DAO_DEPOTDBDAO"
    DAO_CONFIGDBDAO = "DAO_CONFIGDBDAO"

    daodict = {
        DAO_DEPOTDBDAO:"dao.depotdbdao.DepotDBDao",
        DAO_CONFIGDBDAO:"dao.configdbdao.ConfigDBDao"}