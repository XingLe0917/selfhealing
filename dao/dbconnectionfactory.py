import logging
import threading
import cx_Oracle
from cacheout import LRUCache

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.wbxexception import wbxexception
from common.singleton import Singleton
from common.wbxssh import Wbxssh

logger = logging.getLogger("SELFHEALING")

class wbxcache:
    def __init__(self, size = 10240, ttl = 30 * 60):
        self._dbcache = LRUCache(maxsize=size, ttl=ttl)

    def get(self, key):
        return self._dbcache.get(key)

    def add(self, key, val):
        self._dbcache.add(key, val)

    def size(self):
        return self._dbcache.size()

    def isexist(self, key):
        self._dbcache.get(key)

@Singleton
class dbconnectionFactory:
    _lock = threading.Lock()

    def __init__(self):
        self._dbcache = wbxcache(10240,30 * 60)
        self._max_db_count = 0
        self._servercache = wbxcache(10240, 60 * 60)
        self._max_server_count = 0

    def getConnection(self, db_name, username):
        logger.info("Build connect to %s" % db_name)
        cparams = self.getDBConnectionInfo(db_name)
        connect = cx_Oracle.connect(**cparams)
        return connect

    def getDBConnectionInfo(self, db_name):
        if db_name == "DEFAULT":
            return {"user": "depot", "password": "depot",
                        "dsn": "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.7.79)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.7.83)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.7.82)(PORT=1701))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=auditdbha.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))"}

        dbinfo = self._dbcache.get(db_name)
        if dbinfo is None:
            self.reloadDBConnectionInfo()
        return self._dbcache.get(db_name)

    def reloadDBConnectionInfo(self):
        with self._lock:
            cparams = self.getDBConnectionInfo("DEFAULT")
            connection = None
            try:
                connection = cx_Oracle.connect(**cparams)
                cursor = connection.cursor()
                SQL = '''SELECT distinct db.db_name, '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip1 ||')(PORT = '|| db.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip2 ||')(PORT = '|| db.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip3 ||')(PORT = '|| db.listener_port ||'))(LOAD_BALANCE = yes)(FAILOVER = on)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = '|| db.service_name ||'.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' connectionurl
                                                                    FROM database_info db, instance_info ii, host_info hi  
                                                                    WHERE db.trim_host=ii.trim_host  
                                                                    AND db.db_name=ii.db_name
                                                                    AND ii.trim_host=hi.trim_host
                                                                    AND ii.host_name=hi.host_name
                                                                    AND upper(db.db_vendor)='ORACLE'
                                                                    AND db.db_type='PROD'
                                                                    AND db.db_type<> 'DECOM' '''
                # if self._dbcache.size() < 10:
                #
                # else:
                #     SQL = """SELECT distinct '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip1 ||')(PORT = '|| db.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip2 ||')(PORT = '|| db.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip3 ||')(PORT = '|| db.listener_port ||'))(LOAD_BALANCE = yes)(FAILOVER = on)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = '|| db.service_name ||'.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' connectionurl
                #                                                     FROM database_info db, instance_info ii, host_info hi
                #                                                     WHERE db.trim_host=ii.trim_host
                #                                                     AND db.db_name=ii.db_name
                #                                                     AND ii.trim_host=hi.trim_host
                #                                                     AND ii.host_name=hi.host_name
                #                                                     AND db.db_name = :db_name
                #                                                     AND upper(db.db_vendor)='ORACLE'
                #                                                     AND db.db_type='PROD'
                #                                                     AND db.db_type<> 'DECOM'"""
                rows = cursor.execute(SQL).fetchall()
                self._max_db_count = len(rows)
                for row in rows:
                    self._dbcache.add(row[0], {"user": "system", "password": "sysnotallow", "dsn": row[0]})
                # else:
                #     # If illegal db_name come into this function, add this db_name to avoid repeated query
                #     self._dbcache.add(db_name, None)
            except Exception as e:
                if connection is not None:
                    connection.rollback()
            finally:
                if connection is not None:
                    connection.close()

    def getServer(self, host_name):
        serverinfo = self._servercache.get(host_name)
        if serverinfo is None:
            self.reloadServerInfo()
        serverinfo = self._servercache.get(host_name)
        return serverinfo

    def reloadServerInfo(self):
        with self._lock:
            cparams = self.getDBConnectionInfo("DEFAULT")
            connection = None
            try:
                connection = cx_Oracle.connect(**cparams)
                cursor = connection.cursor()
                SQL = '''select h.host_name, h.username, f_get_deencrypt(h.pwd) pwd ,hi.ssh_port
                                    from host_user_info h,host_info hi ,database_info di, instance_info ii
                                    where h.host_name = hi.host_name
                                    AND di.trim_host=ii.trim_host                           
                                    AND di.db_name=ii.db_name
                                    AND ii.host_name=hi.host_name
                                    and di.db_type = 'PROD' '''
                # if self._servercache.size() < 10:
                #
                # else:
                #     SQL = """select h.host_name, h.username, f_get_deencrypt(h.pwd) pwd ,hi.ssh_port
                #     from host_user_info h,host_info hi ,database_info di, instance_info ii
                #     where h.host_name = hi.host_name
                #     AND di.trim_host=ii.trim_host
                #     AND di.db_name=ii.db_name
                #     AND ii.host_name=hi.host_name
                #     and di.db_type = 'PROD'
                #     and h.host_name=:host_name"""
                rows = cursor.execute(SQL).fetchall()
                self._max_server_count = len(rows)
                for row in rows:
                    self._servercache.add(row[0], {"username": row[1], "pwd": row[2], "ssh_port": row[3],"host_name":row[0]})

            except Exception as e:
                if connection is not None:
                    connection.rollback()
            finally:
                if connection is not None:
                    connection.close()




