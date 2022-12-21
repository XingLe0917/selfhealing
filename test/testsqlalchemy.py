from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker
import cx_Oracle
import threading
from common.singleton import Singleton
#
threadlocal = threading.local()
threadlocal.db_name = None
threadlocal.isTransactionStart = False

@Singleton
class dbconnectionFactory:
    def getConnection(self, **cparams):
        print("Build connect to %s" % threadlocal.db_name)
        connect = None
        try:
            cparams["user"] = "depot"
            cparams["password"] = "depot"
            cparams["dsn"] = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.7.79)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.7.83)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.7.82)(PORT=1701))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=auditdbha.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))"
            connect = cx_Oracle.connect(**cparams)
        except Exception as e:
            pass
        return connect

def connection(db_name):
    threadlocal.db_name = db_name
    # connectionurl = "depot:depot@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.8.105)(PORT= 1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.8.106)(PORT= 1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.8.107)(PORT= 1701))(LOAD_BALANCE=yes)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=auditdbha.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD=BASIC)(RETRIES= 3)(DELAY=5))))"
    connectionurl = ":@"
    _engine = create_engine('oracle+cx_oracle://%s' % (connectionurl), pool_recycle=600,
                            pool_size=5, max_overflow=0,
                            echo=False)

    @event.listens_for(_engine, "do_connect")
    def receive_do_connect(dialect, conn_rec, cargs, cparams):
        factory = dbconnectionFactory()
        return factory.getConnection(**cparams)

    sessionclz = sessionmaker(bind=_engine, expire_on_commit=True)
    _session = sessionclz()
    _session.begin()
    row = _session.execute("select count(1) from instance_info").one()
    print(row)
    _session.commit()
    _session.close()
#
# if __name__ == "__main__":
connection("DEFAULT")