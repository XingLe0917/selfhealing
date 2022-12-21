from dao.dbconnectionfactory import dbconnectionFactory
from dao.daofactory import wbxdaomanagerfactory, DaoKeys

def testdbfactory():
    dbfactory = dbconnectionFactory()
    dbfactory.reloadAllDBConnectionInfo()
    connect = dbfactory.getConnection("RACVVWEB")
    if connect is not None:
        connect.close()

def testdaofactory():
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanager = daomanagerfactory.getDeafultDaoManager()
    try:
        daomanager.startTransaction()
        dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.getAutoTaskByTaskid("f377f02356a346a4ab85d3999b641d90")
        daomanager.commit()
    except Exception as e:
        daomanager.rollback()
    finally:
        daomanager.close()

if __name__ == "__main__":
    testdaofactory()
    # st = {"a":1,"b":2}
    # print(st.get("c",4))