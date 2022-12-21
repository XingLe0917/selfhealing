import logging

from dao.wbxdao import wbxdao
from dao.wbxservermanager import wbxservermanagerfactory

logger = logging.getLogger("SELFHEALING")

def loadDepotServerInfo():
    wbxservermanagerFactory = wbxservermanagerfactory.getwbxserverManagerFactory()
    depotdbdao = wbxdao()
    serverList = []
    dbList = []
    try:
        depotdbdao.connect()
        depotdbdao.startTransaction()
        serverList = depotdbdao.getServerInfo()
        dbList = depotdbdao.getDBInfo()
        depotdbdao.commit()
    except Exception as e:
        depotdbdao.rollback()
        logger.error(e)
    finally:
        depotdbdao.close()

    wbxservermanagerFactory.clearCache()
    logger.info("loadDepotServerInfo,dbList={0},serverList={1}".format(len(dbList),len(serverList)))
    for wbxloginuser in serverList:
        loginuser = dict(wbxloginuser)
        wbxservermanagerFactory.addServer(loginuser)

    for db in dbList:
        tns_vo = {}
        item = dict(db)
        db_name = item['db_name']
        listener_port = item['listener_port']
        service_name = "%s.webex.com" % item['service_name']
        key = str(db_name)
        value = '(DESCRIPTION ='
        if item['scan_ip1']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip1'], listener_port)
        if item['scan_ip2']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip2'], listener_port)
        if item['scan_ip3']:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                value, item['scan_ip3'], listener_port)
        value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
            value, service_name)
        tns_vo['db_name'] = key
        tns_vo['tns'] = value
        wbxservermanagerFactory.addDB(tns_vo)





