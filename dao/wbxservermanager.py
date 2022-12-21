
from common.wbxexception import wbxexception
from common.wbxssh import Wbxssh
from dao.dbconnectionfactory import dbconnectionFactory

class wbxservermanagerfactory(object):
    servermanagerfactory = None

    @staticmethod
    def getwbxserverManagerFactory():
        if wbxservermanagerfactory.servermanagerfactory is None:
            wbxservermanagerfactory.servermanagerfactory = object.__new__(wbxservermanagerfactory)
        return wbxservermanagerfactory.servermanagerfactory

    def getServer(self, hostname):
        dbconnectfactory = dbconnectionFactory()
        server = dbconnectfactory.getServer(hostname)
        if server is not None:
            return Wbxssh(server['host_name'], server['ssh_port'], server['username'], server['pwd'])
        else:
            raise wbxexception("Server %s does not exist" % hostname)



