import json
import os
import base64

from influxdb import InfluxDBClient

from common.wbxexception import wbxexception


class Config:
    def __init__(self):
        self.CONFIGFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), "conf")
        self.configdict = None
        self.loadConfig()

    @staticmethod
    def getConfig():
        return Config()

    def loadConfig(self):
        if self.configdict is None:
            configfiledir = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), "config")
            with open(os.path.join(configfiledir, "config.json")) as f:
                self.configdict = json.load(f)

    def getLoggerConfigFile(self):
        logger_config_file = os.path.join(self.CONFIGFILE_DIR, "logger.conf")
        if not os.path.isfile(logger_config_file):
            raise wbxexception("%s does not exist" % logger_config_file)
        return logger_config_file

    def getDepotConnectionURL(self):
        return self.configdict["depotdb"]

    def getInfluxDB_SJC_client(self):
        database = "oraclemetric"
        return InfluxDBClient(self.configdict['influxDB_ip_SJC'], int(self.configdict['influxDB_port']),
                              self.configdict['influxDB_user'],
                              base64.b64decode(self.configdict['influxDB_pwd']).decode("utf-8"), database)

    def get_task_type(self):
        return self.configdict['task_type']

