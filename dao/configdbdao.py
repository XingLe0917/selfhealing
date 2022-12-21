import json
import logging

from dao.wbxdao import wbxdao
from dao.vo.autotaskvo import wbxautotaskjobvo

logger = logging.getLogger("DBAMONITOR")

class ConfigDBDao(wbxdao):
    def __init__(self):
        super().__init__()
