import logging

from common.wbxautotask import threadlocal
from common.wbxcache import addLog


class wbxmemoryhandler(logging.Handler):
    def __init__(self, filefmt=None):
        self.filefmt = filefmt
        logging.Handler.__init__(self, logging.INFO)

    def emit(self, record):
        msg = self.format(record)
        if hasattr(threadlocal,"current_jobid"):
            addLog(threadlocal.current_jobid,msg)
