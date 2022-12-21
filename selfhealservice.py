import logging.config
from biz.taskdispatcher import TaskDispatcher
from common.config import Config

logger = None

def init():
    global logger
    config = Config.getConfig()
    logconfigfile = config.getLoggerConfigFile()
    logging.config.fileConfig(logconfigfile)
    logger = logging.getLogger("SELFHEALING")

if __name__=='__main__':
    init()
    dispatcher = TaskDispatcher()
    dispatcher.start()