import uuid
import logging
import threading
from abc import abstractmethod

from common.config import Config
from common.wbxcache import getLog, removeLog, addTaskToCache
from datetime import datetime

from dao.wbxdao import wbxdao
from dao.daofactory import wbxdaomanagerfactory, DaoKeys

threadlocal = threading.local()
logger = logging.getLogger("SELFHEALING")

class wbxautotask:
        def __init__(self, taskid, taskType):
            if taskid is None:
                self._taskid = uuid.uuid4().hex
            else:
                self._taskid = taskid
            self._taskType = taskType
            self.config = Config.getConfig()

        @abstractmethod
        def preverify(self,*args):
            pass

        @abstractmethod
        def fix(self,*args):
            pass

        @abstractmethod
        def postverify(self,*args):
            pass

        def initialize(self):
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            daomanager = daomanagerfactory.getDeafultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                daomanager.startTransaction()
                taskvo = dao.getAutoTaskByTaskid(self._taskid)[0]
                joblist = dao.getAutoTaskJobByTaskid(self._taskid)
                taskvo['joblist'] = joblist
                daomanager.commit()
                addTaskToCache(self._taskid, self)
                return taskvo
            except Exception as e:
                daomanager.rollback()
                raise e
            finally:
                daomanager.close()

        def updateJobStatus(self, jobid, status):
            logger.info("Update job status to be %s in depotdb with jobid=%s" %(status, jobid))
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            daomanager = daomanagerfactory.getDeafultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                daomanager.startTransaction()
                jobvo = dao.getAutoTaskJobByJobid(jobid)
                if jobvo is not None:
                    resultmsg = None
                    if status in ('SUCCEED','FAILED'):
                        jobvo.end_time = datetime.now()
                        # Do not remove below log, it will add the summarized line to the output log
                        logger.info("The automation task %s job %s %s" % (self._taskType, jobvo.job_action, status))
                        resultmsg = getLog(jobid)
                        threadlocal.current_jobid = None
                    elif status == "RUNNING":
                        jobvo.start_time = datetime.now()
                        jobvo.resultmsg1=None
                        jobvo.resultmsg2=None
                        jobvo.resultmsg3=None
                        if jobvo.status in ("FAILED", "RUNNING"):
                            removeLog(jobid)
                        threadlocal.current_jobid = jobid

                    if resultmsg is not None and resultmsg != "":
                        colwidth = 3900
                        resList = [resultmsg[x - colwidth:x] for x in range(colwidth, len(resultmsg) + colwidth, colwidth)]
                        jobvo.resultmsg1 = resList[0]
                        if len(resList) > 1:
                            jobvo.resultmsg2 = resList[1]
                        if len(resList) > 2:
                            jobvo.resultmsg3 = resList[-1]
                    jobvo.status = status
                daomanager.commit()
                return jobvo
            except Exception as e:
                daomanager.rollback()
                raise e
            finally:
                daomanager.close()

        def getTaskJobsByTaskid(self, taskid):
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            daomanager = daomanagerfactory.getDeafultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                daomanager.startTransaction()
                taskvo = dao.getAutoTaskJobByTaskid(taskid)
                daomanager.commit()
                return taskvo
            except Exception as e:
                daomanager.rollback()
                raise e
            finally:
                daomanager.close()

        def taskStart(self, taskid, attemptcount):
            return self._updateAlertStatus(taskid,'RUNNING',attemptcount)

        def taskSucceed(self,taskid):
            return self._updateAlertStatus(taskid,'SUCCEED')

        def taskFailed(self,taskid):
            return self._updateAlertStatus(taskid,'FAILED')

        def _updateAlertStatus(self,taskid,status, attemptcount=0):
            daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
            daomanager = daomanagerfactory.getDeafultDaoManager()
            dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            try:
                daomanager.startTransaction()
                count = dao.updateWbxmonitoralert2Status(taskid,status,attemptcount)
                daomanager.commit()
                return count
            except Exception as e:
                daomanager.rollback()
                raise e
            finally:
                daomanager.close()




