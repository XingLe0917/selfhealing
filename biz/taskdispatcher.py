import logging
import time
import os

from common.config import Config
from dao.daofactory import wbxdaomanagerfactory, DaoKeys
from biz.autotask.influxdbissuetask import influxdbtask


from concurrent.futures.thread import ThreadPoolExecutor
import queue
logger = logging.getLogger("SELFHEALING")

class WBXThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, maxsize=20, *args, **kwargs):
        super(WBXThreadPoolExecutor, self).__init__(*args, **kwargs)
        self._work_queue = queue.PriorityQueue(maxsize=maxsize)

class TaskDispatcher:
    def __init__(self):
        self._futuredict = {}
        self._thread_count = 1
        # self._thread_count = os.cpu_count() * 2
        self._executor = WBXThreadPoolExecutor(max_workers=self._thread_count)

    def getAutoTask(self):
        config = Config.getConfig()
        task_type_list = config.get_task_type()
        daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
        daomanager = daomanagerfactory.getDeafultDaoManager()
        try:
            daomanager.startTransaction()
            dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            taskList = dao.getAutoTaskJob(task_type_list, self._thread_count)
            daomanager.commit()
            return taskList
        except Exception as e:
            daomanager.rollback()
            logger.error("Error occurred during ProducerJob.getAutoTask()", exc_info =e )
        finally:
            daomanager.close()
        return []

    # If start N selfhealing service, a task maybe got by all selfhealing services;
    # But because consumer update the task status before executing this task, so a task is not processed repeatly;
    # If we realy need to start multiple selfhealing services in future, we will have other method to support scalability;
    # 1. different client for different task type;
    # 2. All consumers get tasks from a global Redis instead of separate Producer job
    def start(self):
        logger.info("Start ProducerJob")
        while True:
            try:
                if self._executor._work_queue.qsize() <= 0:
                    autotasklist = self.getAutoTask()
                    logger.info("get {0} autotasks from db".format(len(autotasklist)))
                    if len(autotasklist) == 0:
                        time.sleep(60)
                    else:
                        for autotask in autotasklist:
                            f = self._executor.submit(self.jobHandler, autotask)
                            f.add_done_callback(self.onCompleted)
                else:
                    time.sleep(60)

            except Exception as e:
                logger.error(e)

    def onCompleted(self, fn):
        if fn.cancelled():
            logger.info('Task {}: canceled'.format(fn.result()))
        elif fn.done():
            error = fn.exception()
            if error:
                logger.info('Task {}: error returned: {}'.format(fn.result(), error))
            else:
                logger.info('Task {} completed'.format(fn.result()))

    def jobHandler(self, alertinfo):
        try:
            logger.info("start process alert %s" % alertinfo)
            alert_type = alertinfo['task_type']
            taskid = alertinfo['taskid']
            attempcount=alertinfo['attemptcount']
            autotask = None
            if alert_type == "INFLUXDB_ISSUE_TASK":
                autotask = influxdbtask(taskid)
            # todo (You can add other autotasks in here.)
            if autotask:
                count = autotask.taskStart(taskid, attempcount)
                if count == 0:
                    logger.info("The task status is updated by other service. Skip it. task_type={0}, taskid={1}".format(alert_type, taskid))
                    return alertinfo
                autotask.initialize()
                try:
                    res1 = autotask.preverify()
                    if res1:
                        res2 = autotask.fix()
                        if res2:
                            time.sleep(60)
                            autotask.postverify()
                    taskvo = autotask.getTaskJobsByTaskid(taskid)
                    task_fixed = True
                    for job in taskvo:
                        if job['status'] == "FAILED":
                            task_fixed = False
                            break
                    if task_fixed:
                        autotask.taskSucceed(taskid)
                    else:
                        autotask.taskFailed(taskid)

                except Exception as e:
                    logger.error("Fix autotask failed. task_type={0}, taskid={1},e={2}".format(alert_type, taskid,str(e)))
                    count2 = autotask.taskFailed(taskid)
                    logger.info("updateAlertStatus to FAILED, task_type={0}, taskid={1}, update_count={2}".
                                format(alert_type,taskid,count2))
            else:
                logger.info("Skip this autotask, task_type={0}, taskid={1}".format(alert_type, taskid))
        except Exception as e:
            logger.error(str(e))
        return alertinfo