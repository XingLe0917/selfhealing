import datetime
import json
import logging
import time

from common.config import Config
from common.wbxautotask import wbxautotask
from common.wbxchatbot import wbxchatbot
from common.wbxexception import wbxexception
from dao.wbxservermanager import wbxservermanagerfactory

logger = logging.getLogger("SELFHEALING")

'''
Case 1. DB metadata in Depotdb not right, a db is removed from RAC, but depotdb metadata is not updated timely
Case 2. telegraf not installed or not Kafka broker configuration issue
Case 3: Telegraf installed and Kafka brokers is configured, but can not connect to any broker

'''
class influxdbtask(wbxautotask):
    def __init__(self, taskid = None):
        super(influxdbtask,self).__init__(taskid, "INFLUXDB_ISSUE_TASK")
        self._autotaskid = None
        self._host_name =None
        self._db_name =None
        self._instance_name = None
        self._config = Config()
        self._influxdb_client = self._config.getInfluxDB_SJC_client()
        self._joblist = None

    def initialize(self):
        taskvo = super(influxdbtask, self).initialize()
        parameter = ""
        self._autotaskid = taskvo['taskid']
        if "parameter" in taskvo:
            parameter = taskvo['parameter']
        res = json.loads(parameter)
        if "host_name" in res:
            self._host_name = str(res['host_name']).split(".")[0]
        if "db_name" in res:
            self._db_name = res['db_name']
        if "instance_name" in res:
            self._instance_name = res['instance_name']
        jobmap = {}
        for job in taskvo['joblist']:
            job_action = job['job_action']
            jobmap[job_action] = job['jobid']
        self._joblist = jobmap
        return taskvo

    def preverify(self):
        chatjob = wbxchatbot()
        jobid = self._joblist['preverify']
        logger.info("[preverify] taskType={0}, taskid={1}, host_name={2}, jobid={3}".format(self._taskType,self._autotaskid,self._host_name,jobid))
        try:
            self.updateJobStatus(jobid, "RUNNING")
            wbxservermanagerFactory = wbxservermanagerfactory.getwbxserverManagerFactory()
            server = wbxservermanagerFactory.getServer(self._host_name)
            server.connect()
            continue_check = True
            logger.info("************* check instance *************")
            cmd = ". /home/oracle/.bash_profile;ps aux | grep lgwr | grep -v grep | grep -vi ASM | awk '{print $NF}' | awk -F_ '{print $NF}'"
            try:
                nodes = server.exec_command(cmd)
                nodes_list = nodes.split('\n')
                if self._instance_name not in nodes_list:
                    logger.info(
                        "instance_name={0} not on the {1}, please check info.".format(self._instance_name,
                                                                                      self._host_name))
                    # todo send to chatbot room
                    self.updateJobStatus(jobid, "UNSOLVED")
                    continue_check = False
            except Exception as e:
                logger.error(str(e))
                self.updateJobStatus(jobid, "FAILED")
                raise wbxexception(str(e))

            if continue_check:
                logger.info("************* check kafka *************")
                cmd = "sudo cat /etc/telegraf/telegraf.conf | grep -m 1 brokers"
                try:
                    res = server.exec_command(cmd)
                    if res:
                        logger.info(res)
                        # groups = re.search(r'"(.*?)"', res)
                        if "brokers" in str(res).strip():
                            arr = str(res).strip().split("=")[1].strip()
                            json_brokers = json.loads(arr)
                            ip = str(json_brokers[0]).split(":")[0]
                            port = str(json_brokers[0]).split(":")[1]
                            cmd = "python -c \"import telnetlib; tel=telnetlib.Telnet('%s','%s',3); print tel; tel.close()\"" % (
                            ip, port)
                            logger.info(cmd)
                            res = server.exec_command(cmd)
                            if "error" in res:
                                msg1 = "#### From self-healing message:\n"
                                msg1 += "💔 Check kafka has issue. db_name={0},host_name={1}".format(self._db_name,
                                                                                                  self._host_name)
                                logger.error(msg1)
                                chatjob.alert_msg(msg1)
                                logger.error(res)
                                raise wbxexception(res)
                    else:
                        self.updateJobStatus(jobid, "FAILED")
                        msg1 = "#### From self-healing message:\n"
                        msg1 += "💔 kafka brokers not found in telegraf.conf. db_name={0},host_name={1}".format(self._db_name,self._host_name)
                        logger.error(msg1)
                        chatjob.alert_msg(msg1)

                        logger.error("kafka brokers not found in telegraf.conf")
                        raise wbxexception("kafka brokers not found in telegraf.conf")
                except Exception as e:
                    self.updateJobStatus(jobid, "FAILED")
                    raise wbxexception(e)

        except Exception as e:
            self.updateJobStatus(jobid, "FAILED")
            raise wbxexception(e)
        self.updateJobStatus(jobid, "SUCCEED")
        return True

    def postverify(self):
        chatjob = wbxchatbot()
        jobid = self._joblist['postverify']
        logger.info("[postverify] taskType={0}, taskid={1}, host_name={2}, jobid={3}".format(self._taskType,self._autotaskid, self._host_name, jobid))
        try:
            self.updateJobStatus(jobid, "RUNNING")
            logger.info("check influx db data in last 5 mins")
            flag = self.check_influxdb_data()
            if flag:
                self.updateJobStatus(jobid, "SUCCEED")
                msg1 = "#### From self-healing message:\n"
                msg1 += "👏 The issue of missing data in influxdb has been resolved. db_name={0}, host_name={1}, taskid={2}".format(self._db_name, self._host_name,self._autotaskid)
                logger.error(msg1)
                chatjob.alert_msg_to_dbateam(msg1)
            else:
                logger.info("The issue has not been fixed.")
                self.updateJobStatus(jobid, "FAILED")
        except Exception as e:
            self.updateJobStatus(jobid, "FAILED")
            raise wbxexception(str(e))
        return True

    def fix(self):
        chatjob = wbxchatbot()
        # roomid = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5"
        # roomid = "Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi"
        jobid = self._joblist['fix']
        logger.info(
            "[fix] taskType={0}, taskid={1}, host_name={2},db_name={3}, jobid={4}".format(self._taskType,self._autotaskid, self._host_name,self._db_name,jobid))
        fix_flag = False
        try:
            self.updateJobStatus(jobid, "RUNNING")
            wbxservermanagerFactory = wbxservermanagerfactory.getwbxserverManagerFactory()
            server = wbxservermanagerFactory.getServer(self._host_name)
            try:
                logger.info("************* check dbmonitor log path and file *************")
                cmd = "ls -l /var/log/dbmonitor/data/ | grep %s" % (self._db_name)
                logger.info(cmd)
                res_log = server.exec_command(cmd)
                # log_file_list = str(res_log).split("\n")
                logger.info(res_log)
                if "cannot access" in res_log or len(str(res_log).split("\n")) < 2:
                    logger.info("Try to reinstall relegraf...")
                    install_telegraf_file = "/staging/gates/telegraf/install_telegraf.sh"
                    cmd = "ls %s" % (install_telegraf_file)
                    res = server.exec_command(cmd)
                    if "install_telegraf.sh" not in res:
                        error_msg = "No found %s on %s" % (install_telegraf_file, self._host_name)
                        raise wbxexception(error_msg)
                    cmd = "sh %s" % (install_telegraf_file)
                    logger.info(cmd)
                    res = server.exec_command(cmd)
                    logger.info(res)
                    logger.info("check influxdb data, db_name={0}, instance_name={1}".format(self._db_name,
                                                                                             self._instance_name))
                    logger.info("Please wait a moment.")
                    flag = self.check_influxdb_data()
                    logger.info("check influxdb data result=%s" % (flag))
                    if flag:
                        fix_flag = True
                        self.updateJobStatus(jobid, "SUCCEED")

                if not fix_flag:
                    logger.info("************* check dba_scheduler_jobs ************* ")
                    rest_list = ""
                    owner = ""
                    try:
                        sql = "select owner||'=='||job_name||'=='||to_char(last_start_date,'yyyy-mm-dd hh24:mi:ss') ||'=='||ENABLED ||'=='||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')||'==isflag' from dba_scheduler_jobs where job_name like " + "'%MONITOR%'"
                        logger.info(sql)
                        cmd1 = """
                                                                                 . /home/oracle/.bash_profile
                                                                                        db
                                                                                        export ORACLE_SID=%s
                                                                                        sqlplus / as sysdba << EOF
                                                                                        set linesize 1000;
                                                                                        %s;
                                                                                        exit;
                                                                                        EOFF
                                                                                """ % (self._instance_name, sql)
                        logger.debug(cmd1)
                        rest_list = server.exec_command(cmd1)
                        logger.info(rest_list)
                        if "no rows selected" in rest_list:
                            self.updateJobStatus(jobid, "UNSOLVED")
                            msg1 = "#### From self-healing message:\n"
                            msg1 += "💔 MONITOR job is null in dba_scheduler_jobs, need to deploy dbpatch. db_name={0},host_name={1}".format(
                                self._db_name, self._host_name)
                            logger.error(msg1)
                            chatjob.alert_msg(msg1)
                            raise wbxexception(msg1)
                        else:
                            for res_info in str(rest_list).split("\n"):
                                if "isflag" in res_info:
                                    res_info_arr = res_info.strip().split("==")
                                    owner = res_info_arr[0]
                                    job_name = res_info_arr[1]
                                    last_start_date = res_info_arr[2]
                                    ENABLED = res_info_arr[3]
                                    current_time = res_info_arr[4]
                                    if ENABLED == "FALSE":
                                        sql2 = "exec dbms_scheduler.enable(name=>'%s.%s');" % (owner, job_name)
                                        cmd2 = """
                                                  . /home/oracle/.bash_profile
                                                  db
                                                  %s
                                                  exit;
                                                  EOF
                                                  """ % (sql2)
                                        logger.info(cmd2)
                                        try:
                                            rest_list_2 = server.exec_command(cmd2)
                                            logger.info(rest_list_2)
                                        except Exception as e:
                                            raise wbxexception(
                                                "Error occurred: exec dbms_scheduler.enable, sql={0}, e={1}".format(
                                                    sql2, str(e)))
                                    lastStartDate = datetime.datetime.strptime(last_start_date, '%Y-%m-%d %H:%M:%S')
                                    currentTime = datetime.datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
                                    if "DB_MONITOR_MINUTELY_COLLECTJOB" in job_name:
                                        if lastStartDate < (currentTime - datetime.timedelta(minutes=5)):
                                            logger.info("DB_MONITOR_MINUTELY_COLLECTJOB is not executed.")
                                            msg1 = "#### From self-healing message:\n"
                                            msg1 += "💔 DB_MONITOR_MINUTELY_COLLECTJOB is not executed, last_start_date={0}, current_time={1}. db_name={2},host_name={3}".format(
                                                last_start_date,current_time,
                                                self._db_name, self._host_name)
                                            logger.error(msg1)
                                            chatjob.alert_msg(msg1)
                                            raise wbxexception(msg1)

                    except Exception as e:
                        raise wbxexception(
                                    "Error occurred: view dba_scheduler_jobs, instance_name={0},host_name={1}, e={2}".format(
                                        self._instance_name, self._host_name, str(e)))

                    logger.info("owner={0}" .format(owner))
                    logger.info("************* check  WBXDB_MONITOR_CONFIG************* ")
                    sql = "select * from %s.WBXDB_MONITOR_CONFIG" % (owner)
                    cmd = """
                                                                   . /home/oracle/.bash_profile
                                                                   db
                                                                   export ORACLE_SID=%s
                                                                   sqlplus / as sysdba << EOF 
                                                                   set linesize 1000;
                                                                   %s;
                                                                   exit;
                                                                   EOFF
                                                                    """ % (self._instance_name, sql)
                    try:
                        logger.info(cmd)
                        res3 = server.exec_command(cmd)
                        if "no rows selected" in res3:
                            self.updateJobStatus(jobid, "FAILED")
                            msg2 = "#### From self-healing message:\n"
                            msg2 += "💔 Need to deploy dbpatch, db_name={0},host_name={1}" .format(self._db_name,self._host_name)
                            logger.error(msg2)
                            chatjob.alert_msg(msg2)
                            raise wbxexception(msg2)
                    except Exception as e:
                        raise wbxexception(str(e))

                    # logger.info("************* check 16017 ************* ")
                    # sql = "select * from %s.wbxdatabase where version = '16017' " % (owner)
                    # cmd = """
                    #                                                                    . /home/oracle/.bash_profile
                    #                                                                    db
                    #                                                                    export ORACLE_SID=%s
                    #                                                                    sqlplus / as sysdba << EOF
                    #                                                                    set linesize 1000;
                    #                                                                    %s;
                    #                                                                    exit;
                    #                                                                    EOFF
                    #                                                                     """ % (self._instance_name, sql)
                    # try:
                    #     logger.info(cmd)
                    #     res4 = server.exec_command(cmd)
                    #     if "no rows selected" in res4:
                    #         self.updateJobStatus(jobid, "FAILED")
                    #         msg4 = "Missing dbpatch 16017 and need to deploy dbpatch. db_name={0},host_name={1}" .format(self._db_name,self._host_name)
                    #         logger.error(msg4)
                    #         chatjob.alert_msg(msg4)
                    #         raise wbxexception(msg4)
                    # except Exception as e:
                    #     raise wbxexception(str(e))

                    logger.info("************* check wbxproclog ************* ")
                    sql = '''
                                                select p1 from %s.wbxproclog where procname 
                                                in ('CollectODMMonitorDataSecondly','CollectODMMonitorDataMinutely',
                                                'OutputODMMonitorData','CollectOSSMonitorData','OutputOSSMonitorData',
                                                'CollectOWIMonitorData','OutputOWIMonitorData','CollectSQLServiceNameMapData',
                                                'CollectSQLMonitorData','OutputSQLMonitorData','CollectSessionMonitorData',
                                                'OutputSessionMonitorData','CollectArchivedLog','OutputArchivedLog','CollectRmanLog',
                                                'OutputRmanLog','CollectOracleJobMonitorData','OutputOracleJobMonitorData',
                                                'CollectTableSpaceUsage','OutputTableSpaceUsage','CollectOSSTATMonitorData',
                                                'OutputOSSTATMonitorData') and logtime > sysdate - 10/60/24 ;
                                                ''' % (owner)
                    cmd = """
                                                . /home/oracle/.bash_profile
                                                db
                                                export ORACLE_SID=%s
                                                sqlplus / as sysdba << EOF 
                                                set linesize 1000;
                                                %s;
                                                exit;
                                                EOFF
                                                 """ % (self._instance_name, sql)
                    # logger.info(cmd)
                    try:
                        rest_list = server.exec_command(cmd)
                        logger.info(rest_list)
                        if "no rows selected" not in rest_list:
                            logger.info("************* create or replace directory MONITOR_LOG_DIR ************* ")
                            sql = " create or replace directory MONITOR_LOG_DIR as '/var/log/dbmonitor/data' "
                            cmd = '''
                            . /home/oracle/.bash_profile
                            db
                            export ORACLE_SID=%s
                            sqlplus / as sysdba << EOF 
                            set linesize 1000;
                            %s;
                            exit;
                            EOFF
                            '''% (self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(str(e))
                            logger.info("************* exec grant ************* ")
                            sql = "	grant execute on UTL_FILE to %s" % (owner)
                            cmd = """
                                                                                                               . /home/oracle/.bash_profile
                                                                                                               db
                                                                                                               export ORACLE_SID=%s
                                                                                                               sqlplus / as sysdba << EOF 
                                                                                                               set linesize 1000;
                                                                                                               %s;
                                                                                                               exit;
                                                                                                               EOFF
                                                                                                                """ % (
                            self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(str(e))

                            sql = "	GRANT CREATE ANY DIRECTORY TO %s" % (owner)
                            cmd = """
                                                             . /home/oracle/.bash_profile
                                                             db
                                                             export ORACLE_SID=%s
                                                             sqlplus / as sysdba << EOF 
                                                             set linesize 1000;
                                                             %s;
                                                             exit;
                                                             EOFF
                                                             """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(str(e))

                            sql = "	GRANT SELECT ANY DICTIONARY TO %s" % (owner)
                            cmd = """
                                                            . /home/oracle/.bash_profile
                                                            db
                                                            export ORACLE_SID=%s
                                                            sqlplus / as sysdba << EOF 
                                                            set linesize 1000;
                                                            %s;
                                                            exit;
                                                            EOFF
                                                            """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(str(e))

                            sql = "	GRANT EXECUTE ON DBMS_LOCK to %s" % (owner)
                            cmd = """
                                                             . /home/oracle/.bash_profile
                                                             db
                                                             export ORACLE_SID=%s
                                                             sqlplus / as sysdba << EOF 
                                                             set linesize 1000;
                                                             %s;
                                                             exit;
                                                            EOFF
                                                            """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(str(e))

                            sql = "	select count(1) from dba_objects where object_name='UTL_FILE' and object_type='PACKAGE' and owner='SYS'"
                            cmd = """
                                                                                                 . /home/oracle/.bash_profile
                                                                                                 db
                                                                                                 export ORACLE_SID=%s
                                                                                                 sqlplus / as sysdba << EOF 
                                                                                                 set linesize 1000;
                                                                                                 %s;
                                                                                                 exit;
                                                                                                EOFF
                                                                                                """ % (
                                self._instance_name, sql)
                            try:
                                logger.info(cmd)
                                res = server.exec_command(cmd)
                                logger.info(res)
                            except Exception as e:
                                raise wbxexception(str(e))

                        logger.info("************* telegraf restart ************* ")
                        cmd = ". /home/oracle/.bash_profile;sudo service telegraf restart"
                        try:
                            logger.info(cmd)
                            res = server.exec_command(cmd)
                            logger.info(res)
                        except Exception as e:
                            raise wbxexception(str(e))
                        time.sleep(3)
                        cmd = ". /home/oracle/.bash_profile;sudo service telegraf status"
                        try:
                            logger.info(cmd)
                            res = server.exec_command(cmd)
                            logger.info(res.encode('utf8'))
                        except Exception as e:
                            raise wbxexception(str(e))

                        logger.info("************* view telegraf.log ************* ")
                        cmd = "cat /var/log/telegraf/telegraf.log  | tail -10"
                        try:
                            logger.info(cmd)
                            res = server.exec_command(cmd)
                            logger.info(res)
                        except Exception as e:
                            raise wbxexception(str(e))
                    except Exception as e:
                        raise wbxexception(str(e))

            except Exception as e:
                raise wbxexception(str(e))

        except Exception as e:
            self.updateJobStatus(jobid, "FAILED")
            raise wbxexception(str(e))
        self.updateJobStatus(jobid, "SUCCEED")
        return True

    def check_influxdb_data(self):
        flag = self.exec_check()
        num1 = 0
        while not flag and num1 < 4:
            time.sleep(5)
            flag = self.exec_check()
            num1 += 1
        return flag

    def exec_check(self):
        sql = "select * from wbxdb_monitor_odm where time > now() - 5m and db_name = '%s'  " % (self._db_name)
        results = self._influxdb_client.query(sql)
        points = results.get_points()
        for data in points:
            vo = dict(data)
            if self._instance_name in vo['db_inst_name']:
                return True
        return False

