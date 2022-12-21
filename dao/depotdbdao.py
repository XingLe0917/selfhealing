import json
import logging

from dao.wbxdao import wbxdao
from dao.vo.autotaskvo import wbxautotaskjobvo

logger = logging.getLogger("DBAMONITOR")

class DepotDBDao(wbxdao):
    def __init__(self):
        super().__init__()

    def getServerInfo(self):
        session = self.getLocalSession()
        SQL = ''' 
             select h.host_name, h.username, f_get_deencrypt(h.pwd) pwd ,hi.ssh_port
            from host_user_info h,host_info hi ,database_info di, instance_info ii
            where h.host_name = hi.host_name
            AND di.trim_host=ii.trim_host                           
            AND di.db_name=ii.db_name
            AND ii.host_name=hi.host_name
            and di.db_type = 'PROD'
    '''
        rows = session.execute(SQL).fetchall()
        return [dict(row) for row in rows]

    def getDBInfo(self):
        session = self.getLocalSession()
        SQL = '''
             with tmp as (
            select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3
            from database_info di ,instance_info ii,host_info hi
            where di.db_name= ii.db_name
            and di.trim_host = ii.trim_host
            and ii.host_name = hi.host_name
            and (di.db_type in ('PROD'))
            and hi.scan_ip1 is not null
            order by di.db_name,trim_host
            )
            select * from (
            select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
            ) where rn = 1 order by db_name
            '''
        rows = session.execute(SQL).fetchall()
        return [dict(row) for row in rows]

    def getAutoTaskJobByJobid(self, jobid):
        session = self.getLocalSession()
        return session.query(wbxautotaskjobvo).filter(wbxautotaskjobvo.jobid == jobid).first()

    def getAutoTaskJob(self, task_types,row_count):
        session = self.getLocalSession()
        sql = '''select taskid, priority, task_type, attemptcount, fixtime  from (
            select t1.taskid,t1.priority,t1.task_type, t2.attemptcount, t2.fixtime
            from wbxautotask t1,wbxmonitoralert2 t2
            where t1.taskid = t2.autotaskid
            and t2.status in ('PENDING','FAILED','UNSOLVE')
            and t2.first_alert_time > sysdate - 1
            --and t2.autotaskid = '5d32e455ca6943d785c7e8b4a37f7a2d'
            and (t2.attemptcount = 0 or nvl(t2.fixtime, sysdate) < sysdate-nvl(t2.attemptcount,1) * 10/60/24 )
            '''
        sql += " and t1.task_type in ("
        for index, task_type in enumerate(task_types):
            sql += "'" + task_type + "'"
            if index != len(task_types) - 1:
                sql += ","
        sql += ") "
        sql += " order by t1.priority,t2.first_alert_time) where rownum <= %s" % row_count
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def getAutoTaskByTaskid(self, taskid):
        sql = '''
            select taskid,task_type,priority,parameter from wbxautotask where taskid= '%s'
            ''' % (taskid)
        session = self.getLocalSession()
        rows = session.execute(sql)
        return [dict(row) for row in rows]

    def getAutoTaskJobByTaskid(self, taskid):
        session = self.getLocalSession()
        sql = '''
            select jobid,processorder,job_action,db_name,host_name,splex_port,status, start_time,end_time,resultmsg1,resultmsg2,resultmsg3 
            from wbxautotaskjob 
            where taskid= '%s' order by processorder
            ''' % (taskid)
        rows = session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def updateWbxmonitoralert2Status(self, taskid, status, attemptcount=0):
        session = self.getLocalSession()
        SQL = '''update wbxmonitoralert2 set status= '%s' where autotaskid = '%s' 
                ''' % (status, taskid)
        if "RUNNING" == status:
            SQL = '''update wbxmonitoralert2 set status= '%s',fixtime=sysdate,attemptcount=nvl(attemptcount,0)+1 
                     where autotaskid = '%s' 
                     and status !='RUNNING'
                     and attemptcount=%s
                ''' % (status, taskid, attemptcount)
        iresult = session.execute(SQL)
        return iresult.rowcount
