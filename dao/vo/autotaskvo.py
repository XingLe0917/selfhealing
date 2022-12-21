from sqlalchemy import Column,Integer,String, DateTime, func
from dao.vo.wbxvo import Base

class wbxautotaskvo(Base):
    __tablename__ = "wbxautotask"
    taskid = Column(String(64), primary_key=True)
    task_type = Column(String(32))
    parameter = Column(String(64))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())
    self_heal = Column(String(8))

class wbxautotaskjobvo(Base):
    __tablename__ = "wbxautotaskjob"
    jobid = Column(String(64), primary_key=True)
    taskid = Column(String(64))
    db_name = Column(String(30))
    host_name = Column(String(30))
    splex_port = Column(Integer)
    job_action = Column(String(30))
    execute_method = Column(String(30))
    processorder = Column(Integer, default=1)
    parameter = Column(String(64))
    status = Column(String(16))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    resultmsg1 = Column(String)
    resultmsg2 = Column(String)
    resultmsg3 = Column(String)
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())