from sqlalchemy import Column,String, DateTime, func
from dao.vo.wbxvo import Base

class wbxloginuser(Base):
    __tablename__ = "host_user_info"
    host_name = Column(String(30), primary_key=True)
    trim_host = Column(String(30))
    username = Column(String(30))
    pwd = Column(String(64))
    createtime = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())
