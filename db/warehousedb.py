from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean, event
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timedelta
import json
import os

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

Base = declarative_base()

class ChaturbateUserActivity(Base):
    __tablename__ = "ChaturbateUserActivity"

    id = Column(Integer, primary_key=True)
    chaturbate_user_id = Column(ForeignKey('ChaturbateUser.id'))
    minutes_used = Column(Integer, nullable=False)
    bn2_task_id = Column(Integer, nullable=False)
    bn2_job_id = Column(Integer, nullable=False)
    was_targeted = Column(Boolean, nullable=False)
    time_ended = Column(DateTime, nullable=False)

    def __init__(self, payload):
        for key in payload.keys():
            setattr(self, key, payload[key])

class ChaturbateUser (Base):
    __tablename__ = "ChaturbateUser"

    id = Column(Integer, primary_key=True)
    username = Column(String(40), nullable=False, unique=True)
    password = Column(String(40), nullable=False)
    browser_session = Column(Text, default='{}')
    email = Column(String(50), nullable=True, unique=True)
    created_time = Column(DateTime, nullable=False)
    in_use = Column(Boolean, default=False)
    last_used = Column(DateTime, nullable=False)


    def __init__(self, payload):
        for key in payload.keys():
            setattr(self, key, payload[key])

        self.created_time = datetime.utcnow()
        self.last_used = self.created_time



