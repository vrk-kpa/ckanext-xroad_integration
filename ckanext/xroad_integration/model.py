import uuid
import logging

from ckan import model
from ckan.lib import dictization
from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
log = logging.getLogger(__name__)

def make_uuid():
    return unicode(uuid.uuid4())

class XRoadError(Base):

    __tablename__ = 'xroad_errors'

    id = Column(types.UnicodeText, primary_key=True, default=make_uuid)
    message = Column(types.UnicodeText, nullable=False)
    code = Column(types.Integer, nullable=False)
    created = Column(types.DateTime, nullable=False)

    @classmethod
    def create(cls, message, code, created):
        xroad_error = XRoadError(message=message, code=code, created=created)
        model.Session.add(xroad_error)
        model.repo.commit()

    def as_dict(self):

        context = {'model': model}
        error_dict = dictization.table_dictize(self, context)

        return error_dict

    @classmethod
    def get_last_date(cls):
        last = model.Session.query(XRoadError).order_by(XRoadError.created.desc()).first()
        if last:
            return last.created
        else:
            return None

class XRoadStat(Base):

    __tablename__ = 'xroad_stats'

    id = Column(types.UnicodeText, primary_key=True, default=make_uuid)
    date = Column(types.DateTime, nullable=False)
    soap_service_count = Column(types.Integer, nullable=False)
    rest_service_count = Column(types.Integer, nullable=False)
    distinct_service_count = Column(types.Integer, nullable=False)
    unknown_service_count = Column(types.Integer, nullable=False)


    @classmethod
    def create(cls, date, soap_service_count, rest_service_count, distinct_service_count, unknown_service_count):
        xroad_stat = XRoadStat(date=date, soap_service_count=soap_service_count, rest_service_count=rest_service_count,
                               distinct_service_count=distinct_service_count,
                               unknown_service_count=unknown_service_count)

        model.Session.add(xroad_stat)
        model.repo.commit()

    def as_dict(self):
        context = {'model': model}
        stat_dict = dictization.table_dictize(self, context)

        return stat_dict

    @classmethod
    def get_by_date(cls, date):
        stat = model.Session.query(XRoadStat).filter(cls.date == date).one_or_none()
        return stat

    @classmethod
    def save(cls, obj):
        model.Session.add(obj)
        model.repo.commit()

def init_table(engine):
    Base.metadata.create_all(engine)
    log.info("Table for xroad errors is set-up")