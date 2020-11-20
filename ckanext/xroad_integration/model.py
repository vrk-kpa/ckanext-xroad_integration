import uuid
import logging

from ckan import model
from ckan.lib import dictization
from sqlalchemy import Column, ForeignKey, types, and_
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
log = logging.getLogger(__name__)


def make_uuid():
    return unicode(uuid.uuid4())


class AsDictMixin:
    def as_dict(self):
        context = {'model': model}
        result = dictization.table_dictize(self, context)

        return result


class XRoadError(Base, AsDictMixin):

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

    @classmethod
    def get_last_date(cls):
        last = model.Session.query(XRoadError).order_by(XRoadError.created.desc()).first()
        if last:
            return last.created
        else:
            return None


class XRoadStat(Base, AsDictMixin):

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

    @classmethod
    def get_by_date(cls, date):
        stat = model.Session.query(XRoadStat).filter(cls.date == date).one_or_none()
        return stat

    @classmethod
    def save(cls, obj):
        model.Session.add(obj)
        model.repo.commit()


class XRoadServiceList(Base, AsDictMixin):
    __tablename__ = 'xroad_service_lists'

    id = Column(types.Integer, primary_key=True)
    timestamp = Column(types.DateTime, nullable=False)

    @classmethod
    def create(cls, timestamp):
        service_list = XRoadServiceList(timestamp=timestamp)
        model.Session.add(service_list)
        model.repo.commit()
        return service_list

    @classmethod
    def within_range(cls, start, end):
        return (model.Session.query(XRoadServiceList)
                .filter(and_(XRoadServiceList.timestamp > start),
                            (XRoadServiceList.timestamp < end)).all())

    def security_servers(self):
        return (model.Session.query(XRoadServiceListSecurityServer)
                .filter(XRoadServiceListSecurityServer.xroad_service_list_id == self.id)
                .all())

    def members(self):
        return (model.Session.query(XRoadServiceListMember)
                .filter(XRoadServiceListMember.xroad_service_list_id == self.id)
                .all())

    def as_dict_full(self):
        service_list = self.as_dict()
        service_list['security_servers'] = [ss.as_dict() for ss in self.security_servers()]
        service_list['members'] = [m.as_dict_full() for m in self.members()]
        return service_list


class XRoadServiceListSecurityServer(Base, AsDictMixin):
    __tablename__ = 'xroad_service_list_security_servers'

    id = Column(types.Integer, primary_key=True)
    xroad_service_list_id = Column(types.Integer, ForeignKey("xroad_service_lists.id"), nullable=False)
    instance = Column(types.Unicode, nullable=False)
    member_class = Column(types.Unicode, nullable=False)
    member_code = Column(types.Unicode, nullable=False)
    server_code = Column(types.Unicode, nullable=False)
    address = Column(types.Unicode, nullable=False)

    @classmethod
    def create(cls, xroad_service_list_id, instance, member_class, member_code, server_code, address):
        security_server = XRoadServiceListSecurityServer(
                xroad_service_list_id=xroad_service_list_id,
                instance=instance,
                member_class=member_class,
                member_code=member_code,
                server_code=server_code,
                address=address)
        model.Session.add(security_server)
        model.repo.commit()
        return security_server


class XRoadServiceListMember(Base, AsDictMixin):
    __tablename__ = 'xroad_service_list_members'

    id = Column(types.Integer, primary_key=True)
    xroad_service_list_id = Column(types.Integer, ForeignKey("xroad_service_lists.id"), nullable=False)
    created = Column(types.DateTime, nullable=False)
    instance = Column(types.Unicode, nullable=False)
    member_class = Column(types.Unicode, nullable=False)
    member_code = Column(types.Unicode, nullable=False)
    name = Column(types.Unicode, nullable=False)

    @classmethod
    def create(cls, xroad_service_list_id, created, instance, member_class, member_code, name):
        member = XRoadServiceListMember(
                xroad_service_list_id=xroad_service_list_id,
                created=created,
                instance=instance,
                member_class=member_class,
                member_code=member_code,
                name=name)
        model.Session.add(member)
        model.repo.commit()
        return member

    def subsystems(self):
        return (model.Session.query(XRoadServiceListSubsystem)
                .filter(XRoadServiceListSubsystem.xroad_service_list_member_id == self.id)
                .all())

    def as_dict_full(self):
        member = self.as_dict()
        member['subsystems'] = [s.as_dict_full() for s in self.subsystems()]
        return member


class XRoadServiceListSubsystem(Base, AsDictMixin):
    __tablename__ = 'xroad_service_list_subsystems'

    id = Column(types.Integer, primary_key=True)
    xroad_service_list_member_id = Column(types.Integer, ForeignKey("xroad_service_list_members.id"), nullable=False)
    created = Column(types.DateTime, nullable=False)
    subsystem_code = Column(types.Unicode, nullable=False)

    @classmethod
    def create(cls, xroad_service_list_member_id, created, subsystem_code):
        subsystem = XRoadServiceListSubsystem(
                xroad_service_list_member_id=xroad_service_list_member_id,
                created=created,
                subsystem_code=subsystem_code)
        model.Session.add(subsystem)
        model.repo.commit()
        return subsystem

    def services(self):
        return (model.Session.query(XRoadServiceListService)
                .filter(XRoadServiceListService.xroad_service_list_subsystem_id == self.id)
                .all())

    def as_dict_full(self):
        subsystem = self.as_dict()
        subsystem['services'] = [s.as_dict() for s in self.services()]
        return subsystem


class XRoadServiceListService(Base, AsDictMixin):
    __tablename__ = 'xroad_service_list_services'

    id = Column(types.Integer, primary_key=True)
    xroad_service_list_subsystem_id = Column(types.Integer, ForeignKey("xroad_service_list_subsystems.id"), nullable=False)
    created = Column(types.DateTime, nullable=False)
    service_code = Column(types.Unicode, nullable=False)
    service_version = Column(types.Unicode, nullable=True)

    @classmethod
    def create(cls, xroad_service_list_subsystem_id, created, service_code, service_version):
        service = XRoadServiceListService(
                xroad_service_list_subsystem_id=xroad_service_list_subsystem_id,
                created=created,
                service_code=service_code,
                service_version=service_version)
        model.Session.add(service)
        model.repo.commit()
        return service


def init_table(engine):
    Base.metadata.create_all(engine)
    log.info("Table for xroad data is set-up")
