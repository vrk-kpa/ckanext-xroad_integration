from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime

from .xroad_types_utils import Base, optional, date_value, class_value, xroad_list_value, xroad_service_version_value


@dataclass
class ServiceDescription(Base):
    field_map = {'externalId': 'external_id'}
    value_map = {
            'created': date_value,
            'changed': optional(date_value),
            'fetched': optional(date_value),
            'removed': optional(date_value),
            }
    external_id: str
    created: datetime
    changed: Optional[datetime] = field(default=None)
    fetched: Optional[datetime] = field(default=None)
    removed: Optional[datetime] = field(default=None)
    data: Optional[str] = field(default=None)


@dataclass
class Service(Base):
    field_map = {'serviceCode': 'service_code',
                 'serviceVersion': 'service_version'}
    value_map = {
            'service_version': xroad_service_version_value,
            'wsdl': optional(class_value(ServiceDescription)),
            'openapi': optional(class_value(ServiceDescription)),
            'created': date_value,
            'changed': optional(date_value),
            'fetched': optional(date_value),
            'removed': optional(date_value),
            }
    service_code: str
    created: datetime
    service_version: Optional[str] = field(default=None)
    service_type: Optional[str] = field(default=None)
    wsdl: Optional[ServiceDescription] = field(default=None)
    openapi: Optional[ServiceDescription] = field(default=None)
    changed: Optional[datetime] = field(default=None)
    fetched: Optional[datetime] = field(default=None)
    removed: Optional[datetime] = field(default=None)


@dataclass
class Subsystem(Base):
    field_map = {'subsystemCode': 'subsystem_code'}
    value_map = {
            'services': xroad_list_value('service', Service),
            'created': date_value,
            'changed': optional(date_value),
            'fetched': optional(date_value),
            'removed': optional(date_value),
            }
    subsystem_code: str
    created: datetime
    changed: Optional[datetime] = field(default=None)
    fetched: Optional[datetime] = field(default=None)
    removed: Optional[datetime] = field(default=None)
    services: List[Service] = field(default_factory=list)


@dataclass
class Member(Base):
    field_map = {'memberCode': 'member_code',
                 'memberClass': 'member_class',
                 'xRoadInstance': 'instance'}
    value_map = {
            'subsystems': xroad_list_value('subsystem', Subsystem),
            'created': date_value,
            'changed': optional(date_value),
            'fetched': optional(date_value),
            'removed': optional(date_value),
            }
    instance: str
    member_code: str
    member_class: str
    name: str
    created: datetime
    changed: Optional[datetime] = field(default=None)
    fetched: Optional[datetime] = field(default=None)
    removed: Optional[datetime] = field(default=None)
    member_type: Optional[str] = field(default=None)
    subsystems: List[Subsystem] = field(default_factory=list)


@dataclass
class MemberList(Base):
    field_map = {'memberList': 'members'}
    value_map = {'members': xroad_list_value('member', Member)}
    members: List[Member] = field(default_factory=list)
