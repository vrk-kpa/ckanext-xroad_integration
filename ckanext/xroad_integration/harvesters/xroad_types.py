from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime

from .xroad_types_utils import (Base, optional, date_value, class_value, xroad_list_value,
                                xroad_service_version_value, class_list_value)


@dataclass
class Error(Base):
    actor: str
    code: str
    string: str
    detail: str


@dataclass
class RestServiceEndpoint(Base):
    method: str
    path: str


@dataclass
class RestService(Base):
    field_map = {'endpointList': 'endpoints',
                 'xroadInstance': 'instance',
                 'memberClass': 'member_class',
                 'memberCode': 'member_code',
                 'subsystemCode': 'subsystem_code',
                 'serviceCode': 'service_code',
                 'serviceVersion': 'service_version'}
    value_map = {'endpoints': class_list_value(RestServiceEndpoint)}
    endpoints: List[RestServiceEndpoint]
    instance: str
    member_class: str
    member_code: str
    subsystem_code: str
    service_code: str
    service_version: str


@dataclass
class RestServices(Base):
    field_map = {'listOfServices': 'services'}
    value_map = {'services': class_list_value(RestService)}
    services: List[RestService]


@dataclass
class ServiceDescription(Base):
    field_map = {'externalId': 'external_id'}
    value_map = {
            'created': date_value,
            'changed': date_value,
            'fetched': date_value,
            'removed': optional(date_value),
            }
    external_id: str
    created: datetime
    changed: datetime
    fetched: datetime
    removed: Optional[datetime] = field(default=None)
    data: Optional[str] = field(default=None)


@dataclass
class Service(Base):
    field_map = {'serviceCode': 'service_code',
                 'serviceVersion': 'service_version',
                 'serviceType': 'service_type'}
    value_map = {
            'service_version': xroad_service_version_value,
            'wsdl': optional(class_value(ServiceDescription)),
            'openapi': optional(class_value(ServiceDescription)),
            'created': date_value,
            'changed': date_value,
            'fetched': date_value,
            'removed': optional(date_value),
            }
    service_code: str
    created: datetime
    changed: datetime
    fetched: datetime
    service_version: Optional[str] = field(default=None)
    service_type: Optional[str] = field(default=None)
    wsdl: Optional[ServiceDescription] = field(default=None)
    openapi: Optional[ServiceDescription] = field(default=None)
    removed: Optional[datetime] = field(default=None)
    rest_services: Optional[RestServices] = field(default=None)


@dataclass
class Subsystem(Base):
    field_map = {'subsystemCode': 'subsystem_code'}
    value_map = {
            'services': xroad_list_value('service', Service),
            'created': date_value,
            'changed': date_value,
            'fetched': date_value,
            'removed': optional(date_value),
            }
    subsystem_code: str
    created: datetime
    changed: datetime
    fetched: datetime
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
            'changed': date_value,
            'fetched': date_value,
            'removed': optional(date_value),
            }
    instance: str
    member_code: str
    member_class: str
    name: str
    created: datetime
    changed: datetime
    fetched: datetime
    removed: Optional[datetime] = field(default=None)
    member_type: Optional[str] = field(default=None)
    subsystems: List[Subsystem] = field(default_factory=list)


@dataclass
class MemberList(Base):
    field_map = {'memberList': 'members'}
    value_map = {'members': xroad_list_value('member', Member)}
    members: List[Member] = field(default_factory=list)
    error: Optional[Error] = field(default=None)
