
from ckan.plugins import toolkit

def xroad_subsystem_path(dataset_dict):
    field_names = ('xroad_instance', 'xroad_memberclass', 'xroad_membercode', 'xroad_subsystemcode')
    fields = [dataset_dict.get(field) for field in field_names]
    return '.'.join(fields) if all(fields) else None


def get_xroad_environment() -> str:
    return toolkit.config.get('ckanext.xroad_integration.xroad_environment')


def get_xroad_stats() -> list:
    return toolkit.get_action('xroad_stats')({}, {})


def get_xroad_distinct_services() -> list:
    return toolkit.get_action('xroad_distinct_service_stats')({}, {})