
def xroad_subsystem_path(dataset_dict):
    field_names = ('xroad_instance', 'xroad_memberclass', 'xroad_membercode', 'xroad_subsystemcode')
    fields = [dataset_dict.get(field) for field in field_names]
    return '.'.join(fields) if all(fields) else None
