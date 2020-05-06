
def xroad_subsystem_path(dataset_dict):
    field_names = ('xroad_instance', 'xroad_member_code', 'xroad_subsystem_code')
    fields = [dataset_dict.get(field) for field in field_names]
    return '.'.join(fields) if all(fields) else None
