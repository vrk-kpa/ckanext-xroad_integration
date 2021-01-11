from ckan.plugins.toolkit import check_access, NotAuthorized, _


def xroad_error_list(context, data_dict):

    if not data_dict.get('organization'):
        return check_access('sysadmin', context)
    else:
        try:
            check_access('organization_update', context, {"id": data_dict.get('organization')})
            return {"success": True}
        except NotAuthorized:
            return {"success": False, "msg": _(u"User not authorized to view X-Road error list for organization")}
