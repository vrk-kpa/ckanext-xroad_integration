from flask import Blueprint

import ckan.model as model
from ckan.plugins.toolkit import render, check_access, NotAuthorized, abort, _, g, get_action, ObjectNotFound, Invalid

xroad = Blueprint(u'xroad', __name__, url_prefix=u'/ckan-admin/xroad')


def errors(date=None):
    try:
        context = dict(model=model, user=g.user, auth_user_obj=g.userobj)
        check_access(u'sysadmin', context)
    except NotAuthorized:
        abort(403, _(u'Need to be system administrator to administer'))

    error_list = get_action('xroad_error_list')({}, {"date": date})

    return render('admin/xroad_errors.html', extra_vars={"error_list": error_list})


xroad.add_url_rule(u'/errors/<date>', view_func=errors, strict_slashes=False)
xroad.add_url_rule(u'/errors', view_func=errors, strict_slashes=False)


def stats():
    try:
        context = dict(model=model, user=g.user, auth_user_obj=g.userobj)
        check_access(u'sysadmin', context)
    except NotAuthorized:
        abort(403, _(u'Need to be system administrator to administer'))

    xroad_stats = get_action('xroad_stats')({}, {})

    return render('/admin/xroad_stats.html', extra_vars={'xroad_stats': xroad_stats})


xroad.add_url_rule(u'/stats', view_func=stats)


def services():
    try:
        context = dict(model=model, user=g.user, auth_user_obj=g.userobj)
        check_access(u'sysadmin', context)
    except NotAuthorized:
        abort(403, _(u'Need to be system administrator to administer'))

    xroad_services = get_action('xroad_service_list')({}, {})
    latest = max(iter(xroad_services), key=lambda x: x['timestamp']) if xroad_services else None
    members = latest['members'] if latest else []

    for member in members:
        security_servers = [ss for ss in latest['security_servers']
                            if all(ss[key] == member[key] for key in ('instance', 'member_class', 'member_code'))]
        member['security_servers'] = security_servers

    return render('/admin/xroad_services.html', extra_vars={'service_list': latest})


xroad.add_url_rule(u'/services', view_func=services)


xroad_organization = Blueprint(u'xroad_organization', __name__, url_prefix=u'/organization/xroad')

def organization_errors(organization, date=None):
    try:
        context = dict(model=model, user=g.user, auth_user_obj=g.userobj)
        check_access(u'organization_update', context, {'id': organization})
    except NotAuthorized:
        abort(403, _(u'Need to be organization administrator to administer'))

    try:
        error_list = get_action('xroad_error_list')({}, {"date": date, "organization": organization})
    except ObjectNotFound:
        abort(404, _(u'Organization not found'))
    except Invalid as e:
        abort(404, e.error)


    return render('organization/xroad_errors.html', extra_vars={"error_list": error_list})


xroad_organization.add_url_rule(u'/<organization>/errors/<date>', view_func=organization_errors, strict_slashes=False)
xroad_organization.add_url_rule(u'/<organization>/errors', view_func=organization_errors, strict_slashes=False)

def get_blueprints():
    return [xroad, xroad_organization]
