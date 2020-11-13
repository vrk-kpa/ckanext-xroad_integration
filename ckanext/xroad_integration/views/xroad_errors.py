from flask import Blueprint

import ckan.model as model
from ckan.plugins.toolkit import render, check_access, NotAuthorized, abort, _, g, get_action

xroad_errors = Blueprint(u'xroad_error', __name__, url_prefix=u'/ckan-admin/xroad_errors')

def index(date=None):
    try:
        context = dict(model=model, user=g.user, auth_user_obj=g.userobj)
        check_access(u'sysadmin', context)
    except NotAuthorized:
        abort(403, _(u'Need to be system administrator to administer'))

    error_list = get_action('xroad_error_list')({}, {"date": date})

    return render('admin/xroad_errors.html', extra_vars={"error_list": error_list})

xroad_errors.add_url_rule(u'/', view_func=index, strict_slashes=False)
xroad_errors.add_url_rule(u'/<date>', view_func=index, strict_slashes=False)

def get_blueprints():
    return [xroad_errors]