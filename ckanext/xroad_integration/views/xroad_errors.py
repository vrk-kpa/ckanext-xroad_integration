from flask import Blueprint

import ckan.model as model
import logging
from ckan.plugins.toolkit import render, check_access, NotAuthorized, abort, _, g

log = logging.getLogger(__name__)

xroad_errors = Blueprint(u'xroad_error', __name__, url_prefix=u'/admin/xroad_errors')

def index():
    try:
        context = dict(model=model, user=g.user, auth_user_obj=g.userobj)
        check_access(u'sysadmin', context)
    except NotAuthorized:
        abort(403, _(u'Need to be system administrator to administer'))

    return render('admin/xroad_errors.html')

xroad_errors.add_url_rule(u'/', view_func=index, strict_slashes=False)

def get_blueprints():
    return [xroad_errors]