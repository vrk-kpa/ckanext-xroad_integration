from flask import Blueprint

xroad_errors = Blueprint(u'xroad_error', __name__, url_prefix=u'/xroad_errors')

def index():

    return render('admin/xroad_errors.html')

xroad_errors.add_url_rule(u'/', view_func=index)

def get_blueprints():
    return [xroad_errors]