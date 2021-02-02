from flask import Blueprint

import ckan.model as model
from ckan.plugins.toolkit import render, check_access, NotAuthorized, abort, _, g, get_action, ObjectNotFound, Invalid
from datetime import datetime
from flask import request, make_response
import logging
import csv

try:
    from ckanext.apicatalog_ui.plugin import get_translated
except ImportError:
    from ckan.lib.helpers import get_translated

xroad = Blueprint(u'xroad', __name__, url_prefix=u'/ckan-admin/xroad')
log = logging.getLogger(__name__)


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

    # Sort by date, pick first data point, last data point and the first data point of each month, then crop to last 24
    xroad_stats_sorted = sorted(xroad_stats, key=lambda d: d.get('date'))

    data_format = request.args.get('format')

    if data_format is not None and data_format.lower() == 'csv':
        response = make_response()
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'inline; filename="xroad_stats.csv"'
        fieldnames = ['date', 'soap_service_count', 'rest_service_count', 'distinct_service_count', 'unknown_service_count']
        writer = csv.DictWriter(response.stream, fieldnames=fieldnames)
        writer.writeheader()

        for stat in xroad_stats:
            row = {'date': stat.get('date'),
                   'soap_service_count': stat.get('soap_service_count'),
                   'rest_service_count': stat.get('rest_service_count'),
                   'distinct_service_count': stat.get('distinct_service_count'),
                   'unknown_service_count': stat.get('unknown_service_count')
                   }
            writer.writerow(row)

        return response
    else:
        xroad_stats_service_graph_data = [d for i, d in enumerate(xroad_stats_sorted)
                                          if i == 0 or i == len(xroad_stats_sorted) - 1
                                          or d.get('date', '')[:7] != xroad_stats_sorted[i - 1].get('date', '')[:7]][-24:]

        return render('/admin/xroad_stats.html', extra_vars={'xroad_stats': xroad_stats,
                                                             'xroad_stats_service_graph_data':
                                                                 xroad_stats_service_graph_data})


xroad.add_url_rule(u'/stats', view_func=stats)


def services():
    try:
        context = dict(model=model, user=g.user, auth_user_obj=g.userobj)
        check_access(u'sysadmin', context)
    except NotAuthorized:
        abort(403, _(u'Need to be system administrator to administer'))

    date = request.args.get('date')
    data_format = request.args.get('format')

    xroad_services = get_action('xroad_service_list')({}, {'date': date})
    latest = max(iter(xroad_services), key=lambda x: x['timestamp']) if xroad_services else None
    members = latest['members'] if latest else []

    for member in members:
        security_servers = [ss for ss in latest['security_servers']
                            if all(ss[key] == member[key] for key in ('instance', 'member_class', 'member_code'))]
        member['security_servers'] = security_servers

    date_format = '%Y-%m-%d'
    selected_date = datetime.now()

    if date is not None:
        try:
            selected_date = datetime.strptime(date, date_format)
        except ValueError:
            log.warn('Invalid date format: %s', date)

    if data_format is not None and data_format.lower() == 'csv':
        response = make_response()
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'inline; filename="xroad_services_{}.csv"'.format(
            selected_date.strftime(date_format))
        fieldnames = ['member', 'member_name', 'member_type', 'security_servers', 'subsystem', 'service', 'active', 'created']
        writer = csv.DictWriter(response.stream, fieldnames=fieldnames)
        writer.writeheader()
        for member in (latest or {}).get('members', []):
            for subsystem in member.get('subsystems', []):
                for service in subsystem.get('services'):
                    member_string = '.'.join([member.get(f, '') for f in ('instance', 'member_class', 'member_code')])
                    if service.get('service_version'):
                        service_string = '.'.join([service.get(f, '') for f in ('service_code', 'service_version')])
                    else:
                        service_string = service.get('service_code')

                    security_servers_string = '\n'.join('{} <{}>'.format(ss.get('server_code', ''), ss.get('address'))
                                                        for ss in member.get('security_servers', []))

                    row = {'member': member_string.encode('utf-8'),
                           'member_name': get_translated(member, 'title').encode('utf-8'),
                           'member_type': 'provider' if member.get('resource_count', 0) > 0 else 'consumer',
                           'security_servers': security_servers_string.encode('utf-8'),
                           'subsystem': subsystem.get('subsystem_code', '').encode('utf-8'),
                           'service': service_string.encode('utf-8'),
                           'active': 'Yes' if service.get('active') else 'No',
                           'created': service.get('created', '')
                           }

                    writer.writerow(row)

        return response

    else:
        return render('/admin/xroad_services.html',
                      extra_vars={'service_list': latest, 'selected_date': selected_date.strftime(date_format)})


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
