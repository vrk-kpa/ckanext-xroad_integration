import logging
import json
import os.path

from dateutil import relativedelta
from sqlalchemy import and_, not_
from typing import List

import requests
import datetime
import six

from ckan import model
from requests.exceptions import ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ckan.plugins import toolkit
from pprint import pformat

from ckanext.xroad_integration.model import (XRoadError, XRoadStat, XRoadServiceList, XRoadServiceListMember,
                                             XRoadServiceListSubsystem, XRoadServiceListService,
                                             XRoadServiceListSecurityServer, XRoadBatchResult, XRoadDistinctServiceStat,
                                             XRoadHeartbeat)

PUBLIC_ORGANIZATION_CLASSES = ['GOV', 'MUN', 'ORG']
COMPANY_CLASSES = ['COM']

DEFAULT_TIMEOUT = 3  # seconds
DEFAULT_DAYS_TO_FETCH = 1
DEFAULT_LIST_ERRORS_HISTORY_IN_DAYS = 90
DEFAULT_LIST_ERRORS_PAGE_LIMIT = 20


# Add default timeout
class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super(TimeoutHTTPAdapter, self).__init__(*args, **kwargs)


retry_strategy = Retry(
    total=3,
    backoff_factor=1
)

adapter = TimeoutHTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("http://", adapter)

log = logging.getLogger(__name__)


class ContentFetchError(Exception):
    pass


def update_xroad_organizations(context, data_dict):
    toolkit.check_access('update_xroad_organizations', context)
    harvest_source_list = toolkit.get_action('harvest_source_list')
    organization_list = toolkit.get_action('organization_list')
    organization_show = toolkit.get_action('organization_show')
    organization_patch = toolkit.get_action('organization_patch')

    harvest_sources = harvest_source_list(context, {})
    organization_names = organization_list(context, {})
    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    harvest_source_error_limit = 10

    errors_by_source = {}

    updated = 0

    for harvest_source in harvest_sources:
        if harvest_source.get('type') != 'xroad':
            continue

        source_config = json.loads(harvest_source.get('config', '{}'))
        source_url = harvest_source.get('url')
        source_title = harvest_source.get('title')

        if source_config.get('disable_xroad_organization_updates') is True:
            log.info("XRoad organization updates disabled for %s, skipping...", source_title)
            continue

        errors = []

        for organization_name in organization_names:
            organization = organization_show(context, {'id': organization_name})

            if not organization.get('xroad_membercode'):
                continue

            last_updated = organization.get('metadata_updated_from_xroad_timestamp') or '2011-01-01T00:00:00'
            try:
                patch = _prepare_xroad_organization_patch(organization, source_url, last_updated)

                if patch is not None:
                    log.debug('Updating organization %s data from %s', organization_name, source_title)
                    patch['metadata_updated_from_xroad_timestamp'] = timestamp
                    try:
                        organization_patch(context, patch)
                        updated += 1
                    except toolkit.ValidationError:
                        log.debug('Validation error updating %s from %s: %s', organization_name, source_title, pformat(patch))

                else:
                    log.debug('Nothing to do for %s from %s', organization_name, source_title)
            except ContentFetchError as cfe:
                errors.append(', '.join(repr(a) for a in cfe.args))

                if len(errors) > harvest_source_error_limit:
                    break

        if errors:
            errors_by_source[source_title] = list(set(errors))

    if errors_by_source:
        return {'success': False, 'message': json.dumps(errors_by_source)}
    else:
        return {'success': True, 'message': 'Updated {} organizations'.format(updated)}


def _prepare_xroad_organization_patch(organization, source_url, last_updated):
    member_class = organization.get('xroad_memberclass')
    member_code = organization.get('xroad_membercode')
    organization_name = organization.get('name')

    if not member_code:
        log.info('Organization %s has no X-Road member code, skipping...', organization_name)
        return None

    organization_dict = {'id': organization['id']}

    if member_class in PUBLIC_ORGANIZATION_CLASSES:
        try:
            if 'organization_guid' in organization:
                organization_changed = not last_updated or \
                                       _get_organization_changes(source_url, organization.get('organization_guid'),
                                                                 last_updated)

                if not organization_changed:
                    log.info('No changes to organization %s since last update at %s, skipping...',
                             organization_name, last_updated)
                    return None

            org_information_list = _get_organization_information(source_url, member_code)

            organization_info = None

            if not org_information_list:
                return None

            else:
                # If PTV has only one matching organization
                if type(org_information_list) is dict:
                    log.info("Only one matching organization in ptv for organization %s" % organization_name)
                    organization_info = org_information_list
                else:
                    log.info("Multiple organizations found in ptv for organization %s" % organization_name)
                    # Match only if PTV title matches our organization title
                    organization_info = _parse_organization_info(org_information_list, organization_name)

                if not organization_info:
                    log.warn('Could not parse organization information for %s', organization_name)
                    return None
                else:
                    log.info("Parsing organization information for %s" % organization_name)

                    if organization_info.get('organizationNames', {}):
                        org_names = _convert_xroad_value_to_uniform_list(
                            organization_info.get('organizationNames', {}).get('organizationName', {}))

                        org_names_translated = {
                            "fi": next((name.get('value', '')
                                        for name in org_names if (name.get('language') == 'fi')
                                        and name.get('type') == "Name"), ""),
                            "sv": next((name.get('value', '')
                                        for name in org_names if (name.get('language') == 'sv')
                                        and name.get('type') == "Name"), ""),
                            "en": next((name.get('value', '')
                                        for name in org_names if (name.get('language') == 'en')
                                        and name.get('type') == "Name"), ""),
                        }

                        organization_dict['title_translated'] = org_names_translated

                    if organization_info.get('organizationDescriptions', {}):
                        org_descriptions = _convert_xroad_value_to_uniform_list(
                            organization_info.get('organizationDescriptions', {}).get('organizationDescription', {}))

                        org_descriptions_translated = {
                            "fi": next((description.get('value', '')
                                        for description in org_descriptions
                                        if description.get('language') == 'fi'), ""),
                            "sv": next((description.get('value', '')
                                        for description in org_descriptions
                                        if description.get('language') == 'sv'), ""),
                            "en": next((description.get('value', '')
                                        for description in org_descriptions
                                        if description.get('language') == 'en'), "")
                        }

                        organization_dict['description_translated'] = org_descriptions_translated

                    if organization_info.get('webPages', {}):
                        webpages = _convert_xroad_value_to_uniform_list(
                            organization_info.get('webPages', {}).get('webPage', {}))

                        webpage_addresses = {
                            "fi": next((webpage.get('url', '')
                                        for webpage in webpages
                                        if webpage.get('language') == 'fi'), ""),
                            "sv": next((webpage.get('url', '')
                                        for webpage in webpages
                                        if webpage.get('language') == 'sv'), ""),
                            "en": next((webpage.get('url', '')
                                        for webpage in webpages
                                        if webpage.get('language') == 'en'), "")
                        }

                        webpage_descriptions = {
                            "fi": next((webpage.get('value', '')
                                        for webpage in webpages
                                        if webpage.get('language') == 'fi'), ""),
                            "sv": next((webpage.get('value', '')
                                        for webpage in webpages
                                        if webpage.get('language') == 'sv'), ""),
                            "en": next((webpage.get('value', '')
                                        for webpage in webpages
                                        if webpage.get('language') == 'en'), "")
                        }

                        organization_dict['webpage_address'] = webpage_addresses
                        organization_dict['webpage_description'] = webpage_descriptions

                    emails_field = organization_info.get('emails', {})
                    if emails_field:
                        email_data = _convert_xroad_value_to_uniform_list(emails_field.get('email'))
                        if email_data:
                            languages = set(item['language'] for item in email_data)
                            emails = {lang: [item['value']
                                             for item in email_data
                                             if 'value' in item and item.get('language') == lang]
                                      for lang in languages}
                            if emails:
                                organization_dict['email_address'] = emails

                        organization_dict['organization_guid'] = organization_info.get('guid', '')

        except Exception:
            log.warn("Failed to fetch organization information with id %s", member_code)
            raise

    elif member_class in COMPANY_CLASSES:
        try:
            company_changed = not last_updated or _get_company_changes(source_url, member_code, last_updated)

            if not company_changed:
                log.debug('No changes to company %s since last update at %s, skipping...', organization_name, last_updated)
                return None

            company = _get_companies_information(source_url, member_code)

            if company is None:
                log.warn('Received empty company information')
                return None

            if type(company) is dict:
                if company.get('companyForms'):
                    company_forms = _convert_xroad_value_to_uniform_list(company.get('companyForms', {})
                                                                         .get('companyForm', {}))
                    forms = {
                        "fi": next((form.get('name')
                                    for form in company_forms
                                    if form.get('language') == 'FI'), ""),
                        "sv": next((form.get('name')
                                    for form in company_forms
                                    if form.get('language') == 'SE'), ""),
                        "en": next((form.get('name')
                                    for form in company_forms
                                    if form.get('language') == 'EN'), "")
                    }

                    organization_dict['company_type'] = forms

                if company.get('businessAddresses'):
                    business_addresses = _convert_xroad_value_to_uniform_list(company.get('businessAddresses', {})
                                                                              .get('businessAddress', {}))

                    business_address = business_addresses[0] if business_addresses else None

                    # TODO: language should be country
                    organization_dict['postal_address'] = \
                        business_address.get('street') + ', ' \
                        + str(business_address.get('postCode')) + ', ' \
                        + business_address.get('city') + ', ' + business_address.get('language')

                if company.get('languages'):
                    languages = _convert_xroad_value_to_uniform_list(company.get('languages', {}).get('language', {}))
                    company_languages = {
                        "fi": next((language.get('name', '')
                                    for language in languages
                                    if language.get('language') == 'FI'), ""),
                        "sv": next((language.get('name', '')
                                    for language in languages
                                    if language.get('language') == 'SE'), ""),
                        "en": next((language.get('name', '')
                                    for language in languages
                                    if language.get('language') == 'EN'), "")
                    }

                    organization_dict['company_language'] = company_languages

                # Convert "2001-06-11T00:00:00.000+03:00" to "2001-06-11T00:00:00"
                organization_dict['company_registration_date'] = company.get('registrationDate', '').split(".")[0]

                if company.get('businessIdChanges'):
                    business_id_changes = _convert_xroad_value_to_uniform_list(
                        company.get('businessIdChanges', {}).get('businessIdChange', {}))

                    old_business_ids = [str(business_id_change.get('oldBusinessId'))
                                        for business_id_change in business_id_changes]
                    organization_dict['old_business_ids'] = json.dumps(old_business_ids)

        except Exception:
            log.warn("Failed to fetch company information with id %s", member_code)
            raise

    else:
        log.debug('Skipping %s because of class %s', organization_name, member_class)
        return None

    return organization_dict


def _get_organization_information(url, business_code):
    try:
        r = http.get(url + '/Consumer/GetOrganizations', params={'businessCode': business_code},
                     headers={'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.info(response_json.get("error").get("string"))
            raise ContentFetchError(response_json.get("error").get("string"))

        if response_json.get('organizationList', {}).get('organization') is dict:
            return [response_json['organizationList']['organization']]

        return response_json.get('organizationList', {}).get('organization')
    except ConnectionError:
        log.error("Calling XRoad service GetOrganizations failed")
        raise ContentFetchError("Calling XRoad service GetOrganizations failed")


def _parse_organization_info(data, organization_name):
    organization_info = next((org_info for org_info in data for name in
                              _convert_xroad_value_to_uniform_list(org_info.get('organizationNames', {})
                                                                   .get('organizationName'))
                              if name.get('value', {}) == organization_name), None)

    return organization_info


def _get_companies_information(url, business_id):
    try:
        r = http.get(url + '/Consumer/GetCompanies', params={'businessId': business_id},
                     headers={'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.warn(response_json.get("error").get("string"))
            raise ContentFetchError(response_json.get("error").get("string"))

        return response_json.get('companyList', {}).get('company')
    except ConnectionError:
        log.error("Calling XRoad service GetCompanies failed")
        raise ContentFetchError("Calling XRoad service GetCompanies failed")


def _get_organization_changes(url, guid, changed_after):
    try:
        r = http.get(url + '/Consumer/HasOrganizationChanged', params={'guid': guid, 'changedAfter': changed_after},
                     headers={'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.warn(response_json.get("error").get("string"))
            raise ContentFetchError(response_json.get("error").get("string"))

        return r.json()
    except ConnectionError:
        log.error("Calling XRoad service HasOrganizationChanged failed")
        raise ContentFetchError("Calling XRoad service HasOrganizationChanged failed")


def _get_company_changes(url, business_id, changed_after):
    try:
        r = http.get(url + '/Consumer/HasCompanyChanged', params={'businessId': business_id,
                                                                  'changedAfter': changed_after},
                     headers={'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.warn(response_json.get("error").get("string"))
            raise ContentFetchError(response_json.get("error").get("string"))

        return r.json()
    except ConnectionError:
        log.error("Calling XRoad service HasCompanyChanged failed")
        raise ContentFetchError("Calling XRoad service HasCompanyChanged failed")


def _convert_xroad_value_to_uniform_list(value):
    if isinstance(value, six.string_types):
        return []

    if type(value) is dict:
        return [value]

    return value


def fetch_xroad_errors(context, data_dict):
    toolkit.check_access('fetch_xroad_errors', context)
    results = []
    errors = []
    harvest_sources = xroad_harvest_sources(context)

    for harvest_source in harvest_sources:
        source_title = harvest_source.get('title', '')
        source_url = harvest_source.get('url', '')

        if data_dict.get('since'):
            last_fetched = data_dict.get('since') + "T:00:00:00"
        else:
            last_fetched = XRoadError.get_last_date()
            if last_fetched is None:
                last_fetched = '2016-01-01T00:00:00'
            else:
                last_fetched = last_fetched.strftime('%Y-%m-%dT%H:%M:%S')

        log.info("Fetching errors since %s for %s" % (last_fetched, source_title))

        try:
            r = http.get(source_url + '/Consumer/GetErrors', params={'since': last_fetched},
                         headers={'Accept': 'application/json'})

            error_log = r.json().get('errorLogList', {}).get('errorLog', [])
            for error in error_log:
                mapped_error = {
                    "message": error['message'],
                    "code": error['code'],
                    "created": error['created'],
                    "xroad_instance": error['xRoadInstance'],
                    "member_class": error['memberClass'],
                    "member_code": error['memberCode'],
                    "subsystem_code": error['subsystemCode'],
                    "service_code": error.get('serviceCode', ''),
                    "service_version": error.get('serviceVersion', ''),
                    "server_code": error.get('serverCode', ''),
                    "security_category_code": error.get('securityCategoryCode', ''),
                    "group_code": error.get('groupCode', '')
                }

                XRoadError.create(**mapped_error)

            results.append({"message": "%d errors stored to database." % len(error_log)})
        except ConnectionError:
            log.warn("Calling GetErrors failed for %s!", source_title)
            errors.append("Calling GetErrors failed for %s!" % source_title)

    if errors:
        return {"success": False, "message": ", ".join(errors)}
    else:
        return {"success": True, "results": results, "message": 'Fetched errors for {} harvest sources'.format(len(results))}


def xroad_catalog_query(service, params='', content_type='application/json', accept='application/json', pagination=None):
    xroad_catalog_address = toolkit.config.get('ckanext.xroad_integration.xroad_catalog_address', '')  # type: str
    xroad_catalog_certificate = toolkit.config.get('ckanext.xroad_integration.xroad_catalog_certificate')
    xroad_client_id = toolkit.config.get('ckanext.xroad_integration.xroad_client_id')
    xroad_client_certificate = toolkit.config.get('ckanext.xroad_integration.xroad_client_certificate')

    if not xroad_catalog_address.startswith('http'):
        log.warn("Invalid X-Road catalog url %s" % xroad_catalog_address)
        raise ContentFetchError("Invalid X-Road catalog url %s" % xroad_catalog_address)

    url = '{address}/{service}'.format(address=xroad_catalog_address, service=service)
    for param in params:
        url += '/' + param

    headers = {'Accept': accept,
               'Content-Type': content_type,
               'X-Road-Client': xroad_client_id}

    certificate_args = {}
    if xroad_catalog_certificate and os.path.isfile(xroad_catalog_certificate):
        certificate_args['verify'] = xroad_catalog_certificate
    else:
        certificate_args['verify'] = False

    if xroad_client_certificate and os.path.isfile(xroad_client_certificate):
        certificate_args['cert'] = xroad_client_certificate

    return http.get(url, headers=headers, **certificate_args)


def fetch_xroad_service_list(context, data_dict):
    toolkit.check_access('fetch_xroad_service_list', context)
    days = data_dict.get('days', DEFAULT_DAYS_TO_FETCH)

    log.info("Fetching X-Road services for the last %s days" % days)

    try:
        service_list_data = xroad_catalog_query('getListOfServices', [str(days)]).json()
    except ConnectionError:
        log.warn("Connection error calling getListOfServices")
        return {'success': False, 'message': 'Connection error calling getListOfServices'}

    if service_list_data is None:
        log.warn('Invalid configuration for calling getListOfServices')
        return {'success': False, 'message': 'Invalid configuration for calling getListOfServices'}
    elif 'memberData' not in service_list_data:
        print(service_list_data)
        return {'success': False, 'message': 'Calling getListOfServices returned message: "{}"'.format(service_list_data.get('message', ''))}

    for member_list_data in service_list_data.get('memberData', []):
        fetch_timestamp = parse_xroad_catalog_datetime(member_list_data.get('date'))
        instances = set(m['xroadInstance'] for m in member_list_data['memberDataList'] if m.get('xroadInstance'))
        default_instance = next(iter(instances)) if len(instances) == 1 else None
        service_list = XRoadServiceList.create(fetch_timestamp)

        for security_server_data in service_list_data.get('securityServerData', []):
            instance = security_server_data.get('xroadInstance', default_instance)
            member_class = security_server_data.get('memberClass')
            member_code = security_server_data.get('memberCode')
            server_code = security_server_data.get('serverCode')
            address = security_server_data.get('address')

            if not all((instance, member_class, member_code, server_code, address)):
                log.warn('Security server %s.%s (%s) is missing required information, skipping.',
                         member_class, member_code, server_code)
                continue

            XRoadServiceListSecurityServer.create(service_list.id, instance, member_class, member_code,
                                                  server_code, address)

        for member_data in member_list_data.get('memberDataList', []):
            created = parse_xroad_catalog_datetime(member_data.get('created'))
            instance = member_data.get('xroadInstance', default_instance)
            member_class = member_data.get('memberClass')
            member_code = member_data.get('memberCode')
            name = member_data.get('name')
            is_provider = member_data.get('provider')

            if not all((instance, member_class, member_code, created, name)):
                log.warn('Member %s.%s is missing required information, skipping.', member_class, member_code)
                continue

            member = XRoadServiceListMember.create(service_list.id, created, instance, member_class,
                                                   member_code, name, is_provider)

            for subsystem_data in member_data.get('subsystemList', []):
                created = parse_xroad_catalog_datetime(subsystem_data.get('created'))
                subsystem_code = subsystem_data.get('subsystemCode')

                if not all((created, subsystem_code)):
                    log.warn('Subsystem %s.%s.%s is missing required information, skipping.', member_class,
                             member_code, subsystem_code)
                    continue

                subsystem = XRoadServiceListSubsystem.create(member.id, created, subsystem_code)

                for service_data in subsystem_data.get('serviceList', []):
                    created = parse_xroad_catalog_datetime(service_data.get('created'))
                    service_code = service_data.get('serviceCode')
                    service_version = service_data.get('serviceVersion')
                    active = service_data.get('active')

                    if not all((created, service_code)):
                        log.warn('Service %s.%s.%s.%s.%s is missing required information, skipping.',
                                 member_class, member_code, subsystem_code, service_code, service_version)
                        continue

                    XRoadServiceListService.create(subsystem.id, created, service_code, service_version, active)

    return {"success": True, "message": "Statistics for %s days stored in database." %
                                        len(service_list_data.get('memberData', []))}


def xroad_service_list(context, data_dict):
    toolkit.check_access('xroad_service_list', context)
    date = data_dict.get('date')
    if date:
        start = datetime.datetime.strptime(date, "%Y-%m-%d")
    else:
        start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    end = start.replace(hour=23, minute=59, second=59)

    service_lists = XRoadServiceList.within_range(start, end)
    results = [sl.as_dict_full() for sl in service_lists]

    # Enrich data with current information
    organization_ids = set(xroad_member_id(m) for sl in results for m in sl['members'])

    organization_titles = {k: json.loads(v) for k, v in (
        model.Session.query(model.Group.id, model.GroupExtra.value)
            .filter(model.Group.id.in_(organization_ids))
            .filter(model.Group.id == model.GroupExtra.group_id)
            .filter(model.GroupExtra.key == 'title_translated')
            .filter(model.GroupExtra.state == 'active')
    ).all()}

    from sqlalchemy import func
    resource_counts = dict((
                               model.Session.query(model.Group.id, func.count(model.Resource.id))
                                   .join(model.Package, model.Package.owner_org == model.Group.id)
                                   .join(model.Resource, model.Resource.package_id == model.Package.id)
                                   .filter(model.Group.id.in_(organization_ids))
                                   .filter(model.Package.state == 'active')
                                   .group_by(model.Group.id)
                           ).all())
    # from pprint import pformat
    # log.info(pformat(organization_titles))
    # log.info(pformat(resource_counts))

    # Remove unnecessary data from response
    for sl in results:
        del sl['id']
        for ss in sl['security_servers']:
            del ss['id']
            del ss['xroad_service_list_id']
        for m in sl['members']:
            member_id = xroad_member_id(m)
            m['resource_count'] = resource_counts.get(member_id, 0)
            m['title'] = organization_titles.get(member_id, {'fi': m['name']})
            del m['id']
            del m['xroad_service_list_id']
            for ss in m['subsystems']:
                del ss['id']
                del ss['xroad_service_list_member_id']
                for sr in ss['services']:
                    del sr['id']
                    del sr['xroad_service_list_subsystem_id']

    return results


def parse_xroad_catalog_datetime(dt):
    if not dt:
        return dt
    if type(dt) is dict:
        return datetime.datetime(dt['year'], dt['monthValue'], dt['dayOfMonth'], dt['hour'], dt['minute'], dt['second'])
    elif type(dt) is list:
        return datetime.datetime(dt[0], dt[1], dt[2], dt[3], dt[4], dt[5])
    else:
        return None


def xroad_error_list(context, data_dict):
    toolkit.check_access('xroad_error_list', context, data_dict)

    date = data_dict.get('date')
    if date:
        start = datetime.datetime.strptime(date, "%Y-%m-%d")
    else:
        start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    end = start.replace(hour=23, minute=59, second=59)
    date_start = start
    date_end = start
    show_history = False

    rest_services_failed_errors = model.Session.query(XRoadError) \
        .filter(XRoadError.message.like("Fetch of REST services failed%")) \
        .filter(and_(XRoadError.created >= start), (XRoadError.created <= end))

    other_errors = model.Session.query(XRoadError) \
        .filter(not_(XRoadError.message.like("Fetch of REST services failed%"))) \
        .filter(and_(XRoadError.created >= start), (XRoadError.created <= end))

    list_errors = model.Session.query(XRoadError) \
        .filter(and_(XRoadError.created >= start), (XRoadError.created <= end))

    page = 0
    if "page" in data_dict and data_dict.get('page') is not None:
        page = int(data_dict.get('page'))

    organization_id = data_dict.get('organization')
    if organization_id:
        try:
            toolkit.get_action('organization_show')({}, {"id": organization_id})
        except toolkit.ObjectNotFound:
            raise toolkit.ObjectNotFound(toolkit._(u"Organization not found"))

        data_dict['organization'] = organization_id
        xroad_id = organization_id.split('.')
        if len(xroad_id) == 3:  # Valid xroad id
            rest_services_failed_errors = rest_services_failed_errors.filter(XRoadError.xroad_instance == xroad_id[0]) \
                .filter(XRoadError.member_class == xroad_id[1]) \
                .filter(XRoadError.member_code == xroad_id[2])

            other_errors = other_errors.filter(XRoadError.xroad_instance == xroad_id[0]) \
                .filter(XRoadError.member_class == xroad_id[1]) \
                .filter(XRoadError.member_code == xroad_id[2])

            list_errors = list_errors.filter(XRoadError.xroad_instance == xroad_id[0]) \
                .filter(XRoadError.member_class == xroad_id[1]) \
                .filter(XRoadError.member_code == xroad_id[2])

            date_start = start - datetime.timedelta(days=DEFAULT_LIST_ERRORS_HISTORY_IN_DAYS)
            date_end = start
            show_history = True
        else:
            raise toolkit.Invalid(toolkit._(u"Organization id is not valid X-Road id"))

    rest_services_failed_errors = rest_services_failed_errors.all()
    other_errors = other_errors.all()

    max_pages = 1
    if list_errors.count() > DEFAULT_LIST_ERRORS_PAGE_LIMIT:
        max_pages = int(list_errors.count() / DEFAULT_LIST_ERRORS_PAGE_LIMIT)
    previous_page = (page - 1) if (page > 0) else 0
    next_page = (page + 1) if (page < max_pages) else max_pages
    page += 1

    return {
        "rest_services_failed_errors": [error.as_dict() for error in rest_services_failed_errors],
        "list_errors": [error.as_dict() for error in list_errors],
        "other_errors": [error.as_dict() for error in other_errors],
        "date": start,
        "date_start": date_start,
        "date_end": date_end,
        "show_history": show_history,
        "previous": (start - relativedelta.relativedelta(days=1)).date(),
        "next": (start + relativedelta.relativedelta(days=1)).date(),
        "organization": organization_id,
        "previous_page": previous_page,
        "next_page": next_page
    }


def fetch_xroad_stats(context, data_dict):
    toolkit.check_access('fetch_xroad_stats', context)

    days = data_dict.get('days', DEFAULT_DAYS_TO_FETCH)

    log.info("Fetching X-Road stats for the last %s days" % days)

    try:
        statistics_data = xroad_catalog_query('getServiceStatistics', [str(days)]).json()

        if statistics_data is None:
            log.warn("Calling getServiceStatistics failed!")
            return {'success': False, 'message': 'Calling getServiceStatistics failed!'}
        elif 'serviceStatisticsList' not in statistics_data:
            return {'success': False, 'message': 'Calling getServiceStatistics returned message: "{}"'.format(statistics_data.get('message', ''))}

        statistics_list = statistics_data.get('serviceStatisticsList', [])

        for statistics in statistics_list:
            created = statistics['created']
            date = parse_xroad_catalog_datetime(created).replace(hour=0, minute=0, second=0, microsecond=0)

            stat = XRoadStat.get_by_date(date)
            if stat:
                stat.soap_service_count = statistics['numberOfSoapServices']
                stat.rest_service_count = statistics['numberOfRestServices']
                stat.openapi_service_count = statistics['numberOfOpenApiServices']
                XRoadStat.save(stat)
            else:
                XRoadStat.create(date, statistics['numberOfSoapServices'], statistics['numberOfRestServices'],
                                 statistics['numberOfOpenApiServices'])

        return {"success": True, "message": "Statistics for %s days stored in database." % len(statistics_list)}

    except ConnectionError as e:
        log.warn("Calling getServiceStatistics failed!")
        log.info(e)
        return {"success": False, "message": "Fetching statistics failed."}


def fetch_distinct_service_stats(context, data_dict):
    toolkit.check_access('fetch_distinct_service_stats', context)

    days = data_dict.get('days', DEFAULT_DAYS_TO_FETCH)

    log.info("Fetching X-Road distinct service stats for the last %s days" % days)

    try:
        statistics_data = xroad_catalog_query('getDistinctServiceStatistics', [str(days)]).json()

        if statistics_data is None:
            log.warn("Calling getDistinctServiceStatistics failed!")
            return {'success': False, 'message': 'Calling getDistinctServiceStatistics failed!'}
        elif 'distinctServiceStatisticsList' not in statistics_data:
            return {'success': False, 'message': 'Calling getDistinctServiceStatistics returned message: "{}"'.format(statistics_data.get('message', ''))}

        statistics_list = statistics_data.get('distinctServiceStatisticsList', [])

        for statistics in statistics_list:
            created = statistics['created']
            date = parse_xroad_catalog_datetime(created).replace(hour=0, minute=0, second=0, microsecond=0)

            stat = XRoadDistinctServiceStat.get_by_date(date)
            if stat:
                stat.distinct_service_count = statistics['numberOfDistinctServices']
                XRoadDistinctServiceStat.save(stat)
            else:
                XRoadDistinctServiceStat.create(date, statistics['numberOfDistinctServices'])

        return {"success": True, "message": "Distinct service statistics for %s days stored in database." % len(statistics_list)}

    except ConnectionError as e:
        log.warn("Calling getDistinctServiceStatistics failed!")
        log.info(e)
        return {"success": False, "message": "Fetching distinct service statistics failed."}


def xroad_stats(context, data_dict):
    toolkit.check_access('xroad_stats', context)

    stats = model.Session.query(XRoadStat).order_by(XRoadStat.date.desc()).all()

    return [stat.as_dict() for stat in stats]


def xroad_distinct_service_stats(context, data_dict):
    toolkit.check_access('xroad_distinct_service_stats', context)

    stats = model.Session.query(XRoadDistinctServiceStat).order_by(XRoadDistinctServiceStat.date.desc()).all()

    return [stat.as_dict() for stat in stats]


def xroad_batch_result_create(context, data_dict):
    toolkit.check_access('xroad_batch_result', context)
    XRoadBatchResult.create(data_dict['service'], data_dict['success'], params=data_dict.get('params'), message=data_dict.get('message'))
    return {'success': True}


def xroad_latest_batch_results(context, data_dict):
    toolkit.check_access('xroad_batch_result', context)
    results = XRoadBatchResult.get_latest_entry_for_each_service()
    return {'success': True, 'results': [r.as_dict() for r in results]}


def xroad_harvest_sources(context):
    harvest_sources = toolkit.get_action('harvest_source_list')(context, {})  # type: List[dict]

    for harvest_source in harvest_sources:
        if harvest_source.get('type') != 'xroad':
            continue

        source_url = harvest_source.get('url', '')
        if not source_url.startswith('http'):
            log.warn("Invalid source url %s" % source_url)
            continue

        yield harvest_source


def xroad_member_id(member_dict):
    xroad_instance = member_dict.get('instance')
    member_class = member_dict.get('member_class')
    member_code = member_dict.get('member_code')
    return '.'.join((xroad_instance, member_class, member_code))


def fetch_xroad_heartbeat(context, data_dict):
    toolkit.check_access('fetch_xroad_heartbeat', context)
    log.info('Checking X-Road catalog heartbeat')

    xroad_catalog_address = toolkit.config.get('ckanext.xroad_integration.xroad_catalog_address', '')  # type: str
    xroad_catalog_certificate = toolkit.config.get('ckanext.xroad_integration.xroad_catalog_certificate')
    xroad_client_id = toolkit.config.get('ckanext.xroad_integration.xroad_client_id')
    xroad_client_certificate = toolkit.config.get('ckanext.xroad_integration.xroad_client_certificate')

    if not xroad_catalog_address.startswith('http'):
        return False

    service = 'heartbeat'
    url = '{address}/{service}'.format(address=xroad_catalog_address, service=service)

    headers = {'X-Road-Client': xroad_client_id}

    certificate_args = {}
    if xroad_catalog_certificate and os.path.isfile(xroad_catalog_certificate):
        certificate_args['verify'] = xroad_catalog_certificate
    else:
        certificate_args['verify'] = False

    if xroad_client_certificate and os.path.isfile(xroad_client_certificate):
        certificate_args['cert'] = xroad_client_certificate

    try:
        response = http.get(url, headers=headers, **certificate_args)
        result = response.status_code == 200
    except Exception:
        result = False

    log.info('X-Road catalog is %s', 'UP' if result else 'DOWN')
    XRoadHeartbeat.create(result)

    return {'success': True, 'heartbeat': result}


def xroad_heartbeat(context, data_dict):
    toolkit.check_access('xroad_heartbeat', context)
    heartbeat = XRoadHeartbeat.get_latest().as_dict()
    return {'success': True, 'heartbeat': heartbeat}


def xroad_heartbeat_history(context, data_dict):
    toolkit.check_access('xroad_heartbeat', context)

    def parse_datetime_or_now(s):
        if s is None:
            return datetime.datetime.now()
        else:
            return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')

    since = parse_datetime_or_now(data_dict.get('since', '1900-01-01T00:00:00'))
    until = parse_datetime_or_now(data_dict.get('until'))
    items = [i.as_dict() for i in XRoadHeartbeat.get_between(since, until)]
    return {'success': True, 'items': items}
