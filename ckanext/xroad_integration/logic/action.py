import logging
import json
import os.path

import iso8601
from dateutil import relativedelta
from sqlalchemy import and_, not_

import datetime
import six

from ckan import model
from requests.exceptions import ConnectionError
from ckan.plugins import toolkit
from pprint import pformat

from ckanext.xroad_integration.model import (XRoadError, XRoadStat, XRoadServiceList, XRoadServiceListMember,
                                             XRoadServiceListSubsystem, XRoadServiceListService,
                                             XRoadServiceListSecurityServer, XRoadBatchResult, XRoadDistinctServiceStat,
                                             XRoadHeartbeat)
from ckanext.xroad_integration.xroad_utils import xroad_catalog_query_json, ContentFetchError, http


# PUBLIC_ORGANIZATION_CLASSES = ['GOV', 'MUN', 'ORG']
# COMPANY_CLASSES = ['COM']

DEFAULT_DAYS_TO_FETCH = 1
DEFAULT_LIST_ERRORS_HISTORY_IN_DAYS = 90
DEFAULT_LIST_ERRORS_PAGE_LIMIT = 20

log = logging.getLogger(__name__)


def update_xroad_organizations(context, data_dict):
    toolkit.check_access('update_xroad_organizations', context)
    harvest_source_list = toolkit.get_action('harvest_source_list')
    organization_list = toolkit.get_action('organization_list')
    organization_show = toolkit.get_action('organization_show')
    organization_patch = toolkit.get_action('organization_patch')

    harvest_sources = harvest_source_list(context, {})
    organization_names = organization_list(context, {})
    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    errors_by_source = {}

    updated = 0

    for harvest_source in harvest_sources:
        if harvest_source.get('type') != 'xroad':
            continue

        source_config = json.loads(harvest_source.get('config', '{}'))
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
                patch = _prepare_xroad_organization_patch(organization, last_updated)

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

        if errors:
            errors_by_source[source_title] = list(set(errors))

    if errors_by_source:
        return {'success': False, 'message': json.dumps(errors_by_source)}
    else:
        return {'success': True, 'message': 'Updated {} organizations'.format(updated)}


def _prepare_xroad_organization_patch(organization, last_updated):
    member_code = organization.get('xroad_membercode')
    organization_name = organization.get('name')

    if not member_code:
        log.info('Organization %s has no X-Road member code, skipping...', organization_name)
        return None

    organization_dict = {'id': organization['id']}

    try:
        if last_updated or _get_organization_changes(member_code, last_updated):
            org_information_list = _get_organization_information(member_code)
        else:
            log.info('No changes to organization %s since last update at %s, skipping...', organization_name, last_updated)
            return None

        if not org_information_list:
            return None
        else:
            if org_information_list.get('organizationData'):
                organization_info = org_information_list.get('organizationData')
                if not organization_info:
                    log.warn('Could not parse organization information for %s', organization_name)
                    return None
                else:
                    log.info("Parsing organization information for %s" % organization_name)

                    if organization_info.get('organizationNames'):
                        org_names = _convert_xroad_value_to_uniform_list(organization_info.get('organizationNames', {}))

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

                    if organization_info.get('organizationDescriptions'):
                        org_descriptions = \
                            _convert_xroad_value_to_uniform_list(organization_info.get('organizationDescriptions', {}))

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

                    if organization_info.get('webPages'):
                        webpages = _convert_xroad_value_to_uniform_list(organization_info.get('webPages', {}))

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

                    if organization_info.get('emails'):
                        email_validator = toolkit.get_validator('email_validator')

                        def is_valid_email(x):
                            try:
                                email_validator(x, {})
                                return True
                            except toolkit.Invalid:
                                log.warning(f'Invalid email address for {member_code}: "{x}"')
                                return False

                        email_data = _convert_xroad_value_to_uniform_list(organization_info.get('emails'))
                        if email_data:
                            languages = ['fi', 'sv', 'en']
                            emails = {lang: next((item['value']
                                                 for item in email_data
                                                 if 'value' in item
                                                 and item.get('language') == lang
                                                 and is_valid_email(item['value'])), '')
                                      for lang in languages}
                            if emails:
                                organization_dict['email_address_translated'] = emails

                    organization_dict['organization_guid'] = organization_info.get('guid', '')
            else:
                company = org_information_list.get('companyData')
                if type(company) is dict:
                    if company.get('companyForms'):
                        company_forms = _convert_xroad_value_to_uniform_list(company.get('companyForms', {}))
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
                        business_addresses = _convert_xroad_value_to_uniform_list(company.get('businessAddresses', {}))
                        business_address = business_addresses[0] if business_addresses else None

                        # TODO: language should be country
                        organization_dict['postal_address'] = \
                            business_address.get('street') + ', ' \
                            + str(business_address.get('postCode')) + ', ' \
                            + business_address.get('city') + ', ' + business_address.get('language')

                    if company.get('languages'):
                        languages = _convert_xroad_value_to_uniform_list(company.get('languages', {}))
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
                    organization_dict['company_registration_date'] = \
                        _convert_xroad_datetime_list_to_datetime(company.get('registrationDate', ''))

                    if company.get('businessIdChanges'):
                        business_id_changes = _convert_xroad_value_to_uniform_list(company.get('businessIdChanges', {}))

                        old_business_ids = [str(business_id_change.get('oldBusinessId'))
                                            for business_id_change in business_id_changes]
                        organization_dict['old_business_ids'] = old_business_ids

    except Exception:
        log.warning("Exception while processing %s (%s)", organization_name, member_code)
        raise

    return organization_dict


def _get_organization_information(business_code):
    try:
        organization_json = xroad_catalog_query_json('getOrganization', params=[business_code])

        if organization_json is None:
            error = "XRoad service getOrganization returned an empty response for member {}".format(business_code)
            log.warning(error)
            raise ContentFetchError(error)
        elif organization_json.get('organizationData') or organization_json.get('companyData'):
            return organization_json
        else:
            return None
    except ConnectionError:
        log.error("Calling XRoad service getOrganization failed")
        raise ContentFetchError("Calling XRoad service getOrganization failed")


def _get_organization_changes(business_code, changed_after):
    try:
        since = datetime.datetime.strptime(changed_after, '%Y-%m-%dT%H:%M:%S').date().strftime('%Y-%m-%d')

        queryparams = {
            'startDate': since,
            'endDate': datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d")
        }

        organization_changes = xroad_catalog_query_json('getOrganizationChanges',
                                                        params=[business_code], queryparams=queryparams)
        return organization_changes.get('changed')

    except ConnectionError:
        log.error("Calling XRoad service getOrganizationChanges failed")
        raise ContentFetchError("Calling XRoad service getOrganizationChanges failed")


def _convert_xroad_value_to_uniform_list(value):
    if isinstance(value, six.string_types):
        return []

    if type(value) is dict:
        return [value]

    return value


def _convert_xroad_datetime_list_to_datetime(value):
    if type(value) is list:
        return "{0}-{1}-{2}T0{3}:0{4}:00".format(value[0], value[1], value[2], value[3], value[4])
    else:
        return None


def string_to_date(date):
    if date:
        return datetime.datetime.strptime(date, '%Y-%m-%d')
    else:
        return None


def date_to_string(date):
    return datetime.datetime.strftime(date, "%Y-%m-%d")


def set_date_range_defaults(start_date: datetime.datetime = None, end_date: datetime.datetime = None,
                            is_range: bool = False):
    """
    If is_range is True, returns current date as default, otherwise yesterday.
    """

    if end_date and not start_date:
        raise ValueError("Please give start date to go with the end date")

    yesterday = datetime.datetime.now() - relativedelta.relativedelta(days=+1)
    start_date = start_date or yesterday

    if is_range is True:
        end_date = end_date or datetime.datetime.now()
    else:
        end_date = end_date or yesterday

    return start_date, end_date


def validate_date_range(start_date, end_date):
    if start_date and start_date > datetime.datetime.now():
        raise ValueError("Start date cannot be in the future")

    if start_date and end_date and start_date > end_date:
        raise ValueError("Start date cannot be later than end date")

    if end_date and end_date > datetime.datetime.now():
        raise ValueError("End date cannot be in the future")


def fetch_xroad_errors(context, data_dict):
    toolkit.check_access('fetch_xroad_errors', context)
    errors = []
    error_count = 0

    start_date = string_to_date(data_dict.get('start_date'))
    end_date = string_to_date(data_dict.get('end_date'))

    try:
        start_date, end_date = set_date_range_defaults(start_date, end_date, is_range=True)
        validate_date_range(start_date, end_date)
    except ValueError as e:
        return {'success': False, 'message': str(e)}

    queryparams = {
        'startDate': date_to_string(start_date),
        'endDate': date_to_string(end_date)
    }

    page = 0
    if "page" in data_dict and data_dict.get('page') is not None:
        page = data_dict.get('page')
    limit = DEFAULT_LIST_ERRORS_PAGE_LIMIT
    if "limit" in data_dict and data_dict.get('limit') is not None:
        limit = data_dict.get('limit')

    log.info("Fetching errors from %s to %s" % (start_date, end_date))

    organizations = toolkit.get_action('organization_list')(context, {})
    try:
        for org_name in organizations:
            organization = toolkit.get_action('organization_show')(context, {'id': org_name})
            if not (organization.get('xroad_instance') and organization.get('xroad_memberclass') and
                    organization.get('xroad_membercode')):
                log.warning('Invalid xroad organization: %s, not fetching errors for it', organization['id'])
                continue

            params = [organization.get('xroad_instance'),
                      organization.get('xroad_memberclass'),
                      organization.get('xroad_membercode')]
            pagination = {"page": str(page), "limit": str(limit)}
            try:
                no_of_pages, added_errors_count = _fetch_error_page(params=params, queryparams=queryparams,
                                                                    pagination=pagination)
                error_count += added_errors_count
            except ValueError:
                return {'success': False, 'message': 'Calling listErrors failed!'}

            for page_no in range(1, no_of_pages):
                try:
                    pagination = {"page": str(page_no), "limit": str(limit)}
                    try:
                        no_of_pages, added_errors_count = _fetch_error_page(params=params, queryparams=queryparams,
                                                                            pagination=pagination)
                        error_count += added_errors_count
                    except ValueError:
                        return {'success': False, 'message': 'Calling listErrors failed!'}

                except ConnectionError as e:
                    log.warning("Calling listErrors failed!")
                    log.info(e)
                    return {"success": False, "message": "Fetching errors failed."}

    except ConnectionError as e:
        log.warning("Calling listErrors failed!")
        log.info(e)
        return {"success": False, "message": "Fetching errors failed."}

    results = {"message": "%d errors stored to database." % error_count}
    if errors:
        return {"success": False, "message": ", ".join(errors)}
    else:
        return {"success": True, "results": results,
                "message": 'Fetched errors for xroad'}


def _fetch_error_page(params, queryparams, pagination) -> (int, int):

    error_count = 0
    error_data = xroad_catalog_query_json('listErrors',
                                          params=params,
                                          queryparams=queryparams,
                                          pagination=pagination)

    if error_data is None:
        return 0, 0

    error_log_list = error_data.get('errorLogList', [])

    for error in error_log_list:
        mapped_error = {
            "message": error.get('message', ''),
            "code": error.get('code', ''),
            "created": parse_xroad_catalog_datetime(error['created']),
            "xroad_instance": error.get('xroadInstance', ''),
            "member_class": error.get('memberClass', ''),
            "member_code": error.get('memberCode', ''),
            "subsystem_code": error.get('subsystemCode', ''),
            "service_code": error.get('serviceCode', ''),
            "service_version": error.get('serviceVersion', ''),
            "server_code": error.get('serverCode', ''),
            "security_category_code": error.get('securityCategoryCode', ''),
            "group_code": error.get('groupCode', ''),
        }
        XRoadError.create(**mapped_error)
        error_count = error_count + 1

    return error_data.get('numberOfPages', 0), error_count


def fetch_xroad_service_list(context, data_dict):
    toolkit.check_access('fetch_xroad_service_list', context)

    start_date = string_to_date(data_dict.get('start_date'))
    end_date = string_to_date(data_dict.get('end_date'))

    try:
        start_date, end_date = set_date_range_defaults(start_date, end_date, is_range=False)
        validate_date_range(start_date, end_date)
    except ValueError as e:
        return {'success': False, 'message': str(e)}

    queryparams = {
        'startDate': date_to_string(start_date),
        'endDate': date_to_string(end_date)
    }

    log.info("Fetching X-Road services from %s to %s" % (queryparams['startDate'], queryparams['endDate']))

    try:
        service_list_data = xroad_catalog_query_json('getListOfServices', queryparams=queryparams)
    except ConnectionError as e:
        log.warning("Connection error calling getListOfServices")
        log.info(e)
        return {'success': False, 'message': 'Connection error calling getListOfServices'}

    if service_list_data is None:
        log.warning('Invalid configuration for calling getListOfServices')
        return {'success': False, 'message': 'Invalid configuration for calling getListOfServices'}
    elif 'memberData' not in service_list_data:
        return {'success': False, 'message': 'Calling getListOfServices returned message: "{}"'
                .format(service_list_data.get('message', ''))}

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
                log.warning('Security server %s.%s (%s) is missing required information, skipping.',
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

    return {"success": True, "message": "Services from %s to %s stored in database."
                                        % (queryparams['startDate'], queryparams['endDate'])}


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

    organization_titles = {
        k: json.loads(v) for k, v in (model.Session.query(model.Group.id, model.GroupExtra.value)
                                      .filter(model.Group.id.in_(organization_ids))
                                      .filter(model.Group.id == model.GroupExtra.group_id)
                                      .filter(model.GroupExtra.key == 'title_translated')
                                      .filter(model.GroupExtra.state == 'active')).all()
    }

    from sqlalchemy import func
    resource_counts = dict((model.Session.query(model.Group.id, func.count(model.Resource.id))
                            .join(model.Package, model.Package.owner_org == model.Group.id)
                            .join(model.Resource, model.Resource.package_id == model.Package.id)
                            .filter(model.Group.id.in_(organization_ids))
                            .filter(model.Package.state == 'active')
                            .group_by(model.Group.id)).all()
                           )

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
    elif type(dt) is str:
        return iso8601.parse_date(dt)
    elif type(dt) is list:
        # Remove microseconds, as they might be too precise
        if len(dt) == 7:
            del dt[6]
        return datetime.datetime(*dt)
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

        else:
            raise toolkit.ValidationError(toolkit._(u"Organization id is not valid X-Road id"))

    rest_services_failed_errors = rest_services_failed_errors.all()
    other_errors = other_errors.all()
    list_errors = list_errors.all()

    max_pages = 1
    if len(list_errors) > DEFAULT_LIST_ERRORS_PAGE_LIMIT:
        max_pages = int(len(list_errors) / DEFAULT_LIST_ERRORS_PAGE_LIMIT)
    previous_page = (page - 1) if (page > 0) else 0
    next_page = (page + 1) if (page < max_pages) else max_pages
    page += 1

    return {
        "rest_services_failed_errors": [error.as_dict() for error in rest_services_failed_errors],
        "list_errors": [error.as_dict() for error in list_errors],
        "other_errors": [error.as_dict() for error in other_errors],
        "date": start.isoformat(),
        "previous": (start - relativedelta.relativedelta(days=1)).date().strftime("%Y-%m-%d"),
        "next": (start + relativedelta.relativedelta(days=1)).date().strftime("%Y-%m-%d"),
        "organization": organization_id,
        "previous_page": previous_page,
        "next_page": next_page
    }


def fetch_xroad_stats(context, data_dict):
    toolkit.check_access('fetch_xroad_stats', context)

    start_date = string_to_date(data_dict.get('start_date'))
    end_date = string_to_date(data_dict.get('end_date'))

    try:
        start_date, end_date = set_date_range_defaults(start_date, end_date, is_range=False)
        validate_date_range(start_date, end_date)
    except ValueError as e:
        return {'success': False, 'message': str(e)}

    queryparams = {
        'startDate': date_to_string(start_date),
        'endDate': date_to_string(end_date)
    }

    log.info("Fetching X-Road stats from %s to %s" % (queryparams['startDate'], queryparams['endDate']))

    try:
        statistics_data = xroad_catalog_query_json('getServiceStatistics', queryparams=queryparams)

        if statistics_data is None:
            log.warning("Calling getServiceStatistics failed!")
            return {'success': False, 'message': 'Calling getServiceStatistics failed!'}
        elif 'serviceStatisticsList' not in statistics_data:
            return {'success': False, 'message': 'Calling getServiceStatistics returned message: "{}"'
                    .format(statistics_data.get('message', ''))}

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

        return {"success": True, "message": "Statistics from %s to %s stored in database." %
                (queryparams['startDate'], queryparams['endDate'])}

    except ConnectionError as e:
        log.warn("Calling getServiceStatistics failed!")
        log.info(e)
        return {"success": False, "message": "Fetching statistics failed."}


def fetch_distinct_service_stats(context, data_dict):
    toolkit.check_access('fetch_distinct_service_stats', context)

    start_date = string_to_date(data_dict.get('start_date'))
    end_date = string_to_date(data_dict.get('end_date'))

    try:
        start_date, end_date = set_date_range_defaults(start_date, end_date, is_range=False)
        validate_date_range(start_date, end_date)
    except ValueError as e:
        return {'success': False, 'message': str(e)}

    queryparams = {
        'startDate': date_to_string(start_date),
        'endDate': date_to_string(end_date)
    }

    log.info("Fetching X-Road distinct service stats from %s to %s" % (queryparams['startDate'], queryparams['endDate']))

    try:
        statistics_data = xroad_catalog_query_json('getDistinctServiceStatistics', queryparams=queryparams)

        if statistics_data is None:
            log.warning("Calling getDistinctServiceStatistics failed!")
            return {'success': False, 'message': 'Calling getDistinctServiceStatistics failed!'}
        elif 'distinctServiceStatisticsList' not in statistics_data:
            return {'success': False, 'message': 'Calling getDistinctServiceStatistics returned message: "{}"'
                    .format(statistics_data.get('message', ''))}

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

        return {"success": True, "message": "Distinct service statistics from %s to %s stored in database."
                                            % (queryparams['startDate'], queryparams['endDate'])}

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
    XRoadBatchResult.create(data_dict['service'], data_dict['success'], params=data_dict.get('params'),
                            message=data_dict.get('message'))
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
    heartbeat = XRoadHeartbeat.get_latest()
    if heartbeat is None:
        return {'success': False, 'heartbeat': None}
    else:
        return {'success': True, 'heartbeat': heartbeat.as_dict()}


def xroad_heartbeat_history(context, data_dict):
    toolkit.check_access('xroad_heartbeat', context)

    def parse_datetime_or_now(s):
        if isinstance(s, datetime.datetime):
            return s
        else:
            return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')

    since = parse_datetime_or_now(data_dict.get('since', datetime.datetime.min))
    until = parse_datetime_or_now(data_dict.get('until', datetime.datetime.now()))
    items = [i.as_dict() for i in XRoadHeartbeat.get_between(since, until)]
    return {'success': True, 'items': items}
