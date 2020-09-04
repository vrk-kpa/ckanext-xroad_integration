import logging
import json
import requests
import datetime
from requests.exceptions import ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ckan.plugins import toolkit
from pprint import pformat

PUBLIC_ORGANIZATION_CLASSES = ['GOV', 'MUN', 'ORG']
COMPANY_CLASSES = ['COM']

DEFAULT_TIMEOUT = 3  # seconds

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


def update_xroad_organizations(context, data_dict):
    harvest_source_list = toolkit.get_action('harvest_source_list')
    organization_list = toolkit.get_action('organization_list')
    organization_show = toolkit.get_action('organization_show')
    organization_patch = toolkit.get_action('organization_patch')

    harvest_sources = harvest_source_list(context, {})
    organization_names = organization_list(context, {})
    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    for harvest_source in harvest_sources:
        if harvest_source.get('type') != 'xroad':
            continue

        source_url = harvest_source.get('url')
        source_title = harvest_source.get('title')

        for organization_name in organization_names:
            organization = organization_show(context, {'id': organization_name})

            if not organization.get('xroad_membercode'):
                continue

            last_updated = organization.get('metadata_updated_from_xroad_timestamp') or '2011-01-01T00:00:00'
            patch = _prepare_xroad_organization_patch(organization, source_url, last_updated)
            if patch is not None:
                log.debug('Updating organization %s data from %s', organization_name, source_title)
                patch['metadata_updated_from_xroad_timestamp'] = timestamp
                try:
                    organization_patch(context, patch)
                except toolkit.ValidationError:
                    log.debug('Validation error updating %s from %s: %s', organization_name, source_title, pformat(patch))

            else:
                log.debug('Nothing to do for %s from %s', organization_name, source_title)


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
            organization_changed = not last_updated or _get_organization_changes(source_url, member_code, last_updated)

            if not organization_changed:
                log.debug('No changes to organization %s since last update at %s, skipping...', organization_name, last_updated)
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
                    return None
                else:
                    log.info("Parsing organization information for %s" % organization_name)

                    if organization_info.get('organizationNames', {}):
                        org_names = _convert_xroad_value_to_uniform_list(
                                organization_info.get('organizationNames', {}).get('organizationName', {}))

                        org_names_translated = {
                                "fi": next((name.get('value', '') for name in org_names if (name.get('language') == 'fi') and name.get('type') == "Name"), ""),
                                "sv": next((name.get('value', '') for name in org_names if (name.get('language') == 'sv') and name.get('type') == "Name"), ""),
                                "en": next((name.get('value', '') for name in org_names if (name.get('language') == 'en') and name.get('type') == "Name"), ""),
                                }

                        organization_dict['title_translated'] = org_names_translated

                    if organization_info.get('organizationDescriptions', {}):
                        org_descriptions = _convert_xroad_value_to_uniform_list(
                                organization_info.get('organizationDescriptions', {}).get('organizationDescription', {}))

                        org_descriptions_translated = {
                                "fi": next((description.get('value', '') for description in org_descriptions if description.get('language') == 'fi'), ""),
                                "sv": next((description.get('value', '') for description in org_descriptions if description.get('language') == 'sv'), ""),
                                "en": next((description.get('value', '') for description in org_descriptions if description.get('language') == 'en'), "")
                                }

                        organization_dict['description_translated'] = org_descriptions_translated

                    if organization_info.get('webPages', {}):
                        webpages = _convert_xroad_value_to_uniform_list(
                                organization_info.get('webPages', {}).get('webPage', {}))

                        webpage_addresses = {
                                "fi": next((webpage.get('url', '') for webpage in webpages if webpage.get('language') == 'fi'), ""),
                                "sv": next((webpage.get('url', '') for webpage in webpages if webpage.get('language') == 'sv'), ""),
                                "en": next((webpage.get('url', '') for webpage in webpages if webpage.get('language') == 'en'), "")
                                }

                        webpage_descriptions = {
                                "fi": next((webpage.get('value', '') for webpage in webpages if webpage.get('language') == 'fi'), ""),
                                "sv": next((webpage.get('value', '') for webpage in webpages if webpage.get('language') == 'sv'), ""),
                                "en": next((webpage.get('value', '') for webpage in webpages if webpage.get('language') == 'en'), "")
                                }


                        organization_dict['webpage_address'] = webpage_addresses
                        organization_dict['webpage_description'] = webpage_descriptions

                    email_data = organization_info.get('emails', {}).get('email')
                    if email_data:
                        emails = {item['language']: item['value']
                                  for item in email_data
                                  if all(field in item for field in ['language', 'value'])}
                        if emails:
                            organization_dict['email_address'] = emails

                    organization_dict['organization_guid'] = organization_info.get('guid', '')


        except:
            log.warn("Failed to fetch organization information with id %s", member_code)

    elif member_class in COMPANY_CLASSES:
        try:
            company_changed = not last_updated or _get_company_changes(source_url, member_code, last_updated)

            if not company_changed:
                log.debug('No changes to company %s since last update at %s, skipping...', organization_name, last_updated)
                return None

            company = _get_companies_information(source_url, member_code)

            if not company:
                return None

            if type(company) is dict:
                if company.get('companyForms'):
                    company_forms = _convert_xroad_value_to_uniform_list(company.get('companyForms', {}).get('companyForm', {}))
                    forms = {
                            "fi": next((form.get('name') for form in company_forms if form.get('language') == 'FI'), ""),
                            "sv": next((form.get('name') for form in company_forms if form.get('language') == 'SE'), ""),
                            "en": next((form.get('name') for form in company_forms if form.get('language') == 'EN'), "")
                            }

                    organization_dict['company_type'] = forms

                if company.get('businessAddresses'):
                    business_addresses = _convert_xroad_value_to_uniform_list(company.get('businessAddresses', {}).get('businessAddress', {}))

                    business_address = business_addresses[0] if business_addresses else None

                    # TODO: language should be country
                    organization_dict['postal_address'] = business_address.get('street') + ', ' + str(business_address.get('postCode'))\
                            + ', ' + business_address.get('city') + ', ' + business_address.get('language')

                if company.get('languages'):
                    languages = _convert_xroad_value_to_uniform_list(company.get('languages', {}).get('language', {}))
                    company_languages = {
                            "fi": next((language.get('name', '') for language in languages if language.get('language') == 'FI'), ""),
                            "sv": next((language.get('name', '') for language in languages if language.get('language') == 'SE'), ""),
                            "en": next((language.get('name', '') for language in languages if language.get('language') == 'EN'), "")
                            }

                    organization_dict['company_language'] = company_languages

                # Convert "2001-06-11T00:00:00.000+03:00" to "2001-06-11T00:00:00"
                organization_dict['company_registration_date'] = company.get('registrationDate').split(".")[0]

                if company.get('businessIdChanges'):
                    business_id_changes = _convert_xroad_value_to_uniform_list(
                            company.get('businessIdChanges', {}).get('businessIdChange', {}))

                    old_business_ids = [str(business_id_change.get('oldBusinessId')) for business_id_change in business_id_changes]
                    organization_dict['old_business_ids'] = json.dumps(old_business_ids)

        except:
            log.warn("Failed to fetch company information with id %s", member_code)

    else:
        log.debug('Skipping %s because of class %s', organization_name, member_class)
        return None

    return organization_dict



def _get_organization_information(url, business_code):
    try:
        r = http.get(url + '/Consumer/GetOrganizations', params = {'businessCode': business_code},
                headers = {'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.info(response_json.get("error").get("string"))
            return []

        if response_json.get('organizationList', {}).get('organization') is dict:
            return [response_json['organizationList']['organization']]

        return response_json.get('organizationList', {}).get('organization')
    except ConnectionError:
        log.error("Calling XRoad service GetOrganizations failed")
        return None


def _parse_organization_info(data, organization_name):
    organization_info = next((org_info for org_info in data for name in
        _convert_xroad_value_to_uniform_list(org_info.get('organizationNames', {})
            .get('organizationName'))
        if name.get('value', {}) == organization_name), None)

    return  organization_info


def _get_companies_information(url, business_id):
    try:
        r = http.get(url + '/Consumer/GetCompanies', params = {'businessId': business_id},
                headers = {'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.info(response_json.get("error").get("string"))
            return ""

        return response_json.get('companyList', {}).get('company')
    except ConnectionError:
        log.error("Calling XRoad service GetCompanies failed")
        return None


def _get_organization_changes(url, guid, changed_after):
    try:
        r = http.get(url + '/Consumer/HasOrganizationChanged', params = {'guid': guid, 'changedAfter': changed_after},
                headers = {'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.info(response_json.get("error").get("string"))
            return ""

        return r.json()
    except ConnectionError:
        log.error("Calling XRoad service HasOrganizationChanged failed")
        return None


def _get_company_changes(url, business_id, changed_after ):
    try:
        r = http.get(url + '/Consumer/HasCompanyChanged', params = {'businessId': business_id, 'changedAfter': changed_after},
                headers = {'Accept': 'application/json'})

        response_json = r.json()
        if response_json.get("error"):
            log.info(response_json.get("error").get("string"))
            return ""

        return r.json()
    except ConnectionError:
        log.error("Calling XRoad service HasCompanyChanged failed")
        return None


def _convert_xroad_value_to_uniform_list(value):
    if isinstance(value, basestring):
        return []

    if type(value) is dict:
        return [value]

    return value
