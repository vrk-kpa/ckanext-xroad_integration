from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
from sqlalchemy import text
import ckan.plugins as p
from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents
from datetime import datetime
from cgi import FieldStorage

import logging
import json
import requests
from requests.exceptions import ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
import tempfile
from sqlalchemy import exists
import lxml.etree as etree
import six
from ckan import model

from ckan import logic

try:
    from ckan.common import asbool  # CKAN 2.9
except ImportError:
    from paste.deploy.converters import asbool

from typing import Union


NotFound = logic.NotFound

log = logging.getLogger(__name__)

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


class XRoadHarvesterPlugin(HarvesterBase):
    config = None

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):

        return {
            "name": "xroad",
            "title": "X-Road Rest Gateway",
            "description": "Server that provides Rest Gateway for X-Road. "
                           "Valid config keys: force_all, force_organization_update, force_resource_update, since"
        }

    def validate_config(self, config):
        if not config:
            return config

        config_obj = json.loads(config)
        for key in ('force_all', 'force_organization_update', 'force_resource_update'):
            if key in config_obj:
                if not isinstance(config_obj[key], bool):
                    raise ValueError('%s must be boolean' % key)
        if 'since' in config_obj:
            try:
                datetime.strptime(config_obj['since'], '%Y-%m-%d')
            except ValueError:
                raise ValueError("%s must be in format YYYY-MM-DD" % 'since')

        return config

    def gather_stage(self, harvest_job):
        log.debug('In xroad harvester gather_stage')

        self._set_config(harvest_job.source.config)

        if self.config.get('force_all', False) is True:
            last_time = "2011-01-01"
        elif self.config.get('since'):
            last_time = self.config.get('since')
        else:
            last_time = self._last_error_free_job_time(harvest_job) or "2011-01-01"

        log.info('Searching for apis modified since: %s UTC',
                 last_time)
        try:
            catalog = self._get_xroad_catalog(harvest_job.source.url, last_time)
            members = self._parse_xroad_data(catalog)
        except ContentFetchError as e:
            self._save_gather_error('%r' % e.message, harvest_job)
            return False
        except KeyError as e:
            self._save_gather_error('Failed to parse response: %r' % e, harvest_job)
            return False

        # Member = organization
        # Subsystem = package = API
        # Service = resource = WSDL

        object_ids = []
        for member in members:  # type: dict
            if isinstance(member, six.string_types):
                continue

            # If member has been deleted in exchange layer
            if member.get('removed', None):
                continue

            # if there is only 1 subsystem, wrap it with list
            if member['subsystems'] and (type(member['subsystems']['subsystem']) is dict):
                member['subsystems']['subsystem'] = [member['subsystems']['subsystem']]

            # TODO: X-Road Catalog IsProvider is not in use for now, restore by uncommenting lines below
            # Fetch member type
            # member_type = self._get_member_type(harvest_job.source.url, member['xRoadInstance'], member['memberClass'],
            #                                    member['memberCode'])

            # If X-Road catalog is not used, following sets member_type to provider
            # if subsystem has at least one active service
            member_type = 'consumer'
            if member['subsystems']:
                for subsystem in member['subsystems'].get('subsystem', []):  # type: dict
                    services = subsystem.get('services', [])
                    if type(services) is dict:
                        services = [services]
                    for service in services:  # type: Union[str, dict]
                        if type(service) is dict and not service.get('removed'):
                            member_type = 'provider'

            # Create organization id
            org_id = substitute_ascii_equivalents(u'.'.join(six.text_type(member.get(p, ''))
                                                            for p in ('xRoadInstance', 'memberClass', 'memberCode')))

            organization_dict = {
                'id': org_id,
                'title_translated': {
                  "fi": member['name']
                },
                'name': member['name'],
                'member_type': member_type,
                'created': member['created'],
                'changed': member['changed'],
                'removed': member.get('removed', None),
                'xroad_instance': member['xRoadInstance'],
                'xroad_memberclass': member['memberClass'],
                'xroad_membercode': member['memberCode']
            }

            org = self._create_or_update_organization(organization_dict, harvest_job)

            if org is None:
                self._save_gather_error('Failed to create organization with id: %s and name: %s'
                                        % (org_id, member['name']), harvest_job)
                continue

            if member.get('subsystems'):
                for subsystem in member['subsystems']['subsystem']:

                    # Generate GUID
                    guid = substitute_ascii_equivalents(u'%s.%s'
                                                        % (org_id, six.text_type(subsystem.get('subsystemCode', ''))))

                    # Create harvest object
                    obj = HarvestObject(guid=guid, job=harvest_job,
                                        content=json.dumps({
                                            'xRoadInstance': member.get('xRoadInstance', ''),
                                            'xRoadMemberClass': member.get('memberClass', ''),
                                            'xRoadMemberCode': member.get('memberCode', ''),
                                            'owner': org,
                                            'subsystem': subsystem
                                        }))

                    obj.save()
                    object_ids.append(obj.id)

        return object_ids

    def fetch_stage(self, harvest_object):
        log.info('In xroad harvester fetch_stage')
        self._set_config(harvest_object.job.source.config)

        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.info('Could not parse content for object {0}'.format(harvest_object.id),
                     harvest_object, 'Import')
            self._save_object_error('Could not parse content for object {0}'.format(harvest_object.id),
                                    harvest_object, 'Fetch')
            return False

        services = xroad_list_to_list(dataset['subsystem'], 'services', 'service')
        try:
            if services:
                for service in services:
                    if 'removed' in service:
                        log.info("Service %s has been removed, no need to fetch api descriptions or types,"
                                 " skipping.." % service['serviceCode'])
                        continue
                    if 'wsdl' in service:
                        service['wsdl']['data'] = self._get_wsdl(harvest_object.source.url,
                                                                 service['wsdl']['externalId']).get('wsdl', '')
                    if 'openapi' in service:
                        service['openapi']['data'] = self._get_openapi(harvest_object.source.url,
                                                                       service['openapi']['externalId']).get('openapi', '')

                    service_version = parse_service_version(service.get('serviceVersion'))
                    if service_version:
                        service['serviceVersion'] = service_version

                    service['type'] = self._get_service_type(harvest_object.source.url,
                                                             dataset.get('xRoadInstance'),
                                                             dataset.get('xRoadMemberClass'),
                                                             dataset.get('xRoadMemberCode'),
                                                             dataset['subsystem']['subsystemCode'],
                                                             service['serviceCode'],
                                                             service.get('serviceVersion'))
                    if type(service['type']) is dict and service['type'].get('error'):

                        # Don't generate error if the error is unknown service
                        if "Unknown service" in service['type'].get('error'):
                            log.info(service['type'].get('error'))
                        else:
                            self._save_object_error(service['type'].get('error'), harvest_object, 'Fetch')
                    if not service['type']:
                        log.info("Service type in unknown for subsystem %s service %s"
                                 % (dataset['subsystem']['subsystemCode'], service['serviceCode']))
                harvest_object.content = json.dumps(dataset)
                harvest_object.save()
        except (TypeError, ContentFetchError):
            self._save_object_error('Could not parse WSDL content for object {0}'.format(harvest_object.id),
                                    harvest_object, 'Fetch')
            return False
        # TODO: Should fetch WSDLs
        return True

    def import_stage(self, harvest_object):
        log.info('In xroad harvester import stage')
        self._set_config(harvest_object.job.source.config)

        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.info('Could not parse content for object {0}'.format(harvest_object.id), harvest_object, 'Import')
            self._save_object_error('Could not parse content for object {0}'.format(harvest_object.id),
                                    harvest_object, 'Import')
            return False

        context = {
            'user': self._get_user_name(),
            'return_id_only': True,
            'ignore_auth': True,
        }

        removed = dataset['subsystem'].get('removed', None)

        try:
            package_dict = p.toolkit.get_action('package_show')(context, {'id': harvest_object.guid})
        except NotFound:
            if removed is not None:
                log.info("Subsystem has been removed, not creating..")
                return "unchanged"

            package_dict = {'id': harvest_object.guid}

        # Get rid of auth audit on the context otherwise we'll get an
        # exception
        context.pop('__auth_audit', None)

        # Local harvest source organization
        source_dataset = p.toolkit.get_action('package_show')(context, {'id': harvest_object.source.id})
        local_org = source_dataset.get('owner_org')

        # Create org
        log.info("Organization: " + dataset['owner']['name'])

        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True,
        }

        if removed is not None:
            log.info("Removing API %s", package_dict['name'])
            p.toolkit.get_action('package_delete')(context, {'id': package_dict['id']})
            harvest_object.current = False
            return True

        contains_wsdls = False
        contains_openapi_descriptions = False
        services = xroad_list_to_list(dataset['subsystem'], 'services', 'service')
        contains_wsdls = any('data' in s.get('wsdl', {}) for s in services)
        contains_openapi_descriptions = any('data' in s.get('openapi', {}) for s in services)

        if dataset['owner'] is not None:
            local_org = dataset['owner']['name']
        package_dict['owner_org'] = local_org

        # Munge name
        if not package_dict.get('title'):
            package_dict['title_translated'] = {
                    "fi": dataset['subsystem']['subsystemCode'],
                    "en": dataset['subsystem']['subsystemCode'],
                    "sv": dataset['subsystem']['subsystemCode']}

        package_dict['name'] = munge_title_to_name(dataset['subsystem']['subsystemCode'])
        package_dict['shared_resource'] = "no"
        package_dict['private'] = False

        package_dict['xroad_instance'] = dataset['xRoadInstance']
        package_dict['xroad_memberclass'] = dataset['xRoadMemberClass']
        package_dict['xroad_membercode'] = dataset['xRoadMemberCode']
        package_dict['xroad_subsystemcode'] = dataset['subsystem']['subsystemCode']

        result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')

        # Process removed services
        for service in services:
            service_code = service.get('serviceCode', None)
            name = generate_service_name(service)

            try:
                service_removed_string = service.get('removed')
                service_removed = (
                        datetime.strptime(service_removed_string.split('.', 2)[0], '%Y-%m-%dT%H:%M:%S')
                        if service_removed_string else None)
                if service_removed:
                    named_resources = [r for r in package_dict.get('resources', {}) if r['name'] == name]
                    for resource in named_resources:
                        log.info("Service deleted: %s", name)
                        p.toolkit.get_action('resource_delete')(context, {'id': resource['id']})
            except ValueError as e:
                log.error('Error parsing Service remove timestamp: %s' % e)

        if result not in (True, "unchanged"):
            return result

        unknown_service_link_url = p.toolkit.config.get('ckanext.xroad_integration.unknown_service_link_url')

        for service in services:
            # Removed services already processed
            if 'removed' in service:
                continue

            # Parse service name, version and description

            service_code = service.get('serviceCode', None)
            if service_code is None:
                continue

            service_version = parse_service_version(service.get('serviceVersion', None))
            service_type = service.get('type')
            name = generate_service_name(service)

            service_description = service.get('wsdl') or service.get('openapi') or {}

            # Parse last change timestamp

            if service_description:
                changed_string = service_description.get('changed', service_description.get('created'))
            else:
                changed_string = service.get('changed', service.get('created'))
            try:
                changed = (
                        datetime.strptime(changed_string.split('.', 2)[0], '%Y-%m-%dT%H:%M:%S')
                        if changed_string else None)
            except ValueError as e:
                log.error('Error parsing service timestamp: %s' % e)
                continue

            # Construct updated resource data

            resource_data = {
                "package_id": package_dict['id'],
                "name": name,
                "xroad_servicecode": service_code,
                "xroad_serviceversion": service_version,
                "xroad_service_type": service_type,
                "harvested_from_xroad": True,
            }

            if service_description:
                service_description_data = service_description.get('data')
                service_description_data_utf8 = service_description_data.encode('utf-8')

                wsdl = service.get('wsdl')
                openapi = service.get('openapi')

                # Todo: Validity of openapi ?
                if wsdl:
                    valid_wsdl = self._is_valid_wsdl(service_description_data_utf8)
                    timestamp_field = "wsdl_timestamp"
                    target_name = 'service.wsdl'
                    resource_format = 'wsdl'
                else:
                    valid_wsdl = True
                    timestamp_field = "openapi_timestamp"
                    target_name = 'service.json'
                    resource_format = 'openapi-json'

                f = tempfile.NamedTemporaryFile(delete=False)
                f.write(service_description_data_utf8)
                f.close()
                file_name = f.name

                # Prepare file upload
                upload_field_storage = FieldStorage()
                upload_field_storage.file = open(file_name, 'rb')
                upload_field_storage.filename = file_name

                resource_data['url'] = ''
                resource_data['upload'] = upload_field_storage
                resource_data['format'] = resource_format
                resource_data['valid_content'] = "yes" if valid_wsdl else "no"
            elif unknown_service_link_url is None:
                log.warn('Unknown type service %s.%s harvested, but ckanext.xroad_integration.unknown_service_link_url is not set!',
                         package_dict['id'], name)
                continue
            else:
                file_name = None
                upload_field_storage = None
                resource_data['url'] = unknown_service_link_url
                timestamp_field = 'unknown_timestamp'

            resource_data[timestamp_field] = changed.strftime('%Y-%m-%dT%H:%M:%S')

            named_resources = [r for r in package_dict.get('resources', {}) if r['name'] == name]

            # Create or update resources

            if not named_resources:
                p.toolkit.get_action('resource_create')(context, resource_data)
                result = True
            else:
                for resource in named_resources:
                    try:
                        previous_string = resource.get(timestamp_field, None)
                        previous = (
                                datetime.strptime(previous_string.split('.', 2)[0], '%Y-%m-%dT%H:%M:%S')
                                if previous_string else None)
                    except ValueError as e:
                        log.error('Error parsing previous timestamp: %s' % e)
                        continue

                    if not previous or (changed and changed > previous) or self.config.get('force_resource_update'):
                        log.info('Service changed after last harvest, replacing...')
                        resource_data['id'] = resource['id']
                        p.toolkit.get_action('resource_patch')(context, resource_data)
                        result = True

            if file_name:
                os.unlink(file_name)

        log.info('Created API %s', package_dict['name'])

        return result


    def _get_xroad_catalog(self, url, changed_after):
        # type: (str, str) -> dict
        try:
            r = http.get(url + '/Consumer/ListMembers', params={'changedAfter': changed_after},
                         headers={'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError("Calling XRoad service ListMembers failed!")
        if r.status_code != requests.codes.ok:
            raise ContentFetchError("Calling XRoad service ListMembers failed!")
        try:
            result = r.json()
        except ValueError:
            raise ContentFetchError("ListMembers JSON parse failed")
        return result

    def _parse_xroad_data(self, res):
        # type: (dict) -> list
        if isinstance(res['memberList'], six.string_types):
            return []
        if type(res['memberList']['member']) is dict:
            return [res['memberList']['member']]
        return res['memberList']['member']

    def _get_wsdl(self, url, external_id):
        try:
            r = http.get(url + '/Consumer/GetWsdl', params={'externalId': external_id},
                         headers={'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError("Calling XRoad service GetWsdl failed!")
        if r.status_code != requests.codes.ok:
            raise ContentFetchError("Calling XRoad service GetWsdl failed!")
        return r.json()

    def _get_openapi(self, url, external_id):
        try:
            r = http.get(url + '/Consumer/GetOpenAPI', params={'externalId': external_id},
                         headers={'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError("Calling XRoad service GetOpenAPI failed!")
        if r.status_code != requests.codes.ok:
            raise ContentFetchError("Calling XRoad service GetOpenAPI failed!")
        return r.json()

    @staticmethod
    def _get_service_type(url, xroad_instance, member_class, member_code, subsystem, service_code, service_version):
        try:
            params = {'xRoadInstance': xroad_instance,
                      'memberClass': member_class,
                      'memberCode': member_code,
                      'subsystemCode': subsystem,
                      'serviceCode': service_code
                      }

            if service_version:
                params['serviceVersion'] = service_version

            r = http.get(url + '/Consumer/GetServiceType', params=params,
                         headers={'Accept': 'application/json'})

            response_json = r.json()

            if response_json.get("error"):
                return {"error": response_json.get("error").get('string')}

            if response_json.get('type'):
                return response_json.get('type')

        except ConnectionError:
            raise ContentFetchError("Calling XRoad service GetServiceType failed")

        return ''

    @staticmethod
    def _get_member_type(url, xroad_instance, member_class, member_code):
        try:
            r = http.get(url + '/Consumer/IsProvider', params={'xRoadInstance': xroad_instance,
                                                               'memberClass': member_class,
                                                               'memberCode': member_code},
                         headers={'Accept': 'application/json'})

            is_provider = asbool(r.json().get('provider'))

            if is_provider is True:
                return "provider"
            elif is_provider is False:
                return "consumer"

        except ConnectionError:
            raise ContentFetchError("Calling XRoad service IsProvider failed")

        return ""

    @classmethod
    def _last_error_free_job(cls, harvest_job):
        # TODO weed out cancelled jobs somehow.
        # look for jobs with no gather errors
        jobs = model.Session.query(HarvestJob)\
                .filter(HarvestJob.source_id == harvest_job.source_id)\
                .filter(
                    HarvestJob.gather_started != None  # noqa
                )\
                .filter(HarvestJob.status == 'Finished')\
                .filter(HarvestJob.id != harvest_job.id)\
                .filter(
                    ~exists().where(
                        HarvestGatherError.harvest_job_id == HarvestJob.id))\
                .order_by(HarvestJob.gather_started.desc())
        # now check them until we find one with no fetch/import errors
        # (looping rather than doing sql, in case there are lots of objects
        # and lots of jobs)
        for job in jobs:
            for obj in job.objects:
                if obj.current is False and \
                                obj.report_status != 'not modified':
                    # unsuccessful, so go onto the next job
                    break
            else:
                log.info("Returning job %s", job)
                return job

    @classmethod
    def _last_error_free_job_time(cls, harvest_job):
        query = model.Session.query(HarvestJob.gather_started).from_statement(text('''
            select gather_started
            from harvest_job hj
            join (
              select hj.id,
                rank() over (partition by hj.source_id order by hj.gather_started desc, hj.id)
              from harvest_job hj
              left join harvest_gather_error hge on hj.id = hge.harvest_job_id
              left join (
                select distinct harvest_job_id as id
                from harvest_object
                where current = false
                  and report_status <> 'not modified'
              ) ushj on ushj.id = hj.id
              where hj.gather_started is not null
                and hj.source_id = :source
                and hj.id <> :notid
                and hj.status = 'Finished'
                and hge.id is null
                and ushj.id is null
            ) ranked on ranked.id = hj.id
            where ranked.rank = 1;
            '''))
        result = query.params(source=harvest_job.source_id, notid=harvest_job.id).first()
        return result.gather_started.isoformat() if result else None

    @classmethod
    def _last_finished_job(self, harvest_job):
        job = model.Session.query(HarvestJob)\
            .filter(HarvestJob.source == harvest_job.source)\
            .filter(
                HarvestJob.finished != None  # noqa
            )\
            .filter(HarvestJob.id != harvest_job.id)\
            .order_by(HarvestJob.finished.desc()).first()

        return job

    def _get_api_key(self):

        context = {'model': model,
                   'ignore_auth': True,
                   }

        site_user = p.toolkit.get_action('get_site_user')(context, {})

        return site_user['apikey']

    def _ensure_own_unique_name(self, org_id, org_name, context):
        organization_show_action = p.toolkit.get_action('organization_show')

        def organization_show(name_or_id):
            return organization_show_action(context, {
                'id': name_or_id,
                'include_datasets': False,
                'include_dataset_count': False,
                'include_extras': False,
                'include_users': False,
                'include_groups': False,
                'include_tags': False,
                'include_followers': False})

        # Find any existing organization with the name
        try:
            org_with_name = organization_show(org_name)
        except NotFound:
            org_with_name = None

        # If the name is free or already reserved to this organization, reuse the name
        if org_with_name is None or org_with_name['id'] == org_id:
            return org_name

        # Try to get current name as fallback
        try:
            org = organization_show(org_id)
            return org['name']
        except NotFound:
            pass

        # Try to find a fallback with a limited pool of variants
        name_candidates = ('%s_%i' % (org_name, i) for i in range(2, 20))
        for name in name_candidates:
            try:
                organization_show(name)
            except NotFound:
                return name

        # All variants were taken as well, probably some kind of error
        return None

    def _create_or_update_organization(self, data_dict, harvest_job):

        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True,
        }

        try:
            org = p.toolkit.get_action('organization_show')(context, {'id': data_dict['id']})
        except NotFound:
            org = None

        if org:

            if data_dict['removed']:
                log.info("Organization was removed, removing from catalog..")
                p.toolkit.get_action('organization_delete')(context, org)
                return None

            if self.config.get('force_all', False) is True:
                last_time = "2011-01-01"
            else:
                last_time = self._last_error_free_job_time(harvest_job)
            if (last_time and last_time < data_dict['changed']) or self.config.get('force_organization_update'):
                munged_title = munge_title_to_name(data_dict['name'])
                if org['name'] == munged_title:
                    org_name = munged_title
                else:
                    org_name = self._ensure_own_unique_name(data_dict['id'], munged_title, context)

                if org_name is None:
                    log.error("Organization name %s and tried variants already in use!" % munged_title)
                    return None

                org_description = org.get('description_translated', {}) \
                    if (org.get('description_translated') != {"fi": "", "sv": "", "en": ""}
                        and org.get('description_translated') != {"fi": ""}) \
                    else data_dict.get('description_translated', {})

                if not org.get('description_translated_modified_in_catalog', False):
                    org_description = data_dict.get('description_translated', {})

                org_data = {
                    'title_translated': data_dict['title_translated'],
                    'name': org_name,
                    'id': data_dict['id'],
                    'xroad_instance': data_dict['xroad_instance'],
                    'xroad_memberclass': data_dict['xroad_memberclass'],
                    'xroad_membercode': data_dict['xroad_membercode'],
                    'xroad_member_type': data_dict['member_type'],
                    'description_translated': org_description,
                    'organization_guid': data_dict.get('organization_guid'),
                    'company_type': data_dict.get('company_type', {}),
                    'postal_address': data_dict.get('postal_address'),
                    'company_language': data_dict.get('company_language', {}),
                    'company_registration_date': data_dict.get('company_registration_date'),
                    'old_business_ids': data_dict.get('old_business_ids')}

                if not org.get('webpage_address_modified_in_catalog', False):
                    org_data['webpage_address'] = data_dict.get('webpage_address', {})

                if not org.get('webpage_description_modified_in_catalog', False):
                    org_data['webpage_description'] = data_dict.get('webpage_description', {})

                log.info("Patching organization %s" % org_name)
                org = p.toolkit.get_action('organization_patch')(context, org_data)

        else:
            log.info("Organization %s not found, creating...", data_dict['name'])

            if data_dict['removed']:
                log.info("Organization was removed, not creating..")
                return None

            # Get rid of auth audit on the context otherwise we'll get an
            # exception
            context.pop('__auth_audit', None)

            org_name = self._ensure_own_unique_name(
                    data_dict['id'], munge_title_to_name(data_dict['name']), context)

            if org_name is None:
                log.error("Organization name %s and tried variants already in use!"
                          % munge_title_to_name(data_dict['name']))
                return None

            # Get rid of auth audit on the context otherwise we'll get an
            # exception
            context.pop('__auth_audit', None)

            org_data = {
                'title_translated': data_dict['title_translated'],
                'name': org_name,
                'id': data_dict['id'],
                'xroad_instance': data_dict['xroad_instance'],
                'xroad_memberclass': data_dict['xroad_memberclass'],
                'xroad_membercode': data_dict['xroad_membercode'],
                'xroad_member_type': data_dict['member_type'],
                'description_translated': data_dict.get('description_translated', {}),
                'organization_guid': data_dict.get('organization_guid'),
                'company_type': data_dict.get('company_type', {}),
                'postal_address': data_dict.get('postal_address'),
                'company_language': data_dict.get('company_language', {}),
                'company_registration_date': data_dict.get('company_registration_date'),
                'old_business_ids': data_dict.get('old_business_ids'),
                'webpage_address': data_dict.get('webpage_address', {}),
                'webpage_description': data_dict.get('webpage_description', {})}

            log.info("Creating organization %s" % org_name)
            org = p.toolkit.get_action('organization_create')(context, org_data)

        return org

    def _is_valid_wsdl(self, text_content):
        try:
            text_bytes = text_content.encode('utf-8') if type(text_content) is six.text_type else text_content
            wsdl_content = etree.fromstring(text_bytes)
            xml_namespaces = {
                    'soap-env': 'http://schemas.xmlsoap.org/soap/envelope/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema'}

            soap_faults = wsdl_content.xpath('//soap-env:Fault', namespaces=xml_namespaces)
            if len(soap_faults) > 0:
                return False
        except etree.XMLSyntaxError:
            return False

        return True


class ContentFetchError(Exception):
    pass


def xroad_list_to_list(obj, key1, key2):
    maybe_list = (obj.get(key1) or {}).get(key2) or []
    if type(maybe_list) is list:
        return maybe_list
    elif type(maybe_list) is dict:
        return [maybe_list]
    else:
        return []


def parse_service_version(v):
    if v is None:
        return v
    elif type(v) in (str, six.text_type):
        return v
    elif type(v) is int:
        return six.text_type('{}.0'.format(v))
    elif type(v) is float:
        return six.text_type(v)
    else:
        raise Exception('Unexpected service version type: {}'.format(repr(type(v))))

def generate_service_name(service):
    service_code = service.get('serviceCode')
    if service_code is None:
        return None

    service_version = parse_service_version(service.get('serviceVersion'))
    if service_version is None:
        return service_code

    return '%s.%s' % (service_code, service_version)

