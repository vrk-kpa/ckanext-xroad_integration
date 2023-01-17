import os
import tempfile
import logging
import json
import requests
import lxml.etree as etree
import six
import iso8601
from typing import Optional, Dict, Any

from sqlalchemy import text, exists
from datetime import datetime
from requests.exceptions import ConnectionError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
import ckan.plugins as p

from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents
from ckan import model
from ckan import logic

from werkzeug.datastructures import FileStorage as FlaskFileStorage

from .xroad_types import MemberList, Subsystem

try:
    from ckan.common import asbool  # CKAN 2.9
except ImportError:
    from paste.deploy.converters import asbool


NotFound = logic.NotFound

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 3  # seconds


# Add default timeout
class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if 'timeout' in kwargs:
            self.timeout = kwargs['timeout']
            del kwargs['timeout']
        super(TimeoutHTTPAdapter, self).__init__(*args, **kwargs)


retry_strategy = Retry(
    total=3,
    backoff_factor=1
)

adapter = TimeoutHTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount('http://', adapter)


class XRoadHarvesterPlugin(HarvesterBase):
    config = {}

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
            'name': 'xroad',
            'title': 'X-Road Rest Gateway',
            'description': 'Server that provides Rest Gateway for X-Road. '
                           'Valid config keys: force_all, force_organization_update, force_resource_update, since'
        }

    def validate_config(self, config):
        if not config:
            return config

        config_obj = json.loads(config)
        for key in ('force_all', 'force_organization_update', 'force_resource_update'):
            if key in config_obj:
                if not isinstance(config_obj[key], bool):
                    raise ValueError(f'{key} must be boolean')
        since = config_obj.get('since')
        if since:
            try:
                datetime.strptime(since, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f'{since} must be in format YYYY-MM-DD')

        return config

    def gather_stage(self, harvest_job):
        log.debug('In xroad harvester gather_stage')

        self._set_config(harvest_job.source.config)

        if self.config.get('force_all', False) is True:
            last_time = '2011-01-01'
        elif self.config.get('since'):
            last_time = str(self.config.get('since'))
        else:
            last_time = self._last_error_free_job_time(harvest_job) or '2011-01-01'

        log.info('Searching for apis modified since: %s UTC', last_time)
        try:
            catalog = self._get_xroad_catalog(harvest_job.source.url, last_time)

            member_list = MemberList.from_dict(catalog)
        except ContentFetchError as e:
            self._save_gather_error('%r' % e.args, harvest_job)
            return False
        except KeyError as e:
            self._save_gather_error('Failed to parse response: %r' % e, harvest_job)
            return False
        except ValueError as e:
            self._save_gather_error('Failed to parse response: %r' % e, harvest_job)
            return False

        # Member = organization
        # Subsystem = package = API
        # Service = resource = WSDL

        object_ids = []
        for member in member_list.members:
            # If member has been deleted in exchange layer
            if member.removed:
                continue

            # TODO: X-Road Catalog IsProvider is not in use for now, restore by utilizing _get_member_type

            # If X-Road catalog is not used, following sets member_type to provider
            # if subsystem has at least one active service
            if all(service.removed for subsystem in member.subsystems for service in subsystem.services):
                member.member_type = 'consumer'
            else:
                member.member_type = 'provider'

            # Create organization id
            org_id = substitute_ascii_equivalents(f'{member.instance}.{member.member_class}.{member.member_code}')

            try:
                org = self._create_or_update_organization(org_id, member, harvest_job)
            except p.toolkit.ValidationError as e:
                log.warning(f'Validation error creating/updating organization {org_id}: {e}')
                self._save_gather_error(f'Validation error creating/updating organization {org_id}: {e}', harvest_job)
                continue

            if org is None:
                # Organization has been removed
                continue

            for subsystem in member.subsystems:
                # Generate GUID
                guid = substitute_ascii_equivalents(f'{org_id}.{subsystem.subsystem_code}')

                # Create harvest object
                obj = HarvestObject(guid=guid, job=harvest_job,
                                    content=json.dumps({
                                        'xRoadInstance': member.instance,
                                        'xRoadMemberClass': member.member_class,
                                        'xRoadMemberCode': member.member_code,
                                        'owner_name': org.get('name'),
                                        'subsystem_dict': json.loads(subsystem.serialize_json()),
                                        'subsystem_pickled': subsystem.serialize(),
                                    }))

                obj.save()
                object_ids.append(obj.id)

        return object_ids

    def fetch_stage(self, harvest_object):
        log.info('In xroad harvester fetch_stage')
        self._set_config(harvest_object.job.source.config)

        try:
            dataset = json.loads(harvest_object.content)
            subsystem = Subsystem.deserialize(dataset['subsystem_pickled'])
        except ValueError:
            log.info('Could not parse content for object {0}'.format(harvest_object.id),
                     harvest_object, 'Import')
            self._save_object_error(f'Could not parse content for object {harvest_object.id}', harvest_object, 'Fetch')
            return False

        try:
            if subsystem.services:
                for service in subsystem.services:
                    if service.removed:
                        log.info(f'Service {service.service_code} has been removed, '
                                 'no need to fetch api descriptions or types, skipping...')
                        continue

                    if service.wsdl:
                        wsdl_data = self._get_wsdl(harvest_object.source.url, service.wsdl.external_id)
                        if wsdl_data:
                            service.wsdl.data = wsdl_data
                        else:
                            log.warn(f'Empty WSDL service description returned for {generate_service_name(service)}')

                    if service.openapi:
                        openapi_data = self._get_openapi(harvest_object.source.url, service.openapi.external_id)
                        if openapi_data:
                            service.openapi.data = openapi_data
                        else:
                            log.warn(f'Empty OpenApi service description returned for {generate_service_name(service)}')

                    service_type = self._get_service_type(harvest_object.source.url,
                                                          dataset.get('xRoadInstance'),
                                                          dataset.get('xRoadMemberClass'),
                                                          dataset.get('xRoadMemberCode'),
                                                          subsystem.subsystem_code,
                                                          service.service_code,
                                                          service.service_version)
                    if type(service_type) is dict:
                        # Don't generate error if the error is unknown service
                        if service_type.get('error') == 'Unknown service':
                            log.info(service_type.get('error'))
                        else:
                            self._save_object_error(service_type.get('error'), harvest_object, 'Fetch')
                    elif not service_type:
                        log.info(f'Service type is unknown for subsystem {subsystem.subsystem_code} '
                                 f'service {service.service_code}')
                    else:
                        service.service_type = service_type

                dataset['subsystem_pickled'] = subsystem.serialize()
                dataset['subsystem_dict'] = json.loads(subsystem.serialize_json())
                harvest_object.content = json.dumps(dataset)
                harvest_object.save()
        except (TypeError, ContentFetchError):
            self._save_object_error(f'Could not parse WSDL content for object {harvest_object.id}', harvest_object, 'Fetch')
            return False

        return True

    def import_stage(self, harvest_object):
        log.info('In xroad harvester import stage')
        self._set_config(harvest_object.job.source.config)

        try:
            dataset = json.loads(harvest_object.content)
            subsystem = Subsystem.deserialize(dataset['subsystem_pickled'])
        except ValueError:
            log.info(f'Could not parse content for object {harvest_object.id}', harvest_object, 'Import')
            self._save_object_error(f'Could not parse content for object {harvest_object.id}', harvest_object, 'Import')
            return False

        context = {
            'user': self._get_user_name(),
            'return_id_only': True,
            'ignore_auth': True,
        }

        try:
            package_dict = p.toolkit.get_action('package_show')(context, {'id': harvest_object.guid})
        except NotFound:
            if subsystem.removed:
                log.info('Subsystem has been removed, not creating..')
                return 'unchanged'

            package_dict = {'id': harvest_object.guid}

        # Get rid of auth audit on the context otherwise we'll get an
        # exception
        context.pop('__auth_audit', None)

        # Local harvest source organization
        source_dataset = p.toolkit.get_action('package_show')(context, {'id': harvest_object.source.id})
        local_org = source_dataset.get('owner_org')

        # Create org
        owner_name = dataset.get('owner_name')
        log.info(f'Organization: {owner_name}')

        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True,
        }

        if subsystem.removed:
            log.info('Removing API %s', package_dict.get('name'))
            p.toolkit.get_action('package_delete')(context, {'id': package_dict['id']})
            harvest_object.current = False
            return True

        if owner_name is not None:
            local_org = owner_name
        package_dict['owner_org'] = local_org

        # Munge name
        if not package_dict.get('title'):
            package_dict['title_translated'] = {
                    'fi': subsystem.subsystem_code,
                    'en': subsystem.subsystem_code,
                    'sv': subsystem.subsystem_code}

        package_dict['name'] = munge_title_to_name(subsystem.subsystem_code)
        package_dict['shared_resource'] = 'no'
        package_dict['private'] = False
        package_dict['access_restriction_level'] = 'public'

        package_dict['xroad_instance'] = dataset['xRoadInstance']
        package_dict['xroad_memberclass'] = dataset['xRoadMemberClass']
        package_dict['xroad_membercode'] = dataset['xRoadMemberCode']
        package_dict['xroad_subsystemcode'] = subsystem.subsystem_code

        result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')

        # Process removed services
        for service in subsystem.services:
            name = generate_service_name(service)

            try:
                if service.removed:
                    named_resources = [r for r in package_dict.get('resources', []) if r.get('name') == name]
                    for resource in named_resources:
                        log.info(f'Service deleted: {name}')
                        p.toolkit.get_action('resource_delete')(context, {'id': resource['id']})
            except iso8601.ParseError as e:
                log.error(f'Error parsing Service remove timestamp: {e}')

        if result not in (True, 'unchanged'):
            return result

        unknown_service_link_url = p.toolkit.config.get('ckanext.xroad_integration.unknown_service_link_url')

        for service in subsystem.services:
            # Removed services already processed
            if service.removed:
                continue

            # Parse service name, version and description

            name = generate_service_name(service)

            service_description = service.wsdl or service.openapi or None

            changed = service_description.changed if service_description else service.changed

            # Construct updated resource data

            resource_data = {
                'package_id': package_dict['id'],
                'name': name,
                'xroad_servicecode': service.service_code,
                'xroad_serviceversion': service.service_version,
                'xroad_service_type': service.service_type,
                'harvested_from_xroad': True,
                'access_restriction_level': 'public'
            }

            if service_description:
                service_description_data_utf8 = service_description.data.encode('utf-8')

                # Todo: Validity of openapi ?
                if service.wsdl:
                    valid_wsdl = self._is_valid_wsdl(service_description_data_utf8)
                    timestamp_field = 'wsdl_timestamp'
                    target_name = 'service.wsdl'
                    resource_format = 'wsdl'
                else:
                    valid_wsdl = True
                    timestamp_field = 'openapi_timestamp'
                    target_name = 'service.json'
                    resource_format = 'openapi-json'

                f = tempfile.NamedTemporaryFile(delete=False)
                f.write(service_description_data_utf8)
                f.close()
                file_name = f.name

                # Prepare file upload
                content_length = len(service_description_data_utf8)
                log.debug('Uploading service %s description (size: %d bytes)',
                          service_version_name(service.service_code, service.service_version), content_length)
                resource_data['upload'] = FlaskFileStorage(open(file_name, 'rb'), target_name, content_length=content_length)
                resource_data['format'] = resource_format
                resource_data['valid_content'] = 'yes' if valid_wsdl else 'no'
            elif unknown_service_link_url is None:
                log.warn('Unknown type service %s.%s harvested, but '
                         'ckanext.xroad_integration.unknown_service_link_url is not set!',
                         package_dict['id'], name)
                continue
            else:
                file_name = None
                resource_data['url'] = unknown_service_link_url
                timestamp_field = 'unknown_timestamp'

            resource_data[timestamp_field] = changed.strftime('%Y-%m-%dT%H:%M:%S')

            named_resources = [r for r in package_dict.get('resources', []) if r.get('name') == name]

            # Create or update resources

            try:
                if not named_resources:
                    p.toolkit.get_action('resource_create')(context, resource_data)
                    result = True
                else:
                    for resource in named_resources:
                        try:
                            previous_string = resource.get(timestamp_field, None)
                            previous = iso8601.parse_date(previous_string) if previous_string else None
                        except iso8601.ParseError as e:
                            log.error('Error parsing previous timestamp: %s' % e)
                            continue

                        if not previous or (changed and changed > previous) or self.config.get('force_resource_update'):
                            log.info('Service %s.%s changed after last harvest, replacing...',
                                     resource.get('xroad_servicecode'), resource.get('xroad_serviceversion'))
                            resource_data['id'] = resource['id']
                            p.toolkit.get_action('resource_patch')(context, resource_data)
                            result = True
            except p.toolkit.ValidationError as e:
                log.warning(f'Validation error while updating/creating {name}: {e}')
                self._save_object_error(f'Validation error processing {owner_name}.{subsystem.subsystem_code}.{name} '
                                        f'in {harvest_object.id}', harvest_object, 'Import')

            if file_name:
                os.unlink(file_name)

        log.info('Created API %s', package_dict['name'])

        return result

    def _get_xroad_catalog(self, url, changed_after: str):
        # type: (str, str) -> dict
        try:
            r = http.get(url + '/Consumer/ListMembers', params={'changedAfter': changed_after},
                         headers={'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError('Calling XRoad service ListMembers failed!')
        if r.status_code != requests.codes.ok:
            raise ContentFetchError('Calling XRoad service ListMembers failed!')
        try:
            result = r.json()
        except ValueError:
            raise ContentFetchError('ListMembers JSON parse failed')
        return result

    def _get_wsdl(self, url, external_id):
        try:
            r = http.get(url + '/Consumer/GetWsdl', params={'externalId': external_id},
                         headers={'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError('Calling XRoad service GetWsdl failed!')
        if r.status_code != requests.codes.ok:
            raise ContentFetchError('Calling XRoad service GetWsdl failed!')
        return r.json().get('wsdl')

    def _get_openapi(self, url, external_id):
        try:
            r = http.get(url + '/Consumer/GetOpenAPI', params={'externalId': external_id},
                         headers={'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError('Calling XRoad service GetOpenAPI failed!')
        if r.status_code != requests.codes.ok:
            raise ContentFetchError('Calling XRoad service GetOpenAPI failed!')
        return r.json().get('openapi')

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

            if response_json.get('error'):
                return {'error': response_json.get('error').get('string')}

            if response_json.get('type'):
                return response_json.get('type')

        except ConnectionError:
            raise ContentFetchError('Calling XRoad service GetServiceType failed')

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
                return 'provider'
            elif is_provider is False:
                return 'consumer'

        except ConnectionError:
            raise ContentFetchError('Calling XRoad service IsProvider failed')

        return ''

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
                log.info(f'Returning job {job}')
                return job

    @classmethod
    def _last_error_free_job_time(cls, harvest_job) -> Optional[str]:
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
    def _last_finished_job(cls, harvest_job):
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

    def _create_or_update_organization(self, org_id, member, harvest_job) -> Optional[Dict[str, Any]]:
        '''Creates or updates an organization based on member.
        Raises a ValidationError if contents are invalid or there's no suitable name available
        Returns updated organization on success or None if the organization has been removed.
        '''
        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True,
        }

        munged_title = munge_title_to_name(member.name)

        try:
            org = p.toolkit.get_action('organization_show')(context, {'id': org_id})
        except NotFound:
            org = None

        if org:

            if member.removed:
                log.info('Organization was removed, removing from catalog..')
                p.toolkit.get_action('organization_delete')(context, org)
                return None

            if self.config.get('force_all', False) is True:
                last_time = iso8601.parse_date('2011-01-01')
            else:
                last_time = self._last_error_free_job_time(harvest_job)
                if last_time is not None:
                    last_time = iso8601.parse_date(last_time)

            if (last_time and last_time < member.changed) or self.config.get('force_organization_update'):
                if org['name'] == munged_title:
                    org_name = munged_title
                else:
                    org_name = self._ensure_own_unique_name(org_id, munged_title, context)

                if org_name is None:
                    raise p.toolkit.ValidationError(f'Organization name {munged_title} and tried variants already in use!')

                org_description = org.get('description_translated', {}) \
                    if (org.get('description_translated') != {'fi': '', 'sv': '', 'en': ''}
                        and org.get('description_translated') != {'fi': ''}) \
                    else {}

                if not org.get('description_translated_modified_in_catalog', False):
                    org_description = {}

                org_data = {
                    'title_translated': {'fi': member.name},
                    'name': org_name,
                    'id': org_id,
                    'xroad_instance': member.instance,
                    'xroad_memberclass': member.member_class,
                    'xroad_membercode': member.member_code,
                    'xroad_member_type': member.member_type,
                    'description_translated': org_description,
                }

                if not org.get('webpage_address_modified_in_catalog', False):
                    org_data['webpage_address'] = {}

                if not org.get('webpage_description_modified_in_catalog', False):
                    org_data['webpage_description'] = {}

                log.info(f'Patching organization {org_name}')
                org = p.toolkit.get_action('organization_patch')(context, org_data)

        else:
            log.info(f'Organization {member.name} not found, creating...')

            if member.removed:
                log.info('Organization was removed, not creating..')
                return None

            # Get rid of auth audit on the context otherwise we'll get an
            # exception
            context.pop('__auth_audit', None)

            org_name = self._ensure_own_unique_name(org_id, munged_title, context)

            if org_name is None:
                raise p.toolkit.ValidationError(f'Organization name {munged_title} and tried variants already in use!')

            # Get rid of auth audit on the context otherwise we'll get an
            # exception
            context.pop('__auth_audit', None)

            org_data = {
                'title_translated': {'fi': member.name},
                'name': org_name,
                'id': org_id,
                'xroad_instance': member.instance,
                'xroad_memberclass': member.member_class,
                'xroad_membercode': member.member_code,
                'xroad_member_type': member.member_type,
            }

            log.info(f'Creating organization {org_name}')
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


def generate_service_name(service) -> Optional[str]:
    if service.service_code is None:
        return None

    return service_version_name(service.service_code, service.service_version)


def service_version_name(service_code: str, service_version: Optional[str]) -> str:
    if service_version is None:
        return service_code

    return f'{service_code}.{service_version}'
