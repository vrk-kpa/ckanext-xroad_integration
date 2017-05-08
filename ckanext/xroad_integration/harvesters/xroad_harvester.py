from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
from sqlalchemy import text
import ckan.plugins as p
from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents


import logging
import json
import requests
from requests.exceptions import ConnectionError
import os
import tempfile
from sqlalchemy import exists
import lxml.etree as etree
from ckan import model

from ckan import logic
NotFound = logic.NotFound

log = logging.getLogger(__name__)

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
            "title": "XRoad Rest Gateway",
            "description": "Server that provides Rest Gateway for XRoad"
        }

    def validate_config(self, config):
        if not config:
            return config

        config_obj = json.loads(config)
        for key in ('force_all',):
            if key in config_obj:
                if not isinstance(config_obj[key], bool):
                    raise ValueError('%s must be boolean' % key)

        return config

    def gather_stage(self, harvest_job):
        log.debug('In xroad harvester gather_stage')

        self._set_config(harvest_job.source.config)

        if self.config.get('force_all', False) is True:
            last_time = "2011-01-01"
        else:
            last_time = self._last_error_free_job_time(harvest_job) or "2011-01-01"

        log.info('Searching for apis modified since: %s UTC',
                 last_time)
        try:
            catalog = self._get_xroad_catalog(harvest_job.source.url, last_time)
            members = self._parse_xroad_data(catalog)
        except ContentFetchError, e:
            self._save_gather_error('%r' % e.message, harvest_job)
            return False
        except KeyError, e:
            self._save_gather_error('Failed to parse response: %r' % e, harvest_job)
            return False

        # Member = organization
        # Subsystem = package = API
        # Service = resource = WSDL

        object_ids = []
        for member in members:
            if isinstance(member, basestring):
                continue

            # if there is only 1 subsystem, wrap it with list
            if member['subsystems'] and (type(member['subsystems']['subsystem']) is dict):
                member['subsystems']['subsystem'] = [member['subsystems']['subsystem']]

            # Create organization id
            org_id = substitute_ascii_equivalents(u'.'.join(unicode(member.get(p, ''))
                for p in ('xRoadInstance', 'memberClass', 'memberCode')))

            org = self._create_or_update_organization({
                'id': org_id,
                'name': member['name'],
                'created': member['created'],
                'changed': member['changed'],
                'removed': member.get('removed', None)
                }, harvest_job)

            if org is None:
                continue

            if self._organization_has_wsdls(member):
                for subsystem in member['subsystems']['subsystem']:

                    # Generate GUID
                    guid = substitute_ascii_equivalents(u'%s.%s' % (org_id, unicode(subsystem.get('subsystemCode', ''))))

                    # Create harvest object
                    obj = HarvestObject(guid=guid, job=harvest_job,
                                        content=json.dumps({
                                            'owner': org,
                                            'subsystem': subsystem
                                        }))

                    obj.save()
                    object_ids.append(obj.id)

        return object_ids

    def fetch_stage(self, harvest_object):
        log.info('In xroad harvester fetch_stage')
        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.info('Could not parse content for object {0}'.format(harvest_object.id),
                     harvest_object, 'Import')
            self._save_object_error('Could not parse content for object {0}'.format(harvest_object.id),
                                    harvest_object, 'Import')
            return False

        services = dataset['subsystem'].get('services', None)
        try:
            if services:
                # If there is only 1 service, wrap it with a list
                if type(services['service']) is dict:
                    services['service'] = [services['service']]
                for service in services['service']:
                    if 'wsdl' in service:
                        service['wsdl']['data']  = self._get_wsdl(harvest_object.source.url, service['wsdl']['externalId']).get('wsdl', '')
                harvest_object.content = json.dumps(dataset)
                harvest_object.save()
        except TypeError, ContentFetchError:
            self._save_object_error('Could not parse WSDL content for object {0}'.format(harvest_object.id),
                                    harvest_object, 'Import')
            return False
        # TODO: Should fetch WSDLs
        return True

    def import_stage(self, harvest_object):
        log.info('In xroad harvester import stage')

        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.info('Could not parse content for object {0}'.format(harvest_object.id),
                harvest_object, 'Import')
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
            package_dict = p.toolkit.get_action('package_show')(context, {'id': harvest_object.guid })
        except NotFound:
            if removed is not None:
                log.info("Subsystem has been removed, not creating..")
                return "unchanged"

            package_dict = {'id': harvest_object.guid }

        # Get rid of auth audit on the context otherwise we'll get an
        # exception
        context.pop('__auth_audit', None)

        # Local harvest source organization
        source_dataset = p.toolkit.get_action('package_show')(context, {'id': harvest_object.source.id})
        local_org = source_dataset.get('owner_org')

        # Create org
        log.info("Organization: " + dataset['owner']['name'] )

        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True,
        }

        if removed is not None:
            log.info("Removing service %s", package_dict['name'])
            p.toolkit.get_action('package_delete')(context, {'id': package_dict['id']})
            harvest_object.current = False
            return True

        contains_wsdls = False
        services = dataset['subsystem'].get('services', {})
        if not isinstance(services, basestring):
            contains_wsdls = any('data' in s.get('wsdl', {}) for s in services['service'])

        if contains_wsdls:
            if dataset['owner'] is not None:
                local_org = dataset['owner']['name']
            package_dict['owner_org'] = local_org

            # Munge name
            if not package_dict.get('title'):
                package_dict['title'] = dataset['subsystem']['subsystemCode']
            package_dict['name'] = munge_title_to_name(dataset['subsystem']['subsystemCode'])
            package_dict['shared_resource'] = "no"
            package_dict['private'] = False

            result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')
            apikey = self._get_api_key()

            if result not in(True, "unchanged"):
                return result

            for service in services['service']:
                wsdl_data = service.get('wsdl', {}).get('data')
                if not wsdl_data:
                    continue

                wsdl_data_utf8 = wsdl_data.encode('utf-8')
                valid_wsdl = self._is_valid_wsdl(wsdl_data_utf8)

                service_code = service.get('serviceCode', None)
                if service_code is None:
                    continue

                service_version = service.get('serviceVersion', None)

                if service_version is None:
                    name = service_code
                else:
                    name = '%s.%s' % (service_code, service_version)

                f = tempfile.NamedTemporaryFile(delete=False)
                f.write(wsdl_data_utf8)
                f.close()

                # TODO: resource_create and resource_update should not create resources without wsdls
                named_resources = [r for r in package_dict.get('resources', {}) if r['name'] == name]

                for resource in named_resources:
                    changed = service['wsdl'].get('changed', None)
                    if changed and changed > resource['created']:
                        log.info('WSDL changed after last harvest, replacing...')
                        resource_data = {
                                "package_id":package_dict['id'],
                                "url": "",
                                "name": name,
                                "id": resource['id'],
                                "valid_content": "yes" if valid_wsdl else "no"
                                }
                        self._patch_resource(resource_data, apikey, f.name)
                        result = True

                if not named_resources:
                    resource_data = {
                            "package_id":package_dict['id'],
                            "url": "",
                            "name": name,
                            "valid_content": "yes" if valid_wsdl else "no"
                            }
                    self._create_resource(resource_data, apikey, f.name)
                    result = True

                os.unlink(f.name)

            log.info('Created API %s', package_dict['name'])

            return result

        log.info("API %s did not have WSDLs, not creating..", dataset['subsystem']['subsystemCode'])
        return "unchanged"

    def _get_xroad_catalog(self, url, changed_after):
        try:
            r = requests.get(url + '/Consumer/ListMembers', params = {'changedAfter' : changed_after}, headers = {'Accept': 'application/json'})
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
        if isinstance(res['memberList'], basestring):
            return {}
        return res['memberList']['member']

    def _get_wsdl(self, url, external_id):
        try:
            r = requests.get(url + '/Consumer/GetWsdl', params = {'externalId' : external_id}, headers = {'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError("Calling XRoad service GetWsdl failed!")
        if r.status_code != requests.codes.ok:
            raise ContentFetchError("Calling XRoad service GetWsdl failed!")
        return r.json()

    @classmethod
    def _last_error_free_job(cls, harvest_job):
        # TODO weed out cancelled jobs somehow.
        # look for jobs with no gather errors
        jobs = \
            model.Session.query(HarvestJob) \
                .filter(HarvestJob.source_id == harvest_job.source_id) \
                .filter(HarvestJob.gather_started != None) \
                .filter(HarvestJob.status == 'Finished') \
                .filter(HarvestJob.id != harvest_job.id) \
                .filter(
                ~exists().where(
                    HarvestGatherError.harvest_job_id == HarvestJob.id)) \
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
        query =  model.Session.query(HarvestJob.gather_started).from_statement(text('''
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
            .filter(HarvestJob.finished != None)\
            .filter(HarvestJob.id != harvest_job.id)\
            .order_by(HarvestJob.finished.desc()).first()

        return job

    def _get_api_key(self):

        context = {'model': model,
           'ignore_auth': True,
        }

        site_user = p.toolkit.get_action('get_site_user')(context, {})

        return site_user['apikey']

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
            log.info("found %s", org)

            if data_dict['removed']:
                log.info("Organization was removed, removing from catalog..")
                p.toolkit.get_action('organization_delete')(context, org)
                return None

            if self.config.get('force_all', False) is True:
                last_time = "2011-01-01"
            else:
                last_time = self._last_error_free_job_time(harvest_job)
            if last_time and last_time < data_dict['changed']:
                org_data = {
                        'title': data_dict['name'],
                        'name': munge_title_to_name(data_dict['name']),
                        'id': data_dict['id']}
                org = p.toolkit.get_action('organization_patch')(context, org_data)

        else:
            log.info("Organization %s not found, creating...", data_dict['name'])

            if data_dict['removed']:
                log.info("Organization was removed, not creating..")
                return None

            # Get rid of auth audit on the context otherwise we'll get an
            # exception
            context.pop('__auth_audit', None)

            org_data = {
                    'title': data_dict['name'],
                    'name': munge_title_to_name(data_dict['name']),
                    'id': data_dict['id']}
            org = p.toolkit.get_action('organization_create')(context, org_data)
            log.info(org)

        return org

    def _organization_has_apis(self, member):
        if member['subsystems'] and len(member['subsystems']['subsystem']) > 0:
            return True
        return False

    def _api_has_wsdls(self, subsystem):
        services = subsystem.get('services', {})
        if not isinstance(services, basestring):
            if type(services['service']) is dict:
                services['service'] = [services['service']]
            for service in services['service']:
                if 'wsdl' in service:
                    return True
        return False

    def _organization_has_wsdls(self, member):
        if self._organization_has_apis(member):
           for subsystem in member['subsystems']['subsystem']:
              if self._api_has_wsdls(subsystem) is True:
                  return True
        return False

    def _patch_resource(self, data, apikey, filename):
        requests.post('https://0.0.0.0/api/action/resource_patch',
                data=data,
                headers={"X-CKAN-API-Key": apikey },
                files={'upload': ('service.wsdl',file(filename))},
                verify=False)

    def _create_resource(self, data, apikey, filename):
        requests.post('https://0.0.0.0/api/action/resource_create',
                data=data,
                headers={"X-CKAN-API-Key": apikey },
                files={'upload': ('service.wsdl',file(filename))},
                verify=False)

    def _is_valid_wsdl(self, text_content):
        try:
            text_bytes = text_content.encode('utf-8') if type(text_content) is unicode else text_content
            wsdl_content = etree.fromstring(text_bytes)
            xml_namespaces = {
                    'soap-env': 'http://schemas.xmlsoap.org/soap/envelope/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema'}

            soap_faults = wsdl_content.xpath('//soap-env:Fault', namespaces=xml_namespaces)
            if len(soap_faults) > 0:
                return False
        except etree.XMLSyntaxError as e:
            return False

        return True

class ContentFetchError(Exception):
    pass
