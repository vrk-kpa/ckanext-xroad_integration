from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
import ckan.plugins as p
from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents


import logging
import json
import requests
from requests.exceptions import ConnectionError
import os
import tempfile
from sqlalchemy import exists
from ckan import model

from ckan import logic
NotFound = logic.NotFound

log = logging.getLogger(__name__)

class XRoadHarvesterPlugin(HarvesterBase):

    def info(self):

        return {
            "name": "xroad",
            "title": "XRoad Rest Gateway",
            "description": "Server that provides Rest Gateway for XRoad"
        }

    def validate_config(self, config):

        return ""

    def gather_stage(self, harvest_job):
        log.debug('In xroad harvester gather_stage')

        last_error_free_job = self._last_error_free_job(harvest_job)
        if last_error_free_job:
            last_time = last_error_free_job.gather_started.isoformat()
        else:
            last_time = "2011-01-01"

        log.info('Searching for apis modified since: %s UTC',
                 last_time)
        '''
        try:
            members = self._get_xroad_catalog(last_time)
        except ContentFetchError, e:
            self._save_gather_error('%r' % e.message, harvest_job)
            return False
            '''
        #members = self.get_xroad_catalog("http://localhost:9090/rest-gateway-0.0.8-SNAPSHOT/Consumer/catalog", "2011-01-01")
        file = open(os.path.join(os.path.dirname(__file__), '../tests/response.json'))
        members = json.load(file)

        object_ids = []
        for member in self._parse_xroad_data(members):
            log.info(json.dumps(member))
            #log.info(type(member['subsystems']['subsystem']))
            if member['subsystems'] and (type(member['subsystems']['subsystem']) is list):
                for subsystem in member['subsystems']['subsystem']:

                    # Generate GUID
                    guid = substitute_ascii_equivalents(unicode(member.get('xRoadInstance', '')) + '.' + unicode(member.get('memberClass', '')) + '.' + unicode(member.get('memberCode', '')) + '.' + unicode(subsystem.get('subsystemCode', '')))
                    # Create harvest object
                    obj = HarvestObject(guid=guid, job=harvest_job,
                                        content=json.dumps({
                                            'owner': member['name'],
                                            'subsystem': subsystem
                                        }))

                    obj.save()
                    object_ids.append(obj.id)

            elif member['subsystems'] and (type(member['subsystems']['subsystem']) is dict):
                # Generate GUID
                guid = substitute_ascii_equivalents(unicode(member.get('xRoadInstance', '')) + '.' + unicode(member.get('memberClass', '')) + '.' + unicode(member.get('memberCode', '')) + '.' + unicode(member['subsystems']['subsystem'].get('subsystemCode', '')))
                # Create harvest object
                obj = HarvestObject(guid=guid, job=harvest_job,
                                    content=json.dumps({
                                    'owner': member['name'],
                                     'subsystem': member['subsystems']['subsystem']
                                        }))

                obj.save()
                object_ids.append(obj.id)

        return object_ids

    def fetch_stage(self, harvest_object):
        log.info('Doing nothing in xroad harvester fetch stage')
        '''
        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.info('Could not parse content for object {0}'.format(harvest_object.id),
                     harvest_object, 'Import')
            self._save_object_error('Could not parse content for object {0}'.format(harvest_object.id),
                                    harvest_object, 'Import')
            return False



        services = dataset['subsystem'].get('services', None)
        if services:
            log.info("SERVICES")
            log.info(services['service'])
            #log.info( services['service']['serviceCode'] + '.' + services['service']['serviceVersion'])
            for service in services['service']:
                if 'wsdl' in service:
                    log.info("WSDL")
                    log.info(service['wsdl'])
                    service['wsdl']['data']  = self._get_wsdl(service['wsdl']['externalId'])['GetWsdlResponse']['wsdl']

            harvest_object.content = json.dumps(dataset)
            harvest_object.save()
'''
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

        # Set id
        dataset['id'] = harvest_object.guid

        # Local harvest source organization
        source_dataset = p.toolkit.get_action('package_show')(context, {'id': harvest_object.source.id})
        local_org = source_dataset.get('owner_org')


        # Create org
        log.info("Organization: " + dataset['owner'] )


        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True,
        }

        try:
            org = p.toolkit.get_action('organization_show')(context, {'id': munge_title_to_name(dataset['owner'])})
            log.info(org)
        except NotFound:
            log.info("Organization %s not found, creating...", dataset['owner'])

            # Get rid of auth audit on the context otherwise we'll get an
            # exception
            context.pop('__auth_audit', None)

            org = p.toolkit.get_action('organization_create')(context, {'title': dataset['owner'], 'name': munge_title_to_name(dataset['owner'])})
            log.info(org)

        if org is not None:
            local_org = org['name']

        dataset['owner_org'] = local_org
        # Munge name

        dataset['title'] = dataset['subsystem']['subsystemCode']
        dataset['name'] = munge_title_to_name(dataset['subsystem']['subsystemCode'])
        #dataset['notes'] = "this is example"
        dataset['shared_resource'] = "no"
        log.info(dataset)



        result = self._create_or_update_package(dataset, harvest_object, package_dict_form='package_show')
        apikey = self._get_api_key()
        '''
        if result:
                log.info(dataset['subsystem'])
                services = dataset['subsystem'].get('services', None)
                if services:
                    log.info("SERVICES")
                    log.info(services['service'])
                    #log.info( services['service']['serviceCode'] + '.' + services['service']['serviceVersion'])
                    for service in services['service']:
                        if 'wsdl' in service:
                            log.info("WSDL")
                            log.info(service['wsdl'])

                            f = tempfile.NamedTemporaryFile(delete=False)
                            f.write(service['wsdl']['data'])
                            f.close()



                            response = requests.post('https://0.0.0.0/api/action/resource_create',
                                                     data={
                                                         "package_id":dataset['id'],
                                                         "url": "",
                                                         "name": service['serviceCode'] + "." + service['serviceVersion']
                                                     },
                                                     headers={"X-CKAN-API-Key": apikey },
                                                     files={'upload': ('service.wsdl',file(f.name))},
                                                     verify=False)
                            log.info(response.json())

                            os.unlink(f.name)
                        else:
                            return False
        '''
        log.info('Created dataset %s', dataset['name'])


        return result

    def _get_xroad_catalog(self, changed_after):
        url = "http://localhost:9090/rest-gateway-0.0.8-SNAPSHOT/Consumer/ListMembers"
        try:
            r = requests.get(url, params = {'changedAfter' : changed_after}, headers = {'Accept': 'application/json'})
        except ConnectionError:
            raise ContentFetchError("Calling XRoad service ListMembers failed!")
        if r.status_code != requests.codes.ok:
            raise ContentFetchError("Calling XRoad service ListMembers failed!")
        return r.json()

    def _parse_xroad_data(self, res):
        #return res.json()['ListMembersResponse']['memberList']['members']
        return res['ListMembersResponse']['memberList']['members']

    def _get_wsdl(self, external_id):
        url = "http://localhost:9090/rest-gateway-0.0.8-SNAPSHOT/Consumer/GetWsdl"
        try:
            r = requests.get(url, params = {'externalId' : external_id}, headers = {'Accept': 'application/json'})
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
                .filter(HarvestJob.source == harvest_job.source) \
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
                return job
    def _get_api_key(self):

        context = {'model': model,
           'ignore_auth': True,
        }

        site_user = p.toolkit.get_action('get_site_user')(context, {})

        return site_user['apikey']

class ContentFetchError(Exception):
    pass