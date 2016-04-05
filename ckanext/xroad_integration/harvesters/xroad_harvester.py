from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
import ckan.plugins as p
from ckan.lib.munge import munge_title_to_name


import logging
import json
import requests
import os
import tempfile

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

        #members = self.get_xroad_catalog(harvest_job.source.url, harvest_job.since_date)
        file = open(os.path.join(os.path.dirname(__file__), '../tests/catalog-mock.json'))
        members = json.load(file)

        object_ids = []
        for member in self._parse_xroad_data(members):
            log.info(json.dumps(member))


            # Generate GUID
            guid = str(member.get('instance', '')) + '.' + str(member.get('memberClass', '')) + '.' + str(member.get('memberCode', '')) + '.' + str(member.get('subsystemCode', ''))
            # Create harvest object
            obj = HarvestObject(guid=guid, job=harvest_job,
                                content=json.dumps(member))

            obj.save()
            object_ids.append(obj.id)

        return object_ids

    def fetch_stage(self, harvest_object):
        log.debug('Doing nothing in xroad harvester fetch stage')
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

        dataset['owner_org'] = local_org

        # Munge name
        dataset['title'] = dataset['name']
        dataset['name'] = munge_title_to_name(dataset['name'])


        result = self._create_or_update_package(dataset, harvest_object, package_dict_form='package_show')
        if result:
            subsystems = dataset.get('subsystems', None)
            if subsystems:
                services = subsystems['subsystem'].get('services', None)
                if services:
                    log.info(services['service'])
                    log.info( services['service']['serviceCode'] + '.' + services['service']['serviceVersion'])
                    if 'wsdl' in services['service'] and 'data' in services['service']['wsdl']:
                        log.info(services['service']['wsdl']['data'])
                        f = tempfile.NamedTemporaryFile(delete=False)
                        f.write(services['service']['wsdl']['data'])
                        f.close()
                        response = requests.post('https://0.0.0.0/api/action/resource_create',
                                                 data={
                                                     "package_id":dataset['id'],
                                                     "url": "",
                                                     "name": services['service']['serviceCode'] + "." + services['service']['serviceVersion']
                                                 },
                                                 headers={"X-CKAN-API-Key": '1cae37bb-eda1-4a20-aa74-53d522055a99' },
                                                 files={'upload': ('service.wsdl',file(f.name))}, verify=False)
                        log.info(response.json())

                        os.unlink(f.name)
                    else:
                        return False


        log.info('Created dataset %s', dataset['name'])


        return result

    def _get_xroad_catalog(self, changed_after):
        url = "http://localhost:9090/rest-gateway-0.0.8-SNAPSHOT/Consumer/ListMembers"
        r = requests.get(url, parameters = {'changedAfter' : changed_after}, headers = {'Accept': 'application/json'})
        if r.status_code != requests.codes.ok:
            raise HarvestGatherError(msg = "Calling XRoad service ListMembers failed!")
        return r.json()

    def _parse_xroad_data(self, res):
        #return res.json()['ListMembersResponse']['memberList']['members']
        return res['ListMembersResponse']['memberList']['members']

    def _get_wsdl(external_id):
        url = "http://localhost:9090/rest-gateway-0.0.8-SNAPSHOT/Consumer/GetWsdl"
        r = requests.get(url, parameters = {'externalId' : external_id}, headers = {'Accept': 'application/json'})
        if r.status_code != requests.codes.ok:
            raise HarvestGatherError(msg = "Calling XRoad service GetWsdl failed!")
        return r.json()
