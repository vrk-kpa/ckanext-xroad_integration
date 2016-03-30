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
        #members = self.get_xroad_catalog("http://localhost:9090/rest-gateway-0.0.8-SNAPSHOT/Consumer/catalog", "2011-01-01")
        file = open(os.path.join(os.path.dirname(__file__), '../tests/catalog-mock.json'))
        members = json.load(file)

        object_ids = []
        for member in self._parse_xroad_data(members):
            log.info(json.dumps(member))


            # Generate GUID
            guid = member.get('memberCode')
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

        # Get existing dataset
        existing_dataset = None
        try:
            existing_dataset = p.toolkit.get_action('package_show')(context, {'id': str(dataset['memberCode'])})
        except NotFound:
            pass
        if existing_dataset:
            dataset['name'] = existing_dataset['name']
            dataset['id'] = existing_dataset['id']

            try:
                p.toolkit.get_action('package_update')(context, dataset)
            except p.toolkit.ValidationError, e:
                self._save_object_error('Update validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False

            log.info('Updated dataset %s', dataset['name'])

        else:
            # Local harvest source organization
            source_dataset = p.toolkit.get_action('package_show')(context, {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            dataset['owner_org'] = local_org

            # Munge name
            dataset['name'] = munge_title_to_name(dataset['name'])

            # Set id
            dataset['id'] = dataset['memberCode']
            try:
                p.toolkit.get_action('package_create')(context, dataset)
            except p.toolkit.ValidationError, e:
                log.info('Create validation Error: %s' % str(e.error_summary))
                self._save_object_error('Create validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False

            log.info('Created dataset %s', dataset['name'])


        return True

    def _get_xroad_catalog(self, url, changed_after):
        r = requests.get(url, parameters = {'changedAfter' : changed_after}, headers = {'Accept': 'application/json'})
        if r.status_code != requests.codes.ok:
            raise HarvestGatherError(msg = "Harvest failed!")
        return r.json()

    def _parse_xroad_data(self, res):
        #return res.json()['ListMembersResponse']['memberList']['members']
        return res['ListMembersResponse']['memberList']['members']
