from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError

import logging
import json
import requests

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
        members = self.get_xroad_catalog("http://localhost:9090/rest-gateway-0.0.8-SNAPSHOT/Consumer/catalog", "2011-01-01")

        for member in self._parse_xroad_data(members):
            log.info(json.dumps(member))

        return []

    def fetch_stage(self, harvest_object):
        log.debug('Doing nothing in xroad harvester fetch stage')
        return True

    def import_stage(self, harvest_object):
        log.debug('In xroad havester import stage')
        return True

    def _get_xroad_catalog(self, url, changed_after):
        r = requests.get(url, parameters = {'changedAfter' : changed_after}, headers = {'Accept': 'application/json'})
        if r.status_code != requests.codes.ok:
            raise HarvestGatherError(msg = "Harvest failed!")
        return r.json()

    def _parse_xroad_data(self, res):
        return res.json()['ListMembersResponse']['memberList']['members']
