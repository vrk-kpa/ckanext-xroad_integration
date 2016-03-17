from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters import HarvesterBase

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

        return []


    def fetch_stage(self, harvest_object):

        return True

    def import_stage(self, harvest_object):

        return True
