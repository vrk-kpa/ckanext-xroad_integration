
from datetime import datetime, timedelta
from typing import Union
from ckan.plugins import toolkit

import requests
import json


def xroad_subsystem_path(dataset_dict):
    field_names = ('xroad_instance', 'xroad_memberclass', 'xroad_membercode', 'xroad_subsystemcode')
    fields = [dataset_dict.get(field) for field in field_names]
    return '.'.join(fields) if all(fields) else None


XROAD_STATS_CACHE = None
XROAD_HISTORY_CACHE = None


def fetch_xroad_statistics(env: str = 'all', history: bool = True, return_json: bool = False,
                           cache_duration: timedelta = timedelta(hours=1)) -> Union[dict, str]:

    global XROAD_STATS_CACHE
    global XROAD_HISTORY_CACHE
    if history:
        xroad_cache = XROAD_HISTORY_CACHE
    else:
        xroad_cache = XROAD_STATS_CACHE

    if xroad_cache is None or datetime.now() - xroad_cache[0] > cache_duration:
        try:
            xroad_stats_api_base_url = 'https://api.stats.x-road.global/v1'
            fi_test_instance = 'FI-TEST'
            fi_prod_instance = 'FI'

            stats_collection = {}
            if env == fi_test_instance or env == 'all':
                stats_collection[fi_test_instance] = {}
                stats_collection[fi_test_instance]['stats'] = \
                    requests.get('{}/instances/{}'.format(xroad_stats_api_base_url, fi_test_instance)).json()
                if history:
                    stats_collection[fi_test_instance]['history'] = \
                        requests.get('{}/instances/{}/history'.format(xroad_stats_api_base_url, fi_test_instance)).json()
            if env == fi_prod_instance or env == 'all':
                stats_collection[fi_prod_instance] = {}
                stats_collection[fi_prod_instance]['stats'] = \
                    requests.get('{}/instances/{}'.format(xroad_stats_api_base_url, fi_prod_instance)).json()
                if history:
                    stats_collection[fi_prod_instance]['history'] = \
                        requests.get('{}/instances/{}/history'.format(xroad_stats_api_base_url, fi_prod_instance)).json()

        except Exception:
            # Fetch failed for some reason, keep old value until cache invalidates
            if xroad_cache is None:
                stats_collection = {}
            else:
                stats_collection = xroad_cache[1]

        xroad_stats_cache_timestamp = datetime.now()
        xroad_cache = (xroad_stats_cache_timestamp, stats_collection)
    else:
        xroad_stats_cache_timestamp, stats_collection = xroad_cache

    if return_json:
        return json.dumps(stats_collection)
    else:
        return stats_collection


def get_xroad_environment() -> str:
    return toolkit.config.get('ckanext.xroad_integration.xroad_environment')


def get_xroad_stats() -> list:
    return toolkit.get_action('xroad_stats')({}, {})


def get_xroad_distinct_services() -> list:
    return toolkit.get_action('xroad_distinct_service_stats')({}, {})
