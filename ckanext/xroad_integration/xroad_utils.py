import os
from ckan.plugins import toolkit
from typing import Dict, Any, List, Union
from logging import getLogger
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from simplejson.scanner import JSONDecodeError

DEFAULT_TIMEOUT = 3  # seconds

log = getLogger(__name__)

# Type for json
Json = Union[Dict[str, "Json"], List["Json"], str, int, float, bool, None]


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


class ContentFetchError(Exception):
    pass


def xroad_catalog_query(service, params: List = None,
                        queryparams: Dict[str, Any] = None, content_type='application/json', accept='application/json',
                        pagination: Dict[str, str] = None):
    if params is None:
        params = []
    if queryparams is None:
        queryparams = {}

    xroad_catalog_address = toolkit.config.get('ckanext.xroad_integration.xroad_catalog_address', '')  # type: str
    xroad_catalog_certificate = toolkit.config.get('ckanext.xroad_integration.xroad_catalog_certificate')
    xroad_client_id = toolkit.config.get('ckanext.xroad_integration.xroad_client_id')
    xroad_client_certificate = toolkit.config.get('ckanext.xroad_integration.xroad_client_certificate')

    if not xroad_catalog_address.startswith('http'):
        log.warn("Invalid X-Road catalog url %s" % xroad_catalog_address)
        raise ContentFetchError("Invalid X-Road catalog url %s" % xroad_catalog_address)

    url = '{address}/{service}'.format(address=xroad_catalog_address, service=service)

    if pagination:
        queryparams['page'] = pagination['page']
        queryparams['limit'] = pagination['limit']

    for param in params:
        url += '/' + param

    headers = {'Accept': accept,
               'Content-Type': content_type,
               'X-Road-Client': xroad_client_id}

    certificate_args = {}
    if xroad_catalog_certificate and os.path.isfile(xroad_catalog_certificate):
        certificate_args['verify'] = xroad_catalog_certificate
    else:
        certificate_args['verify'] = False

    if xroad_client_certificate and os.path.isfile(xroad_client_certificate):
        certificate_args['cert'] = xroad_client_certificate

    return http.get(url, params=queryparams, headers=headers, **certificate_args)


def xroad_catalog_query_json(service, params: List = None, queryparams: Dict[str, Any] = None,
                             pagination: Dict[str, str] = None) -> Json:
    if params is None:
        params = []
    if queryparams is None:
        queryparams = {}
    response = xroad_catalog_query(service, params=params, queryparams=queryparams, pagination=pagination)
    if response.status_code == 204:
        log.warning("Received empty response for service %s", service)
        return
    elif response.status_code == 404:
        log.warning("Resource not found: %s/%s", service, '/'.join(params))
        return
    try:
        return response.json()
    except JSONDecodeError as e:
        raise ContentFetchError(f'Expected JSON: {e}')
