from multiprocessing import Process

import os
import pytest

from ckanext.xroad_integration.tests.xroad_mock import xroad_rest_adapter_mock as adapter_mock
from ckanext.xroad_integration.tests.xroad_mock import xroad_rest_mock as rest_mock


XROAD_REST_ADAPTERS = {
    'base': {'host': '127.0.0.1', 'port': 9091, 'content': 'xroad-catalog-mock-responses/test_listmembers.json'},
    'delete_one_of_each': {'host': '127.0.0.1', 'port': 9092,
                           'content': 'xroad-catalog-mock-responses/test_delete_listmembers.json'}
}

XROAD_REST_SERVICES = {
    'heartbeat': {'host': '127.0.0.1', 'port': 9191, 'content': 'xroad-catalog-mock-responses/test_heartbeat.json'},
    'getOrganizationOrganizationData': {
            'host': '127.0.0.1',
            'port': 9192,
            'content': 'xroad-catalog-mock-responses/test_getorganizations_organization_data.json'},
    'getOrganizationCompanyData': {
        'host': '127.0.0.1',
        'port': 9193,
        'content': 'xroad-catalog-mock-responses/test_getorganizations_company_data.json'},
    'getOrganizationEmptyData': {
        'host': '127.0.0.1',
        'port': 9194,
        'content': 'xroad-catalog-mock-responses/test_getorganizations_empty_data.json'}
}


@pytest.fixture(scope='module')
def xroad_rest_adapter_mocks():
    procs = []

    for adapter in XROAD_REST_ADAPTERS.values():
        data_path = os.path.join(os.path.dirname(__file__), adapter['content'])
        xroad_rest_adapter_mock_app = adapter_mock.instance(data_path)

        mock_proc = Process(target=xroad_rest_adapter_mock_app.run, kwargs={
            'host': adapter['host'],
            'port': adapter['port']})
        mock_proc.start()
        procs.append(mock_proc)

    yield XROAD_REST_ADAPTERS.keys()

    for mock_proc in procs:
        mock_proc.terminate()
        mock_proc.join()


@pytest.fixture(scope='module')
def xroad_rest_mocks():
    procs = []

    for service in XROAD_REST_SERVICES.values():
        data_path = os.path.join(os.path.dirname(__file__), service['content'])
        xroad_rest_mock_app = rest_mock.create_app(data_path)

        mock_proc = Process(target=xroad_rest_mock_app.run, kwargs={
            'host': service['host'],
            'port': service['port']
        })
        mock_proc.start()
        procs.append(mock_proc)

    yield XROAD_REST_SERVICES.keys()

    for mock_proc in procs:
        mock_proc.terminate()
        mock_proc.join()


@pytest.fixture
def xroad_database_setup():
    import ckan.model as model
    from ckanext.xroad_integration.model import init_table
    init_table(model.meta.engine)


def xroad_rest_adapter_url(adapter_name):
    return 'http://{host}:{port}/rest-adapter-service'.format(**XROAD_REST_ADAPTERS[adapter_name])


def xroad_rest_service_url(service_name):
    return 'http://{host}:{port}'.format(**XROAD_REST_SERVICES[service_name])
