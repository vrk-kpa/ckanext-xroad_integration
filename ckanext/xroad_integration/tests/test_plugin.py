"""Tests for plugin.py."""
from ckan import model
from ckan.plugins import toolkit
from ckanext.xroad_integration.tests.xroad_mock import xroad_rest_adapter_mock as adapter_mock
from ckanext.xroad_integration.tests.xroad_mock import xroad_rest_mock as rest_mock
import pytest
import json
import os
from multiprocessing import Process
from ckanext.xroad_integration.harvesters.xroad_harvester import XRoadHarvesterPlugin
from ckantoolkit.tests.helpers import call_action
from ckanext.harvest.tests.lib import run_harvest


XROAD_REST_ADAPTERS = {
        'base': {'host': '127.0.0.1', 'port': 9091, 'content': 'xroad-catalog-mock-responses/test_listmembers.json'},
        'delete_one_of_each': {'host': '127.0.0.1', 'port': 9092,
                               'content': 'xroad-catalog-mock-responses/test_delete_listmembers.json'},
        'get_organizations': {'host': '127.0.0.1', 'port': 9093,
                              'content': 'xroad-catalog-mock-responses/test_getorganizations.json'}
        }

XROAD_REST_SERVICES = {
    'heartbeat': {'host': '127.0.0.1', 'port': 9191, 'content': 'xroad-catalog-mock-responses/test_heartbeat.json'}
}


def xroad_rest_adapter_url(adapter_name):
    return 'http://{host}:{port}/rest-adapter-service'.format(**XROAD_REST_ADAPTERS[adapter_name])


def xroad_rest_service_url(service_name):
    return 'http://{host}:{port}/'.format(**XROAD_REST_SERVICES[service_name])


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


@pytest.fixture(scope='module')
def xroad_database_setup():
    from ckanext.xroad_integration.utils import init_db, drop_db

    init_db()

    yield

    drop_db()


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckan.plugins', 'harvest xroad_harvester')
def test_base(xroad_rest_adapter_mocks):

    results = run_harvest(
            url=xroad_rest_adapter_url('base'),
            harvester=XRoadHarvesterPlugin(),
            config=json.dumps({"force_all": True}))

    # Check that all subsystems were harvested
    assert('TEST.ORG.000003-3.RemovedSubsystem' in results)
    assert('TEST.MUN.000002-2.EmptySubsystem' in results)
    assert('TEST.ORG.000003-3.EmptySubsystem' in results)
    assert('TEST.ORG.000003-3.OneEmptyServiceSubsystem' in results)
    assert('TEST.ORG.000003-3.LargeSubsystem' in results)

    # Check that the removed subsystem was removed correctly
    assert('dataset' not in results['TEST.ORG.000003-3.RemovedSubsystem'])

    # Two subsystems with no services
    assert(len(results['TEST.MUN.000002-2.EmptySubsystem']['dataset']['resources']) == 0)
    assert(len(results['TEST.ORG.000003-3.EmptySubsystem']['dataset']['resources']) == 0)

    # Subsystem with one unknown type service
    assert(len(results['TEST.ORG.000003-3.OneEmptyServiceSubsystem']['dataset']['resources']) == 1)

    # Subsystem with two unknown type services, one soap service and one rest service
    assert(len(results['TEST.ORG.000003-3.LargeSubsystem']['dataset']['resources']) == 4)


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckan.plugins', 'harvest xroad_harvester')
def test_base_twice(xroad_rest_adapter_mocks):
    harvester = XRoadHarvesterPlugin()
    url = xroad_rest_adapter_url('base')
    config = json.dumps({"force_all": True})
    run_harvest(url=url, harvester=harvester, config=config)
    run_harvest(url=url, harvester=harvester, config=config)


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckan.plugins', 'harvest xroad_harvester')
def test_delete(xroad_rest_adapter_mocks):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester)

    to_be_removed_org = call_action('organization_show', id='one-wsdl-subsystem-organization')
    assert(to_be_removed_org.get('state') == 'active')

    results = run_harvest(url=xroad_rest_adapter_url('delete_one_of_each'), harvester=harvester)
    large = call_action('package_show', id='TEST.ORG.000003-3.LargeSubsystem')

    # FIXME: Unclear whether organizations should actually be removed
    # hould_be_removed_org = call_action('organization_show', id='one-wsdl-subsystem-organization')
    # assert(should_be_removed_org.get('state') == 'deleted')
    assert(results['TEST.ORG.000003-3.EmptySubsystem']['report_status'] == 'deleted')
    assert(set(r['name'] for r in large.get('resources', [])) == set(['openapiService.1', 'unknown', 'unknownWithVersion.1']))


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_gateway_address', xroad_rest_adapter_url('get_organizations'))
def test_update_xroad_organizations(xroad_rest_adapter_mocks):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester, config="{}")

    user = toolkit.get_action('get_site_user')(
        {'model': model, 'ignore_auth': True}, {}
    )['name']

    context = {'model': model, 'session': model.Session,
               'user': user, 'api_version': 3, 'ignore_auth': True}

    result = call_action('update_xroad_organizations', context=context)
    assert result['message'] == 'Updated 4 organizations'

    updated_organization = call_action('organization_show', context=context, id='TEST.ORG.000000-0')
    assert updated_organization['title_translated']['fi'] == "Empty organization in finnish"
    assert updated_organization['title_translated']['sv'] == "Empty organization in swedish"
    assert updated_organization['title_translated']['en'] == "Empty organization in english"

    assert updated_organization['email_address'] == ['othervalue@example.com', 'value@example.com']


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
def test_xroad_errors(xroad_rest_adapter_mocks, xroad_database_setup):
    call_action('fetch_xroad_errors')
    call_action('xroad_error_list')

@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address', xroad_rest_service_url('heartbeat'))
def test_xroad_heartbeat(xroad_rest_mocks, xroad_database_setup):
    result = call_action('fetch_xroad_heartbeat')
    assert result['heartbeat'] is True
    assert result['success'] is True
