"""Tests for plugin.py."""
from ckan import model
from ckan.plugins import toolkit
import pytest
import json
from ckanext.xroad_integration.harvesters.xroad_harvester import XRoadHarvesterPlugin
from ckantoolkit.tests.helpers import call_action
from ckanext.harvest.tests.lib import run_harvest

from .fixtures import xroad_rest_service_url, xroad_rest_adapter_url


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckan.plugins', 'harvest xroad_harvester')
def test_base(xroad_rest_adapter_mocks):

    results = run_harvest(
            url=xroad_rest_adapter_url('base'),
            harvester=XRoadHarvesterPlugin(),
            config=json.dumps({"force_all": True}))

    # Check that all subsystems were harvested
    assert ('TEST.ORG.000003-3.RemovedSubsystem' in results)
    assert ('TEST.MUN.000002-2.EmptySubsystem' in results)
    assert ('TEST.ORG.000003-3.EmptySubsystem' in results)
    assert ('TEST.ORG.000003-3.OneEmptyServiceSubsystem' in results)
    assert ('TEST.ORG.000003-3.LargeSubsystem' in results)

    # Check that the removed subsystem was removed correctly
    assert ('dataset' not in results['TEST.ORG.000003-3.RemovedSubsystem'])

    # Two subsystems with no services
    assert (len(results['TEST.MUN.000002-2.EmptySubsystem']['dataset']['resources']) == 0)
    assert (len(results['TEST.ORG.000003-3.EmptySubsystem']['dataset']['resources']) == 0)

    # Subsystem with one unknown type service
    assert (len(results['TEST.ORG.000003-3.OneEmptyServiceSubsystem']['dataset']['resources']) == 1)

    # Subsystem with two unknown type services, one soap service and one rest service
    assert (len(results['TEST.ORG.000003-3.LargeSubsystem']['dataset']['resources']) == 4)


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
    assert (to_be_removed_org.get('state') == 'active')

    results = run_harvest(url=xroad_rest_adapter_url('delete_one_of_each'), harvester=harvester)
    large = call_action('package_show', id='TEST.ORG.000003-3.LargeSubsystem')

    # FIXME: Unclear whether organizations should actually be removed
    # hould_be_removed_org = call_action('organization_show', id='one-wsdl-subsystem-organization')
    # assert(should_be_removed_org.get('state') == 'deleted')
    assert (results['TEST.ORG.000003-3.EmptySubsystem']['report_status'] == 'deleted')
    assert (set(r['name'] for r in large.get('resources', [])) == set(['openapiService.1', 'unknown', 'unknownWithVersion.1']))


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
def test_xroad_errors(xroad_rest_adapter_mocks, xroad_database_setup):
    call_action('fetch_xroad_errors')
    call_action('xroad_error_list')


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup', 'xroad_database_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address', xroad_rest_service_url('heartbeat'))
def test_xroad_heartbeat(xroad_rest_mocks):
    result = call_action('fetch_xroad_heartbeat')
    assert result['heartbeat'] is True
    assert result['success'] is True


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup', 'xroad_database_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('getOrganizationOrganizationData'))
def test_xroad_get_organizations_organization_data(xroad_rest_mocks):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester, config=json.dumps({"force_all": True}))
    user = toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})['name']
    context = {'model': model, 'session': model.Session, 'user': user, 'api_version': 3, 'ignore_auth': True}
    result = call_action('update_xroad_organizations', context=context)
    assert result['success'] is True
    assert result['message'] == 'Updated 4 organizations'
    updated_organization = call_action('organization_show', context=context, id='TEST.ORG.000000-0')
    assert updated_organization['title_translated']['fi'] == "Testiorganisaatio"
    assert updated_organization['title_translated']['sv'] == ""
    assert updated_organization['title_translated']['en'] == ""
    assert updated_organization['description_translated']['fi'] == "Tämä on testiorganisaatio"
    assert updated_organization['description_translated']['sv'] == ""
    assert updated_organization['description_translated']['en'] == ""
    assert updated_organization['webpage_address']['fi'] == "https://www.testiorganisaatio.fi"
    assert updated_organization['webpage_address']['sv'] == ""
    assert updated_organization['webpage_address']['en'] == ""
    assert updated_organization['webpage_description']['fi'] == "Testiorganisaation kotisivu"
    assert updated_organization['webpage_address']['sv'] == ""
    assert updated_organization['webpage_address']['en'] == ""


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup', 'xroad_database_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('getOrganizationCompanyData'))
def test_xroad_get_organizations_company_data(xroad_rest_mocks):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester, config=json.dumps({"force_all": True}))
    user = toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})['name']
    context = {'model': model, 'session': model.Session, 'user': user, 'api_version': 3, 'ignore_auth': True}
    result = call_action('update_xroad_organizations', context=context)
    assert result['success'] is True
    assert result['message'] == 'Updated 4 organizations'
    updated_organization = call_action('organization_show', context=context, id='TEST.ORG.000000-0')
    assert updated_organization['company_type']['fi'] == "Osakeyhtiö"
    assert updated_organization['company_type']['sv'] == "Aktiebolag"
    assert updated_organization['company_type']['en'] == "Limited company"
    assert updated_organization['postal_address'] == "Katu 14, 12345, ESPOO, FI"
    assert updated_organization['company_language']['fi'] == "Suomi"
    assert updated_organization['company_language']['sv'] == "Finska"
    assert updated_organization['company_language']['en'] == "Finnish"
    assert updated_organization['company_registration_date'] == "1993-03-19T00:00:00"
    assert updated_organization['old_business_ids'] == ""


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup', 'xroad_database_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address', xroad_rest_service_url('getOrganizationEmptyData'))
def test_xroad_get_organizations_empty_data(xroad_rest_mocks):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester, config=json.dumps({"force_all": True}))
    user = toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})['name']
    context = {'model': model, 'session': model.Session, 'user': user, 'api_version': 3, 'ignore_auth': True}
    result = call_action('update_xroad_organizations', context=context)
    assert result['success'] is True
    assert result['message'] == 'Updated 0 organizations'
