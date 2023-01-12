"""Tests for plugin.py."""
from datetime import datetime
from ckanext.xroad_integration.model import XRoadServiceList, XRoadStat, XRoadDistinctServiceStat, XRoadError
from ckan import model
from ckan.plugins import toolkit
import pytest
import json
from ckanext.xroad_integration.harvesters.xroad_harvester import XRoadHarvesterPlugin
from ckantoolkit.tests.helpers import call_action
from ckanext.harvest.tests.lib import run_harvest
from .fixtures import xroad_rest_service_url, xroad_rest_adapter_url

import logging
log = logging.getLogger(__name__)


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckan.plugins', 'harvest xroad_harvester')
def test_base(xroad_rest_adapter_mocks):

    results = run_harvest(
        url=xroad_rest_adapter_url('base'),
        harvester=XRoadHarvesterPlugin(),
        config=json.dumps({"force_all": True}))

    # Check that all subsystems were harvested
    assert 'TEST.ORG.000003-3.RemovedSubsystem' in results
    assert 'TEST.MUN.000002-2.EmptySubsystem' in results
    assert 'TEST.ORG.000003-3.EmptySubsystem' in results
    assert 'TEST.ORG.000003-3.OneEmptyServiceSubsystem' in results
    assert 'TEST.ORG.000003-3.LargeSubsystem' in results

    # Check that the removed subsystem was removed correctly
    assert 'dataset' not in results['TEST.ORG.000003-3.RemovedSubsystem']

    # Two subsystems with no services
    assert len(results['TEST.MUN.000002-2.EmptySubsystem']['dataset']['resources']) == 0
    assert len(results['TEST.ORG.000003-3.EmptySubsystem']['dataset']['resources']) == 0

    # Subsystem with one unknown type service
    assert len(results['TEST.ORG.000003-3.OneEmptyServiceSubsystem']['dataset']['resources']) == 1

    # Subsystem with two unknown type services, one soap service and one rest service
    assert len(results['TEST.ORG.000003-3.LargeSubsystem']['dataset']['resources']) == 4


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
    assert to_be_removed_org.get('state') == 'active'

    results = run_harvest(url=xroad_rest_adapter_url('delete_one_of_each'), harvester=harvester)
    large = call_action('package_show', id='TEST.ORG.000003-3.LargeSubsystem')

    # FIXME: Unclear whether organizations should actually be removed
    # hould_be_removed_org = call_action('organization_show', id='one-wsdl-subsystem-organization')
    # assert(should_be_removed_org.get('state') == 'deleted')
    assert results['TEST.ORG.000003-3.EmptySubsystem']['report_status'] == 'deleted'
    assert set(r['name'] for r in large.get('resources', [])) == set(['openapiService.1', 'unknown', 'unknownWithVersion.1'])


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('get_list_errors_data'))
def test_xroad_errors(xroad_rest_adapter_mocks, xroad_rest_mocks, xroad_database_setup):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester)

    result = call_action('fetch_xroad_errors', start_date="2023-01-01", end_date="2023-01-05", limit=1)
    assert result['message'] == 'Fetched errors for 1 harvest sources'
    assert result['results'][0]['message'] == '12 errors stored to database.'

    db_entry_count = model.Session.query(XRoadError).count()
    assert db_entry_count == 12

    first = model.Session.query(XRoadError).first().as_dict()
    assert first['xroad_instance'] == 'FI-TEST'
    assert first['member_class'] == 'GOV'
    assert first['member_code'] == '1234567-8'
    assert first['subsystem_code'] == 'some_member'
    assert first['message'] == 'Fetch of REST services failed(url: https://somedomain/r1/' \
                               'FI-TEST/GOV/1234567-8/some_member/listMethods): 500 Server Error'


@pytest.mark.freeze_time('2022-01-02')
@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address', xroad_rest_service_url('getListOfServices'))
def test_fetch_xroad_service_list(xroad_rest_mocks, xroad_database_setup):
    result = call_action('fetch_xroad_service_list')
    sl = model.Session.query(XRoadServiceList).all()
    assert len(sl) == 1
    sl = sl[0].as_dict_full()
    assert result['message'] == 'Services from 2022-01-01 to 2022-01-01 stored in database.'
    assert len(sl['security_servers']) == 3
    org = sl.get("security_servers")[0]
    assert org["server_code"] == "Commercial"
    assert org["address"] == "example.com"
    assert org["member_class"] == "COM"
    assert org["member_code"] == "1234567-1"
    assert org["instance"] == "FI-TEST"


@pytest.mark.freeze_time('2022-01-07')
@pytest.mark.parametrize(
    "start_date, end_date, exp",
    [
        (
            "2022-01-01",
            None,
            'Services from 2022-01-01 to 2022-01-06 stored in database.'
        ),
        (
            None,
            "2022-01-05",
            'Please give start date to go with the end date'
        ),
        (
            "2022-01-02",
            "2022-01-06",
            'Services from 2022-01-02 to 2022-01-06 stored in database.'
        ),
        (
            "2022-01-10",
            "2022-01-11",
            'Start date cannot be in the future'
        ),
        (
            "2022-01-02",
            "2022-01-10",
            'End date cannot be in the future'
        ),
    ]
)
@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('getListOfServices'))
def test_fetch_xroad_service_list_with_date_ranges(xroad_rest_mocks,
                                                   xroad_database_setup, start_date, end_date, exp):
    result = call_action('fetch_xroad_service_list', {}, start_date=start_date, end_date=end_date)
    assert result['message'] == exp


@pytest.mark.freeze_time('2022-01-02')
@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address', xroad_rest_service_url('getServiceStatistics'))
def test_fetch_xroad_service_statistics(xroad_rest_mocks, xroad_database_setup):
    result = call_action('fetch_xroad_stats')
    stats = model.Session.query(XRoadStat).first()
    assert result['message'] == 'Statistics from 2022-01-01 to 2022-01-01 stored in database.'
    assert stats.date == datetime(2022, 1, 1, 0, 0)
    assert stats.soap_service_count == 1
    assert stats.rest_service_count == 1
    assert stats.openapi_service_count == 1


@pytest.mark.freeze_time('2022-01-07')
@pytest.mark.parametrize(
    "start_date, end_date, exp",
    [
        (
            "2022-01-01",
            None,
            'Statistics from 2022-01-01 to 2022-01-06 stored in database.'
        ),
        (
            None,
            "2022-01-05",
            'Please give start date to go with the end date'
        ),
        (
            "2022-01-02",
            "2022-01-06",
            'Statistics from 2022-01-02 to 2022-01-06 stored in database.'
        ),
        (
            "2022-01-10",
            "2022-01-11",
            'Start date cannot be in the future'
        ),
        (
            "2022-01-02",
            "2022-01-10",
            'End date cannot be in the future'
        ),
    ]
)
@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('getServiceStatistics'))
def test_fetch_xroad_service_statistics_with_date_ranges(xroad_rest_mocks,
                                                         xroad_database_setup, start_date, end_date, exp):
    result = call_action('fetch_xroad_stats', {}, start_date=start_date, end_date=end_date)
    assert result['message'] == exp


@pytest.mark.freeze_time('2022-01-02')
@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('getDistinctServiceStatistics'))
def test_fetch_xroad_distinct_service_statistics(xroad_rest_mocks, xroad_database_setup):
    result = call_action('fetch_distinct_service_stats')
    stats = model.Session.query(XRoadDistinctServiceStat).first()
    assert result['message'] == 'Distinct service statistics from 2022-01-01 to 2022-01-01 stored in database.'
    assert stats.date == datetime(2022, 1, 1, 0, 0)
    assert stats.distinct_service_count == 1


@pytest.mark.freeze_time('2022-01-07')
@pytest.mark.parametrize(
    "start_date, end_date, exp",
    [
        (
            "2022-01-01",
            None,
            "Distinct service statistics from 2022-01-01 to 2022-01-06 stored in database."
        ),
        (
            None,
            "2022-01-05",
            'Please give start date to go with the end date'
        ),
        (
            "2022-01-02",
            "2022-01-06",
            'Distinct service statistics from 2022-01-02 to 2022-01-06 stored in database.'
        ),
        (
            "2022-01-10",
            "2022-01-11",
            'Start date cannot be in the future'
        ),
        (
            "2022-01-02",
            "2022-01-10",
            'End date cannot be in the future'
        ),
    ]
)
@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('getDistinctServiceStatistics'))
def test_fetch_xroad_distinct_service_statistics_with_date_ranges(xroad_rest_mocks,
                                                                  xroad_database_setup, start_date, end_date, exp):
    result = call_action('fetch_distinct_service_stats', {}, start_date=start_date, end_date=end_date)
    assert result['message'] == exp


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
def test_xroad_get_organizations_company_data(xroad_rest_adapter_mocks, xroad_rest_mocks):
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
    assert 'old_business_ids' not in updated_organization


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup', 'xroad_database_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address',
                         xroad_rest_service_url('getOrganizationCompanyDataWithBusinessIdChanges'))
def test_xroad_get_organizations_company_data_with_business_id_changes(xroad_rest_adapter_mocks, xroad_rest_mocks):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester, config=json.dumps({"force_all": True}))
    user = toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})['name']
    context = {'model': model, 'session': model.Session, 'user': user, 'api_version': 3, 'ignore_auth': True}
    result = call_action('update_xroad_organizations', context=context)
    assert result['success'] is True
    assert result['message'] == 'Updated 4 organizations'
    updated_organization = call_action('organization_show', context=context, id='TEST.ORG.000000-0')

    assert updated_organization['old_business_ids'] == ['124567-8', '7654321-8']


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup', 'xroad_database_setup')
@pytest.mark.ckan_config('ckanext.xroad_integration.xroad_catalog_address', xroad_rest_service_url('getOrganizationEmptyData'))
def test_xroad_get_organizations_empty_data(xroad_rest_adapter_mocks, xroad_rest_mocks):
    harvester = XRoadHarvesterPlugin()
    run_harvest(url=xroad_rest_adapter_url('base'), harvester=harvester, config=json.dumps({"force_all": True}))
    user = toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})['name']
    context = {'model': model, 'session': model.Session, 'user': user, 'api_version': 3, 'ignore_auth': True}
    result = call_action('update_xroad_organizations', context=context)
    assert result['success'] is True
    assert result['message'] == 'Updated 0 organizations'
