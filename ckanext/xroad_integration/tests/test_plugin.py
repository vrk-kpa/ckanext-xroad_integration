"""Tests for plugin.py."""

import xroad_mock.xroad_rest_adapter_mock

import pytest
import json
import os
from multiprocessing import Process
from ckanext.xroad_integration.harvesters.xroad_harvester import XRoadHarvesterPlugin
from ckantoolkit.tests.helpers import call_action
from ckanext.harvest.tests.lib import run_harvest


XROAD_REST_ADAPTER_HOST = '127.0.0.1'
XROAD_REST_ADAPTER_PORT = 9091
XROAD_REST_ADAPTER_URL = 'http://{host}:{port}/rest-adapter-service'.format(
        host=XROAD_REST_ADAPTER_HOST, port=XROAD_REST_ADAPTER_PORT)


@pytest.fixture(scope='module')
def xroad_rest_adapter_mock():
    data_path = os.path.join(os.path.dirname(__file__), 'test_listmembers.json')
    xroad_rest_adapter_mock_app = xroad_mock.xroad_rest_adapter_mock.instance(data_path)

    mock_proc = Process(target=xroad_rest_adapter_mock_app.run, kwargs={
        'host': XROAD_REST_ADAPTER_HOST,
        'port': XROAD_REST_ADAPTER_PORT})
    mock_proc.start()

    yield mock_proc

    mock_proc.terminate()
    mock_proc.join()


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckan.plugins', 'harvest xroad_harvester')
def test_full(xroad_rest_adapter_mock):

    results = run_harvest(url=XROAD_REST_ADAPTER_URL, harvester=XRoadHarvesterPlugin())

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
    assert(len(results['TEST.ORG.000003-3.OneEmptyServiceSubsystem']['dataset']['resources']) == 0)

    # Subsystem with two unknown type services, one soap service and one rest service
    assert(len(results['TEST.ORG.000003-3.LargeSubsystem']['dataset']['resources']) == 2)
