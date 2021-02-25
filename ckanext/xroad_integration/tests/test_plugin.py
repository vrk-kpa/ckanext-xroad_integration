"""Tests for plugin.py."""

import pytest
from ckanext.xroad_integration.harvesters.xroad_harvester import XRoadHarvesterPlugin
from ckantoolkit.tests.helpers import call_action
from ckanext.harvest.tests.lib import run_harvest

XROAD_REST_ADAPTER_URL = 'http://localhost:9091/rest-adapter-service'


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
@pytest.mark.ckan_config('ckan.plugins', 'harvest xroad_harvester')
def test_full():
    results = run_harvest(url=XROAD_REST_ADAPTER_URL,
                          harvester=XRoadHarvesterPlugin())

    assert('TEST.ORG.000003-3.RemovedSubsystem' in results)
    assert('TEST.MUN.000002-2.EmptySubsystem' in results)
    assert('TEST.ORG.000003-3.EmptySubsystem' in results)
    assert('TEST.ORG.000003-3.OneEmptyServiceSubsystem' in results)
    assert('TEST.ORG.000003-3.LargeSubsystem' in results)

    assert('dataset' not in results['TEST.ORG.000003-3.RemovedSubsystem'])
    assert(len(results['TEST.MUN.000002-2.EmptySubsystem']['dataset']['resources']) == 0)
    assert(len(results['TEST.ORG.000003-3.EmptySubsystem']['dataset']['resources']) == 0)
    assert(len(results['TEST.ORG.000003-3.OneEmptyServiceSubsystem']['dataset']['resources']) == 1)
    assert(len(results['TEST.ORG.000003-3.LargeSubsystem']['dataset']['resources']) == 4)
