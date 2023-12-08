# -*- coding: utf-8 -*-
import pytest


pytest_plugins = [
    u'ckanext.harvest.tests.fixtures',
    u'ckanext.xroad_integration.tests.fixtures'
]


@pytest.fixture
def clean_db(reset_db, migrate_db_for):
    reset_db()
    migrate_db_for("harvest")