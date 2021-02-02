# -*- coding: utf-8 -*-

from __future__ import print_function

import click

import ckanext.xroad_integration.utils as utils



def get_commands():
    return [xroad]


@click.group()
def xroad():
    """X-Road related commands.
    """
    pass


@xroad.command()
def init_db():
    """Creates the necessary tables in the database.
    """
    utils.init_db()
    click.secho(u"DB tables created", fg=u"green")