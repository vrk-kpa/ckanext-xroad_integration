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
def initdb():
    """Creates the necessary tables in the database.
    """
    utils.initdb()
    click.secho(u"DB tables created", fg=u"green")