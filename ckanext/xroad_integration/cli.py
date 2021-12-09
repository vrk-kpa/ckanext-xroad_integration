# -*- coding: utf-8 -*-
from datetime import datetime

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


@xroad.command()
@click.option(u'--yes-i-am-sure')
def drop_db(yes_i_am_sure):
    """Removes tables created by init_db in the database.
    """
    if yes_i_am_sure:
        utils.drop_db()
        click.secho(u"DB tables dropped", fg=u"green")
    else:
        click.secho(u"This will delete all xroad data in the database! If you are sure, run this command with the --yes-i-am-sure option.", fg=u"yellow")


@xroad.command()
@click.pass_context
def update_xroad_organizations(ctx):
    'Updates harvested organizations\' metadata'
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.update_xroad_organization()


@xroad.command()
@click.pass_context
@click.option(u'--since')
def fetch_errors(ctx, since):
    """Fetches error log from catalog lister"""
    if since:
        try:
            datetime.strptime(since, "%Y-%m-%d")
        except ValueError:
            click.secho("Since dates should be given in format YYYY-MM-DD", fg="red")
            return

    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_errors(since)


@xroad.command()
@click.pass_context
@click.option(u'--days', type=int)
def fetch_stats(ctx, days):
    'Fetches X-Road stats from catalog lister'
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_stats(days)


@xroad.command()
@click.pass_context
@click.option(u'--days', type=int)
def fetch_distinct_service_stats(ctx, days):
    'Fetches X-Road distinct service stats from catalog lister'
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_distinct_service_stats(days)


@xroad.command()
@click.pass_context
@click.option(u'--days', type=int)
def fetch_service_list(ctx, days):
    'Fetches X-Road services from catalog lister'
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_service_list(days)


@xroad.command()
@click.pass_context
def fetch_heartbeat(ctx):
    'Fetches X-Road catalog heartbeat'
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_xroad_heartbeat()
