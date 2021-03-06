# -*- coding: utf-8 -*-

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


@xroad.command(
    u'update_xroad_organizations',
    help='Updates harvested organizations\' metadata'
)
@click.pass_context
def update_xroad_organizations(ctx, config):
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.update_xroad_organization()

@xroad.command(
    u'fetch_errors',
    help='Fetches error log from catalog lister'
)
@click.pass_context
def fetch_errors(ctx, config):
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_errors()

@xroad.command(
    u'fetch_stats',
    help='Fetches X-Road stats from catalog lister'
)
@click.pass_context
@click.option(u'--days', type=int)
def fetch_stats(ctx, config, days):

    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_stats(days)

@xroad.command(
    u'fetch_service_list',
    help='Fetches X-Road services from catalog lister'
)
@click.pass_context
@click.option(u'--days', type=int)
def fetch_service_list(ctx, config, days):

    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.fetch_service_list(days)
