import click

from ckan.lib.cli import load_config, click_config_option, paster_click_group
from ckan.plugins.toolkit import get_action
import ckanext.xroad_integration.utils as utils


xroad_commands = paster_click_group(
        summary=u'X-Road related commands'
)


@xroad_commands.command(
    u'update_xroad_organizations',
    help='Updates harvested organizations\' metadata'
)
@click_config_option
@click.pass_context
def update_xroad_organizations(ctx, config):
    load_config(config or ctx.obj['config'])
    utils.update_xroad_organizations


@xroad_commands.command(
    u'fetch_errors',
    help='Fetches error log from catalog lister'
)
@click_config_option
@click.pass_context
def fetch_errors(ctx, config):
    load_config((config or ctx.obj['config']))
    utils.fetch_errors()


@xroad_commands.command(
    u'fetch_stats',
    help='Fetches X-Road stats from catalog lister'
)
@click_config_option
@click.pass_context
@click.option(u'--days', type=int)
def fetch_stats(ctx, config, days):
    load_config((config or ctx.obj['config']))

    utils.fetch_stats(days)


@xroad_commands.command(
    u'fetch_service_list',
    help='Fetches X-Road services from catalog lister'
)
@click_config_option
@click.pass_context
@click.option(u'--days', type=int)
def fetch_service_list(ctx, config, days):
    load_config((config or ctx.obj['config']))
    utils.fetch_service_list(days)


@xroad_commands.command(
    u'init_db',
    help="Initializes databases for xroad"
)
@click_config_option
@click.pass_context
def init_db(ctx, config):
    load_config((config or ctx.obj['config']))

    utils.init_db()
