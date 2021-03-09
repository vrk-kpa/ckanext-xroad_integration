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
    utils.update_xroad_organizations()


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
    u'latest_batch_run_results',
    help='Prints the results of the latest batch runs'
)
@click_config_option
@click.pass_context
def latest_batch_run_results(ctx, config):
    load_config((config or ctx.obj['config']))
    results = utils.latest_batch_run_results()

    columns = ['service', 'result', 'timestamp', 'message']
    rows = [[r.get('service'),
             'success' if r.get('success') else 'failure',
             r.get('timestamp'),
             r.get('message') or '']
            for r in results]

    row_format = '{:<30} {:<10} {:<28} {}'
    print(row_format.format(*columns))
    print('\n'.join(row_format.format(*row) for row in rows))


@xroad_commands.command(
    u'init_db',
    help="Initializes databases for xroad"
)
@click_config_option
@click.pass_context
def init_db(ctx, config):
    load_config((config or ctx.obj['config']))

    utils.init_db()


@xroad_commands.command(
        u'drop_db',
        help='Removes tables created by init_db in the database.')
@click.option(u'--yes-i-am-sure/--no-i-am-not-sure', default=False)
@click_config_option
@click.pass_context
def drop_db(ctx, config, yes_i_am_sure):
    load_config((config or ctx.obj['config']))
    if yes_i_am_sure:
        utils.drop_db()
        click.secho(u"DB tables dropped", fg=u"green")
    else:
        click.secho(u"This will delete all xroad data in the database! If you are sure, run this command with the --yes-i-am-sure option.", fg=u"yellow")
