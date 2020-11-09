import click

from ckan.lib.cli import load_config, paster_click_group, click_config_option
from ckan.plugins.toolkit import get_action


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
    get_action('update_xroad_organizations')({'ignore_auth': True}, {})


@xroad_commands.command(
    u'fetch_errors',
    help='Fetches error log from catalog lister'
)
@click_config_option
@click.pass_context
def fetch_errors(ctx, config):
    load_config((config or ctx.obj['config']))
    results = get_action('fetch_xroad_errors')({'ignore_auth': True}, {})

    if results.get("success") is True:
        for result in results.get('results', []):
            print(result['message'])

    else:
        print(results['message'])




@xroad_commands.command(
    u'init_db',
    help="Initializes databases for xroad"
)
@click_config_option
@click.pass_context
def init_db(ctx, config):
    load_config((config or ctx.obj['config']))

    import ckan.model as model
    from ckanext.xroad_integration.model import init_table
    init_table(model.meta.engine)