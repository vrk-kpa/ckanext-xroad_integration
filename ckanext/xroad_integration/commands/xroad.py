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
