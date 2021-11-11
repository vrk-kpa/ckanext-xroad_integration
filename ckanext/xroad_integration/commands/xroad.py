import click

from ckan.lib.cli import load_config, click_config_option, paster_click_group
from ckan.plugins.toolkit import get_action
import ckan.plugins.toolkit as toolkit
import ckanext.xroad_integration.utils as utils
import ckan.lib.mailer as mailer
from datetime import datetime
import jinja2


xroad_commands = paster_click_group(
        summary=u'X-Road related commands'
)


@xroad_commands.command()
@click_config_option
@click.pass_context
def update_xroad_organizations(ctx, config):
    load_config(config or ctx.obj['config'])
    utils.update_xroad_organizations()


@xroad_commands.command()
@click_config_option
@click.pass_context
def fetch_errors(ctx, config):
    load_config((config or ctx.obj['config']))
    utils.fetch_errors()


@xroad_commands.command()
@click_config_option
@click.pass_context
@click.option(u'--days', type=int)
def fetch_stats(ctx, config, days):
    load_config((config or ctx.obj['config']))

    utils.fetch_stats(days)


@xroad_commands.command()
@click_config_option
@click.pass_context
@click.option(u'--days', type=int)
def fetch_distinct_service_stats(ctx, config, days):
    load_config((config or ctx.obj['config']))

    utils.fetch_distinct_service_stats(days)


@xroad_commands.command()
@click_config_option
@click.pass_context
@click.option(u'--days', type=int)
def fetch_list_errors(ctx, config, days):
    load_config((config or ctx.obj['config']))

    utils.fetch_list_errors(days)


@xroad_commands.command()
@click_config_option
@click.pass_context
@click.option(u'--days', type=int)
def fetch_service_list(ctx, config, days):
    load_config((config or ctx.obj['config']))
    utils.fetch_service_list(days)


@xroad_commands.command()
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


@xroad_commands.command()
@click_config_option
@click.pass_context
def init_db(ctx, config):
    load_config((config or ctx.obj['config']))

    utils.init_db()


@xroad_commands.command()
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


@xroad_commands.command()
@click.option(u'--dryrun', is_flag=True)
@click_config_option
@click.pass_context
def send_latest_batch_run_results_email(ctx, config, dryrun):
    load_config((config or ctx.obj['config']))
    results = utils.latest_batch_run_results()
    failed = [r for r in results if r.get('success') is not True]

    if not failed:
        print('Previous runs for all batch operations were successful')
        return

    email_notification_recipients = toolkit.aslist(toolkit.config.get('ckanext.apicatalog.harvester_status_recipients', ''))

    if not email_notification_recipients and not dryrun:
        print('No recipients configured')
        return

    site_title = toolkit.config.get('ckan.site_title', '')
    today = datetime.now().date().isoformat()

    columns = ['service', 'result', 'timestamp', 'message']
    rows = [[r.get('service'),
             'success' if r.get('success') else 'failure',
             r.get('timestamp'),
             r.get('message') or '']
            for r in results]

    jinja_env = jinja2.Environment(loader=jinja2.PackageLoader('ckanext.xroad_integration', 'templates'))
    template = jinja_env.get_template('email/batch_run_results.html')
    msg = template.render(columns=columns, rows=rows)

    for recipient in email_notification_recipients:
        email = {'recipient_name': '',
                 'recipient_email': recipient,
                 'subject': '%s - Batch run summary %s' % (site_title, today),
                 'body': msg,
                 'headers': {'Content-Type': 'text/html'}}

        if dryrun:
            print('to: %s' % recipient)
        else:
            try:
                mailer.mail_recipient(**email)
            except mailer.MailerException as e:
                print('Sending batch run results to %s failed: %s' % (recipient, e))

    if dryrun:
        print(msg)
