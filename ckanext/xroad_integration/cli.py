# -*- coding: utf-8 -*-

import jinja2
from ckan.lib import mailer

from datetime import datetime

import click

import ckanext.xroad_integration.utils as utils
from ckan.plugins import toolkit


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
        click.secho(u"This will delete all xroad data in the database! If you are sure, "
                    u"run this command with the --yes-i-am-sure option.", fg=u"yellow")


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


@xroad.command()
def latest_batch_run_results():
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


@xroad.command()
@click.option(u'--dryrun', is_flag=True)
def send_latest_batch_run_results_email(dryrun):
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
