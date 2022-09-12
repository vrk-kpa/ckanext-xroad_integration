# -*- coding: utf-8 -*-

import json
import jinja2
from ckan.lib import mailer

from datetime import datetime

import click

import ckanext.xroad_integration.utils as utils
from ckan.plugins import toolkit
get_action = toolkit.get_action


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
        utils.update_xroad_organizations()


@xroad.command()
@click.pass_context
@click.option(u'-s', u'--start-date', type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(u'-e', u'--end-date', type=click.DateTime(formats=["%Y-%m-%d"]))
def fetch_errors(ctx, start_date, end_date):
    """Fetches error log from catalog lister"""
    if end_date and not start_date:
        click.secho("Please give a start date to go with the end date", fg="red")
        return
    if start_date and end_date and end_date < start_date:
        click.secho("Start date cannot be later than end date", fg="red")
        return
    if start_date and start_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (start date cannot be later than current time)", fg="red")
        return
    if end_date and end_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (end date cannot be later than current time)", fg="red")
        return

    if start_date:
        start_date = datetime.strftime(start_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.strftime(end_date, "%Y-%m-%d")

    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        try:
            results = get_action('fetch_xroad_errors')({'ignore_auth': True},
                                                       {'start_date': start_date,
                                                        'end_date': end_date})
        except Exception as e:
            results = {'success': False, 'message': 'Exception: {}'.format(e)}

        success = results.get('success') is True
        get_action('xroad_batch_result_create')({'ignore_auth': True}, {'service': 'fetch_xroad_errors',
                                                                        'success': success,
                                                                        'message': results.get('message')})

        if success:
            for result in results.get('results', []):
                click.secho(result['message'], fg="green")

        else:
            click.secho(results['message'], fg="red")


@xroad.command()
@click.pass_context
@click.option(u'-s', u'--start-date', type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(u'-e', u'--end-date', type=click.DateTime(formats=["%Y-%m-%d"]))
def fetch_stats(ctx, start_date, end_date):
    'Fetches X-Road stats from catalog lister'
    if end_date and not start_date:
        click.secho("Please give a start date to go with the end date", fg="red")
        return
    if start_date and end_date and end_date < start_date:
        click.secho("Start date cannot be later than end date", fg="red")
        return
    if start_date and start_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (start date cannot be later than current time)", fg="red")
        return
    if end_date and end_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (end date cannot be later than current time)", fg="red")
        return

    if start_date:
        start_date = datetime.strftime(start_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.strftime(end_date, "%Y-%m-%d")

    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        data_dict = {"start_date": start_date,
                     "end_date": end_date}
        try:
            results = get_action('fetch_xroad_stats')({'ignore_auth': True}, data_dict)
        except Exception as e:
            results = {'success': False, 'message': 'Exception: {}'.format(e)}

        success = results.get('success') is True
        get_action('xroad_batch_result_create')({'ignore_auth': True}, {'service': 'fetch_xroad_stats',
                                                                        'success': success,
                                                                        'params': json.dumps(data_dict),
                                                                        'message': results.get('message')})

        if success:
            click.secho(results['message'], fg="green")

        else:
            click.secho(results['message'], fg="red")


@xroad.command()
@click.pass_context
@click.option(u'-s', u'--start-date', type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(u'-e', u'--end-date', type=click.DateTime(formats=["%Y-%m-%d"]))
def fetch_distinct_service_stats(ctx, start_date, end_date):
    'Fetches X-Road distinct service stats from catalog lister'
    if end_date and not start_date:
        click.secho("Please give a start date to go with the end date", fg="red")
        return
    if start_date and end_date and end_date < start_date:
        click.secho("Start date cannot be later than end date", fg="red")
        return
    if start_date and start_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (start date cannot be later than current time)", fg="red")
        return
    if end_date and end_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (end date cannot be later than current time)", fg="red")
        return

    if start_date:
        start_date = datetime.strftime(start_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.strftime(end_date, "%Y-%m-%d")
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        data_dict = {"start_date": start_date,
                     "end_date": end_date}

        try:
            results = get_action('fetch_distinct_service_stats')({'ignore_auth': True}, data_dict)
        except Exception as e:
            results = {'success': False, 'message': 'Exception: {}'.format(e)}

        success = results.get('success') is True
        get_action('xroad_batch_result_create')({'ignore_auth': True}, {'service': 'fetch_distinct_service_stats',
                                                                        'success': success,
                                                                        'params': json.dumps(data_dict),
                                                                        'message': results.get('message')})

        if success:
            click.secho(results['message'], fg="green")

        else:
            click.secho(results['message'], fg="red")


@xroad.command()
@click.pass_context
@click.option(u'-s', u'--start-date', type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(u'-e', u'--end-date', type=click.DateTime(formats=["%Y-%m-%d"]))
def fetch_service_list(ctx, start_date, end_date):
    'Fetches X-Road services from catalog lister'
    if end_date and not start_date:
        click.secho("Please give a start date to go with the end date", fg="red")
        return
    if start_date and end_date and end_date < start_date:
        click.secho("Start date cannot be later than end date", fg="red")
        return
    if start_date and start_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (start date cannot be later than current time)", fg="red")
        return
    if end_date and end_date > datetime.now():
        click.secho("We unfortunately cannot predict the future :( (end date cannot be later than current time)", fg="red")
        return

    if start_date:
        start_date = datetime.strftime(start_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.strftime(end_date, "%Y-%m-%d")

    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        data_dict = {"start_date": start_date,
                     "end_date": end_date}

        try:
            results = get_action('fetch_xroad_service_list')({'ignore_auth': True}, data_dict)
        except Exception as e:
            results = {'success': False, 'message': 'Exception: {}'.format(e)}

        success = results.get('success') is True
        get_action('xroad_batch_result_create')({'ignore_auth': True}, {'service': 'fetch_xroad_service_list',
                                                                        'success': success,
                                                                        'params': json.dumps(data_dict),
                                                                        'message': results.get('message')})

        if success:
            click.secho(results['message'], fg="green")

        else:
            click.secho("Error fetching service list!", fg="red")


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
