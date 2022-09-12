from ckan.plugins.toolkit import get_action
from datetime import datetime
import json


def init_db():
    import ckan.model as model
    from ckanext.xroad_integration.model import init_table
    init_table(model.meta.engine)


def drop_db():
    import ckan.model as model
    from ckanext.xroad_integration.model import drop_table
    drop_table(model.meta.engine)


def update_xroad_organizations():
    try:
        result = get_action('update_xroad_organizations')({'ignore_auth': True}, {})
    except Exception as e:
        result = {'success': False, 'message': 'Exception: {}'.format(e)}

    success = result.get('success') is True
    get_action('xroad_batch_result_create')({'ignore_auth': True}, {'service': 'update_xroad_organizations',
                                                                    'success': success,
                                                                    'message': result.get('message')})


def fetch_service_list(days):
    data_dict = {}
    if days:
        data_dict['days'] = days

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
        print(results['message'])

    else:
        print("Error fetching service list!")


def latest_batch_run_results():
    response = get_action('xroad_latest_batch_results')({'ignore_auth': True}, {})
    return response['results']


def fetch_xroad_heartbeat():
    try:
        result = get_action('fetch_xroad_heartbeat')({'ignore_auth': True}, {})

        if result.get('success') is True:
            print('Success:', result.get('heartbeat'))
        else:
            print('Error fetching heartbeat: %s', result.get('message', '(no message)'))
    except Exception as e:
        print('Error fetching heartbeat: \n', e)
