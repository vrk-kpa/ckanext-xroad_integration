from ckan.plugins.toolkit import get_action

def init_db():
    import ckan.model as model
    from ckanext.xroad_integration.model import init_table
    init_table(model.meta.engine)

def update_xroad_organizations():
    get_action('update_xroad_organizations')({'ignore_auth': True}, {})

def fetch_errors():
    results = get_action('fetch_xroad_errors')({'ignore_auth': True}, {})

    if results.get("success") is True:
        for result in results.get('results', []):
            print(result['message'])

    else:
        print(results['message'])


def fetch_stats(days):

    data_dict = {}
    if days:
        data_dict['days'] = days

    results = get_action('fetch_xroad_stats')({'ignore_auth': True}, data_dict)

    if results.get("success") is True:
        print(results['message'])

    else:
        print(results['message'])


def fetch_service_list(days):
    data_dict = {}
    if days:
        data_dict['days'] = days

    results = get_action('fetch_xroad_service_list')({'ignore_auth': True}, data_dict)

    if results.get("success") is True:
        print(results['message'])

    else:
        print("Error fetching service list!")