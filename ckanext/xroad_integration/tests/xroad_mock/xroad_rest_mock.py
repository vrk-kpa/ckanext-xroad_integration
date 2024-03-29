from flask import Flask
import json
from io import open

app = Flask(__name__)


def create_app(input_file):
    app = Flask(__name__)
    mock_data = json.load(open(input_file, 'r', encoding='utf-8'))

    @app.route('/getListOfServices')
    def getListOfServices():
        return mock_data

    @app.route('/getServiceStatistics')
    def getServiceStatistics():
        return mock_data

    @app.route('/getDistinctServiceStatistics')
    def getDistinctServiceStatistics():
        return mock_data

    @app.route('/heartbeat')
    def heartbeat():
        return mock_data

    @app.route('/getOrganization/<business_code>')
    def getOrganization(business_code='000000-0'):
        return mock_data

    @app.route('/listErrors/<instance>/<code>/<member>')
    def list_errors(instance='TEST', code='000000-0', member='some_member'):
        return mock_data

    @app.route('/getRest/<instance>/<member_class>/<member_code>/<subsystem_code>/<service_code>')
    @app.route('/getRest/<instance>/<member_class>/<member_code>/<subsystem_code>/<service_code>/<service_version>')
    def getRest(instance, member_class, member_code, subsystem_code, service_code, service_version=None):
        result = mock_data.copy()
        for service in result.get('listOfServices', []):
            service['xroadInstance'] = instance
            service['memberClass'] = member_class
            service['memberCode'] = member_code
            service['subsystemCode'] = subsystem_code
            service['serviceCode'] = service_code
        return result

    return app


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('port', nargs='?', type=int, default=8088)
    args = parser.parse_args()
    app = create_app(args.input_file)
    app.run(port=args.port)
