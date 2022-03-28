from flask import Flask
import json
from io import open

from datetime import datetime, timedelta

app = Flask(__name__)


def create_app(input_file):
    app = Flask(__name__)
    mock_data = json.load(open(input_file, 'r', encoding='utf-8'))

    @app.route('/getListOfServices/<int:days>')
    def getListOfServices(days=1):
        now = datetime.now()
        member_data = []
        for i in range(days):
            dt = now - timedelta(days=i)
            date = [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond]
            member_data.append({'date': date, 'memberDataList': mock_data['memberData'][0]['memberDataList']})

        data = {'memberData': member_data, 'securityServerData': mock_data['securityServerData']}
        return json.dumps(data)

    @app.route('/getServiceStatistics/<int:days>')
    def getServiceStatistics(days=1):
        return json.dumps({'serviceStatisticsList': []})

    @app.route('/heartbeat')
    def heartbeat():
        return mock_data

    @app.route('/getOrganization/<business_code>')
    def getOrganization(business_code='000000-0'):
        return mock_data

    return app


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('port', nargs='?', type=int, default=8088)
    args = parser.parse_args()
    app = create_app(args.input_file)
    app.run(port=args.port)
