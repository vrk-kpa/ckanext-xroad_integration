from flask import Flask, send_file, jsonify, request
import json
import os
import six


def instance(data_path, app_name='xroad_rest_adapter_mock'):
    data = json.load(open(data_path, 'r'))
    app = Flask(app_name)

    @app.route('/rest-adapter-service/Consumer/ListMembers')
    def list_members():
        return send_file(data_path)

    @app.route('/rest-adapter-service/Consumer/IsProvider')
    def is_provider():
        xroad_instance = request.args.get('xRoadInstance')
        member_class = request.args.get('memberClass')
        member_code = request.args.get('memberCode')
        member = find_member(data, xroad_instance, member_class, member_code)
        subsystem_count = len(xroad_list_to_list(member, 'subsystems', 'subsystem'))
        return jsonify({'provider': subsystem_count > 0})

    @app.route('/rest-adapter-service/Consumer/GetWsdl')
    def get_wsdl():
        wsdl_path = os.path.join(os.path.dirname(__file__), 'service.wsdl')
        return jsonify({'wsdl': open(wsdl_path, 'r').read()})

    @app.route('/rest-adapter-service/Consumer/GetOpenAPI')
    def get_openapi():
        openapi_path = os.path.join(os.path.dirname(__file__), 'petstore.json')
        return jsonify({'openapi': open(openapi_path, 'r').read()})

    @app.route('/rest-adapter-service/Consumer/IsSoapService')
    def is_soap_service():
        xroad_instance = request.args.get('xRoadInstance')
        member_class = request.args.get('memberClass')
        member_code = request.args.get('memberCode')
        subsystem_code = request.args.get('subsystemCode')
        service_code = request.args.get('serviceCode')
        service_version = parse_service_version(request.args.get('serviceVersion'))

        member = find_member(data, xroad_instance, member_class, member_code)
        subsystem = find_subsystem(member, subsystem_code)
        service = find_service(subsystem, service_code, service_version)

        if service is None:
            print('No such service: {}.{}.{}.{}.{}.{}'.format(xroad_instance, member_class, member_code,
                                                              subsystem_code, service_code, service_version))
            return jsonify({'soap': False})

        return jsonify({'soap': 'wsdl' in service})

    @app.route('/rest-adapter-service/Consumer/IsRestService')
    def is_rest_service():
        xroad_instance = request.args.get('xRoadInstance')
        member_class = request.args.get('memberClass')
        member_code = request.args.get('memberCode')
        subsystem_code = request.args.get('subsystemCode')
        service_code = request.args.get('serviceCode')
        service_version = parse_service_version(request.args.get('serviceVersion'))

        member = find_member(data, xroad_instance, member_class, member_code)
        subsystem = find_subsystem(member, subsystem_code)
        service = find_service(subsystem, service_code, service_version)

        if service is None:
            print('No such service: {}.{}.{}.{}.{}.{}'.format(xroad_instance, member_class, member_code,
                                                              subsystem_code, service_code, service_version))
            return jsonify({'rest': False})

        return jsonify({'rest': 'openapi' in service})

    @app.route('/rest-adapter-service/Consumer/GetServiceType')
    def get_service_type():
        xroad_instance = request.args.get('xRoadInstance')
        member_class = request.args.get('memberClass')
        member_code = request.args.get('memberCode')
        subsystem_code = request.args.get('subsystemCode')
        service_code = request.args.get('serviceCode')
        service_version = parse_service_version(request.args.get('serviceVersion'))

        member = find_member(data, xroad_instance, member_class, member_code)
        subsystem = find_subsystem(member, subsystem_code)
        service = find_service(subsystem, service_code, service_version)

        if service is None:
            print('No such service: {}.{}.{}.{}.{}.{}'.format(xroad_instance, member_class, member_code,
                                                              subsystem_code, service_code, service_version))
            return jsonify({'type': None})

        if 'wsdl' in service:
            service_type = 'soap'
        elif 'openapi' in service:
            service_type = 'rest'
        else:
            service_type = None

        return jsonify({'type': service_type})

    @app.route('/rest-adapter-service/Consumer/HasCompanyChanged')
    def has_company_changed():
        # business_code = request.args.get('businessId')
        # changed_after = request.args.get('changedAfter')
        return jsonify({'result': True})

    @app.route('/rest-adapter-service/Consumer/HasOrganizationChanged')
    def has_organization_changed():
        # business_code = request.args.get('guid')
        # changed_after = request.args.get('changedAfter')
        return jsonify({'result': True})

    @app.route('/rest-adapter-service/Consumer/GetOrganizations')
    def get_organizations():
        # business_code = request.args.get('businessCode')
        return send_file(data_path)

    @app.route('/rest-adapter-service/Consumer/GetCompanies')
    def get_companies():
        # business_code = request.args.get('businessId')
        return jsonify({'companyList': {'company': {}}})

    @app.route('/rest-adapter-service/Consumer/GetErrors')
    def get_errors():
        # since = request.args.get('since')
        return jsonify({'errorLogList': {'errorLog': []}})

    def find_member(data, xroad_instance, member_class, member_code):
        return next((m for m in xroad_list_to_list(data, 'memberList', 'member')
                    if m.get('xRoadInstance') == xroad_instance
                    and m.get('memberClass') == member_class
                    and m.get('memberCode') == member_code),
                    None)

    def find_subsystem(member, subsystem_code):
        if member is None:
            return None

        return next((s for s in xroad_list_to_list(member, 'subsystems', 'subsystem')
                    if s.get('subsystemCode') == subsystem_code),
                    None)

    def find_service(subsystem, service_code, service_version):
        if subsystem is None:
            return None

        return next((s for s in xroad_list_to_list(subsystem, 'services', 'service')
                    if s.get('serviceCode') == service_code
                    and (parse_service_version(s.get('serviceVersion')) == service_version
                         or (not s.get('serviceVersion') and not service_version))),
                    None)

    def xroad_list_to_list(obj, key1, key2):
        maybe_list = (obj.get(key1) or {}).get(key2) or []
        if type(maybe_list) is list:
            return maybe_list
        elif type(maybe_list) is dict:
            return [maybe_list]
        else:
            return []

    def parse_service_version(v):
        if v is None:
            return v
        elif type(v) in (str, six.text_type):
            return v
        elif type(v) is int:
            return six.text_type('{}.0'.format(v))
        elif type(v) is float:
            return six.text_type(v)
        else:
            raise Exception('Unexpected service version type: {}'.format(repr(type(v))))

    return app


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('port', nargs='?', type=int, default=9091)
    args = parser.parse_args()
    app = instance(args.input_file)
    app.run(port=args.port)
