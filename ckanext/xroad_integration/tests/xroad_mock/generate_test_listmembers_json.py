import json

#
# Utilities
#


def xroad_list(inner_name, contents):
    if len(contents) == 0:
        return ''
    elif len(contents) == 1:
        return {inner_name: contents[0]}
    else:
        return {inner_name: contents}


def insert_if_not_none(obj, key, value):
    if value is not None:
        obj[key] = value


#
# Create atoms
#

def create_memberlist(members=[]):
    return {'memberList': xroad_list('member', members)}


def create_member(xroad_instance, member_class, member_code, name, created, fetched, changed, removed=None, subsystems=[]):
    member = {'xRoadInstance': xroad_instance,
              'memberClass': member_class,
              'memberCode': member_code,
              'name': name,
              'created': created,
              'fetched': fetched,
              'changed': changed,
              'subsystems': xroad_list('subsystem', subsystems)}

    insert_if_not_none(member, 'removed', removed)

    return member


def create_subsystem(subsystem_code, created, fetched, changed, removed=None, services=[]):
    subsystem = {'subsystemCode': subsystem_code,
                 'created': created,
                 'fetched': fetched,
                 'changed': changed,
                 'services': xroad_list('service', services)}

    insert_if_not_none(subsystem, 'removed', removed)

    return subsystem


def create_service(service_code, created, fetched, changed, removed=None, service_version=None, wsdl=None, openapi=None):
    service = {'serviceCode': service_code,
               'created': created,
               'fetched': fetched,
               'changed': changed}

    insert_if_not_none(service, 'removed', removed)
    insert_if_not_none(service, 'serviceVersion', service_version)
    insert_if_not_none(service, 'wsdl', wsdl)
    insert_if_not_none(service, 'openapi', openapi)

    return service


def create_wsdl(external_id, created, fetched, changed):
    wsdl = {'externalId': external_id,
            'created': created,
            'fetched': fetched,
            'changed': changed}

    return wsdl


def create_openapi(external_id, created, fetched, changed):
    openapi = {'externalId': external_id,
               'created': created,
               'fetched': fetched,
               'changed': changed}

    return openapi


#
# Test constants
#

SOME_DATE = '2020-02-02T20:20:02.020+02:00'
SOME_LATER_DATE = '2020-02-20T02:02:20.202+02:00'

#
# Test data generators
#


def generate_unknown_service():
    return create_service('unknown', SOME_DATE, SOME_DATE, SOME_DATE)


def generate_removed_service():
    return create_service('removedService', SOME_DATE, SOME_DATE, SOME_DATE, removed=SOME_LATER_DATE)


def generate_unknown_service_with_version():
    return create_service('unknownWithVersion', SOME_DATE, SOME_DATE, SOME_DATE, service_version='1')


def generate_service_with_wsdl():
    return create_service('wsdlService', SOME_DATE, SOME_DATE, SOME_DATE, service_version=2,
                          wsdl=create_wsdl('some-wsdl-external-id', SOME_DATE, SOME_DATE, SOME_DATE))


def generate_service_with_openapi():
    return create_service('openapiService', SOME_DATE, SOME_DATE, SOME_DATE, service_version='1',
                          openapi=create_openapi('some-openapi-external-id', SOME_DATE, SOME_DATE, SOME_DATE))


def generate_empty_subsystem():
    return create_subsystem('EmptySubsystem', SOME_DATE, SOME_DATE, SOME_DATE)


def generate_removed_subsystem():
    return create_subsystem('RemovedSubsystem', SOME_DATE, SOME_DATE, SOME_DATE, removed=SOME_LATER_DATE)


def generate_subsystem_with_one_unknown_service():
    services = [
            generate_unknown_service()
            ]

    return create_subsystem('OneEmptyServiceSubsystem', SOME_DATE, SOME_DATE, SOME_DATE, services=services)


def generate_subsystem_with_various_services():
    services = [
            generate_unknown_service(),
            generate_removed_service(),
            generate_unknown_service_with_version(),
            generate_service_with_wsdl(),
            generate_service_with_openapi(),
            ]

    return create_subsystem('LargeSubsystem', SOME_DATE, SOME_DATE, SOME_DATE, services=services)


def generate_empty_member():
    return create_member('TEST', 'ORG', '000000-0', 'Empty organization', SOME_DATE, SOME_DATE, SOME_DATE)


def generate_removed_member():
    return create_member('TEST', 'GOV', '000001-1', 'Removed organization',
                         SOME_DATE, SOME_LATER_DATE, SOME_DATE, removed=SOME_LATER_DATE)


def generate_member_with_one_empty_subsystem():
    subsystems = [generate_empty_subsystem()]
    return create_member('TEST', 'MUN', '000002-2', 'One subsystem organization', SOME_DATE, SOME_DATE, SOME_DATE,
                         subsystems=subsystems)


def generate_member_with_one_wsdl_subsystem():
    subsystems = [
            create_subsystem('WsdlSubsystem', SOME_DATE, SOME_DATE, SOME_DATE, services=[
                generate_service_with_wsdl()
            ])
        ]
    return create_member('TEST', 'MUN', '00000442', 'One wsdl subsystem organization', SOME_DATE, SOME_DATE, SOME_DATE,
                         subsystems=subsystems)


def generate_member_with_various_subsystems():
    subsystems = [
            generate_empty_subsystem(),
            generate_removed_subsystem(),
            generate_subsystem_with_one_unknown_service(),
            generate_subsystem_with_various_services(),
            ]
    return create_member('TEST', 'ORG', '000003-3', 'Large organization', SOME_DATE, SOME_DATE, SOME_DATE,
                         subsystems=subsystems)


def generate_base_memberlist():
    members = [
            generate_empty_member(),
            generate_removed_member(),
            generate_member_with_one_empty_subsystem(),
            generate_member_with_one_wsdl_subsystem(),
            generate_member_with_various_subsystems(),
            ]
    memberlist = create_memberlist(members=members)
    return memberlist


def generate_delete_one_of_each_memberlist():
    return create_memberlist(members=[
                                 dict(generate_member_with_one_wsdl_subsystem(),
                                      fetched=SOME_LATER_DATE,
                                      changed=SOME_LATER_DATE,
                                      removed=SOME_LATER_DATE),
                                 create_member('TEST', 'ORG', '000003-3', 'Updated Large Organization',
                                               SOME_DATE, SOME_LATER_DATE, SOME_LATER_DATE,
                                               subsystems=[
                                                   dict(generate_empty_subsystem(), removed=SOME_LATER_DATE),
                                                   create_subsystem('LargeSubsystem', SOME_DATE, SOME_LATER_DATE,
                                                                    SOME_LATER_DATE, services=[
                                                                        dict(generate_service_with_wsdl(),
                                                                             fetched=SOME_LATER_DATE,
                                                                             changed=SOME_LATER_DATE,
                                                                             removed=SOME_LATER_DATE),
                                                                    ])
                                               ])
                             ])

#
# Main
#


if __name__ == '__main__':
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('generator', nargs='?', default='base')
    args = parser.parse_args()

    generators = {
            'base': generate_base_memberlist,
            'delete_one_of_each': generate_delete_one_of_each_memberlist,
            }
    generator = generators.get(args.generator)

    if generator:
        memberlist = generator()
        json.dump(memberlist, sys.stdout)
    else:
        sys.stderr.write('ERROR: No such generator: {}'.format(args.generator))
        sys.exit(-1)
