import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.plugins import DefaultTranslation
from ckan.logic.auth.get import sysadmin

import ckanext.xroad_integration.helpers as helpers
from ckanext.xroad_integration.views import xroad
from ckanext.xroad_integration.logic import action
from ckanext.xroad_integration.auth import xroad_error_list

import ckanext.xroad_integration.cli as cli


class Xroad_IntegrationPlugin(plugins.SingletonPlugin, DefaultTranslation):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.ITranslation)

    if toolkit.check_ckan_version(min_version="2.9"):
        plugins.implements(plugins.IClick)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'xroad_integration')

    # IConfigurable

    def configure(self, config):
        xroad_catalog_address = toolkit.config.get('ckanext.xroad_integration.xroad_catalog_address')
        xroad_client_id = toolkit.config.get('ckanext.xroad_integration.xroad_client_id')

        if not (xroad_catalog_address or xroad_client_id):
            raise Exception("X-Road catalog has not been configured.")

    # IResourceController

    def after_create(self, context, resource):
        self.set_package_private_if_invalid(context, resource)

    def after_update(self, context, resource):
        self.set_package_private_if_invalid(context, resource)

    def set_package_private_if_invalid(self, context, resource):
        validity = resource.get('extras', {}).get('valid_content', None)
        if validity is False:
            package_id = resource['package_id']

            data_dict = toolkit.get_action('package_show')(context, {'id': package_id})
            toolkit.get_action('package_update')(
                    dict(context, allow_state_change=True),
                    dict(data_dict, private=True))

    # ITemplateHelpers

    def get_helpers(self):
        return {'xroad_subsystem_path': helpers.xroad_subsystem_path}

    # IActions

    def get_actions(self):
        return {
            'update_xroad_organizations': action.update_xroad_organizations,
            'fetch_xroad_errors': action.fetch_xroad_errors,
            'xroad_error_list': action.xroad_error_list,
            'fetch_xroad_stats': action.fetch_xroad_stats,
            'xroad_stats': action.xroad_stats,
            'fetch_xroad_service_list': action.fetch_xroad_service_list,
            'xroad_service_list': action.xroad_service_list,
            'xroad_batch_result_create': action.xroad_batch_result_create,
            'xroad_latest_batch_results': action.xroad_latest_batch_results,
            'fetch_xroad_heartbeat': action.fetch_xroad_heartbeat,
            'xroad_heartbeat': action.xroad_heartbeat,
            'xroad_heartbeat_history': action.xroad_heartbeat_history,
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'fetch_xroad_errors': sysadmin,
            'xroad_error_list': xroad_error_list,
            'update_xroad_organizations': sysadmin,
            'fetch_xroad_stats': sysadmin,
            'xroad_stats': sysadmin,
            'fetch_xroad_service_list': sysadmin,
            'xroad_service_list': sysadmin,
            'xroad_batch_result': sysadmin,
            'fetch_xroad_heartbeat': sysadmin,
            'xroad_heartbeat': sysadmin,
        }

    # IBlueprint
    def get_blueprint(self):
        return xroad.get_blueprints()

    # IClick

    def get_commands(self):
        return cli.get_commands()
