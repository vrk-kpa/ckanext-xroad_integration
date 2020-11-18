import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic.auth.get import sysadmin

import helpers
from views import xroad
from logic import action

class Xroad_IntegrationPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IConfigurable)

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
            'fetch_xroad_stats': action.fetch_xroad_stats
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'fetch_xroad_errors': sysadmin,
            'xroad_error_list': sysadmin,
            'update_xroad_organizations': sysadmin,
            'fetch_xroad_stats': sysadmin
        }

    # IBlueprint
    def get_blueprint(self):
        return xroad.get_blueprints()