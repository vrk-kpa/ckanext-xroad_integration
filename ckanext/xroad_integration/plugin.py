import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import helpers

class Xroad_IntegrationPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'xroad_integration')

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
