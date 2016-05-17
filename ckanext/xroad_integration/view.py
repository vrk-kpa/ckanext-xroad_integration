import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import logging
from pprint import pformat

log = logging.getLogger(__name__)


class WSDL_ViewPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IResourceView, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        pass

    # IResourceView

    def info(self):
        log.warn('Xroad_IntegrationPlugin::info')
        return {'name': 'wsdl_view',
                'title': toolkit._('WSDL'),
                'icon': 'file-text-alt',
                'default_title': toolkit._('WSDL'),
                }

    def can_view(self, data_dict):
        log.warn('Xroad_IntegrationPlugin::can_view %s' % pformat(data_dict))
        resource = data_dict['resource']
        format_lower = resource.get('format', '').lower()
        return format_lower in ['wsdl', 'wsdl+xml']

    def view_template(self, context, data_dict):
        log.warn('Xroad_IntegrationPlugin::can_view %s' % pformat(data_dict))
        return 'wsdl_view.html'
