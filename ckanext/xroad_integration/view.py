import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.lib.uploader as uploader
import logging
from pprint import pformat
import lxml.etree as etree
import urllib2
import os
import os.path
import mimetypes

log = logging.getLogger(__name__)


def open_resource(resource):
    if resource.get('url_type') == 'upload':
        upload = uploader.ResourceUpload(resource)
        filepath = upload.get_path(resource['id'])
        return open(filepath)
    else:
        return urllib2.urlopen(resource['url'])


def render_wsdl_resource(wsdl_to_html):
    def render(resource):
        try:
            resource_file = open_resource(resource)
            wsdl_content = etree.parse(resource_file)

            # FIXME: Remove XSD imports in WSDL documents until dependency resolution can be made
            for element in wsdl_content.xpath('//xsd:import', namespaces={'xsd': 'http://www.w3.org/2001/XMLSchema'}):
                element.getparent().remove(element)
            html_content = wsdl_to_html(wsdl_content)
            return etree.tostring(html_content, pretty_print=True, method='html', encoding='utf-8')
        except urllib2.URLError:
            return '<div class="error-explanation alert alert-error">Invalid URL</div>'
        except urllib2.HTTPError as e:
            return '<div class="error-explanation alert alert-error">HTTP error: %s</div>' % e
        except etree.XMLSyntaxError as e:
            return '<div class="error-explanation alert alert-error">XML syntax error: %s</div>' % e
        except OSError:
            return '<div class="error-explanation alert alert-error">Server error: uploaded file not found</div>'

    return render


class WSDL_ViewPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IResourceView, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'wsdl_view')
        relpath = "./xslt/wsdl_to_html.xslt"
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), relpath)
        self.wsdl_to_html = etree.XSLT(etree.parse(path))
        mimetypes.add_type('wsdl', '.wsdl')

    # IResourceView

    def info(self):
        log.warn('WSDL_ViewPlugin::info')
        return {'name': 'wsdl_view',
                'title': toolkit._('WSDL'),
                'icon': 'file-text-alt',
                'default_title': toolkit._('WSDL'),
                }

    def can_view(self, data_dict):
        log.warn('WSDL_ViewPlugin::can_view %s' % pformat(data_dict))
        resource = data_dict['resource']
        format_lower = resource.get('format', '').lower()
        return format_lower in ['wsdl', 'wsdl+xml']

    def view_template(self, context, data_dict):
        log.warn('WSDL_ViewPlugin::can_view %s' % pformat(data_dict))
        return 'wsdl_view.html'

    # ITemplateHelpers

    def get_helpers(self):
        return {'render_wsdl_resource': render_wsdl_resource(self.wsdl_to_html)}

    def setup_template_variables(self, context, data_dict):
        return {'resource': data_dict['resource']}
