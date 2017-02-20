import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.lib.uploader as uploader
import logging
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
    ERROR_HTML = '<div class="error-explanation alert alert-error">%s</div>'

    def render(resource):
        try:
            resource_file = open_resource(resource)
            wsdl_content = etree.parse(resource_file)

            xml_namespaces = {
                    'soap-env': 'http://schemas.xmlsoap.org/soap/envelope/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema'}

            # FIXME: Should be handled at harvester, temporary cosmetic fix
            soap_faults = wsdl_content.xpath('//soap-env:Fault', namespaces=xml_namespaces)
            if len(soap_faults) > 0:
                try:
                    soap_fault = soap_faults[0]
                    faultcode = soap_fault.find('faultcode', namespaces=xml_namespaces).text
                    faultstring = soap_fault.find('faultstring', namespaces=xml_namespaces).text
                    error_form = ERROR_HTML % 'SOAP error harvesting WSDL: (faultcode: "%s", faultstring: "%s")'
                    error_message = error_form % (faultcode, faultstring)
                except:
                    error_message = ERROR_HTML % 'SOAP error harvesting WSDL'
                return error_message

            # FIXME: Remove XSD imports and includes in WSDL documents until dependency resolution can be made
            for element in wsdl_content.xpath('//xsd:import|//xsd:include', namespaces=xml_namespaces):
                element.getparent().remove(element)
            html_content = wsdl_to_html(wsdl_content)
            return etree.tostring(html_content, pretty_print=True, method='html', encoding=unicode)
        except urllib2.URLError:
            return ERROR_HTML % 'Invalid URL'
        except urllib2.HTTPError as e:
            return ERROR_HTML % 'HTTP error: %s' % e
        except etree.XMLSyntaxError as e:
            return ERROR_HTML % 'XML syntax error: %s' % e
        except OSError:
            return ERROR_HTML % 'Server error: uploaded file not found'

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
        return {'name': 'wsdl_view',
                'title': toolkit._('WSDL'),
                'icon': 'file-text-alt',
                'default_title': toolkit._('WSDL'),
                }

    def can_view(self, data_dict):
        resource = data_dict['resource']
        format_lower = resource.get('format', '').lower()
        return format_lower in ['wsdl', 'wsdl+xml']

    def view_template(self, context, data_dict):
        return 'wsdl_view.html'

    # ITemplateHelpers

    def get_helpers(self):
        return {'render_wsdl_resource': render_wsdl_resource(self.wsdl_to_html)}

    def setup_template_variables(self, context, data_dict):
        return {'resource': data_dict['resource']}
