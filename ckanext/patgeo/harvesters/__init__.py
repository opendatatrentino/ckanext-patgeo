# CKAN Harvester per Geodati
# coding: utf-8

import os
import logging

from hashlib import sha1

try:
    import simplejson as json
except ImportError:
    import json

import requests
import datetime
import libxml2 # FIXME: port to lxml
import lxml.html
import re
import zipfile
import shutil

from urllib2 import urlparse

from tempfile import mkstemp, mkdtemp

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

from ckan.logic import get_action
from ckan import model

from .ogr2reclinejs import OGR2Reclinejs, ProjectionException

tags_remove = [
    'rndt', 'siat', 'pup', 'db prior 10k', 'pup; rndt', 'inquadramenti di base',
    'suap', 'scritte', 'pupagri', 'pupasc', 'pupbos',
]

tags_subs = {
        u'bosc' : u'boschi',
        u'comun' : u'comuni',
        u'siti archeolog' : u'siti archeologici',
        u'archeolog' : u'archeologia',
        u'specchio d\'acqua' : u'specchi d\'acqua',
        u'tratte': u'tratte ferroviarie',
        u'viabilità di progetto': u'viabilità',
        u'viabilità ferroviaria': u'viabilità',
        u'viafer': u'viabilità',
        u'viabilit': u'viabilità',
        u'viabilità forestale': u'viabilità',
        u'zps': u'zone protezione speciale',
        u'udf': u'distretti forestali',
        u'uffici distrettuali forestali': u'distretti forestali',
        u'pascolo' : u'pascoli',
        u'idrografici' : u'idrografia',
        }

def clean_tags(taglist):
    """
    Tags are only alphanum with '_-.'
    """
    tags = []
    for word in (tag.lower().replace('  ', ' ') for tag in taglist):
        # split on ","
        for cleaned in (w.strip() for w in word.split(',')):
            if cleaned in tags_remove:
                continue
            tag = tags_subs.get(cleaned, cleaned)
            if len(tag) > 1:
                # "'" are not accepted by ckan
                tag = tag.replace("'", " ")
                try:
                    tag = tag.decode('utf8')
                except UnicodeEncodeError:
                    pass
                # remove scale
                if u'1:' in tag:
                    continue
                tags.append(tag)
    return tags

def _post_multipart(self, selector, fields, files):
    '''Post fields and files to an http host as multipart/form-data.

    :param fields: a sequence of (name, value) tuples for regular form
        fields
    :param files: a sequence of (name, filename, value) tuples for data to
        be uploaded as files

    :returns: the server's response page

    '''

    from urlparse import urljoin, urlparse

    content_type, body = self._encode_multipart_formdata(fields, files)

    headers = self._auth_headers()
    url = urljoin(self.base_location + urlparse(self.base_location).netloc, selector)
    req = requests.post(url, data=dict(fields), files={files[0][0]: files[0][1:]}, headers=headers)

    # requests annoying API change
    try:
        # requests==0.14
        err = req.error
    except AttributeError:
        err = req.reason

    return req.status_code, err, req.headers, req.text


import ckanclient

# FIXME: no monkey patching here
ckanclient.CkanClient._post_multipart = _post_multipart

log = logging.getLogger(__name__)

# patched ckanclient functions for upload

CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB

PORTAL_ROOT = 'http://www.territorio.provincia.tn.it/portal/'
SEARCH_FORM = PORTAL_ROOT + 'server.pt/community/sgc_-_geocatalogo/862/sgc_-_geocatalogo/32157'
GATEWAY = PORTAL_ROOT + 'server.pt/gateway/PTARGS_0_18720_2521_862_0_43/http%3B/172.20.3.95%3B8380/geoportlet/'
FETCH_INDEX = GATEWAY + 'srv/it/main.present.embedded?from=1&to=160'
USER_AGENT = 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1'

def extract_page_metadata(page_content):
    dom = lxml.html.fromstring(page_content)
    link_re = re.compile('getGatewayedAction\(\'([^\']+)\'\)')
    for tr in dom.cssselect('tr'):
        h1_list = tr.cssselect('h1')
        if not h1_list: continue
        title       = h1_list[0].text.strip().replace('\n', '')
        description = tr.cssselect('h2')[0].text.strip().replace('\n', '')
        meta_clk    = tr.cssselect('.button' )[0].get('onclick')
        xml_url     = tr.cssselect('.button1')[0].get('href').strip()
        zip_url     = tr.cssselect('.button1')[1].get('href').strip()
        rdf_url     = tr.cssselect('.button1')[2].get('href').strip()
        curator     = tr.cssselect('span')[0].text.strip()
        tags        = tr.cssselect('span')[1].text.strip()
        license_src = tr.cssselect('img')[1].get('src')
        if tags:
            tags = ','.join( map(lambda(tag): tag.strip(), tags.split(',') ) )
        meta_url = GATEWAY + link_re.findall(meta_clk)[0]
        license  = license_src[ license_src.rfind('/') + 1: license_src.rfind('.') ]
        yield {
            'title'       : title,
            'description' : description,
            'meta_clk'    : meta_clk,
            'xml_url' : xml_url,
            'zip_url' : zip_url,
            'rdf_url' : rdf_url,
            'curator' : curator,
            'tags'    : tags.split(','),
            'meta_url': meta_url,
            'license' : license
        }


def download_index():
    import mechanize
    import cookielib

    br = mechanize.Browser()
    cj = cookielib.LWPCookieJar()
    br.set_cookiejar(cj)
    br.addheaders = [('User-agent', USER_AGENT)]

    br.open(SEARCH_FORM)
    br.select_form('search')
    br.submit()

    r2 = br.open(FETCH_INDEX)

    return r2.read()

def extract_metadata(xml_file):
    XPATH_RULES = os.path.join(os.path.dirname(__file__), 'xpath_rules.lst')
    # Extract metadata JSON
    dom = libxml2.parseFile(xml_file)
    ctxt = dom.xpathNewContext()
    ctxt.xpathRegisterNs("gmd", "http://www.isotc211.org/2005/gmd")
    ctxt.xpathRegisterNs("gml", "http://www.opengis.net/gml/3.2")
    ctxt.xpathRegisterNs("gco", "http://www.isotc211.org/2005/gco")
    ctxt.xpathRegisterNs("xlink", "http://www.w3.org/1999/xlink")

    # Compose metadata
    metadata = {}
    with open(XPATH_RULES, 'r') as rules:
        for rule in rules:
            try:
                desc, xpath = rule.split('|')
                matches = ctxt.xpathEval(xpath)
                for match in matches:
                    metadata[desc.decode('utf-8')] = match.content.strip().decode('utf-8')
            except Exception as e:
                log.debug('ERROR while processing line [%s] %s', (rule, e))

    data = metadata.pop('Informazioni di Identificazione: Data')
    year, month, day = [int(i) for i in data.split('-')]
    data = datetime.datetime(year, month, day).isoformat()


    meta_constant = {
        u'Titolare' : 'Provincia Autonoma di Trento',
        u'Codifica Caratteri': metadata.pop('Informazioni di Identificazione: Set dei caratteri dei metadati'),
        u'Copertura Temporale (Data di inizio)' : data,
        u'Copertura Temporale (Data di fine)' : '',
        u'Data di pubblicazione' : data,
        u'Data di aggiornamento' : data,
        u'Aggiornamento': 'Non programmato',
        u'Data di creazione' : data,
        u'URL sito': metadata.pop("Informazioni di Identificatione: Punto di Contatto: Risorsa Online"),
    }
    metadata.update(meta_constant)
    return metadata

def unzip(file, out_dir):
    zfile = zipfile.ZipFile(file)
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    for name in zfile.namelist():
        fd = open(out_dir + '/' + name, "w")
        fd.write(zfile.read(name))
        fd.close()

def prunedir(path):
    log.debug('Removing directory %s', path)
    if not path in ['/', '/home', '/home/marco']: # FIXME: do something smarter
        shutil.rmtree(path)
    else:
        log.error('Attempt to prune a vital directory!')

def zip_decompress(zip_file):
    # Decompress Zip
    decompressed_zip_dir = mkdtemp(suffix='_unzip')
    work_dir = mkdtemp()
    unzip(zip_file, decompressed_zip_dir)

    # Convert zip/shp files to CSV
    csv_file_list = []
    csv_file = None # TODO: manage more than once
    for file in os.listdir(decompressed_zip_dir):
        if file.endswith(".shp"):
            shp_file = decompressed_zip_dir + '/' + file
            log.debug('Converting file ... %s', shp_file)
            try:
                ogr2reclinejs = OGR2Reclinejs(shp_file, True)
                ogr2reclinejs.conversion(work_dir)
                csv_file = os.path.join(work_dir, file.replace('.shp', '.csv'))
                csv_file_list.append(csv_file)
            except (ProjectionException, UnicodeEncodeError), e:
                log.error("Dataset: %s, e:%s", shp_file, e)

    prunedir(decompressed_zip_dir)

    log.debug('CSV FILE: %s', csv_file)
    return zip_file, csv_file_list, work_dir

def download_big_file(url):
    """
    Download a file on a tempfile without exploding in memory
    return the created file name
    """
    log.debug('Downloading: %s', url)
    basefile = os.path.basename(urlparse.urlsplit(url).path)
    fd, big_filename = mkstemp(prefix=basefile + '_XXXX')
    with os.fdopen(fd, "w") as f:
        #r = requests.get(url, stream=True)
        r = requests.get(url)

        if not r.ok:
            log.error('Cannot get "%s"', url)
            return None

        for chunk in r.iter_content(CHUNK_SIZE):
            f.write(chunk)

    return big_filename


class PatGeoHarvester(HarvesterBase):
    # in v2 groups are identified by ids instead of names, so stick with v1
    config = {'api_version': 1}

    def info(self):
        return {
            u'name': u'patgeo',
            u'title': u'Servizio Territorio - Provincia Autonoma di Trento',
            u'description': u'Harvester for www.territorio.provincia.tn.it'
        }

    def gather_stage(self, harvest_job):
        log.debug('In PatGeoHarvester gather stage')
        # Get feed contents

        index_content = download_index()

        ids = []
        for elem in extract_page_metadata(index_content):
            obj = HarvestObject(
                guid=sha1(elem['meta_url']).hexdigest(),
                job=harvest_job,
                content=json.dumps(elem)
            )
            obj.save()
            ids.append(obj.id)

        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In PatGeoHarvester fetch_stage')

        elem = json.loads(harvest_object.content)

        # generate prefix name
        dataset_name = elem['title']
        dataset_name = dataset_name[0].upper() + dataset_name[1:]
        elem['name'] = dataset_name

        elem['zip_file'] = download_big_file(elem['zip_url'])
        elem['xml_file'] = download_big_file(elem['xml_url'])

        harvest_object.content = json.dumps(elem)
        harvest_object.save()
        return True

    def import_stage(self, harvest_object):
        log.debug('In PatGeoHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            log.error('Harvest object contentless')
            self._save_object_error(
                'Empty content for object %s' % harvest_object.id,
                harvest_object,
                'Import'
            )
            return False

        # get api user & keys
        user = get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )

        api_key = user.get('apikey')

        from pylons import config
        base_location = config['ckan.site_url']

        ckan_client = ckanclient.CkanClient(
            base_location=base_location + '/api',
            api_key=api_key,
            is_verbose=True,
        )

        elem = json.loads(harvest_object.content)
        metadata = extract_metadata(elem['xml_file'])
        os.remove(elem['xml_file'])
        modified = metadata['Metadato: Data dei metadati']

        package_dict = {
            u'id': sha1(elem['meta_url']).hexdigest(),
            u'groups': ['geodati'],
            u'title': elem['title'],
            u'notes': metadata['Informazioni di Identificazione: Descrizione'],
            u'notes_rendered': metadata['Informazioni di Identificazione: Descrizione'],
            u'url': u'http://www.territorio.provincia.tn.it/',
            u'author': metadata['Informazioni di Identificazione: Nome dell\'Ente'],
            u'author_email': metadata['Informazioni di Identificazione: E-mail'],
            u'maintainer': metadata['Informazioni sulla Distribuzione: Distributore: E-mail'],
            u'maintainer_email': metadata['Informazioni sulla Distribuzione: Distributore: E-mail'],
            u'tags': clean_tags(elem['tags']) + [u'Ambiente'],
            u'extras': metadata,
            u'isopen': True,
            u'license': u'Creative Commons CCZero',
            u'license_id': u'cc-zero',
            u'license_title': u'Creative Commons CCZero',
            u'license_url': u'http://creativecommons.org/publicdomain/zero/1.0/deed.it',
            u'resources': [],
            u'metadata_modified' : modified,
        }

        xml_dict = {
            'url': elem['xml_url'],
            'format': 'xml',
            'mimetype': 'application/xml',
            'resource_type': 'api',
            'description': package_dict['notes'],
            'name': "Metadati in formato XML",
            'last_modified': modified, # FIXME isoformat?
        }
        rdf_dict = {
            'url': elem['rdf_url'],
            'format': 'rdf',
            'mimetype': 'application/rdf+xml',
            'resource_type': 'api',
            'description': package_dict['notes'],
            'name': "Dati in formato RDF",
            'last_modified': modified, # FIXME isoformat?
        }

        # After creating a link to the original source we want a CSV
        zip_file, csv_file_list, work_dir = zip_decompress(elem['zip_file'])

        junkurl, errmsg = ckan_client.upload_file(zip_file)
        zip_url = junkurl.replace('http://', base_location)
        os.remove(zip_file)

        zip_dict = {
            'url': zip_url,
            'format': 'ESRI ShapeFile',
            'mimetype': 'application/zip',
            'mimetype_inner' : 'application/shp',
            'resource_type': 'file',
            'description': package_dict['notes'],
            'name': "Dati in formato Shapefile",
            'last_modified': modified, # FIXME isoformat?
        }

        # add all the good stuff
        package_dict['resources'].extend([xml_dict, rdf_dict, zip_dict])


        for csv_file in csv_file_list:

            junkurl, errmsg = ckan_client.upload_file(csv_file)
            csv_url = junkurl.replace('http://', base_location)
            os.remove(csv_file)

            csv_dict = {
                'url': csv_url,
                'format': 'csv',
                'mimetype': 'text/csv',
                'resource_type': 'file',
                'description': package_dict['notes'],
                'name': os.path.basename(csv_file),
                'last_modified': modified, # FIXME isoformat?
            }
            # add csv
            package_dict['resources'].append(csv_dict)

        prunedir(work_dir)

        package_dict['name'] = self._gen_new_name(package_dict['title'])

        return self._create_or_update_package(package_dict, harvest_object)
