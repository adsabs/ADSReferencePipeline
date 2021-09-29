
from builtins import zip
from builtins import str
from os import path
import time
import re
import json
import requests

from adsputils import setup_logging, load_config

logger = setup_logging('reference-pipeline')
config = {}
config.update(load_config())


DATE_FORMAT = "%d-%02d-%02d %02d:%02d:%02d"


class ReprocessQueryType:
    score, bibstem, year, failed = range(4)


def get_date_created(filename):
    """

    :param filename:
    :return: file's created date in the format YYYY/MM/DD HH:MM:SS
    """
    return DATE_FORMAT%(time.localtime(path.getctime(filename))[:-3])


def get_date_modified(filename):
    """

    :param filename:
    :return: file's modified date in the format YYYY/MM/DD HH:MM:SS
    """
    return DATE_FORMAT%(time.localtime(path.getmtime(filename))[:-3])


def get_date_modified_struct_time(filename):
    """

    :param filename:
    :return: file's modified date in the time.struct_time format
    """
    return time.localtime(path.getmtime(filename))

def get_date_now():
    """

    :return: current time in the format YYYY/MM/DD HH:MM:SS
    """
    return DATE_FORMAT%(time.localtime(time.time())[:-3])

def get_resolved_filename(source_filename):
    """

    :param source_filename:
    :return: resolved file name to save the result of service resolved references
    """
    return source_filename.replace('sources','retrieve') + '.result'


def get_resolved_references(references, service_url):
    """
    send a request to reference service

    :param references: list of references
    :param service_url
    :return:
    """
    if service_url.endswith('text'):
        str_references,ids = map(list, zip(*[ref.values() for ref in references]))
        payload = {'reference': str_references, 'id': ids}
    elif service_url.endswith('xml'):
        payload = {'parsed_reference': references}
    else:
        logger.error('Unrecognizable service url `%s`.'%service_url)
        return None

    headers = {'Content-type': 'application/json',
               'Accept': 'application/json',
               'Authorization': 'Bearer ' + config['REFERENCE_PIPELINE_ADSWS_API_TOKEN']}
    try:
        r = requests.post(url=service_url, data=json.dumps(payload), headers=headers)
        if (r.status_code == 200):
            resolved = json.loads(r.content)['resolved']
            logger.debug('Resolved %d references successfully.' % (len(resolved)))
            return resolved
        logger.error('Attempt at resolving %d %s references failed with status code %s.' % (len(references), type, r.status_code))
        return None
    except requests.exceptions.RequestException as e:
        logger.error('Unable to connect to the service: %s'%str(e))
        return None


def read_classic_resolved_file(source_bibcode, filename):
    """
    read classic resolved file

    :param source_bibcode:
    :param filename:
    :return:
    """
    try:
        resolved = []
        found = False
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                if found:
                    if line.startswith('---<'):
                        break
                    resolved.append(line)
                else:
                    if line.startswith('---<') and line[4:-5] == source_bibcode:
                        found = True
        logger.debug('Read %d references from classic resolved file %s successfully.' % (len(resolved), filename))
        return resolved
    except:
        logger.error('Unable to read references from classic resolved file %s.' % (filename))
        return None

def get_compare_state(service_bibcode, classic_bibcode, classic_score):
    """
    compare service and classic resolved bibcodes and return descriptive state
    :param service_bibcode:
    :param classic_bibcode:
    :param classic_score:
    :return:
    """
    not_found = '.' * 19

    if classic_score == u'5':
        if service_bibcode == not_found:
            return 'UNVER'
        # if service_bibcode != not_found:
        return 'NEWU'
    if classic_score == u'1':
        if classic_bibcode.upper() == service_bibcode.upper():
            return 'MATCH'
        if service_bibcode != not_found:
            return 'DIFF'
        if service_bibcode == not_found:
            return 'MISS'
    if classic_score == u'0':
        if service_bibcode != not_found:
            return 'NEW'
        return 'NONE'

def compare_classic_and_service(service, source_bibcode, classic_filename):
    """
    compare the result of service and classic resolved references

    :param service: resolved references from service, in dict structure
    :param source_bibcode:
    :param classic: resolved references from classic, string format
    :return:
    """
    classic = read_classic_resolved_file(source_bibcode, classic_filename)
    if not classic:
        return None

    compare = []
    if len(service) == len(classic):
        for i, (s, c) in enumerate(zip(service, classic)):
            service_bibcode = s.get('bibcode', None)
            classic_bibcode = c[2:21].strip()
            classic_score = c[0]

            state = get_compare_state(service_bibcode, classic_bibcode, classic_score)
            compare.append((i+1, classic_bibcode, classic_score, state))
    else:
        for i, s in enumerate(service):
            service_reference = s.get('refstring', '')
            # just in case reference is enumerated, remove the enumeration
            service_reference = service_reference[service_reference.find(']') + 1:] if service_reference.startswith(' [') else service_reference
            service_bibcode = s.get('bibcode', '')
            for c in classic:
                classic_reference = c[24:-1].strip()
                classic_bibcode = c[2:21]
                classic_score = c[0]
                if (service_reference == classic_reference) or (service_bibcode == classic_bibcode and service_bibcode != '.' * 19):
                    state = get_compare_state(service_bibcode, classic_bibcode, classic_score)
                    compare.append((i+1, classic_bibcode, classic_score, state))
                    classic.remove(c)
                    break
    return compare
