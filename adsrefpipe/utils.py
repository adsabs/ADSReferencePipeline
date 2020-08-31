
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

def read_reference_text_file(filename):
    """
    read reference file of text format

    :param filename:
    :return:
    """
    try:
        bibcode = None
        with open(filename, 'r') as f:
            reader = f.readlines()
            references = []
            prev_line = ''
            for line in reader:
                if not line.startswith('%'):
                    line = re.search(r'([-.\s]*.*[A-Z].*$)', line)
                    if line:
                        line = fix_inheritance(line.group(), prev_line)
                        references.append(line)
                        prev_line = line
                elif line.startswith('%R'):
                    bibcode = line.split('%R ')[1].strip()
        if len(references) > 0 and bibcode:
            logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
        elif len(references) == 0:
            logger.error('No references found in reference file %s.' % (filename))
        elif bibcode is None:
            logger.error('No bibcode found in reference file %s.' % (filename))
        return bibcode, references
    except Exception as e:
        logger.error('Exception: %s' % (str(e)))
        return None, None


def fix_inheritance(cur_refstr, prev_refstr):
    """
    if author list is the same as the reference above it, a dash is inserted
    get the list of authors from the previous reference and add it to the current one

    :param cur_refstr:
    :param prev_refstr:
    :return:
    """
    match = re.match(r"[-_]{2,}\.?", cur_refstr)
    if match and prev_refstr and len(prev_refstr) > 1:
        try:
            # find the year and return everything that came before it
            prev_authors = re.match(r"(.*)(?=\s\d{4})", prev_refstr)
            if prev_authors:
                cur_refstr = prev_authors.group() + " " + cur_refstr[match.end():]
        except TypeError:
            pass
    return cur_refstr


def resolve_text_references(references):
    """
    send a request to reference service
    if the first attempt fails, try second time
    if the second attempt fails, try third time with signle reference request

    :param references: list of references
    :return:
    """
    url = config['REFERENCE_PIPELINE_SERVICE_TEXT_URL']
    payload = {'reference': references}
    headers = {'Content-type': 'application/json',
               'Accept': 'application/json',
               'Authorization': 'Bearer ' + config['REFERENCE_PIPELINE_ADSWS_API_TOKEN']}

    r = requests.post(url=url, data=json.dumps(payload), headers=headers)
    if (r.status_code == 200):
        resolved = json.loads(r.content)['resolved']
        logger.debug('Resolved %d references successfully.' % (len(resolved)))
        return resolved

    logger.error('Attempt at resolving %d references failed with status code %s.' % (len(references), r.status_code))
    return None

def resolve_text_references_individually(references):
    """
    send a single reference request to reference service

    :param references: list of references
    :return:
    """

    url = config['REFERENCE_PIPELINE_SERVICE_TEXT_URL']
    headers = {'Content-type': 'application/json',
               'Accept': 'application/json',
               'Authorization': 'Bearer ' + config['REFERENCE_PIPELINE_ADSWS_API_TOKEN']}

    resolved = []

    for reference in references:
        payload = {'reference': [reference]}
        r = requests.post(url=url, data=json.dumps(payload), headers=headers)
        if (r.status_code == 200):
            resolved.append(json.loads(r.content)['resolved'])
        else:
            logger.error('Processing single reference returend status_code %s. Quitting now.' % (r.status_code))
            raise Exception('Failed third attempt, sending one reference at a time')
            return None

    resolved = json.loads(r.content)['resolved']
    logger.debug('Resolved %d references individually successfully.' % (len(resolved)))
    return resolved

def read_classic_resolved_file(filename):
    """
    read classic resolved file

    :param filename: 
    :return: 
    """
    try:
        resolved = []
        with open(filename, 'r') as f:
            reader = f.readlines()
            for line in reader[1:]:
                resolved.append(line)
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

def compare_classic_and_service(service, classic_filename):
    """
    compare the result of service and classic resolved references

    :param service: resolved references from service, in dict structure
    :param classic: resolved references from classic, string format
    :return:
    """
    classic = read_classic_resolved_file(classic_filename)
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
            service_reference = s.get('reference', None).strip()
            # just in case reference is enumerated, remove the enumeration
            service_reference = service_reference[service_reference.find(']') + 1:] if service_reference.startswith(' [') else service_reference
            service_bibcode = s.get('bibcode', None)
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
