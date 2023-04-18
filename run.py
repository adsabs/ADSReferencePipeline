import sys
import os, fnmatch
import re

from adsputils import setup_logging, load_config, get_date
from datetime import datetime, timedelta
import time

import argparse
import threading
import pickle

from adsrefpipe import tasks
from adsrefpipe.refparsers.handler import verify
from adsrefpipe.utils import get_date_modified_struct_time, ReprocessQueryType

proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)

app = tasks.app
logger = setup_logging('run.py')


def run_diagnostics(bibcodes, source_filenames):
    """
    Show information about what we have in our storage.

    :param: bibcodes - list of bibcodes
    :param: source_filenames - list of source filenames
    """
    max_entries_diagnostics = config['MAX_ENTRIES_DIAGNOSTICS']
    # make sure we only send max number of entires per bibcode/source_file to be queried
    if bibcodes:
        bibcodes = bibcodes[:max_entries_diagnostics]
    if source_filenames:
        source_filenames = source_filenames[:max_entries_diagnostics]
    results = app.diagnostic_query(bibcodes, source_filenames)
    for result in results:
        print(result)
    return


def get_source_filenames(source_file_path, file_extension, date_cutoff):
    """
    :param source_file_path:
    :param date_cutoff: if modified date is after this date
    :return: list of files in the directory with modified date after the cutoff, if any
    """
    list_files = []
    for root, dirs, files in os.walk(source_file_path):
        for basename in files:
            if fnmatch.fnmatch(basename, file_extension):
                filename = os.path.join(root, basename)
                if get_date_modified_struct_time(filename) >= date_cutoff:
                    list_files.append(filename)
    return list_files


def check_queue(queued_tasks):
    """
    every few seconds check the tasks to see if it was processed

    :param queued_tasks:
    :return:
    """
    to_queue_tasks = []
    # check to see if the queued tasks got processed successfully
    for queued_task in queued_tasks:
        if not queued_task['results'].state == 'SUCCESS':
            if queued_task['attempts'] > 0:
                queued_task['attempts'] -= 1
                to_queue_tasks.append(queued_task)
            else:
                logger.info("Unable to process reference %s with multiple attempts. Skipped!" %queued_task['reference'])

    if len(to_queue_tasks) > 0:
        logger.info('%d/%d task remains for processing.'%(len(to_queue_tasks),len(queued_tasks)))
        threading.Timer(config['QUEUE_AUDIT_INTERVAL'], check_queue, (to_queue_tasks,)).start()
    else:
        logger.info('All tasks consumed.')


def queue_references(references, source_filename, source_bibcode, parsername):
    """

    :param reference:
    :param source_filename:
    :param source_bibcode:
    :param parsername:
    :return:
    """
    resolver_service_url = config['REFERENCE_PIPELINE_SERVICE_URL'] + app.get_reference_service_endpoint(parsername)
    queued_tasks = []
    for reference in references:
        reference_task = {'reference': reference,
                          'source_bibcode': source_bibcode,
                          'source_filename': source_filename,
                          'resolver_service_url': resolver_service_url}
        results = tasks.task_process_reference.delay(reference_task)
        queued_tasks.append({'reference': reference, 'results': results, 'attempts': config['MAX_QUEUE_RETRIES']})
    return queued_tasks


# two ways to queue references: one is to read source files
def process_files(filenames):
    """
    process source reference file

    :param files:
    :return:
    """
    for filename in filenames:
        # first figure out which parser to call
        parser_dict = app.get_parser(filename)
        # parser name
        parser = verify(parser_dict.get('name'))
        # now read the source file
        toREFs = parser(filename=filename, buffer=None)
        if toREFs:
            # next parse the references
            parsed_references = toREFs.process_and_dispatch()
            if not parsed_references:
                logger.error("Unable to parse %s." % toREFs.filename)
                return False

            for block_references in parsed_references:
                # save the initial records in the database,
                # this is going to be useful since it allows us to be able to tell if
                # anything went wrong with the service that we did not get back the
                # resolved reference
                references = app.populate_tables_pre_resolved_initial_status(source_bibcode=block_references['bibcode'],
                                                                             source_filename=toREFs.filename,
                                                                             parsername=parser_dict.get('name'),
                                                                             references=block_references['references'])
                if not references:
                    logger.error("Unable to insert records from %s to db." % toREFs.filename)
                    return []

                queued_tasks = queue_references(references, filename, block_references['bibcode'], parser_dict.get('name'))

            check_queue(queued_tasks)
        else:
            logger.error("Unable to process %s. Skipped!" % toREFs.filename)


# two ways to queue references: the other is to query database
def reprocess_references(reprocess_type, score_cutoff=0, match_bibcode='', date_cutoff=None):
    """

    :param reprocess_type:
    :param param:
    :param date_cutoff:
    :return:
    """
    queued_tasks = []
    records = app.get_reprocess_records(reprocess_type, score_cutoff, match_bibcode, date_cutoff)
    for record in records:
        # first figure out which parser to call
        parser_dict = app.get_parser(record['source_filename'])
        # parser name
        parser = verify(parser_dict.get('name'))
        # now pass the result records from query to the parser object
        toREFs = parser(filename=None, buffer=record,
                        parsername=parser_dict.get('name'), method_identifiers=parser_dict.get('method_identifiers'))
        if toREFs:
            # next parse the references
            parsed_references = toREFs.dispatch()
            if not parsed_references:
                logger.error("Unable to parse %s." % toREFs.filename)
                return False

            queued_tasks = []
            for block_references in parsed_references:
                # save the retry records in the database,
                references = app.populate_tables_pre_resolved_retry_status(source_bibcode=block_references['bibcode'],
                                                                           source_filename=toREFs.filename,
                                                                           source_modified=block_references['source_modified'],
                                                                           retry_records=block_references['references'])
                if not references:
                    logger.error("Unable to insert records from %s to db." % toREFs.filename)
                    return []

                queued_tasks.append(queue_references(references, toREFs.filename, block_references['bibcode'], parser_dict.get('name')))

            check_queue(queued_tasks)
        else:
            logger.error("Unable to process %s. Skipped!" % toREFs.filename)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')

    subparsers = parser.add_subparsers(help='commands', dest="action", required=True)

    diagnostics = subparsers.add_parser('DIAGNOSTICS', help='Show diagnostic message')
    diagnostics.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        nargs='+',
                        default=[],
                        help='List of bibcodes separated by spaces')
    diagnostics.add_argument('-s',
                        '--source_filenames',
                        dest='source_filenames',
                        action='store',
                        nargs='+',
                        default=[],
                        help='List of source_filenames separated by spaces')
    diagnostics.add_argument('-p',
                        '--parse_filename',
                        dest='parse_filename',
                        action='store',
                        default=None,
                        help='Verify that the file can be parsed and references resolved')

    resolve = subparsers.add_parser('RESOLVE', help='Resolve references')
    resolve.add_argument('-s',
                        '--source_filenames',
                        dest='source_filenames',
                        action='store',
                        nargs='+',
                        default=[],
                        help='List of source file names (either xml or raw) separated by spaces')
    resolve.add_argument('-p',
                        '--path',
                        dest='path',
                        action='store',
                        default=None,
                        help='Path of source files for resolving')
    resolve.add_argument('-e',
                        '--extension',
                        dest='extension',
                        action='store',
                        default=None,
                        help='Extension of files to locate in the path directory')
    resolve.add_argument('-d',
                        '--days',
                        dest='days',
                        action='store',
                        default=None,
                        help='Resolve only those that are this many days old')
    resolve.add_argument('-c',
                        '--confidence',
                        dest='confidence',
                        action='store',
                        default=None,
                        help='Reprocess resolved records confidence score lower than this value')
    resolve.add_argument('-b',
                        '--bibstem',
                        dest='bibstem',
                        action='store',
                        default=None,
                        help='Reprocess resolved records having this bibstem')
    resolve.add_argument('-y',
                        '--year',
                        dest='year',
                        action='store',
                        default=None,
                        help='Reprocess resolved records having this year')
    resolve.add_argument('-f',
                        '--fail',
                        dest='fail',
                        action='store_true',
                        help='Reprocess records that failed to get resolved')

    stats = subparsers.add_parser('STATS', help='Print out statistics of the reference source file')
    stats.add_argument('-b',
                        '--bibcode',
                        dest='bibcode',
                        action='store',
                        default=None,
                        help='Statistics of source reference, comparing classic and service reference resolvering if available')
    stats.add_argument('-s',
                        '--source_filename',
                        dest='source_filename',
                        action='store',
                        default=None,
                        help='Statistics of source reference, comparing classic and service reference resolvering if available')
    stats.add_argument('-p',
                        '--publisher',
                        dest='publisher',
                        action='store',
                        default=None,
                        help='To list all source filenames resolved from a specific publisher')
    stats.add_argument('-c',
                       '--count',
                       dest='count',
                       action='store_true',
                       help='Print out the count of records in the four main tables')

    args = parser.parse_args()

    if args.action == 'DIAGNOSTICS':
        if args.parse_filename:
            name = app.get_parser_name(args.parse_filename)
            if name:
                print('Source file `%s` shall be parsed using `%s` parser.' % (args.parse_filename, name))
            else:
                print('No parser yet to parse source file `%s`.' % args.parse_filename)
        # either pass in the list of bibcodes, or list of filenames to query db on
        # if neither bibcode nor filenames are supplied, number of records for the tables are displayed
        else:
            run_diagnostics(args.bibcodes, args.source_filenames)

    elif args.action == 'RESOLVE':
        if args.source_filenames:
            process_files(args.source_filenames)
        elif args.path or args.extension:
            if not args.extension:
                print('Both path and extension are required params. Provide extention by -e <extension of files to locate in the path directory>.')
            elif not args.path:
                print('Both path and extension are required params. Provide path by -p <path of source files for resolving>.')
            else:
                # if days has been specified, read it and only consider files with date from today-days,
                # otherwise we are going with everything
                if args.days:
                    date_cutoff = get_date() - timedelta(days=int(args.days))
                else:
                    date_cutoff = get_date('1972')
                source_filenames = get_source_filenames(args.path, args.extension, date_cutoff.timetuple())
                if len(source_filenames) > 0:
                    process_files(source_filenames)
        elif args.confidence:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            reprocess_references(ReprocessQueryType.score, score_cutoff=float(args.confidence), date_cutoff=date_cutoff)
        elif args.bibstem:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            reprocess_references(ReprocessQueryType.bibstem, match_bibcode=args.bibstem, date_cutoff=date_cutoff)
        elif args.year:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            reprocess_references(ReprocessQueryType.year, match_bibcode=args.bibstem, date_cutoff=date_cutoff)
        elif args.fail:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            reprocess_references(ReprocessQueryType.failed, date_cutoff=date_cutoff)


    # TODO: do we need more command for querying db

    elif args.action == 'STATS':
        if args.bibcode or args.source_filename:
            table, num_references, num_resolved = app.get_service_classic_compare_stats_grid(args.bibcode, args.source_filename)
            print('\n',table,'\n')
            print('Num References:', num_references)
            print('Num References Resolved:', num_resolved)
            print('\n')
        elif args.publisher:
            records = app.query_reference_tbl(parser_type=args.publisher)
            if not records:
                print('No records found for parser %s.'%args.publisher)
            else:
                for record in records:
                    print(record['source_filename'])
        elif args.count:
            results = app.get_count_records()
            print('\n')
            for result in results:
                print('Currently there are %d records in `%s` table, which holds %s.'%(result['count'], result['name'], result['description']))
            print('\n')

    sys.exit(0)