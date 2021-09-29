import sys
import os, fnmatch

from adsputils import setup_logging, load_config, get_date
from datetime import datetime, timedelta

import argparse
import threading

from adsrefpipe import tasks
from adsrefpipe.refparsers.handler import verify
from adsrefpipe.utils import get_date_modified_struct_time, ReprocessQueryType


proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)


app = tasks.app
logger = setup_logging('run.py')


MAX_ENTRIES_DIAGNOSTICS = 5

def run_diagnostics(bibcodes, source_filenames):
    """
    Show information about what we have in our storage.

    :param: bibcodes - list of bibcodes
    :param: source_filenames - list of source filenames
    """
    # make sure we only send max number of entires per bibcode/source_file to be queried
    if bibcodes:
        bibcodes = bibcodes[:MAX_ENTRIES_DIAGNOSTICS]
    if source_filenames:
        source_filenames = source_filenames[:MAX_ENTRIES_DIAGNOSTICS]
    results = app.query_reference_tbl(bibcodes, source_filenames)
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
    def output_message(message):
        """

        :param message:
        :return:
        """
        logger.info(message)
        print(message)


    to_queue_tasks = []
    # check to see when/if the queued tasks got processed successfully
    for queued_task in queued_tasks:
        if queued_task['results'].state == 'SUCCESS' and queued_task['results'].result:
            if 'filename' in queued_task:
                output_message("Reference file %s successfully processed."%queued_task['filename'])
            elif 'reprocess' in queued_task:
                record = queued_task['reprocess']
                output_message("Reprocessed %d reference(s) from source file %s successfully processed." %(len(record['references']), record['source_filename']))
        else:
            # queue the task to check it again in few seconds, if it has not been too long
            if queued_task['results'].date_done:
                if (queued_task['results'].date_done - datetime.now()).seconds > config['TASK_PROCESS_TIME']:
                    output_message("Reference file %s was not processed in allotted time! Removed from verification queue." % queued_task['filename'])
                    continue
            to_queue_tasks.append(queued_task)
    if len(to_queue_tasks) > 0:
        output_message('%d/%d task remains for processing.'%(len(to_queue_tasks),len(queued_tasks)))
        threading.Timer(config['QUEUE_AUDIT_INTERVAL'], check_queue, (to_queue_tasks,)).start()
    else:
        output_message('All tasks consumed.')


def queue_files(filenames):
    """
    queue all the requested files

    :param files:
    :return:
    """
    queued_tasks = []
    for filename in filenames:
        # first figure out which parser to call
        parser_name = app.get_parser_name(filename)
        parser = verify(parser_name)
        # now read the source file
        toREFs = parser(filename=filename, buffer=None, parsername=parser_name)
        if toREFs:
            results = tasks.task_process_references.delay(toREFs)
            queued_tasks.append({'toREFs': toREFs, 'results': results})
        else:
            logger.error("Unable to open and read %s. Skipped!" %filename)
    check_queue(queued_tasks)


def queue_reprocess(reprocess_type, score_cutoff=0, match_bibcode='', date_cutoff=None):
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
        parser_name = app.get_parser_name(record['source_filename'])
        parser = verify(parser_name)
        # now read the source file
        toREFs = parser(filename=None, buffer=record, parsername=None)
        if toREFs:
            tasks.task_reprocess_references(toREFs)
            return
            results = tasks.task_reprocess_references.delay(toREFs)
            queued_tasks.append({'toREFs':toREFs, 'results':results})
    check_queue(queued_tasks)


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
            queue_files(args.source_filenames)
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
                    queue_files(source_filenames)
        elif args.confidence:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.score, score_cutoff=float(args.confidence), date_cutoff=date_cutoff)
        elif args.bibstem:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.bibstem, match_bibcode=args.bibstem, date_cutoff=date_cutoff)
        elif args.year:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.year, match_bibcode=args.bibstem, date_cutoff=date_cutoff)
        elif args.fail:
            date_cutoff = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.failed, date_cutoff=date_cutoff)


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