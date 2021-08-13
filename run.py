
import os, fnmatch

from adsputils import setup_logging, get_date
from datetime import timedelta

import argparse
import threading

from adsrefpipe import tasks
from adsrefpipe.utils import get_date_modified_struct_time, ReprocessQueryType
from adsrefpipe.models import Parser



app = tasks.app
logger = setup_logging('run.py')



def run_diagnostics(bibcodes, source_filenames):
    """
    Show information about what we have in our storage.
    :param: bibcodes - list of bibcodes
    :param: source_filenames - list of source filenames
    """
    if bibcodes or source_filenames:
        results = app.query_reference_tbl(bibcodes, source_filenames)
        for result in results:
            print(result)
        return
    # if no bibcode or source_filenames supplied, list the num records in each table
    print(app.get_count_records())


def get_source_filenames(source_file_path, file_extension, cutoff_date):
    """
    :param source_file_path:
    :param cutoff_date: if modified date is after this date
    :return: list of files in the directory with modified date after the cutoff, if any
    """
    list_files = []
    for root, dirs, files in os.walk(source_file_path):
        for basename in files:
            if fnmatch.fnmatch(basename, file_extension):
                filename = os.path.join(root, basename)
                if get_date_modified_struct_time(filename) >= cutoff_date:
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
        if queued_task['results'].state == "SUCCESS" and queued_task['results'].result:
            if 'filename' in queued_task:
                output_message("reference file %s successfully processed."%queued_task['filename'])
            elif 'reprocess' in queued_task:
                record = queued_task['reprocess']
                output_message("reprocessed %d reference(s) from source file %s successfully processed." %(len(record['references']), record['source_filename']))
        else:
            # to check again in few seconds
            to_queue_tasks.append(queued_task)
    if len(to_queue_tasks) > 0:
        output_message('%d/%d task remains for processing.'%(len(to_queue_tasks),len(queued_tasks)))
        threading.Timer(10, check_queue, (to_queue_tasks,)).start()
    else:
        output_message('All tasks processed successfully.')


def queue_files(filenames):
    """
    queue all the requested files

    :param files:
    :return:
    """
    queued_tasks = []
    for filename in filenames:
        results = tasks.task_process_reference_file.delay(filename)
        queued_tasks.append({'filename':filename, 'results':results})
    check_queue(queued_tasks)


def queue_reprocess(reprocess_type, score_cutoff=0, match_bibcode='', cutoff_date=None):
    """

    :param reprocess_type:
    :param param:
    :param cutoff_date:
    :return:
    """
    queued_tasks = []
    records = app.get_reprocess_records(reprocess_type, score_cutoff, match_bibcode, cutoff_date)
    for record in records:
        results = tasks.task_reprocess_subset_references.delay(record)
        queued_tasks.append({'reprocess':record, 'results':results})
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

    resolve_references = subparsers.add_parser('RESOLVE', help='Resolve references')
    resolve_references.add_argument('-s',
                        '--source_filenames',
                        dest='source_filenames',
                        action='store',
                        nargs='+',
                        default=[],
                        help='List of source file names (either xml or raw) separated by spaces')
    resolve_references.add_argument('-p',
                        '--path',
                        dest='path',
                        action='store',
                        default=None,
                        help='Path of source files for resolving')
    resolve_references.add_argument('-e',
                        '--extension',
                        dest='extension',
                        action='store',
                        default=None,
                        help='Extension of files to locate in the path directory')
    resolve_references.add_argument('-d',
                        '--days',
                        dest='days',
                        action='store',
                        default=None,
                        help='Resolve only those that are this many days old')
    resolve_references.add_argument('-c',
                        '--confidence',
                        dest='confidence',
                        action='store',
                        default=None,
                        help='Reprocess resolved records confidence score lower than this value')
    resolve_references.add_argument('-b',
                        '--bibstem',
                        dest='bibstem',
                        action='store',
                        default=None,
                        help='Reprocess resolved records having this bibstem')
    resolve_references.add_argument('-y',
                        '--year',
                        dest='year',
                        action='store',
                        default=None,
                        help='Reprocess resolved records having this year')
    resolve_references.add_argument('-f',
                        '--fail',
                        dest='fail',
                        action='store_true',
                        help='Reprocess records that failed to get resolved')

    stats_output = subparsers.add_parser('STATS', help='Print out statistics of the reference source file')
    stats_output.add_argument('-s',
                        '--source_filename',
                        dest='source_filename',
                        action='store',
                        default=None,
                        required=True,
                        help='Statistics of source reference file, comparing classic and service reference resolvering')
    stats_output.add_argument('-p',
                        '--parser_type',
                        dest='parser_type',
                        action='store',
                        default=None,
                        required=True,
                        help='To list all source xml filenames resolved by a particular parser')

    args = parser.parse_args()

    if args.action == 'DIAGNOSTICS':
        if args.parse_filename:
            name = Parser().get_name(args.parse_filename)
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
                    cutoff_date = get_date() - timedelta(days=int(args.days))
                else:
                    cutoff_date = get_date('1972')
                source_filenames = get_source_filenames(args.path, args.extension, cutoff_date.timetuple())
                if len(source_filenames) > 0:
                    queue_files(source_filenames)
        elif args.confidence:
            cutoff_date = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.score, score_cutoff=float(args.confidence), cutoff_date=cutoff_date)
        elif args.bibstem:
            cutoff_date = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.bibstem, match_bibcode=args.bibstem, cutoff_date=cutoff_date)
        elif args.year:
            cutoff_date = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.year, match_bibcode=args.bibstem, cutoff_date=cutoff_date)
        elif args.fail:
            cutoff_date = get_date() - timedelta(days=int(args.days)) if args.days else None
            queue_reprocess(ReprocessQueryType.failed, cutoff_date=cutoff_date)


    # TODO: do we need more command for querying db

    elif args.action == 'STATS':
        if args.source_filename:
            print(app.get_stats_compare(args.source_filename))
        elif args.parser_type:
            records = app.query_reference_tbl(parser_type=args.parser_type)
            if not records:
                print('No records found for parser %s.'%args.parser_type)
            else:
                for record in records:
                    print(record['source_filename'])
