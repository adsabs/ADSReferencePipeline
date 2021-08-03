
import os, fnmatch

from adsputils import setup_logging, get_date

import celery
import argparse
import threading

# update state from diff to match after verifiying from solr
# this is temporary for test purposes only
from adsrefpipe.models import Compare

from adsrefpipe import tasks
from adsrefpipe.utils import get_date_modified_struct_time
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

def check_queue(task_id_list):
    """
    every few seconds check the task_id_list
    if there has been a failure, requeue

    :param task_id_list:
    :return:
    """
    # verify that all tasks got processed successfully
    # if not, attempt max_retry before quiting
    for file, task_id, num_retry in task_id_list[:]:
        # grab the AsyncResult
        result = app.AsyncResult(task_id)
        print('......result=', file, result.state, result.result)
        if result.state == "SUCCESS":
            if result.result:
                # successfully processed
                task_id_list.remove((file, task_id, num_retry))
            else:
                # retry
                print('......try again=', file, num_retry)
                task_id_list.remove((file, task_id, num_retry))
                if num_retry > 1:
                    # generate a new task id, since if the same id is used celery gets confused
                    task_id = celery.uuid()
                    task_id_list.append((file, task_id, num_retry - 1))
                    tasks.task_process_reference_file.apply_async(args=[file], task_id=task_id)
    if len(task_id_list) > 0:
        threading.Timer(10, check_queue, (task_id_list,)).start()

def queue_files(files):
    """
    queue all the requested files

    :param files:
    :return:
    """
    task_id_list = []
    max_retry = 3

    for file in files:
        task_id = celery.uuid()
        tasks.task_process_reference_file.apply_async(args=[file], task_id=task_id)
        task_id_list.append((file, task_id, max_retry))

    check_queue(task_id_list)

# def populate_tmp_arxiv_table():
#     """
#     this function reads the csv file containing list of bibcode and arxiv category
#     and populates temporary arxiv table
#
#     :return:
#     """
#     filename = os.getcwd() + "/arxiv_classes_2.csv"
#     with open(filename) as f:
#         reader = csv.reader(f, delimiter=",")
#         next(reader, None)
#         arxiv_list = []
#         for line in reader:
#             arxiv_record = arXiv(bibcode=line[0], category=line[1])
#             arxiv_list.append(arxiv_record)
#         if len(arxiv_list) > 0:
#             with app.session_scope() as session:
#                 try:
#                     session.bulk_save_objects(arxiv_list)
#                     session.flush()
#                     logger.debug("Added `arXiv` records successfully.")
#                     return True
#                 except:
#                     logger.error("Attempt to add `arXiv` records failed.")
#                     return False
#
# def update_compare_tmp_diff_to_match():
#     """
#
#     :return:
#     """
#     filename = os.getcwd() + "/diff_to_match.csv"
#     with open(filename) as f:
#         reader = csv.reader(f, delimiter=",")
#         next(reader, None)
#         with app.session_scope() as session:
#             record = session.query(Compare).filter(and_(Compare.bibcode == bibcode)).all()
#                 user.firstname, user.lastname = user.name.split(' ')
#
#             session.execute(update(Compare).where(
#                 Status.c.id == st.id).values(resource_root=rn))
#             result = session.execute(Compare.update().where(Compare.c.ID == 20).values(USERNAME='k9'))
#
#             session.update().where(account.c.name == op.inline_literal('account 1')).\ \
#                 values({'name': op.inline_literal('account 2')})
#         )
#         for line in reader:
#             bibcode_duo.append(())
#             arxiv_list.append(arxiv_record)
#         if len(arxiv_list) > 0:
#             with app.session_scope() as session:
#                 try:
#                     session.bulk_save_objects(arxiv_list)
#                     session.flush()
#                     logger.debug("Added `arXiv` records successfully.")
#                     return True
#                 except:
#                     logger.error("Attempt to add `arXiv` records failed.")
#                     return False

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
                        required=False,
                        help='List of bibcodes separated by spaces')
    diagnostics.add_argument('-s',
                        '--source_filenames',
                        dest='source_filenames',
                        action='store',
                        nargs='+',
                        default=[],
                        required=False,
                        help='List of source_filenames separated by spaces')

    xml_references = subparsers.add_parser('XML', help='XML source references')
    xml_references.add_argument('-p',
                        '--parse',
                        dest='parse_filename',
                        action='store',
                        required=False,
                        help='Verify that the file can be parsed and references resolved')
    xml_references.add_argument('-s',
                        '--source_filenames',
                        dest='source_filenames',
                        action='store',
                        nargs='+',
                        default=[],
                        required=True,
                        help='List of source xml filenames separated by spaces')

    raw_references = subparsers.add_parser('RAW', help='RAW source references')
    raw_references.add_argument('-s',
                        '--source_filenames',
                        dest='source_filenames',
                        action='store',
                        nargs='+',
                        default=[],
                        required=True,
                        help='List of source raw filenames separated by spaces')

    dir_references = subparsers.add_parser('DIR', help='Process source references in specified directory with an optional cutoff date of modification')
    dir_references.add_argument('-p',
                        '--path',
                        dest='path',
                        action='store',
                        default=None,
                        required=True,
                        help='Path of source files for resolving')
    dir_references.add_argument('-e',
                        '--extension',
                        dest='extension',
                        action='store',
                        default=None,
                        required=True,
                        help='Extension of files to locate in the path directory')
    dir_references.add_argument('-s',
                        '--since',
                        dest='since',
                        action='store',
                        default=None,
                        required=False,
                        help='Starting date for resolving')

    #TODO: add more command for querying db

    args = parser.parse_args()

    # either pass in the list of bibcodes, or list of filenames to query db on
    # if neither is supplied 10 random records are returned
    if args.action == 'DIAGNOSTICS':
        if args.bibcodes:
            run_diagnostics(args.bibcodes, None)
        elif args.source_filenames:
            run_diagnostics(None, args.source_filenames)
        else:
            run_diagnostics(None, None)
    elif args.action == 'XML':
        if args.source_filenames:
            tasks.task_process_reference_xml_file(args.source_filenames)
        elif args.parse_filename:
            name = Parser().get_name(args.source_filename)
            if name:
                print('Source file `%s` shall be parsed using `%s` parser.' % (args.source_filename, name))
            else:
                print('No parser yet to parse source file `%s`.' % args.source_filename)
    elif args.action == 'RAW':
        if args.source_filenames:
            tasks.task_process_reference_text_file(args.source_filenames)
    elif args.action == 'DIR':
        # if date has been specified, read it, otherwise we are going with everything, init above
        if args.since:
            cutoff_date = get_date(args.since)
        else:
            cutoff_date = get_date('1972')
        source_filenames = get_source_filenames(args.path, args.extension, cutoff_date.timetuple())
        # if len(source_filenames) > 0:
        #     queue_files(source_filenames)
