"""
The main application object (it has to be loaded by any worker/script)
in order to initialize the database and get a working configuration.
"""

import re
import time

from builtins import str
from adsputils import ADSCelery
from datetime import datetime, timedelta

from adsrefpipe.models import Action, Parser, ReferenceSource, ProcessedHistory, ResolvedReference, CompareClassic
from adsrefpipe.utils import get_date_created, get_date_modified, get_date_now, get_resolved_filename, \
    compare_classic_and_service, ReprocessQueryType

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, literal
from sqlalchemy.sql import exists
from sqlalchemy.sql.expression import case, func

from texttable import Texttable

class ADSReferencePipelineCelery(ADSCelery):

    RE_PARSE_ID = re.compile(r'^H(?P<history_id>\d+)+I(?P<item_num>\d+)$')
    RE_MATCH_EXT = re.compile(r'.*(\..*?\.[a-z]+)$')

    default_parsers = {}

    def __init__(self, app_name, *args, **kwargs):
        """

        :param app_name:
        :param args:
        :param kwargs:
        """
        ADSCelery.__init__(self, app_name, *args, **kwargs)

    def init_default_parsers(self):
        """
        init default parsers lookup table

        :return:
        """
        # grab the parsers with unique extension, have in memory to speed up
        # looking it up
        with self.session_scope() as session:
            rows = session.query(func.min(Parser.name).label('name'),
                                 Parser.extension_pattern.label('extension_pattern'),
                                 func.min(Parser.reference_service_endpoint).label('reference_service_endpoint'),
                                 func.json_agg(Parser.matches).label('matches'),
                                 func.count(Parser.extension_pattern).label('count')
                                 ).group_by(Parser.extension_pattern).all()
            for row in rows:
                # if unique
                if row.count == 1:
                    to_dict = {
                        'name': row.name,
                        'extension_pattern': row.extension_pattern,
                        'reference_service_endpoint': row.reference_service_endpoint,
                        'matches': row.matches,
                    }
                    self.default_parsers[to_dict['extension_pattern']] = to_dict

    def match_parser(self, rows, journal, volume):
        """

        :param rows:
        :param journal:
        :param volume:
        :return:
        """
        for row in rows:
            for match in row.get_matches():
                if match.get('journal', '') == journal:
                    # could be one of the following formats
                    # {"journal":"ARA&A", "all_volume":True}
                    # {"journal":"ASPC", "volume_begin":93, "volume_end":261}
                    # {"journal":"ASPC", "volume_number":305}
                    # {"journal":"CONF", "partial_bibcode":"1999sf99.proc"}
                    if match.get('all_volume', False) or match.get('partial_bibcode', '') == volume or \
                       match.get('volume_string') == volume:
                        return row.toJSON()
                    if not isinstance(volume, int) and not volume.isdigit():
                        continue
                    volume = int(volume)
                    if (match.get('volume_begin', 9999) <= volume and match.get('volume_end', 0) >= volume) or \
                        match.get('volume_number', -1) == volume:
                        return row.toJSON()
        return {}

    def get_parser(self, source_filename):
        """

        :param source_filename:
        :return:
        """
        if not self.default_parsers:
            self.init_default_parsers()

        journal, volume, basefile = source_filename.split('/')[-3:]
        if journal and volume and basefile:
            match = self.RE_MATCH_EXT.search(basefile)
            if match:
                # with multiple extensions
                extension = match.group(1)
            else:
                # with single extension
                extension = ".%s"%basefile.rsplit('.', 1)[-1]

            # if one of the default ones
            if self.default_parsers.get(extension, None):
                return self.default_parsers[extension]

            with self.session_scope() as session:
                # start_time = time.time()
                rows = session.query(Parser).filter(and_(Parser.extension_pattern == extension,
                                                         Parser.matches.contains([{"journal": journal}]))).all()
                # if no records, try with single extension, if possible
                if not rows and extension.count('.') >= 2:
                    rows = session.query(Parser).filter(and_(Parser.extension_pattern == extension[extension.rfind('.'):],
                                                             Parser.matches.contains([{"journal": journal}]))).all()
                if len(rows) == 1:
                    return rows[0].toJSON()
                if len(rows) > 1:
                    match = self.match_parser(rows, journal, volume)
                    if match:
                        return match
        else:
            self.logger.error("Unrecognizable source file %s."%source_filename)
        return {}

    def get_reference_service_endpoint(self, parsername):
        """
        given parsername find the endpoint that shall be called for the reference to get resolved

        :param parsername:
        :return:
        """
        with self.session_scope() as session:
            rows = session.query(Parser).filter(Parser.name == parsername).all()
            if len(rows) == 1:
                return rows[0].get_endpoint()
            else:
                self.logger.error("No unique record found in table `Parser` matching name %s." % parsername)
        return ''

    def query_reference_source_tbl(self, bibcode_list=None, source_filename_list=None, parsername=None):
        """
        Queries reference table and returns results.

        :param bibcode_list
        :param source_filename_list
        :param parsername
        :return: list of json records or None
        """
        with self.session_scope() as session:
            if bibcode_list and source_filename_list:
                rows = session.query(ReferenceSource) \
                    .filter(and_(ReferenceSource.bibcode.in_(bibcode_list),
                                 ReferenceSource.source_filename.in_(source_filename_list))).all()
                self.logger.info("Fetched records for bibcode = %s and source_filename = %s." % (
                bibcode_list, source_filename_list))
            elif bibcode_list:
                rows = session.query(ReferenceSource).filter(ReferenceSource.bibcode.in_(bibcode_list)).all()
                self.logger.info("Fetched records for bibcode = %s." % (bibcode_list))
            elif source_filename_list:
                rows = session.query(ReferenceSource).filter(ReferenceSource.source_filename.in_(source_filename_list)).all()
                self.logger.info("Fetched records for source_filename = %s." % (source_filename_list))
            elif parsername:
                rows = session.query(ReferenceSource).filter(and_(ReferenceSource.parser_name == parsername)).all()
                self.logger.info("Fetched records for parser = %s." % (parsername))
            else:
                rows = session.query(ReferenceSource).limit(10).all()
                self.logger.info("Fetched records for 10 records.")

            if len(rows) == 0:
                if bibcode_list and source_filename_list:
                    self.logger.error("No records found for bibcode = %s and source_filename = %s." % (
                    bibcode_list, source_filename_list))
                elif bibcode_list:
                    self.logger.error("No records found for bibcode = %s." % (bibcode_list))
                elif source_filename_list:
                    self.logger.error("No records found for source_filename = %s." % (source_filename_list))
                elif parsername:
                    self.logger.error("No records found for parser = %s." % (parsername))
                else:
                    self.logger.error("No records found in table `ReferenceSource`.")

            results = []
            for row in rows:
                results.append(row.toJSON())
            return results

    def insert_reference_source_record(self, session, reference):
        """
        check to see if the record already exists in the db first, if not, then add it in

        :param session:
        :param reference:
        :return:
        """
        found = session.query(exists().where(and_(ReferenceSource.bibcode == reference.bibcode,
                                                  ReferenceSource.source_filename == reference.source_filename))).scalar()
        if found:
            return reference.bibcode, reference.source_filename

        session.add(reference)
        session.flush()
        self.logger.debug("Added a `Reference` record successfully.")
        return reference.bibcode, reference.source_filename

    def insert_history_record(self, session, history):
        """

        :param session:
        :param history:
        :return:
        """
        session.add(history)
        session.flush()
        self.logger.debug("Added a `ProcessedHistory` record successfully.")
        return history.id

    def insert_resolved_referencce_records(self, session, resolved_list):
        """

        :param session:
        :param resolved_list:
        :return:
        """
        session.bulk_save_objects(resolved_list)
        session.flush()
        self.logger.debug("Added `ResolvedReference` records successfully.")
        return True

    def update_resolved_reference_records(self, session, resolved_list):
        """

        :param session:
        :param resolved_list:
        :return:
        """
        session.bulk_update_mappings(ResolvedReference, [r.toJSON() for r in resolved_list])
        session.flush()
        self.logger.debug("Added `ResolvedReference` records successfully.")
        return True

    def insert_compare_records(self, session, compared_list):
        """

        :param session:
        :param compared_list:
        :return:
        """
        session.bulk_save_objects(compared_list)
        session.flush()
        self.logger.debug("Added `CompareClassic` records successfully.")
        return True

    def populate_resolved_reference_records_pre_resolved(self, references, history_id, item_nums=None):
        """
        insert resolved records before sending them to service to be matched
        if we have xml references, then insert populate the xml table as well

        :param references:
        :param history_id:
        :param item_nums:
        :return:
        """
        if not item_nums:
            item_nums = list(range(1, len(references)+1))
        resolved_records = []
        for item_num, ref in zip(item_nums, references):
            resolved_record = ResolvedReference(history_id=history_id,
                                       item_num=item_num,
                                       reference_str=ref.get('refstr', None) or ref.get('refplaintext', None),
                                       bibcode='0000',
                                       score=-1,
                                       reference_raw=ref.get('refraw', None))
            resolved_records.append(resolved_record)
            # add the id and remove xml_reference that is now in database
            ref['id'] = 'H%dI%d' % (history_id, item_num)
            if 'item_num' in ref: del ref['item_num']
        return resolved_records, references

    def populate_tables_pre_resolved_initial_status(self, source_bibcode, source_filename, parsername, references):
        """
        this is called when the references are being processed for the first time, from the file

        :param source_bibcode:
        :param source_filename:
        :param parsername:
        :param references:
        :return:
        """
        try:
            with self.session_scope() as session:
                reference_record = ReferenceSource(bibcode=source_bibcode,
                                             source_filename=source_filename,
                                             resolved_filename=get_resolved_filename(source_filename),
                                             parser_name=parsername)
                bibcode, filename = self.insert_reference_source_record(session, reference_record)
                if bibcode and filename:
                    history_record = ProcessedHistory(bibcode=bibcode,
                                             source_filename=source_filename,
                                             source_modified=get_date_modified(source_filename),
                                             status=Action().get_status_new(),
                                             date=get_date_now(),
                                             total_ref=len(references))
                    history_id = self.insert_history_record(session, history_record)
                    resolved_records, references = self.populate_resolved_reference_records_pre_resolved(references, history_id)
                    self.insert_resolved_referencce_records(session, resolved_records)
                    session.commit()
                    self.logger.info("Source file %s for bibcode %s with %d references, processed successfully." % (source_filename, source_bibcode, len(references)))
                    return references
        except SQLAlchemyError as e:
            self.logger.info("Source file %s information failed to get added to database. Error: %s" % (source_filename, str(e.__dict__['orig'])))
        return []

    def populate_tables_pre_resolved_retry_status(self, source_bibcode, source_filename, source_modified, retry_records):
        """
        this is called when the references are being reprocessed, usually cherry picked from the records in the database

        :param source_bibcode:
        :param source_filename:
        :param source_modified:
        :param retry_records:
        :return:
        """
        try:
            with self.session_scope() as session:
                history_record = ProcessedHistory(bibcode=source_bibcode,
                                         source_filename=source_filename,
                                         source_modified=source_modified,
                                         status=Action().get_status_retry(),
                                         date=get_date_now(),
                                         total_ref=len(retry_records))
                history_id = self.insert_history_record(session, history_record)
                resolved_records, references = self.populate_resolved_reference_records_pre_resolved(retry_records, history_id)
                if resolved_records:
                    self.insert_resolved_referencce_records(session, resolved_records)
                    session.commit()
                    self.logger.info("Source file %s for bibcode %s with %d references, for reprocessing added successfully." % (source_filename, source_bibcode, len(references)))
                    return references
        except SQLAlchemyError as e:
            self.logger.info("Source file %s information for reprocessing failed to get added to database." % (source_filename, str(e.__dict__['orig'])))
        return []

    def populate_tables_post_resolved(self, resolved_reference, source_bibcode, classic_resolved_filename):
        """
        this is called after references has been resolved

        :param resolved_reference:
        :param source_bibcode:
        :param classic_resolved_filename:
        :return:
        """
        try:
            # if the filename for classic resolver output is supplied, read the resolved information
            # make sure that the length matches resolved, classic does some breaking a reference into two
            # and hence messes up the order if we want to compare one-to-one, if that is the case, just
            # ignore the result
            resolved_classic = None
            if classic_resolved_filename:
                resolved_classic = compare_classic_and_service(resolved_reference, source_bibcode, classic_resolved_filename)

            with self.session_scope() as session:
                resolved_records = []
                compare_records = []
                for i, ref in enumerate(resolved_reference):
                    match = self.RE_PARSE_ID.match(ref['id'])
                    history_id = int(match.group('history_id'))
                    item_num = int(match.group('item_num'))
                    # TODO change refstring to refraw for reference_raw
                    resolved_record = ResolvedReference(history_id=history_id,
                                               item_num=item_num,
                                               reference_str=ref.get('refstring', None),
                                               bibcode=ref.get('bibcode', None),
                                               score=ref.get('score', None),
                                               reference_raw=ref.get('refstring', None))
                    resolved_records.append(resolved_record)
                    if resolved_classic:
                        compare_record = CompareClassic(history_id=history_id,
                                                 item_num=item_num,
                                                 bibcode=resolved_classic[i][1],
                                                 score=int(resolved_classic[i][2]),
                                                 state=resolved_classic[i][3])
                        compare_records.append(compare_record)
                if resolved_classic:
                    self.update_resolved_reference_records(session, resolved_records)
                    self.insert_compare_records(session, compare_records)
                else:
                    self.update_resolved_reference_records(session, resolved_records)
                session.commit()
                self.logger.info("Updated %d resolved reference records successfully." % len(resolved_reference))
                return True
        except SQLAlchemyError as e:
            self.logger.info("Failed to update %d resolved reference records successfully. Error %s" % (len(resolved_reference), str(e)))
            return False

    def get_count_reference_source_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(ReferenceSource).count()
        self.logger.debug("Currently there are %d records in `ReferenceSource` table."%rows)
        return rows

    def get_count_processed_history_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(ProcessedHistory).count()
        self.logger.debug("Currently there are %d records in `ProcessedHistory` table."%rows)
        return rows

    def get_count_resolved_reference_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(ResolvedReference).count()
        self.logger.debug("Currently there are %d records in `ResolvedReference` table."%rows)
        return rows

    def get_count_compare_classic_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(CompareClassic).count()
        self.logger.debug("Currently there are %d records in `CompareClassic` table."%rows)
        return rows

    def get_count_records(self):
        """

        :return:
        """
        with self.session_scope() as session:
            results = [
                {
                    'name':'ReferenceSource',
                    'description':'source reference file information',
                    'count':self.get_count_reference_source_records(session),
                },
                {
                    'name':'ProcessedHistory',
                    'description':'top level information for a processed run',
                    'count': self.get_count_processed_history_records(session),
                },
                {
                    'name':'ResolvedReference',
                    'description':'resolved reference information for a processed run',
                    'count': self.get_count_resolved_reference_records(session),

                },
                {
                    'name':'CompareClassic',
                    'description':'comparison of new and classic processed run',
                    'count':self.get_count_compare_classic_records(session),
                }
            ]
            return results

    def get_service_classic_compare_tags(self, session, source_bibcode, source_filename):
        """
        makes a grid of classic and service compared tags and returns the query

        :param session:
        :param source_bibcode:
        :param source_filename:
        :return:
        """
        # given reference source (bibcodes and filenames), have query that would contain
        # all resolved records ids, and if we have reprocessed records, it contains one
        # set of records for those
        resolved_reference_ids = session.query(ResolvedReference.history_id.label('history_id'),
                             ResolvedReference.item_num.label('item_num')) \
            .filter(and_(ProcessedHistory.id == ResolvedReference.history_id,
                         literal(source_bibcode).op('~')(ProcessedHistory.bibcode),
                         literal(source_filename).op('~')(ProcessedHistory.source_filename))) \
            .distinct().subquery()

        return session.query(func.max(case([(CompareClassic.state == 'MATCH', 'MATCH')])).label('MATCH'),
                            func.max(case([(CompareClassic.state == 'MISS', 'MISS')])).label('MISS'),
                            func.max(case([(CompareClassic.state == 'NEW', 'NEW')])).label('NEW'),
                            func.max(case([(CompareClassic.state == 'NEWU', 'NEWU')])).label('NEWU'),
                            func.max(case([(CompareClassic.state == 'DIFF', 'DIFF')])).label('DIFF'),
                            CompareClassic.history_id, CompareClassic.item_num) \
            .select_from(resolved_reference_ids) \
            .outerjoin(CompareClassic, and_(resolved_reference_ids.c.history_id == CompareClassic.history_id,
                                            resolved_reference_ids.c.item_num == CompareClassic.item_num)) \
            .group_by(CompareClassic.history_id, CompareClassic.item_num) \
            .subquery()

    def get_service_classic_compare_stats_grid(self, source_bibcode, source_filename):
        """

        :param source_bibcode:
        :param source_filename:
        :return:
        """
        with self.session_scope() as session:
            compare_grid = self.get_service_classic_compare_tags(session, source_bibcode, source_filename)
            results = session.query(ResolvedReference.reference_str.label('refstr'),
                                    ResolvedReference.bibcode.label('service_bibcode'), CompareClassic.bibcode.label('classic_bibcode'),
                                    ResolvedReference.score.label('service_conf'), CompareClassic.score.label('classic_score'),
                                    compare_grid.c.MATCH.label('match'), compare_grid.c.MISS.label('miss'),
                                    compare_grid.c.NEW.label('new'), compare_grid.c.NEWU.label('newu'),
                                    compare_grid.c.DIFF.label('diff')) \
                .filter(and_(ResolvedReference.history_id == CompareClassic.history_id,
                             ResolvedReference.item_num == CompareClassic.item_num,
                             ResolvedReference.history_id == compare_grid.c.history_id,
                             ResolvedReference.item_num == compare_grid.c.item_num)) \
                .order_by(ResolvedReference.history_id, ResolvedReference.item_num) \
                .all()
            if results:
                # Texttable functionality is here https://pypi.org/project/texttable/
                table = Texttable()
                table.set_cols_width([60,19,19,15,15,5,5,5,5,5])
                table.set_cols_dtype(['t']*10)
                table.set_cols_align(['l']+['c']*9)
                table.header(results[0]._asdict().keys())
                num_resolved = 0
                for result in results:
                    # count how many was resolved on the side of service
                    if not result[1].startswith('.'):
                        num_resolved += 1
                    row = []
                    for item in result:
                        if not item: item = ''
                        row.append(item)
                    table.add_row(row)
                return table.draw(), len(results), num_resolved
        return 'Unable to fetch data for reference source file `%s` from database!'%source_filename

    def get_reprocess_records(self, type, score_cutoff, match_bibcode, date_cutoff):
        """

        :param type:
        :param score_cutoff:
        :param match_bibcode:
        :param date_cutoff:
        :return:
        """
        def apply_filter(query, type, score_cutoff, match_bibcode, date_cutoff):
            """
            apply one of the four selected filter, also apply date if requested

            :param query:
            :param type:
            :param score_cutoff:
            :param match_bibcode:
            :param date_cutoff:
            :return:
            """
            if type == ReprocessQueryType.score:
                query = query.filter(ResolvedReference.score <= "%.2f" % score_cutoff)
            elif type == ReprocessQueryType.bibstem and len(match_bibcode):
                query = query.filter(ResolvedReference.bibcode.like('____%s__________' % match_bibcode))
            elif type == ReprocessQueryType.year and len(match_bibcode):
                query = query.filter(ResolvedReference.bibcode.like('%s_______________' % match_bibcode))
            elif type == ReprocessQueryType.failed:
                query = query.filter(and_(ResolvedReference.bibcode == '0000', ResolvedReference.score == -1))
            if date_cutoff:
                since = datetime.now() - timedelta(days=int(date_cutoff))
                query = query.filter(ProcessedHistory.date >= since)
            return query

        rows = []
        with self.session_scope() as session:
            # have a query containing unique reference source ids (bibcodes and filenames),
            # that have been filtered on one of four possible options and also date if requested
            reference_source_ids = session.query(ProcessedHistory.bibcode, ProcessedHistory.source_filename) \
                .filter(ProcessedHistory.id == ResolvedReference.history_id)
            reference_source_ids = apply_filter(reference_source_ids, type, score_cutoff, match_bibcode, date_cutoff)
            reference_source_ids = reference_source_ids.distinct().all()
            bibcodes = [ids[0] for ids in reference_source_ids]
            filenames = [ids[1] for ids in reference_source_ids]

            # have a query containing unique resolved reference ids (history_id and item_num),
            # that have been filtered on one of four possible options and also date if requested
            resolved_reference_ids = session.query(ResolvedReference.history_id.label('history_id'),
                                                   ResolvedReference.item_num.label('item_num')) \
                .filter(and_(ProcessedHistory.id == ResolvedReference.history_id),
                             ProcessedHistory.bibcode.in_(bibcodes),
                             ProcessedHistory.source_filename.in_(filenames))
            resolved_reference_ids = apply_filter(resolved_reference_ids, type, score_cutoff, match_bibcode, date_cutoff)
            resolved_reference_ids = resolved_reference_ids.distinct().subquery()


            results = session.query(resolved_reference_ids.c.history_id.label('history_id'),
                                    resolved_reference_ids.c.item_num.label('item_num'),
                                    ResolvedReference.reference_str.label('refstr'),
                                    ResolvedReference.reference_raw.label('refraw'),
                                    ProcessedHistory.bibcode.label('source_bibcode'),
                                    ProcessedHistory.source_filename.label('source_filename'),
                                    ProcessedHistory.source_modified.label('source_modified'),
                                    ReferenceSource.parser_name.label('parser_name')) \
                .filter(and_(resolved_reference_ids.c.history_id == ResolvedReference.history_id,
                             resolved_reference_ids.c.item_num == ResolvedReference.item_num,
                             ResolvedReference.history_id == ProcessedHistory.id,
                             ProcessedHistory.bibcode == ReferenceSource.bibcode,
                             ProcessedHistory.source_filename == ReferenceSource.source_filename)) \
                .order_by(ResolvedReference.history_id, ResolvedReference.item_num) \
                .all()

            if results:
                results = [r._asdict() for r in results]
                row = {}
                history_id = -1
                for result in results:
                    if result['history_id'] != history_id:
                        if row:
                            rows.append(row)
                            row = {}
                        history_id = result['history_id']
                        for key in ['source_bibcode', 'source_filename', 'source_modified', 'parser_name']:
                            row[key] = result[key]
                        row['references'] = []
                        reference = {}
                        for key in ['item_num', 'refstr', 'refraw']:
                            reference[key] = result[key]
                        row['references'].append(reference)
                    else:
                        reference = {}
                        for key in ['item_num', 'refstr', 'refraw']:
                            reference[key] = result[key]
                        row['references'].append(reference)
                # last batch, if any
                if row:
                    rows.append(row)
        return rows

