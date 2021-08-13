"""
The main application object (it has to be loaded by any worker/script)
in order to initialize the database and get a working configuration.
"""

import re

from builtins import str
from adsputils import ADSCelery

from adsrefpipe.models import Action, Parser, Reference, History, Resolved, Compare, XML
from adsrefpipe.utils import get_date_created, get_date_modified, get_date_now, get_resolved_filename, \
    compare_classic_and_service, ReferenceType, ReprocessQueryType

from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy import and_
from sqlalchemy.sql import exists

from texttable import Texttable

class ADSReferencePipelineCelery(ADSCelery):

    RE_PARSE_ID = re.compile(r'^H(?P<history_id>\d+)+I(?P<item_num>\d+)$')
    EMPTY_XML_REFERENCE_STR = 'no reference str'

    def get_parser_name(self, source_filename):
        """

        :param source_filename:
        :return:
        """
        try:
            if source_filename.endswith('raw'):
                source_extension = '.' + source_filename.split('.')[-1]
            elif source_filename.endswith('xml'):
                parts = source_filename.split('.')
                source_extension = '.%s.%s'%(parts[-2],parts[-1])
            else:
                source_extension = ''
            if source_extension:
                with self.session_scope() as session:
                    rows = session.query(Parser).filter(Parser.source_extension == source_extension).all()
                    if len(rows) == 1:
                        return rows[0].get_name()
                    else:
                        self.logger.error("No records found in table `Parser` matching %s."%source_extension)
            else:
                self.logger.error("Unrecognizable source file %s."%source_filename)
            return None
        except (SQLAlchemyError, DBAPIError) as e:
            self.logger.error('SQLAlchemy: ' + str(e))
            return None

    def query_reference_tbl(self, bibcode_list=None, source_filename_list=None, parser_type=None):
        """
        Queries reference table and returns results.

        :param bibcode_list:
        :param source_filename_list:
        :return: list of json records or None
        """
        try:
            with self.session_scope() as session:
                if bibcode_list and source_filename_list:
                    rows = session.query(Reference).filter(and_(Reference.bibcode.in_(bibcode_list),
                                                                Reference.source_filename.in_(
                                                                    source_filename_list))).all()
                    self.logger.info("Fetched records for bibcode = %s and source_filename = %s." % (
                    bibcode_list, source_filename_list))
                elif bibcode_list:
                    rows = session.query(Reference).filter(Reference.bibcode.in_(bibcode_list)).all()
                    self.logger.info("Fetched records for bibcode = %s." % (bibcode_list))
                elif source_filename_list:
                    rows = session.query(Reference).filter(Reference.source_filename.in_(source_filename_list)).all()
                    self.logger.info("Fetched records for source_filename = %s." % (source_filename_list))
                elif parser_type:
                    rows = session.query(Reference).filter(and_(Reference.parser == parser_type)).all()
                    self.logger.info("Fetched records for parser = %s." % (parser_type))
                else:
                    rows = session.query(Reference).limit(10).all()
                    self.logger.info("Fetched records for 10 records.")

                if len(rows) == 0:
                    if bibcode_list and source_filename_list:
                        self.logger.error("No records found for bibcode = %s and source_filename = %s." % (
                        bibcode_list, source_filename_list))
                    elif bibcode_list:
                        self.logger.error("No records found for bibcode = %s." % (bibcode_list))
                    elif source_filename_list:
                        self.logger.error("No records found for source_filename = %s." % (source_filename_list))
                    elif parser_type:
                        self.logger.error("No records found for parser = %s." % (parser_type))
                    else:
                        self.logger.error("No records found in table `Reference`.")
                    return None

                results = []
                for row in rows:
                    results.append(row.toJSON())
                return results
        except (SQLAlchemyError, DBAPIError) as e:
            self.logger.error('SQLAlchemy: ' + str(e))
            return None

    def insert_reference_record(self, session, reference):
        """
        check to see if the record already exists in the db first, if not, then add it in

        :param session:
        :param reference:
        :return:
        """
        found = session.query(exists().where(and_(Reference.bibcode == reference.bibcode,
                                                  Reference.source_filename == reference.source_filename))).scalar()
        if found:
            return reference.bibcode, reference.source_filename
        try:
            session.add(reference)
            session.flush()
            self.logger.debug("Added a `Reference` record successfully.")
            return reference.bibcode, reference.source_filename
        except SQLAlchemyError as e:
            self.logger.error("Attempt to add a `Reference` record failed: %s." % str(e.args))
            return None, None

    def insert_history_record(self, session, history):
        """

        :param session:
        :param history:
        :return:
        """
        try:
            session.add(history)
            session.flush()
            self.logger.debug("Added a `History` record successfully.")
            return history.id
        except SQLAlchemyError as e:
            self.logger.error("Attempt to add a `History` record failed: %s." % str(e.args))
            return -1

    def insert_resolved_records(self, session, resolved_list):
        """

        :param session:
        :param resolved_list:
        :return:
        """
        try:
            session.bulk_save_objects(resolved_list)
            session.flush()
            self.logger.debug("Added `Resolved` records successfully.")
            return True
        except SQLAlchemyError as e:
            self.logger.error("Attempt to add `Resolved` records failed: %s." % str(e.args))
            return False

    def update_resolved_records(self, session, resolved_list):
        """

        :param session:
        :param resolved_list:
        :return:
        """
        try:
            session.bulk_update_mappings(Resolved, resolved_list)
            session.flush()
            self.logger.debug("Added `Resolved` records successfully.")
            return True
        except SQLAlchemyError as e:
            self.logger.error("Attempt to add `Resolved` records failed: %s." % str(e.args))
            return False

    def insert_compare_records(self, session, compared_list):
        """

        :param session:
        :param compared_list:
        :return:
        """
        try:
            session.bulk_save_objects(compared_list)
            session.flush()
            self.logger.debug("Added `Compare` records successfully.")
            return True
        except SQLAlchemyError as e:
            self.logger.error("Attempt to add `Compare` records failed: %s." % str(e.args))
            return False

    def insert_xml_records(self, session, xml_list):
        """

        :param session:
        :param xml_list:
        :return:
        """
        try:
            session.bulk_save_objects(xml_list)
            session.flush()
            self.logger.debug("Added `XML` records successfully.")
            return True
        except SQLAlchemyError as e:
            self.logger.error("Attempt to add `XML` records failed: %s." % str(e.args))
            return False

    def populate_tables_new_precede(self, type, bibcode, source_filename, parser_name, references):
        """

        :param type:
        :param bibcode:
        :param source_filename:
        :param parser_name:
        :param references:
        :return:
        """
        with self.session_scope() as session:
            success = False

            reference_record = Reference(bibcode=bibcode,
                                         source_filename=source_filename,
                                         resolved_filename=get_resolved_filename(source_filename),
                                         parser=parser_name)
            bibcode, source_filename = self.insert_reference_record(session, reference_record)
            if bibcode and source_filename:
                history_record = History(bibcode=bibcode,
                                         source_filename=source_filename,
                                         source_modified=get_date_modified(source_filename),
                                         status=Action().get_status_new(),
                                         date=get_date_now(),
                                         total_ref=len(references))
                history_id = self.insert_history_record(session, history_record)
                if history_id != -1:
                    resolved_records, xml_records, references = self.populate_resolved_records_precede(type, references, history_id)
                    success = self.insert_resolved_records(session, resolved_records)
                    if success and xml_records:
                        self.insert_xml_records(session, xml_records)
            if success:
                session.commit()
                self.logger.info("Source file %s for bibcode %s with %d references, processed successfully." % (source_filename, bibcode, len(references)))
            else:
                session.rollback()
                self.logger.info("Source file %s information failed to get added to database." % (source_filename))

        return success, references

    def populate_tables_retry_precede(self, type, bibcode, source_filename, source_modified, references):
        """

        :param type:
        :param bibcode:
        :param source_filename:
        :param source_modified:
        :param parser_name:
        :param references:
        :return:
        """
        with self.session_scope() as session:
            success = False

            history_record = History(bibcode=bibcode,
                                     source_filename=source_filename,
                                     source_modified=source_modified,
                                     status=Action().get_status_retry(),
                                     date=get_date_now(),
                                     total_ref=len(references))
            history_id = self.insert_history_record(session, history_record)
            if history_id != -1:
                resolved_records, xml_records, references = self.populate_resolved_records_precede(type, references, history_id)
                success = self.insert_resolved_records(session, resolved_records)
                if success and xml_records:
                    success = self.insert_xml_records(session, xml_records)
            if success:
                session.commit()
                self.logger.info("Source file %s for bibcode %s with %d references, for reprocessing added successfully." % (source_filename, bibcode, len(references)))
            else:
                session.rollback()
                self.logger.info("Source file %s information for reprocessing failed to get added to database." % (source_filename))

        return success, references

    def populate_resolved_records_precede(self, type, references, history_id):
        """
        insert resolved records before sending them to service to be matched
        if we have xml references, then insert populate the xml table as well

        :param type:
        :param references:
        :param history_id:
        :return:
        """
        if type == ReferenceType.text:
            resolved_records = []
            updated_references = []
            for i, ref in enumerate(references):
                resolved_record = Resolved(history_id=history_id,
                                           item_num=i + 1,
                                           reference_str=ref,
                                           bibcode='0000',
                                           score=-1)
                resolved_records.append(resolved_record)
                # add the id for later to update the record with resolved values
                updated_references.append({'reference':ref, 'id': 'H%dI%d' % (history_id, i + 1)})
            return resolved_records, None, updated_references

        if type == ReferenceType.xml:
            resolved_records = []
            xml_records = []
            for i, ref in enumerate(references):
                resolved_record = Resolved(history_id=history_id,
                                           item_num=i + 1,
                                           reference_str=ref.get('refstr', None) or
                                                         ref.get('refplaintext', None) or
                                                         self.EMPTY_XML_REFERENCE_STR,
                                           bibcode='0000',
                                           score=-1)
                xml_record = XML(history_id=history_id,
                                 item_num=i + 1,
                                 reference=ref.get('xml_reference', None))
                resolved_records.append(resolved_record)
                xml_records.append(xml_record)
                # add the id and remove xml_reference that is now in database
                ref['id'] = 'H%dI%d' % (history_id, i + 1)
                del ref['xml_reference']
            return resolved_records, xml_records, references

        return None, None, None

    def populate_tables_succeed(self, resolved, source_bibcode, classic_resolved_filename):
        """

        :param resolved:
        :param source_bibcode:
        :param classic_resolved_filename:
        :return:
        """
        # if the filename for classic resolver output is supplied, read the resolved information
        # make sure that the length matches resolved, classic does some breaking a reference into two
        # and hence messes up the order if we want to compare one-to-one, if that is the case, just
        # ignore the result
        resolved_classic = None
        if classic_resolved_filename:
            resolved_classic = compare_classic_and_service(resolved, source_bibcode, classic_resolved_filename)

        with self.session_scope() as session:
            resolved_records = []
            compare_records = []
            for i, ref in enumerate(resolved):
                match = self.RE_PARSE_ID.match(ref['id'])
                history_id = int(match.group('history_id'))
                item_num = int(match.group('item_num'))
                resolved_record = {"history_id" : history_id,
                                   "item_num" : item_num,
                                   "reference_str" : ref.get('refstring', None) or self.EMPTY_XML_REFERENCE_STR,
                                   "bibcode" : ref.get('bibcode', None),
                                   "score" : ref.get('score', None)}
                resolved_records.append(resolved_record)
                if resolved_classic:
                    compare_record = Compare(history_id=history_id,
                                             item_num=item_num,
                                             bibcode=resolved_classic[i][1],
                                             score=resolved_classic[i][2],
                                             state=resolved_classic[i][3])
                    compare_records.append(compare_record)
            if resolved_classic:
                success = self.update_resolved_records(session, resolved_records) and \
                          self.insert_compare_records(session, compare_records)
            else:
                success = self.update_resolved_records(session, resolved_records)

            if success:
                session.commit()
                self.logger.info("Updated %d resolved records successfully." % len(resolved))
            else:
                session.rollback()
                self.logger.info("Failed to update %d resolved records successfully." % len(resolved))

            return success

    def get_count_reference_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(Reference).count()
        self.logger.debug("Currently there are %d records in `Reference` table."%rows)
        return rows

    def get_count_history_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(History).count()
        self.logger.debug("Currently there are %d records in `History` table."%rows)
        return rows

    def get_count_resolved_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(Resolved).count()
        self.logger.debug("Currently there are %d records in `Resolved` table."%rows)
        return rows

    def get_count_compare_records(self, session):
        """

        :param session:
        :return:
        """
        rows = session.query(Compare).count()
        self.logger.debug("Currently there are %d records in `Compare` table."%rows)
        return rows

    def get_count_records(self):
        """

        :return:
        """
        results = ''
        with self.session_scope() as session:
            results += "Currently there are %d records in `Reference` table, which holds reference files information.\n" % self.get_count_reference_records(session)
            results += "Currently there are %d records in `History` table, which holds file level information for resolved run.\n" % self.get_count_history_records(session)
            results += "Currently there are %d records in `Resolved` table, which holds reference level information for resolved run.\n" % self.get_count_resolved_records(session)
            results += "Currently there are %d records in `Compare` table, which holds comparison of new and classic resolved run.\n" % self.get_count_compare_records(session)
        return results

    def get_stats_compare(self, source_filename):
        """

        :param source_filename:
        :return:
        """
        query = "SELECT r.bibcode as service_bibcode, c.bibcode as classic_bibcode, " \
                       "r.score as service_confidence, c.score as classic_score, " \
                       "MAX(CASE WHEN c.state = 'MATCH' THEN state END) AS MATCH, " \
                       "MAX(CASE WHEN c.state = 'MISS' THEN state END) AS MISS, " \
                       "MAX(CASE WHEN c.state = 'NEW' THEN state END) AS NEW, " \
                       "MAX(CASE WHEN c.state = 'NEWU' THEN state END) AS NEWU, " \
                       "MAX(CASE WHEN c.state = 'DIFF' THEN state END) AS DIFF " \
                "FROM resolved as r, compare as c, history as h " \
                "WHERE r.history_id = c.history_id AND r.item_num = c.item_num and " \
                      "r.history_id = h.id AND h.source_filename = '%s' " \
                "GROUP BY r.bibcode, c.bibcode, r.score, c.score"

        with self.session_scope() as session:
            results = session.execute(query%source_filename)
            if results.rowcount > 0:
                # Texttable functionality is here https://pypi.org/project/texttable/
                table = Texttable()
                table.set_cols_width([20,20,20,20,5,5,5,5,5])
                table.set_cols_dtype(['t']*9)
                table.set_cols_align(['c']*9)
                table.header(results.keys())
                for result in results:
                    row = []
                    for item in result:
                        if not item: item = ''
                        row.append(item)
                    table.add_row(row)
                return table.draw()
        return 'Unable to fetch data for reference source file `%s` from database!'%source_filename

    def get_reprocess_records(self, type, score_cutoff=0.75, match_bibcode='', cutoff_date=None):
        """

        :param type:
        :param score_cutoff:
        :param match:
        :return:
        """
        results = []

        if type == ReprocessQueryType.score:
            collection = "r.score < %d"%score_cutoff
        elif type == ReprocessQueryType.bibstem and len(match_bibcode):
            collection = "r.bibcode LIKE '____%s__________'"%match_bibcode
        elif type == ReprocessQueryType.year and len(match_bibcode):
            collection = "r.bibcode LIKE '%s_______________'"%match_bibcode
        elif type == ReprocessQueryType.failed:
            collection = "r.bibcode='0000' AND r.score=-1"
        else:
            collection = None

        if collection:
            if cutoff_date:
                collection += " AND h.date >= '%s'"%cutoff_date
            resolved_view = "SELECT DISTINCT ON (h.bibcode, h.source_filename, r.item_num) MAX(h.id) as history_id, " \
                            "r.item_num, r.reference_str, r.bibcode, r.score, h.bibcode as source " \
                            "FROM history as h, resolved as r " \
                            "WHERE h.id = r.history_id " \
                            "GROUP BY h.bibcode, h.source_filename, r.item_num, r.reference_str, r.bibcode, r.score"
            query = "SELECT r.history_id, r.item_num, r.reference_str as refstr, x.reference as xml_reference, " \
                    "f.bibcode as source_bibcode, f.source_filename, h.source_modified, f.parser " \
                    "FROM reference as f, history as h, (%s) as r LEFT JOIN xml as x ON " \
                    "r.history_id = x.history_id and r.item_num = x.item_num " \
                    "WHERE f.bibcode = h.bibcode AND f.source_filename = h.source_filename AND h.id = r.history_id AND %s " \
                    "ORDER BY r.history_id, r.item_num;"%resolved_view
            with self.session_scope() as session:
                rows = session.execute(query%collection)
                if rows.rowcount > 0:
                    result = {}
                    history_id = -1
                    for row in rows:
                        if row.history_id != history_id:
                            if result:
                                results.append(result)
                                result = {}
                            history_id = row.history_id
                            result['source_bibcode'] = row.source_bibcode
                            result['source_filename'] = row.source_filename
                            result['source_modified'] = row.source_modified
                            result['parser'] = row.parser
                            if row.xml_reference:
                                result['references'] = [{'item_num': row.item_num, 'refstr': row.refstr, 'xml_reference': row.xml_reference}]
                            else:
                                result['references'] = [{'item_num': row.item_num, 'refstr': row.refstr}]

                        else:
                            if row.xml_reference:
                                result['references'] += [{'item_num': row.item_num, 'refstr': row.refstr, 'xml_reference': row.xml_reference}]
                            else:
                                result['references'] += [{'item_num': row.item_num, 'refstr': row.refstr}]
                # last batch, if any
                if result:
                    results.append(result)
        return results