"""
The main application object (it has to be loaded by any worker/script)
in order to initialize the database and get a working configuration.
"""

from builtins import str
from adsputils import ADSCelery

from adsrefpipe.models import Action, Parser, Reference, History, Resolved, Compare
from adsrefpipe.utils import get_date_created, get_date_modified, get_date_now, get_resolved_filename, compare_classic_and_service

from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy import and_
from sqlalchemy.sql import exists


class ADSReferencePipelineCelery(ADSCelery):

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
            print("Attempt to add a `History` record failed: %s." % str(e.args))
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
            print("Attempt to add `Compare` records failed: %s." % str(e.args))
            return False

    def populate_tables(self, bibcode, source_filename, resolved, classic_resolved_filename=None):
        """
        need to populate Reference, History, Resolved, and Compare tables

        :param bibcode:
        :param source_filename:
        :param resolved:
        :param classic_resolved_filename:
        :return:
        """
        with self.session_scope() as session:
            success = False

            reference_record = Reference(bibcode=bibcode,
                                         source_filename=source_filename,
                                         resolved_filename=get_resolved_filename(source_filename),
                                         parser=Parser().get_name(source_filename))
            bibcode, source_filename = self.insert_reference_record(session, reference_record)
            if bibcode:
                resolved_ref = 0
                for ref in resolved:
                    if float(ref.get('score', '0.0')) == 1.0:
                        resolved_ref += 1

                history_record = History(bibcode=bibcode,
                                         source_filename=source_filename,
                                         source_modified=get_date_modified(source_filename),
                                         status=Action().get_status_new(),
                                         date=get_date_now(),
                                         total_ref=len(resolved),
                                         resolved_ref=resolved_ref)
                history_id = self.insert_history_record(session, history_record)
                if history_id != -1:
                    resolved_records = []
                    for i, ref in enumerate(resolved):
                        resolved_record = Resolved(history_id=history_id,
                                                   item_num=i+1,
                                                   reference_str=ref.get('refstring', None),
                                                   bibcode=ref.get('bibcode', None),
                                                   score=ref.get('score', None))
                        resolved_records.append(resolved_record)
                    success = self.insert_resolved_records(session, resolved_records)
                    # if the filename for classic resolved is supplied,
                    # read it and populate the two tables of population and compare accordingly
                    if classic_resolved_filename and success:
                        compare_records = []
                        compare = compare_classic_and_service(resolved, classic_resolved_filename)
                        if success:
                            if len(compare) == 0:
                                self.logger.info("Unable to compare service and classic for %s and %s respectively." % (source_filename, classic_resolved_filename))
                            else:
                                for ref in compare:
                                    compare_record = Compare(history_id=history_id,
                                                             item_num=ref[0],
                                                             bibcode=ref[1],
                                                             score=ref[2],
                                                             state=ref[3])
                                    compare_records.append(compare_record)
                                success = self.insert_compare_records(session, compare_records)
            if success:
                session.commit()
                self.logger.info("Source file %s for bibcode %s with %d references, processed successfully." % (source_filename, bibcode, len(resolved)))
            else:
                session.rollback()
                self.logger.info("Source file %s information failed to get added to database." % (source_filename))

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