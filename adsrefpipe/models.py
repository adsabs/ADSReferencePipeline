# -*- coding: utf-8 -*-


from sqlalchemy import Integer, String, Column, ForeignKey, DateTime, func, Numeric, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Action(Base):
    """
    lookup table `action`
    status is `initial` (when reference is first executed ),
              `retry` (subsequent runs),
              `delete` (if by any chance need to remove that reference,
                        mark it as delete, but keep the history)
    """
    __tablename__ = 'action'
    status = Column(String, primary_key=True)

    def get_status_new(self):
        """

        :return:
        """
        return 'initial'

    def get_status_retry(self):
        """

        :return:
        """
        return 'retry'


class Parser(Base):
    """
    lookup table `parser`
    name is   `Text` (text parser),
              `CrossRef` (xml CrossRef parser),
              ...
    """
    __tablename__ = 'parser'
    name = Column(String, primary_key=True)
    source_pattern = Column(String)
    reference_service_endpoint = Column(String)

    def __init__(self, name, source_pattern, reference_service_endpoint):
        """

        :param name:
        :param source_pattern:
        :param reference_service_endpoint:
        """
        self.name = name
        self.source_pattern = source_pattern
        self.reference_service_endpoint = reference_service_endpoint

    def get_name(self):
        """

        :return:
        """
        return self.name

    def get_source_pattern(self):
        """

        :return:
        """
        return self.source_pattern

    def get_endpoint(self):
        """

        :return:
        """
        return self.reference_service_endpoint

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'name': self.name,
            'source_pattern': self.source_pattern,
            'reference_service_endpoint': self.reference_service_endpoint,
        }


class ReferenceSource(Base):
    __tablename__ = 'reference_source'
    bibcode = Column(String, primary_key=True)
    source_filename = Column(String, primary_key=True)
    resolved_filename = Column(String)
    parser_name = Column(String, ForeignKey('parser.name'))

    def __init__(self, bibcode, source_filename, resolved_filename, parser_name):
        """

        :param bibcode:
        :param source_filename:
        :param resolved_filename:
        :param parser_name:
        """
        self.bibcode = bibcode
        self.source_filename = source_filename
        self.resolved_filename = resolved_filename
        self.parser_name = parser_name

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'bibcode': self.bibcode,
            'source_filename': self.source_filename,
            'resolved_filename': self.resolved_filename,
            'parser_name': self.parser_name,
        }


class ProcessedHistory(Base):
    __tablename__ = 'processed_history'
    __table_args__ = (ForeignKeyConstraint( ['bibcode', 'source_filename'], ['reference_source.bibcode', 'reference_source.source_filename']),)
    id = Column(Integer, primary_key=True)
    bibcode = Column(String)
    source_filename = Column(String)
    source_modified = Column(DateTime)
    status = Column(String, ForeignKey('action.status'))
    date = Column(DateTime, default=func.now())
    total_ref = Column(Integer)

    def __init__(self, bibcode, source_filename, source_modified, status, date, total_ref):
        """

        :param bibcode:
        :param source_filename:
        :param source_modified:
        :param status:
        :param date:
        :param total_ref:
        """
        self.bibcode = bibcode
        self.source_filename = source_filename
        self.source_modified = source_modified
        self.status = status
        self.date = date
        self.total_ref = total_ref

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'bibcode': self.bibcode,
            'source_filename': self.source_filename,
            'source_modified': self.source_modified,
            'status': self.status,
            'date': self.date,
            'total_ref' : self.total_ref,
        }


class ResolvedReference(Base):
    __tablename__ = 'resolved_reference'
    history_id = Column(Integer, ForeignKey('processed_history.id'), primary_key=True)
    item_num = Column(Integer, primary_key=True)
    reference_str = Column(String, primary_key=True)
    bibcode = Column(String)
    score = Column(Numeric)
    reference_raw = Column(String)

    def __init__(self, history_id, item_num, reference_str, bibcode, score, reference_raw):
        """

        :param history_id:
        :param item_num
        :param reference_str:
        :param bibcode:
        :param score:
        """
        self.history_id = history_id
        self.item_num = item_num
        self.reference_str = reference_str
        self.bibcode = bibcode
        self.score = score
        self.reference_raw = reference_raw

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        if self.reference_raw:
            return {
                'history_id': self.history_id,
                'reference_str': self.reference_str,
                'bibcode': self.bibcode,
                'score': self.score,
                'item_num': self.item_num,
                'reference_raw': self.reference_raw
            }
        # do not include reference_raw if it is None
        return {
            'history_id': self.history_id,
            'reference_str': self.reference_str,
            'bibcode': self.bibcode,
            'score': self.score,
            'item_num': self.item_num,
        }


class CompareClassic(Base):
    """
    This table is for comparing classic resolver with service reference,
    keeps track of service reference that matched classic reference
    bibcode and score here is for classic

    """
    __tablename__ = 'compare_classic'
    history_id = Column(Integer, ForeignKey('processed_history.id'), primary_key=True)
    item_num = Column(Integer, primary_key=True)
    bibcode = Column(String)
    score = Column(Numeric)
    state = Column(String)

    def __init__(self, history_id, item_num, bibcode, score, state):
        """

        :param history_id:
        :param item_num:
        :param bibcode:
        :param classic_score:
        :param state:
        """
        self.history_id = history_id
        self.item_num = item_num
        self.bibcode = bibcode
        self.score = score
        self.state = state

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'history_id': self.history_id,
            'item_num': self.item_num,
            'bibcode': self.bibcode,
            'score': self.score,
            'state': self.state,
        }

