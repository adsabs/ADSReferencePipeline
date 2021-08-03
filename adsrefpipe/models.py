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

    def get_status_exists(self):
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
    extension = Column(String)

    def get_name(self, source_filename):
        """

        :param source_filename:
        :return:
        """
        if source_filename.endswith('.xref.xml'):
            return 'CrossRef'
        if source_filename.endswith('.elsevier.xml'):
            return 'ELSEVIER'
        if source_filename.endswith('.jats.xml'):
            return 'JATS'
        if source_filename.endswith('.iop.xml'):
            return 'IOP'
        if source_filename.endswith('.springer.xml'):
            return 'SPRINGER'
        if source_filename.endswith('.ref.xml'):
            return 'APS'
        if source_filename.endswith('.nature.xml'):
            return 'NATURE'
        if source_filename.endswith('.aip.xml'):
            return 'AIP'
        if source_filename.endswith('.wiley2.xml'):
            return 'WILEY'
        if source_filename.endswith('.nlm3.xml'):
            return 'NLM'
        if source_filename.endswith('.agu.xml'):
            return 'AGU'
        if source_filename.endswith('.raw'):
            return 'Text'
        return ''


class Reference(Base):
    __tablename__ = 'reference'
    bibcode = Column(String, primary_key=True)
    source_filename = Column(String, primary_key=True)
    resolved_filename = Column(String)
    parser = Column(String, ForeignKey('parser.name'))

    def __init__(self, bibcode, source_filename, resolved_filename, parser):
        """

        :param bibcode:
        :param source_filename:
        :param resolved_filename:
        :param parser:
        """
        self.bibcode = bibcode
        self.source_filename = source_filename
        self.resolved_filename = resolved_filename
        self.parser = parser

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'bibcode': self.bibcode,
            'source_filename': self.source_filename,
            'resolved_filename': self.resolved_filename,
            'parser': self.parser,
        }

class History(Base):
    __tablename__ = 'history'
    __table_args__ = (ForeignKeyConstraint( ['bibcode', 'source_filename'], ['reference.bibcode', 'reference.source_filename']),)
    id = Column(Integer, primary_key=True)
    bibcode = Column(String)
    source_filename = Column(String)
    source_modified = Column(DateTime)
    status = Column(String, ForeignKey('action.status'))
    date = Column(DateTime, default=func.now())
    total_ref = Column(Integer)
    resolved_ref = Column(Integer)

    def __init__(self, bibcode, source_filename, source_modified, status, date, total_ref, resolved_ref):
        """

        :param bibcode:
        :param source_filename:
        :param source_modified:
        :param status:
        :param date:
        :param total_ref:
        :param resolved_ref:
        """
        self.bibcode = bibcode
        self.source_filename = source_filename
        self.source_modified = source_modified
        self.status = status
        self.date = date
        self.total_ref = total_ref
        self.resolved_ref = resolved_ref

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
            'resolved_ref': self.resolved_ref,
        }


class Resolved(Base):
    __tablename__ = 'resolved'
    history_id = Column(Integer, ForeignKey('history.id'), primary_key=True)
    item_num = Column(Integer, primary_key=True)
    reference_str = Column(String, primary_key=True)
    bibcode = Column(String)
    score = Column(Numeric)

    def __init__(self, history_id, item_num, reference_str, bibcode, score):
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

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'history_id': self.history_id,
            'reference_str': self.reference_str,
            'bibcode': self.bibcode,
            'score': self.score,
            'item_num': self.item_num
        }

class Compare(Base):
    """
    This table is for comparing classic resolver with service reference,
    keeps track of service reference that matched classic reference
    bibcode and score here is for classic

    """
    __tablename__ = 'compare'
    history_id = Column(Integer, ForeignKey('history.id'), primary_key=True)
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