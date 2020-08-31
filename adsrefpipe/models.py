# -*- coding: utf-8 -*-


from sqlalchemy import Integer, String, Column, ForeignKey, DateTime, func, Numeric
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
        return 'initial'

    def get_status_exists(self):
        return 'retry'


class Reference(Base):
    __tablename__ = 'reference'
    bibcode = Column(String, primary_key=True)
    source_filename = Column(String, primary_key=True)
    source_create = Column(DateTime)
    resolved_filename = Column(String)

    def __init__(self, bibcode, source_filename, source_create, resolved_filename):
        """

        :param bibcode:
        :param source_filename:
        :param source_create:
        :param resolved_filename:
        """
        self.bibcode = bibcode
        self.source_filename = source_filename
        self.source_create = source_create
        self.resolved_filename = resolved_filename

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'bibcode': self.bibcode,
            'source_filename': self.source_filename,
            'source_create': self.source_create,
            'resolved_filename': self.resolved_filename,
        }

class History(Base):
    __tablename__ = 'history'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String, ForeignKey('reference.bibcode'))
    source_filename = Column(String, ForeignKey('reference.source_filename'))
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

class Population(Base):
    """
    This table is to keep the count of matches/misses to setup confusion matrix

    """
    __tablename__ = 'population'
    history_id = Column(Integer, ForeignKey('history.id'), primary_key=True)
    date = Column(DateTime, default=func.now(), primary_key=True)
    true_positive = Column(Integer)
    true_negative = Column(Integer)
    classic_match = Column(Integer)
    service_match = Column(Numeric)

    def __init__(self, history_id, date, true_positive, true_negative, classic_match, service_match):
        """

        :param history_id:
        :param reference_str:
        :param service_bibcode:
        :param service_score:
        :param classic_match:
        :param classic_score:
        :param state:
        """
        self.history_id = history_id
        self.date = date
        self.true_positive = true_positive
        self.true_negative = true_negative
        self.classic_match = classic_match
        self.service_match = service_match

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'history_id': self.history_id,
            'date': self.date,
            'true_positive': self.true_positive,
            'true_negative': self.true_negative,
            'classic_match': self.classic_match,
            'service_match': self.service_match,
        }

class arXiv(Base):
    """
    This is temporary, was not created using alembic
    want to popluated to be able to use for computing statistics, for example only use the resolved reference count
    for certain class of arXiv bibcodes

    """
    __tablename__ = 'arxiv'
    bibcode = Column(String, primary_key=True)
    category = Column(String, primary_key=True)

    def __init__(self, bibcode, category):
        """

        :param bibcode:
        :param category:
        """
        self.bibcode = bibcode
        self.category = category

    def toJSON(self):
        """
        :return: values formatted as python dict, if no values found returns empty structure, not None
        """
        return {
            'bibcode': self.bibcode,
            'category': self.category,
        }
