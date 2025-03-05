# -*- coding: utf-8 -*-


from sqlalchemy import Integer, String, Column, ForeignKey, DateTime, func, Numeric, ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB
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

    def get_status_new(self) -> str:
        """
        returns the initial status

        :return: string indicating the initial status
        """
        return 'initial'

    def get_status_retry(self) -> str:
        """
        returns the retry status

        :return: string indicating the retry status
        """
        return 'retry'


class Parser(Base):
    """
    lookup table `parser`
    name is   `Text` (text parser),
              `CrossRef` (xml CrossRef parser),
              ...
              `OCR` (OCR parser that has method_identifiers specifiying which flavor of OCR it is to be parsed)
    """
    __tablename__ = 'parser'
    name = Column(String, primary_key=True)
    extension_pattern = Column(String)
    reference_service_endpoint = Column(String)
    matches = Column(JSONB, default=dict)

    def __init__(self, name: str, extension_pattern: str, reference_service_endpoint: str, matches: list = []):
        """
        initializes a parser object

        :param name: name of the parser
        :param extension_pattern: reference file extension pattern used by the parser
        :param reference_service_endpoint: endpoint for the reference service
        :param matches: list of matches for the parser-reference file mapping
        """
        self.name = name
        self.extension_pattern = extension_pattern
        self.reference_service_endpoint = reference_service_endpoint
        self.matches = matches

    def get_name(self) -> str:
        """
        returns the name of the parser

        :return: string indicating the name of the parser
        """
        return self.name

    def get_extension_pattern(self) -> str:
        """
        returns the extension pattern of the reference files processed by the parser

        :return: string indicating the file extension pattern
        """
        return self.extension_pattern

    def get_endpoint(self) -> str:
        """
        returns the reference service endpoint to resolve references

        :return: string indicating the reference service endpoint
        """
        return self.reference_service_endpoint

    def get_matches(self) -> list:
        """
        returns the list of mappings for the parser

        :return: list of matches
        """
        return self.matches

    def toJSON(self) -> dict:
        """
        converts the parser object to a JSON dictionary

        :return: dictionary containing the parser details
        """
        return {
            'name': self.name,
            'extension_pattern': self.extension_pattern,
            'reference_service_endpoint': self.reference_service_endpoint,
            'matches': self.matches,
        }


class ReferenceSource(Base):
    """
    This class represents the source of a reference in the database,
    each entry links a source file with its resolved version and
    the parser used to process the reference.
    It serves as the initial record for the reference processing pipeline.
    """
    __tablename__ = 'reference_source'
    bibcode = Column(String, primary_key=True)
    source_filename = Column(String, primary_key=True)
    resolved_filename = Column(String)
    parser_name = Column(String, ForeignKey('parser.name'))

    def __init__(self, bibcode: str, source_filename: str, resolved_filename: str, parser_name: str):
        """
        initializes a reference source object

        :param bibcode: unique bibcode for the reference source
        :param source_filename: name of the reference file
        :param resolved_filename: name of the resolved file for future use
        :param parser_name: name of the parser used
        """
        self.bibcode = bibcode
        self.source_filename = source_filename
        self.resolved_filename = resolved_filename
        self.parser_name = parser_name

    def toJSON(self) -> dict:
        """
        converts the reference source object to a JSON dictionary

        :return: dictionary containing reference source details
        """
        return {
            'bibcode': self.bibcode,
            'source_filename': self.source_filename,
            'resolved_filename': self.resolved_filename,
            'parser_name': self.parser_name,
        }


class ProcessedHistory(Base):
    """
    This class tracks the processing history of a resolved reference, recording details about the processing status,
    reference file timestamp, and the total number of references parsed.
    """
    __tablename__ = 'processed_history'
    __table_args__ = (ForeignKeyConstraint( ['bibcode', 'source_filename'], ['reference_source.bibcode', 'reference_source.source_filename']),)
    id = Column(Integer, primary_key=True)
    bibcode = Column(String)
    source_filename = Column(String)
    source_modified = Column(DateTime)
    status = Column(String, ForeignKey('action.status'))
    date = Column(DateTime, default=func.now())
    total_ref = Column(Integer)

    def __init__(self, bibcode: str, source_filename: str, source_modified: DateTime, status: str, date: DateTime, total_ref: int):
        """
        initializes a processed history object

        :param bibcode: bibcode for the reference source
        :param source_filename: name of the source reference file
        :param source_modified: timestamp of the reference file at the time it was read
        :param status: first time processing, or reprocessing this list of references
        :param date: date of processing
        :param total_ref: total number of references parsed
        """
        self.bibcode = bibcode
        self.source_filename = source_filename
        self.source_modified = source_modified
        self.status = status
        self.date = date
        self.total_ref = total_ref

    def toJSON(self) -> dict:
        """
        converts the processed history object to a JSON dictionary

        :return: dictionary containing processed history details
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
    """
    This class stores information about references that have been resolved, including the reference string, score,
    and its associated history entry.
    """
    __tablename__ = 'resolved_reference'
    history_id = Column(Integer, ForeignKey('processed_history.id'), primary_key=True)
    item_num = Column(Integer, primary_key=True)
    reference_str = Column(String, primary_key=True)
    bibcode = Column(String)
    score = Column(Numeric)
    reference_raw = Column(String)

    def __init__(self, history_id: int, item_num: int, reference_str: str, bibcode: str, score: float, reference_raw: str):
        """
        initializes a resolved reference object

        :param history_id: ID of the related processed history entry
        :param item_num: order of the reference within the source
        :param reference_str: reference string
        :param bibcode: resolved bibcode
        :param score: confidence score of the resolved reference
        :param reference_raw: raw reference string
        """
        self.history_id = history_id
        self.item_num = item_num
        self.reference_str = reference_str
        self.bibcode = bibcode
        self.score = score
        self.reference_raw = reference_raw

    def toJSON(self) -> dict:
        """
        converts the resolved reference object to a JSON dictionary

        :return: dictionary containing resolved reference details
        """
        return {
            'history_id': self.history_id,
            'reference_str': self.reference_str,
            'bibcode': self.bibcode,
            'score': self.score,
            'item_num': self.item_num,
            **({'reference_raw': self.reference_raw} if self.reference_raw else {})
        }


class CompareClassic(Base):
    """
    This table is for comparing classic resolver with service reference,
    keeps track of service reference that matched classic reference
    bibcode and score here is for classic, should be a temparary class
    only used during development/testing and verification
    """
    __tablename__ = 'compare_classic'
    history_id = Column(Integer, ForeignKey('processed_history.id'), primary_key=True)
    item_num = Column(Integer, primary_key=True)
    bibcode = Column(String)
    score = Column(Numeric)
    state = Column(String)

    def __init__(self, history_id: int, item_num: int, bibcode: str, score: Numeric, state: str):
        """
        initializes a compare classic object

        :param history_id: ID of the related processed history entry
        :param item_num: order of the reference within the source
        :param bibcode: resolved bibcode
        :param score: confidence score of the resolved reference
        :param state: comparison state (ie, matched, unmatched, etc.)
        """
        self.history_id = history_id
        self.item_num = item_num
        self.bibcode = bibcode
        self.score = score
        self.state = state

    def toJSON(self) -> dict:
        """
        converts the compare classic object to a JSON dictionary

        :return: dictionary containing compare classic details
        """
        return {
            'history_id': self.history_id,
            'item_num': self.item_num,
            'bibcode': self.bibcode,
            'score': self.score,
            'state': self.state,
        }

