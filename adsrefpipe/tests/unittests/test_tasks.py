import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import datetime
import unittest
from unittest.mock import Mock, patch
import json
from contextlib import contextmanager

from adsrefpipe import app, tasks, utils
from adsrefpipe.models import Base, Action, Parser, ReferenceSource, ProcessedHistory, ResolvedReference, CompareClassic
from adsrefpipe.refparsers.handler import verify
from adsrefpipe.tests.unittests.stubdata.dbdata import actions_records, parsers_records


class _FakeQuery:
    """Minimal stand-in for SQLAlchemy Query used by this test suite."""
    def __init__(self, model, session):
        self.model = model
        self.session = session

    def delete(self):
        # The original tests call session.query(Model).delete(); it's safe to no-op here.
        self.session._cleared_models.add(self.model)
        return 0


class _FakeSession:
    """Minimal stand-in for a SQLAlchemy Session used by this test suite."""
    def __init__(self):
        self._cleared_models = set()
        self._bulk_saved = []
        self._commits = 0

    def query(self, model):
        return _FakeQuery(model, self)

    def bulk_save_objects(self, objs):
        self._bulk_saved.extend(list(objs))

    def commit(self):
        self._commits += 1


class _FakeApp:
    """
    Lightweight fake for ADSReferencePipelineCelery that:
      - never creates an engine/DB
      - provides the methods used by this test suite
      - maintains in-memory counts to support get_count_records()
    """
    def __init__(self, name, local_config):
        self._name = name
        self._config = dict(local_config or {})
        self.conf = dict(local_config or {})
        self.logger = Mock()

        # In-memory “tables” / counters
        self._reference_sources = set()  # (bibcode, source_filename)
        self._history_ids = []           # list of generated history ids
        self._resolved_count = 0         # ResolvedReference rows
        self._compare_classic_count = 0  # always 0 in these tests

    @contextmanager
    def session_scope(self):
        session = _FakeSession()
        try:
            yield session
        finally:
            pass

    def close_app(self):
        return True

    # ---------------------------
    # Parser / endpoint helpers
    # ---------------------------
    def get_parser(self, filename):
        """
        The real implementation typically queries the Parser table.
        For unit tests, infer parser from filename/path.
        """
        if filename and ('arXiv' in filename or filename.endswith('.raw')):
            return {'name': 'arXiv'}
        return {'name': 'ADStxt'}

    def get_reference_service_endpoint(self, parser_name):
        # Only concatenated by tests; exact value not important.
        return ''

    # ---------------------------
    # Insert helpers used by add_stub_data()
    # ---------------------------
    def insert_reference_source_record(self, session, reference_record):
        key = (getattr(reference_record, 'bibcode', None),
               getattr(reference_record, 'source_filename', None))
        if key not in self._reference_sources:
            self._reference_sources.add(key)
        return key[0], key[1]

    def insert_history_record(self, session, history_record):
        new_id = len(self._history_ids) + 1
        self._history_ids.append(new_id)
        return new_id

    def insert_resolved_reference_records(self, session, resolved_records):
        self._resolved_count += len(resolved_records or [])
        return True

    # ---------------------------
    # Populate helpers used by tasks/tests
    # ---------------------------
    def populate_tables_pre_resolved_initial_status(self, source_bibcode, source_filename, parsername, references):
        # Ensure ReferenceSource exists
        key = (source_bibcode, source_filename)
        if key not in self._reference_sources:
            self._reference_sources.add(key)

        # Create a new ProcessedHistory run
        self._history_ids.append(len(self._history_ids) + 1)

        # Insert placeholder rows for each reference
        self._resolved_count += len(references or [])

        return list(references or [])

    def populate_tables_pre_resolved_retry_status(self, source_bibcode, source_filename, source_modified, retry_records):
        # Create a new ProcessedHistory run
        self._history_ids.append(len(self._history_ids) + 1)

        # Insert placeholder rows for the retry subset only
        self._resolved_count += len(retry_records or [])

        return list(retry_records or [])

    def populate_tables_post_resolved(self, *args, **kwargs):
        # In the real system, post-resolve typically updates placeholder rows, not insert new ones.
        return True

    def get_count_records(self):
        return [
            {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': len(self._reference_sources)},
            {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': len(self._history_ids)},
            {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': int(self._resolved_count)},
            {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': int(self._compare_classic_count)},
        ]


class TestTasks(unittest.TestCase):

    postgresql_url_dict = {
        'port': 5432,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'postgres'
    }
    postgresql_url = 'postgresql://{user}:{user}@{host}:{port}/{database}' \
        .format(user=postgresql_url_dict['user'],
                host=postgresql_url_dict['host'],
                port=postgresql_url_dict['port'],
                database=postgresql_url_dict['database']
                )

    def setUp(self):
        self.test_dir = os.path.join(project_home, 'adsrefpipe/tests')
        unittest.TestCase.setUp(self)

        # Use a fake app that never opens a DB connection.
        self.app = _FakeApp('test', local_config={
            'SQLALCHEMY_URL': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': project_home,
            'TEST_DIR': self.test_dir,
            'COMPARE_CLASSIC': False,
            'REFERENCE_PIPELINE_SERVICE_URL': 'http://0.0.0.0:5000/reference'
        })

        # Monkey-patch tasks to use our fake app.
        tasks.app = self.app

        # Populate stub data through fake session/app helpers.
        self.add_stub_data()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == self.postgresql_url
        assert self.app.conf.get('SQLALCHEMY_URL') == self.postgresql_url

    def add_stub_data(self):
        """Add stub data (DB operations are mocked/in-memory)."""
        self.arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')

        reference_source = [
            ('0002arXiv.........Z', os.path.join(self.arXiv_stubdata_dir, '00002.raw'), '00002.raw.result', 'arXiv'),
        ]

        processed_history = [
            ('2020-04-03 18:08:42', '2020-05-11 11:13:53', '67', '61'),
        ]

        resolved_reference = [
            [
                ('Alsubai, K. A., Parley, N. R., Bramich, D. M., et al. 2011, MNRAS, 417, 709.', '2011MNRAS.417..709A', 1.0),
                ('Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ', '2019A&A...625A.136A', 1.0)
            ],
        ]

        with self.app.session_scope() as session:
            # Keep these lines unchanged; they are safe no-ops under the fake session.
            session.query(Action).delete()
            session.query(Parser).delete()
            session.bulk_save_objects(actions_records)
            session.bulk_save_objects(parsers_records)
            session.commit()

            for i, (a_reference, a_history) in enumerate(zip(reference_source, processed_history)):
                reference_record = ReferenceSource(bibcode=a_reference[0],
                                                   source_filename=a_reference[1],
                                                   resolved_filename=a_reference[2],
                                                   parser_name=a_reference[3])
                bibcode, source_filename = self.app.insert_reference_source_record(session, reference_record)
                self.assertTrue(bibcode == a_reference[0])
                self.assertTrue(source_filename == a_reference[1])

                history_record = ProcessedHistory(bibcode=bibcode,
                                                 source_filename=source_filename,
                                                 source_modified=a_history[0],
                                                 status=Action().get_status_new(),
                                                 date=a_history[1],
                                                 total_ref=a_history[2])
                history_id = self.app.insert_history_record(session, history_record)
                self.assertTrue(history_id != -1)

                resolved_records = []
                for j, service in enumerate(resolved_reference[i]):
                    resolved_record = ResolvedReference(history_id=history_id,
                                                        item_num=j + 1,
                                                        reference_str=service[0],
                                                        bibcode=service[1],
                                                        score=service[2],
                                                        reference_raw=service[0])
                    resolved_records.append(resolved_record)
                success = self.app.insert_resolved_reference_records(session, resolved_records)
                self.assertTrue(success is True)
                session.commit()

    def test_process_references(self):
        """test process_references task"""

        resolved_reference = [
            {
                "score": "1.0",
                "bibcode": "2011LRR....14....2U",
                "refstr": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "id": "H1I1"
            },
            {
                "score": "1.0",
                "bibcode": "2017RPPh...80l6902M",
                "refstr": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "id": "H1I2",
            }
        ]

        # Patch the exact dependency used inside adsrefpipe.tasks.task_process_reference
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference",
                   return_value=resolved_reference), \
             patch("adsrefpipe.tasks.app.populate_tables_post_resolved",
                   return_value=True):

            filename = os.path.join(self.arXiv_stubdata_dir, '00001.raw')
            parser_dict = self.app.get_parser(filename)
            parser = verify(parser_dict.get('name'))

            # now process the source file
            toREFs = parser(filename=filename, buffer=None)
            self.assertTrue(toREFs)
            parsed_references = toREFs.process_and_dispatch()
            self.assertTrue(parsed_references)

            for block_references in parsed_references:
                self.assertTrue('bibcode' in block_references)
                self.assertTrue('references' in block_references)
                references = self.app.populate_tables_pre_resolved_initial_status(
                    source_bibcode=block_references['bibcode'],
                    source_filename=filename,
                    parsername=parser_dict.get('name'),
                    references=block_references['references']
                )
                self.assertTrue(references)

                # Simulate resolving each reference by calling the task (synchronously via .run()).
                for reference in references:
                    ok = tasks.task_process_reference.run({
                        'reference': reference,
                        'resolver_service_url': self.app._config['REFERENCE_PIPELINE_SERVICE_URL'] +
                                               self.app.get_reference_service_endpoint(parser_dict.get('name')),
                        'source_bibcode': block_references['bibcode'],
                        'source_filename': filename
                    })
                    self.assertTrue(ok)

            expected_count = [
                {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 2},
                {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 2},
                {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 4},
                {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}
            ]
            self.assertTrue(self.app.get_count_records() == expected_count)

    def test_reprocess_subset_references(self):
        """test reprocess_subset_references task"""

        reprocess_record = [
            {
                'source_filename': os.path.join(self.arXiv_stubdata_dir, '00002.raw'),
                'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 42),
                'parser_name': 'arXiv',
                'block_references': [{
                    'source_bibcode': '0002arXiv.........Z',
                    'references': [{
                        'item_num': 2,
                        'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                        'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '
                    }]
                }]
            }
        ]

        resolved_reference = [
            {
                "score": "1.0",
                "bibcode": "2019A&A...625A.136A",
                "refstr": "Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ",
                "id": "H1I1"
            }
        ]

        # Patch the exact dependency used inside adsrefpipe.tasks.task_process_reference
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference",
                   return_value=resolved_reference), \
             patch("adsrefpipe.tasks.app.populate_tables_post_resolved",
                   return_value=True):

            parser_dict = self.app.get_parser(reprocess_record[0]['source_filename'])
            parser = verify(parser_dict.get('name'))

            # now process the buffer
            toREFs = parser(filename=None, buffer=reprocess_record[0])
            self.assertTrue(toREFs)
            parsed_references = toREFs.process_and_dispatch()
            self.assertTrue(parsed_references)

            for block_references in parsed_references:
                self.assertTrue('bibcode' in block_references)
                self.assertTrue('references' in block_references)
                references = self.app.populate_tables_pre_resolved_retry_status(
                    source_bibcode=block_references['bibcode'],
                    source_filename=reprocess_record[0]['source_filename'],
                    source_modified=reprocess_record[0]['source_modified'],
                    retry_records=block_references['references']
                )
                self.assertTrue(references)

            for reference in references:
                ok = tasks.task_process_reference.run({
                    'reference': reference,
                    'resolver_service_url': self.app._config['REFERENCE_PIPELINE_SERVICE_URL'] +
                                           self.app.get_reference_service_endpoint(parser_dict.get('name')),
                    'source_bibcode': block_references['bibcode'],
                    'source_filename': reprocess_record[0]['source_filename']
                })
                self.assertTrue(ok)

            expected_count = [
                {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 1},
                {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 2},
                {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 3},
                {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}
            ]
            self.assertTrue(self.app.get_count_records() == expected_count)

    def test_task_process_reference_error(self):
        """test task_process_reference when utils method returns False"""

        reference_task = {
            'reference': [{'item_num': 2,
                           'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                           'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        # mock post_request_resolved_reference to return false to trigger FailedRequest
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", return_value=False):
            with self.assertRaises(tasks.FailedRequest):
                tasks.task_process_reference.run(reference_task)

    def test_task_process_reference_exception(self):
        """test task_process_reference when KeyError is raised"""

        reference_task = {
            'reference': [{'item_num': 2,
                           'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                           'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        # mock post_request_resolved_reference to raise KeyError
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", side_effect=KeyError):
            self.assertFalse(tasks.task_process_reference.run(reference_task))

    def test_task_process_reference_success(self):
        """test task_process_reference successfully returns True"""

        reference_task = {
            'reference': [{'item_num': 2,
                           'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                           'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        # Mock post_request_resolved_reference to return a valid resolved reference
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", return_value=["resolved_ref"]), \
             patch("adsrefpipe.tasks.app.populate_tables_post_resolved", return_value=True):
            self.assertTrue(tasks.task_process_reference.run(reference_task))


if __name__ == '__main__':
    unittest.main()

