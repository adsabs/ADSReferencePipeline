import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import datetime
import unittest
from mock import patch

from adsrefpipe import app, tasks, utils
from adsrefpipe.models import Base, Action, Parser, Reference

class TestWorkers(unittest.TestCase):

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
        self.app = app.ADSReferencePipelineCelery('test', local_config={
            'SQLALCHEMY_URL': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': project_home,
            'TEST_DIR': self.test_dir,
            'COMPARE_CLASSIC': False
        })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == self.postgresql_url
        assert self.app.conf.get('SQLALCHEMY_URL') == self.postgresql_url

    def add_stub_data(self):
        """ Add stub data """
        actions = ['initial', 'retry', 'delete']

        parsers = [
            ('AGU', '.agu.xml'), ('AIP', '.aip.xml'), ('APS', '.ref.xml'), ('CrossRef', '.xref.xml'),
            ('ELSEVIER', '.elsevier.xml'), ('IOP', '.iop.xml'), ('JATS', '.jats.xml'), ('NATURE', '.nature.xml'),
            ('NLM', '.nlm3.xml'), ('SPRINGER', '.springer.xml'), ('Text', '.raw'), ('WILEY', '.wiley2.xml')
        ]
        with self.app.session_scope() as session:
            for action in actions:
                session.add(Action(status=action))
            session.commit()
            for parser in parsers:
                session.add(Parser(name=parser[0], source_extension=parser[1]))
            session.commit()

    def test_process_references(self):
        """ test process_references task """
        self.add_stub_data()
        resolved = [
            {
                "score": "1.0",
                "bibcode": "2011LRR....14....2U",
                "refstring": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "id": "H1I1"
            },
            {
                "score": "1.0",
                "bibcode": "2017RPPh...80l6902M",
                "refstring": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "id": "H1I2",
            }
        ]
        with patch.object(utils, 'resolve_references') as resolve_references, \
            patch.object(tasks.task_process_reference_file, 'apply_async') as process_reference_file:

            resolve_references.return_value = resolved
            tasks.task_process_reference_file(os.path.join(self.test_dir, 'unittests/stubdata','00013.raw'))
            self.assertFalse(process_reference_file.called)

            current_num_records = '' \
                                  'Currently there are 1 records in `Reference` table, which holds reference files information.\n' \
                                  'Currently there are 1 records in `History` table, which holds file level information for resolved run.\n' \
                                  'Currently there are 2 records in `Resolved` table, which holds reference level information for resolved run.\n' \
                                  'Currently there are 0 records in `Compare` table, which holds comparison of new and classic resolved run.\n'
            self.assertTrue(self.app.get_count_records() == current_num_records)

    def test_reprocess_subset_references(self):
        """ test reprocess_subset_references task """
        self.add_stub_data()
        # need a reference record
        with self.app.session_scope() as session:
            success = self.app.insert_reference_record(session,
                                                       Reference(bibcode='2020arXiv200400014K',
                                                                 source_filename=os.path.join(project_home, 'adsrefpipe/tests', 'unittests/stubdata/00014.raw'),
                                                                 resolved_filename='',
                                                                 parser='Text'))
            self.assertTrue(success)

        record = {
             'source_bibcode': '2020arXiv200400014K',
             'source_filename': os.path.join(project_home, 'adsrefpipe/tests', 'unittests/stubdata/00014.raw'),
             'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 42),
             'parser': 'Text',
             'references': [{'item_num': 2, 'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]
        }
        resolved = [
            {
                "score": "1.0",
                "bibcode": "2019A&A...625A.136A",
                "refstring": "Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ",
                "id": "H1I1"
            }
        ]
        with patch.object(utils, 'resolve_references') as resolve_references, \
            patch.object(tasks.task_reprocess_subset_references, 'apply_async') as reprocess_subset_references:

            resolve_references.return_value = resolved
            tasks.task_reprocess_subset_references(record)
            self.assertFalse(reprocess_subset_references.called)

            current_num_records = '' \
                                  'Currently there are 1 records in `Reference` table, which holds reference files information.\n' \
                                  'Currently there are 1 records in `History` table, which holds file level information for resolved run.\n' \
                                  'Currently there are 1 records in `Resolved` table, which holds reference level information for resolved run.\n' \
                                  'Currently there are 0 records in `Compare` table, which holds comparison of new and classic resolved run.\n'
            self.assertTrue(self.app.get_count_records() == current_num_records)
