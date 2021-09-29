import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import datetime
import unittest
from mock import patch

from adsrefpipe import app, tasks, utils
from adsrefpipe.models import Base, Action, Parser, ReferenceSource, ProcessedHistory, ResolvedReference, CompareClassic
from adsrefpipe.refparsers.handler import verify

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

    actions = [
        Action(status='initial'),
        Action(status='retry'),
        Action(status='delete'),
    ]
    parsers = [
        Parser(name='arXiv', source_pattern=r'^.\d{4,5}.raw$', reference_service_endpoint='/text'),
        Parser(name='AGU', source_pattern='^.agu.xml', reference_service_endpoint='/xml'),
        Parser(name='AIP', source_pattern='^.aip.xml', reference_service_endpoint='/xml'),
        Parser(name='APS', source_pattern='^.ref.xml', reference_service_endpoint='/xml'),
        Parser(name='CrossRef', source_pattern='^.xref.xml', reference_service_endpoint='/xml'),
        Parser(name='ELSEVIER', source_pattern='^.elsevier.xml', reference_service_endpoint='/xml'),
        Parser(name='IOP', source_pattern='^.iop.xml', reference_service_endpoint='/xml'),
        Parser(name='JATS', source_pattern='^.jats.xml', reference_service_endpoint='/xml'),
        Parser(name='NATURE', source_pattern='^.nature.xml', reference_service_endpoint='/xml'),
        Parser(name='NLM', source_pattern='^.nlm3.xml', reference_service_endpoint='/xml'),
        Parser(name='SPRINGER', source_pattern='^.springer.xml', reference_service_endpoint='/xml'),
        Parser(name='WILEY', source_pattern='^.wiley2.xml', reference_service_endpoint='/xml'),
    ]

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
        tasks.app = self.app # monkey-patch the app object
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        self.add_stub_data()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == self.postgresql_url
        assert self.app.conf.get('SQLALCHEMY_URL') == self.postgresql_url

    def add_stub_data(self):
        """ Add stub data """
        self.stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata')

        reference_source = [
            ('2020arXiv200400014K',os.path.join(self.stubdata_dir,'00014.raw'),'00014.raw.result'),
        ]

        processed_history = [
            ('2020-04-03 18:08:42', '2020-05-11 11:13:53', '67', '61'),
        ]

        resolved_reference = [
            [
                ('Alsubai, K. A., Parley, N. R., Bramich, D. M., et al. 2011, MNRAS, 417, 709.','2011MNRAS.417..709A',1.0),
                ('Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ','2019A&A...625A.136A',1.0)
            ],
        ]

        with self.app.session_scope() as session:
            session.bulk_save_objects(self.actions)
            session.bulk_save_objects(self.parsers)
            session.commit()

            for i, (a_reference,a_history) in enumerate(zip(reference_source,processed_history)):
                    reference_record = ReferenceSource(bibcode=a_reference[0],
                                                 source_filename=a_reference[1],
                                                 resolved_filename=a_reference[2],
                                                 parser_name=self.app.get_parser_name(a_reference[1]))
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
                                                   item_num=j+1,
                                                   reference_str=service[0],
                                                   bibcode=service[1],
                                                   score=service[2],
                                                   reference_raw=service[0])
                        resolved_records.append(resolved_record)
                    success = self.app.insert_resolved_referencce_records(session, resolved_records)
                    self.assertTrue(success == True)
                    session.commit()

    def test_process_references(self):
        """ test process_references task """

        resolved_reference = [
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

        with patch.object(utils, 'get_resolved_references') as mock_resolved_references:
            mock_resolved_references.return_value = resolved_reference
            filename = os.path.join(self.test_dir, 'unittests/stubdata','00013.raw')
            parser_name = self.app.get_parser_name(filename)
            parser = verify(parser_name)
            # now process the source file
            toREFs = parser(filename=filename, buffer=None, parsername=parser_name)
            self.assertTrue(toREFs)
            tasks.task_process_references(toREFs)
            expected_count = [{'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 2},
                              {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 2},
                              {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 4},
                              {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}]
            self.assertTrue(self.app.get_count_records() == expected_count)

    def test_reprocess_subset_references(self):
        """ test reprocess_subset_references task """

        reprocess_records = [
            {
                'source_bibcode': '2020arXiv200400014K',
                'source_filename': os.path.join(project_home, 'adsrefpipe/tests', 'unittests/stubdata/00014.raw'),
                'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 42),
                'parser_name': 'arXiv',
                'references': [{'item_num': 2, 'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]
            }
        ]
        resolved_references = [
            {
                "score": "1.0",
                "bibcode": "2019A&A...625A.136A",
                "refstring": "Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ",
                "id": "H1I1"
            }
        ]
        with patch.object(utils, 'get_resolved_references') as mock_resolved_references:
            mock_resolved_references.return_value = resolved_references
            parser_name = self.app.get_parser_name(reprocess_records[0]['source_filename'])
            parser = verify(parser_name)
            # now process the buffer
            toREFs = parser(filename=None, buffer=reprocess_records[0], parsername=None)
            if toREFs:
                tasks.task_reprocess_references(toREFs)
            expected_count = [{'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 1},
                              {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 1},
                              {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 2},
                              {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}]
            self.assertTrue(self.app.get_count_records() == expected_count)


if __name__ == '__main__':
    unittest.main()
