import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest
import datetime

from adsrefpipe import app
from adsrefpipe.models import Base, Action, Parser, ReferenceSource, ProcessedHistory, ResolvedReference, CompareClassic
from adsrefpipe.utils import ReprocessQueryType
from adsrefpipe.refparsers.CrossRefXML import CrossRefToREFs
from adsrefpipe.refparsers.ElsevierXML import ELSEVIERtoREFs
from adsrefpipe.refparsers.JATSxml import JATStoREFs
from adsrefpipe.refparsers.IOPxml import IOPtoREFs
from adsrefpipe.refparsers.SpringerXML import SPRINGERtoREFs
from adsrefpipe.refparsers.APSxml import APStoREFs
from adsrefpipe.refparsers.NatureXML import NATUREtoREFs
from adsrefpipe.refparsers.AIPxml import AIPtoREFs
from adsrefpipe.refparsers.WileyXML import WILEYtoREFs
from adsrefpipe.refparsers.NLM3xml import NLMtoREFs
from adsrefpipe.refparsers.AGUxml import AGUtoREFs, AGUreference
from adsrefpipe.refparsers.arXivTXT import ARXIVtoREFs
from adsrefpipe.refparsers.handler import verify
from adsrefpipe.tests.unittests.data_test_db_query import actions_records, parsers_records


class TestDatabase(unittest.TestCase):

    """
    Tests the application's methods
    """

    maxDiff = None

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
        })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        self.add_stub_data()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def add_stub_data(self):
        """ Add stub data """
        self.arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')

        reference_source = [
            ('0001arXiv.........Z',os.path.join(self.arXiv_stubdata_dir,'00001.raw'),os.path.join(self.arXiv_stubdata_dir,'00001.raw.result'),'arXiv'),
            ('0002arXiv.........Z',os.path.join(self.arXiv_stubdata_dir,'00002.raw'),os.path.join(self.arXiv_stubdata_dir,'00002.raw.result'),'arXiv'),
            ('0003arXiv.........Z',os.path.join(self.arXiv_stubdata_dir,'00003.raw'),os.path.join(self.arXiv_stubdata_dir,'00003.raw.result'),'arXiv')
        ]

        processed_history = [
            ('2020-04-03 18:08:46', '2020-05-11 11:13:36', '83', '79'),
            ('2020-04-03 18:08:42', '2020-05-11 11:13:53', '67', '61'),
            ('2020-04-03 18:08:32', '2020-05-11 11:14:28', '128', '109')
        ]

        resolved_reference = [
            [
                ('J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ','2011LRR....14....2U',1.0),
                ('C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.','2017RPPh...80l6902M',1.0)
            ],
            [
                ('Alsubai, K. A., Parley, N. R., Bramich, D. M., et al. 2011, MNRAS, 417, 709.','2011MNRAS.417..709A',1.0),
                ('Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ','2019A&A...625A.136A',1.0)
            ],
            [
                ('Abellan, F. J., Indebetouw, R., Marcaide, J. M., et al. 2017, ApJL, 842, L24','2017ApJ...842L..24A',1.0),
                ('Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 ','2016A&A...586A..71A',1.0)
            ],
        ]

        compare_classic = [
            [
                ('2010arXiv1009.5514U',1,'DIFF'),
                ('2017arXiv170902923M',1,'DIFF')
            ],
            [
                ('2011MNRAS.417..709A',1,'MATCH'),
                ('2019A&A...625A.136A',1,'MATCH')
            ],
            [
                ('2017ApJ...842L..24A',1,'MATCH'),
                ('2016A&A...586A..71A',1,'MATCH')
            ]
        ]

        with self.app.session_scope() as session:
            session.bulk_save_objects(actions_records)
            session.bulk_save_objects(parsers_records)
            session.commit()

            for i, (a_reference,a_history) in enumerate(zip(reference_source,processed_history)):
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
                compare_records = []
                for j, (service,classic) in enumerate(zip(resolved_reference[i],compare_classic[i])):
                    resolved_record = ResolvedReference(history_id=history_id,
                                               item_num=j+1,
                                               reference_str=service[0],
                                               bibcode=service[1],
                                               score=service[2],
                                               reference_raw=service[0])
                    resolved_records.append(resolved_record)
                    compare_record = CompareClassic(history_id=history_id,
                                             item_num=j+1,
                                             bibcode=classic[0],
                                             score=classic[1],
                                             state=classic[2])
                    compare_records.append(compare_record)
                success = self.app.insert_resolved_referencce_records(session, resolved_records)
                self.assertTrue(success == True)
                success = self.app.insert_compare_records(session, compare_records)
                self.assertTrue(success == True)
                session.commit()

    def test_query_reference_tbl(self):
        """ test querying reference_source table """
        result_expected = [
            {
                'bibcode': '0001arXiv.........Z',
                'source_filename': os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                'resolved_filename': os.path.join(self.arXiv_stubdata_dir,'00001.raw.result'),
                'parser_name': 'arXiv',
                'num_runs': 1,
                'last_run_date': '2020-05-11 11:13:36',
                'num_references': 2,
                'num_resolved_references': 2
            }, {
                'bibcode': '0002arXiv.........Z',
                'source_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
                'resolved_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw.result'),
                'parser_name': 'arXiv',
                'num_runs': 1,
                'last_run_date': '2020-05-11 11:13:53',
                'num_references': 2,
                'num_resolved_references': 2
            }, {
                'bibcode': '0003arXiv.........Z',
                'source_filename': os.path.join(self.arXiv_stubdata_dir,'00003.raw'),
                'resolved_filename': os.path.join(self.arXiv_stubdata_dir,'00003.raw.result'),
                'parser_name': 'arXiv',
                'num_runs': 1,
                'last_run_date': '2020-05-11 11:14:28',
                'num_references': 2,
                'num_resolved_references': 2
            }
        ]

        # test querying bibcodes
        bibcodes = ['0001arXiv.........Z', '0002arXiv.........Z', '0003arXiv.........Z']
        result_got = self.app.diagnostic_query(bibcode_list=bibcodes)
        self.assertTrue(result_expected == result_got)

        # test querying filenames
        filenames = [os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                     os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
                     os.path.join(self.arXiv_stubdata_dir,'00003.raw')]
        result_got = self.app.diagnostic_query(source_filename_list=filenames)
        self.assertTrue(result_expected == result_got)

        # test querying both bibcodes and filenames
        result_got = self.app.diagnostic_query(bibcode_list=bibcodes, source_filename_list=filenames)
        self.assertTrue(result_expected == result_got)

        # test if nothing is passed, which return 10 records max
        result_got = self.app.diagnostic_query()
        self.assertTrue(result_expected == result_got)

    def test_query_reference_tbl_when_non_exits(self):
        """ verify non existence reference_source record """

        # test when bibcode does not exist
        self.assertTrue(self.app.diagnostic_query(bibcode_list=['0004arXiv.........Z']) == [])

        # test when filename does not exist
        self.assertTrue(self.app.diagnostic_query(source_filename_list=os.path.join(self.arXiv_stubdata_dir,'00004.raw')) == [])

        # test when both bibcode and filename are passed and nothing is returned
        self.assertTrue(self.app.diagnostic_query(bibcode_list=['0004arXiv.........Z'],
                                                     source_filename_list=os.path.join(self.arXiv_stubdata_dir,'00004.raw')) == [])

    def test_insert_reference_record(self):
        """ test inserting reference_source record """

        # attempt to insert a record that already exists in db
        # see that it is returned without it being inserted
        with self.app.session_scope() as session:
            count = self.app.get_count_reference_source_records(session)
            reference_record = ReferenceSource(bibcode='0001arXiv.........Z',
                                         source_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                                         resolved_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw.result'),
                                         parser_name=self.app.get_parser(os.path.join(self.arXiv_stubdata_dir,'00001.raw')).get('name'))
            bibcode, source_filename = self.app.insert_reference_source_record(session, reference_record)
            self.assertTrue(bibcode == '0001arXiv.........Z')
            self.assertTrue(source_filename == os.path.join(self.arXiv_stubdata_dir,'00001.raw'))
            self.assertTrue(self.app.get_count_reference_source_records(session) == count)

    def test_parser_name(self):
        """ test getting parser name from extension method """
        parser = {
            'CrossRef': ['/PLoSO/0007/10.1371_journal.pone.0048146.xref.xml', CrossRefToREFs],
            'ELSEVIER': ['/AtmEn/0230/iss.elsevier.xml', ELSEVIERtoREFs],
            'JATS': ['/NatSR/0009/iss36.jats.xml', JATStoREFs],
            'IOP': ['/JPhCS/1085/iss4.iop.xml', IOPtoREFs],
            'SPRINGER': ['/JHEP/2019/iss06.springer.xml', SPRINGERtoREFs],
            'APS': ['/PhRvB/0081/2010PhRvB..81r4520P.ref.xml', APStoREFs],
            'NATURE': ['/Natur/0549/iss7672.nature.xml', NATUREtoREFs],
            'AIP': ['/ApPhL/0102/iss7.aip.xml', AIPtoREFs],
            'WILEY': ['/JGR/0101/issD14.wiley2.xml', WILEYtoREFs],
            'NLM': ['/PNAS/0109/iss17.nlm3.xml', NLMtoREFs],
            'AGU': ['/JGR/0101/issD14.agu.xml', AGUtoREFs],
            'arXiv': ['/arXiv/2011/00324.raw', ARXIVtoREFs],
        }
        for name,info in parser.items():
            self.assertEqual(name, self.app.get_parser(info[0]).get('name'))
            self.assertEqual(info[1], verify(name))
        # now verify couple of errors
        self.assertEqual(self.app.get_parser('/RScI/0091/2020RScI...91e3301A.aipft.xml').get('name', {}), {})
        self.assertEqual(self.app.get_parser('/arXiv/2004/15000.1raw').get('name', {}), {})

    def test_reference_service_endpoint(self):
        """ test getting reference service endpoint from parser name method """
        parser = {
            'CrossRef': '/xml',
            'ELSEVIER': '/xml',
            'JATS': '/xml',
            'IOP': '/xml',
            'SPRINGER': '/xml',
            'APS': '/xml',
            'NATURE': '/xml',
            'AIP': '/xml',
            'WILEY': '/xml',
            'NLM': '/xml',
            'AGU': '/xml',
            'arXiv': '/text',
            'AEdRvHTML': '/text',
        }
        for name,endpoint in parser.items():
            self.assertEqual(endpoint, self.app.get_reference_service_endpoint(name))
        # now verify an error
        self.assertEqual(self.app.get_reference_service_endpoint('errorname'), '')

    def test_stats_compare(self):
        """ test the display of statistics comparing classic and new resolver """
        result_expected = "" \
            "+--------------------------------------------------------------+---------------------+---------------------+-----------------+-----------------+-------+-------+-------+-------+-------+\n" \
            "|                            refstr                            |   service_bibcode   |   classic_bibcode   |  service_conf   |  classic_score  | match | miss  |  new  | newu  | diff  |\n" \
            "+==============================================================+=====================+=====================+=================+=================+=======+=======+=======+=======+=======+\n" \
            "| J.-P. Uzan, Varying constants, gravitation and cosmology,    | 2011LRR....14....2U | 2010arXiv1009.5514U |       1.0       |        1        |       |       |       |       | DIFF  |\n" \
            "| Living Rev. Rel. 14 (2011) 2, [1009.5514].                   |                     |                     |                 |                 |       |       |       |       |       |\n" \
            "+--------------------------------------------------------------+---------------------+---------------------+-----------------+-----------------+-------+-------+-------+-------+-------+\n" \
            "| C. J. A. P. Martins, The status of varying constants: A      | 2017RPPh...80l6902M | 2017arXiv170902923M |       1.0       |        1        |       |       |       |       | DIFF  |\n" \
            "| review of the physics, searches and implications,            |                     |                     |                 |                 |       |       |       |       |       |\n" \
            "| 1709.02923.                                                  |                     |                     |                 |                 |       |       |       |       |       |\n" \
            "+--------------------------------------------------------------+---------------------+---------------------+-----------------+-----------------+-------+-------+-------+-------+-------+"
        result_got, num_references, num_resolved = self.app.get_service_classic_compare_stats_grid(source_bibcode='0001arXiv.........Z',
                                                                                                   source_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw'))
        self.assertEqual(result_got, result_expected)
        self.assertEqual(num_references, 2)
        self.assertEqual(num_resolved, 2)

    def test_reprocess_references(self):
        """ test reprocessing references """
        result_expected_year = [
            {'source_bibcode': '0002arXiv.........Z',
             'source_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
             'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 42),
             'parser_name': 'arXiv',
             'references': [{'item_num': 2,
                             'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                             'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]}
        ]
        result_expected_bibstem = [
            {'source_bibcode': '0002arXiv.........Z',
             'source_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
             'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 42),
             'parser_name': 'arXiv',
             'references': [{'item_num': 2,
                             'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                             'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]
            },
            {'source_bibcode': '0003arXiv.........Z',
             'source_filename': os.path.join(self.arXiv_stubdata_dir,'00003.raw'),
             'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 32),
             'parser_name': 'arXiv',
             'references': [{'item_num': 2,
                             'refstr': 'Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 ',
                             'refraw': 'Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 '}]
            }
        ]
        self.assertEqual(self.app.get_reprocess_records(ReprocessQueryType.year, match_bibcode='2019', score_cutoff=None, date_cutoff=None), result_expected_year)
        self.assertEqual(self.app.get_reprocess_records(ReprocessQueryType.bibstem, match_bibcode='A&A..', score_cutoff=None, date_cutoff=None), result_expected_bibstem)

        references_and_ids_year = [
            {'id': 'H4I2', 'reference': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}
        ]
        reprocess_references = self.app.populate_tables_pre_resolved_retry_status(
            source_bibcode=result_expected_year[0]['source_bibcode'],
            source_filename=result_expected_year[0]['source_filename'],
            source_modified=result_expected_year[0]['source_modified'],
            retry_records=result_expected_year[0]['references'])
        self.assertTrue(reprocess_references)
        self.assertTrue(reprocess_references, references_and_ids_year)
        current_num_records = [
            {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 3},
            {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 4},
            {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 7},
            {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 6}
        ]
        self.assertTrue(self.app.get_count_records() == current_num_records)

    def test_match_parser(self):
        """ test match_parser returning correct parser name for the same journal and extension """
        self.assertTrue(self.app.get_parser('OTHER/2007AIPC..948..357M/2007AIPC..948..357M.raw')['name'] == 'ADStxt')
        self.assertTrue(self.app.get_parser('OTHER/Astro2020/2019arXiv190309325N.raw')['name'] == 'arXiv')

class TestDatabaseNoStubdata(unittest.TestCase):

    """
    Tests the application's methods when there is no need for shared stubdata
    """

    maxDiff = None

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

    def test_query_reference_tbl_when_empty(self):
        """ verify reference_source table being empty """
        self.assertTrue(self.app.diagnostic_query() == [])

    def test_populate_tables(self):
        """ test populating all tables """
        references = [
            {
                "refstr": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "refraw": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. "},
            {
                "refstr": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "refraw": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923."
            }
        ]
        references_and_ids = [
            {
                "refstr": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "refraw": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "id": "H1I1"
            },
            {
                "refstr": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "refraw": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "id": "H1I2"}
        ]
        resolved_references = [
            {
                "score": "1.0",
                "bibcode": "2011LRR....14....2U",
                "refstring": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "refraw": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "id": "H1I1"
            },
            {
                "score": "1.0",
                "bibcode": "2017RPPh...80l6902M",
                "refstring": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "refraw": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "id": "H1I2",
            }
        ]
        arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')
        with self.app.session_scope() as session:
            session.bulk_save_objects(actions_records)
            session.bulk_save_objects(parsers_records)
            session.commit()

            references = self.app.populate_tables_pre_resolved_initial_status(
                source_bibcode='0001arXiv.........Z',
                source_filename=os.path.join(arXiv_stubdata_dir,'00001.raw'),
                parsername=self.app.get_parser(os.path.join(arXiv_stubdata_dir,'00001.raw')).get('name'),
                references=references)

            self.assertTrue(references)
            self.assertTrue(references == references_and_ids)

            status = self.app.populate_tables_post_resolved(
                resolved_reference=resolved_references,
                source_bibcode='0001arXiv.........Z',
                classic_resolved_filename=os.path.join(arXiv_stubdata_dir, '00001.raw.result'))
            self.assertTrue(status == True)


if __name__ == '__main__':
    unittest.main()
