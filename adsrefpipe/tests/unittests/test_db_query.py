import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest
import datetime

from adsrefpipe import app
from adsrefpipe.models import Base, Action, Parser, Reference, History, Resolved, Compare

import traceback

class test_database(unittest.TestCase):

    """
    Tests the application's methods
    """

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

    def add_stub_data(self):
        """
        Add stub data
        :return:
        """
        stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata')

        actions = ['initial', 'retry', 'delete']

        parsers = ['AGU', 'AIP', 'APS', 'CrossRef', 'ELSEVIER', 'IOP',
                   'JATS', 'NATURE', 'NLM', 'SPRINGER', 'Text', 'WILEY']

        reference = [
            ('2020arXiv200400013L',os.path.join(stubdata_dir,'00013.raw'),'00013.raw.result'),
            ('2020arXiv200400014K',os.path.join(stubdata_dir,'00014.raw'),'00014.raw.result'),
            ('2020arXiv200400016L',os.path.join(stubdata_dir,'00016.raw'),'00016.raw.result')
        ]

        history = [
            ('2020-04-03 18:08:46', '2020-05-11 11:13:36', '83', '79'),
            ('2020-04-03 18:08:42', '2020-05-11 11:13:53', '67', '61'),
            ('2020-04-03 18:08:32', '2020-05-11 11:14:28', '128', '109')
        ]

        resolved = [
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

        compare = [
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
            for action in actions:
                session.add(Action(status=action))
            session.commit()
            for parser in parsers:
                session.add(Parser(name=parser))
            session.commit()

            for i, (a_reference,a_history) in enumerate(zip(reference,history)):
                reference_record = Reference(bibcode=a_reference[0],
                                             source_filename=a_reference[1],
                                             resolved_filename=a_reference[2],
                                             parser=Parser().get_name(a_reference[1]))
                bibcode, source_filename = self.app.insert_reference_record(session, reference_record)
                self.assertTrue(bibcode == a_reference[0])
                self.assertTrue(source_filename == a_reference[1])

                history_record = History(bibcode=bibcode,
                                         source_filename=source_filename,
                                         source_modified=a_history[0],
                                         status=Action().get_status_new(),
                                         date=a_history[1],
                                         total_ref=a_history[2],
                                         resolved_ref=a_history[3])
                history_id = self.app.insert_history_record(session, history_record)
                self.assertTrue(history_id != -1)

                resolved_records = []
                compare_records = []
                for j, (service,classic) in enumerate(zip(resolved[i],compare[i])):
                    resolved_record = Resolved(history_id=history_id,
                                               item_num=j+1,
                                               reference_str=service[0],
                                               bibcode=service[1],
                                               score=service[2])
                    resolved_records.append(resolved_record)
                    compare_record = Compare(history_id=history_id,
                                             item_num=j+1,
                                             bibcode=classic[0],
                                             score=classic[1],
                                             state=classic[2])
                    compare_records.append(compare_record)
                success = self.app.insert_resolved_records(session, resolved_records)
                self.assertTrue(success == True)
                success = self.app.insert_compare_records(session, compare_records)
                self.assertTrue(success == True)

    def test_query_reference_tbl(self):
        """

        :return:
        """
        self.add_stub_data()
        stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata')
        result_expected = [
            {
                'bibcode': '2020arXiv200400013L',
                'source_filename': os.path.join(stubdata_dir,'00013.raw'),
                'resolved_filename': '00013.raw.result',
                'parser': 'Text',
            }, {
                'bibcode': '2020arXiv200400014K',
                'source_filename': os.path.join(stubdata_dir,'00014.raw'),
                'resolved_filename': '00014.raw.result',
                'parser': 'Text',
            }, {
                'bibcode': '2020arXiv200400016L',
                'source_filename': os.path.join(stubdata_dir,'00016.raw'),
                'resolved_filename': '00016.raw.result',
                'parser': 'Text',
            }
        ]

        # test querying bibcodes
        bibcodes = ['2020arXiv200400013L', '2020arXiv200400014K', '2020arXiv200400016L']
        result_got = self.app.query_reference_tbl(bibcode_list=bibcodes)
        self.assertTrue(result_expected == result_got)

        # test querying filenames
        filenames = [os.path.join(stubdata_dir,'00013.raw'),
                     os.path.join(stubdata_dir,'00014.raw'),
                     os.path.join(stubdata_dir,'00016.raw')]
        result_got = self.app.query_reference_tbl(source_filename_list=filenames)
        self.assertTrue(result_expected == result_got)

        # test querying both bibcodes and filenames
        result_got = self.app.query_reference_tbl(bibcode_list=bibcodes, source_filename_list=filenames)
        self.assertTrue(result_expected == result_got)

        # test if nothing is passed, which return 10 records max
        result_got = self.app.query_reference_tbl()
        self.assertTrue(result_expected == result_got)

    def test_query_reference_tbl_when_non_exits(self):
        """

        :return:
        """
        self.add_stub_data()
        stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata')

        # test when bibcode does not exist
        self.assertTrue(self.app.query_reference_tbl(bibcode_list=['2020arXiv200400015L']) == None)

        # test when filename does not exist
        self.assertTrue(self.app.query_reference_tbl(source_filename_list=os.path.join(stubdata_dir,'00015.raw')) == None)

        # test when both bibcode and filename are passed and nothing is returned
        self.assertTrue(self.app.query_reference_tbl(bibcode_list=['2020arXiv200400015L'],
                                                     source_filename_list=os.path.join(stubdata_dir,'00013.raw')) == None)

    def test_query_reference_tbl_when_empty(self):
        """

        :return:
        """
        self.assertTrue(self.app.query_reference_tbl() == None)


    def test_insert_reference_record(self):
        """

        :return:
        """
        self.add_stub_data()
        stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata')

        # attempt to insert a record that already exists in db
        # see that it is returned without it being inserted
        with self.app.session_scope() as session:
            count = self.app.get_count_reference_records(session)
            reference_record = Reference(bibcode='2020arXiv200400013L',
                                         source_filename=os.path.join(stubdata_dir,'00013.raw'),
                                         resolved_filename='00013.raw.result',
                                         parser='Text')
            bibcode, source_filename = self.app.insert_reference_record(session, reference_record)
            self.assertTrue(bibcode == '2020arXiv200400013L')
            self.assertTrue(source_filename == os.path.join(stubdata_dir,'00013.raw'))
            self.assertTrue(self.app.get_count_reference_records(session) == count)


    def test_populate_tables(self):
        """

        :return:
        """

        stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata')
        actions = ['initial', 'retry', 'delete']
        parsers = ['AGU', 'AIP', 'APS', 'CrossRef', 'ELSEVIER', 'IOP',
                   'JATS', 'NATURE', 'NLM', 'SPRINGER', 'Text', 'WILEY']
        resolved = [
            {
                "score": "1.0",
                "bibcode": "2011LRR....14....2U",
                "refstring": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. "
            },
            {
                "score": "1.0",
                "bibcode": "2017RPPh...80l6902M",
                "refstring": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923."
            }
        ]
        with self.app.session_scope() as session:
            for action in actions:
                session.add(Action(status=action))
            session.commit()
            for parser in parsers:
                session.add(Parser(name=parser))
            session.commit()

            result = self.app.populate_tables(bibcode='2020arXiv200400013L',
                                     source_filename=os.path.join(stubdata_dir,'00013.raw'),
                                     resolved=resolved,
                                     classic_resolved_filename=os.path.join(stubdata_dir,'00013.raw.result'))
            self.assertTrue(result == True)


if __name__ == '__main__':
    unittest.main()
