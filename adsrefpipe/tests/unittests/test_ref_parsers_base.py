import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import re

import unittest
from unittest.mock import patch, mock_open, MagicMock

from adsrefpipe.refparsers.toREFs import toREFs, TXTtoREFs, XMLtoREFs, OCRtoREFs, TEXtoREFs, HTMLtoREFs
from adsrefpipe.refparsers.arXivTXT import ARXIVtoREFs
from adsrefpipe.refparsers.reference import Reference, ReferenceError, XMLreference, LatexReference


class TestToREFs(unittest.TestCase):

    def test_is_bibcode(self):
        """ test is_bibcode """
        torefs = toREFs()

        self.assertTrue(torefs.is_bibcode('2020A&A...640A..49Z'))
        self.assertFalse(torefs.is_bibcode('invalid_bibcode'))

    def test_get_bibcodes(self):
        """ test the get_bibcodes method """
        torefs = toREFs()

        torefs.raw_references = [
            {'bibcode': '2021A&A...12345678A', 'block_references': []},
            {'bibcode': '2022ApJ...98765432B', 'block_references': []},
        ]
        bibcodes = torefs.get_bibcodes()
        self.assertEqual(bibcodes, ['2021A&A...12345678A', '2022ApJ...98765432B'])

        # test when raw_references is empty
        torefs.raw_references = []
        bibcodes = torefs.get_bibcodes()
        self.assertEqual(bibcodes, [])

    def test_process_and_dispatch(self):
        """ test the abstract method """
        torefs = ARXIVtoREFs(filename='', buffer={})

        result = torefs.prcess_and_dispatch()
        self.assertEqual(result, None)

    def test_dispatch(self):
        """ test the dispatch method """
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/txt/arXiv/0/00000.raw')
        torefs = ARXIVtoREFs(filename=filename, buffer={})

        results = torefs.dispatch()
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]['bibcode'], '0001arXiv.........Z')
        self.assertEqual(len(results[0]['references']), 2)

    def test_has_arxiv_id(self):
        """ test has_arxiv_id method """
        torefs = toREFs()

        # test case 1: old format arXiv ID
        reference_with_arxiv = "arXiv:quant-ph/1234567"
        result = torefs.has_arXiv_id(reference_with_arxiv)
        self.assertTrue(result)

        # test case 2: reference without arXiv ID
        reference_without_arxiv = "Some other reference"
        result = torefs.has_arXiv_id(reference_without_arxiv)
        self.assertFalse(result)

        # test case 3: new arXiv ID format
        reference_with_new_arxiv = "arXiv:2025.00123v1"
        result = torefs.has_arXiv_id(reference_with_new_arxiv)
        self.assertTrue(result)

    def test_any_item_num(self):
        """ test the any_item_num method """
        torefs = toREFs()

        # test case 1: valid index
        item_nums = [101, 102, 103, 104]
        idx = 2
        result = torefs.any_item_num(item_nums, idx)
        self.assertEqual(result, {'item_num': 103})

        # test case 2: index out of bounds
        item_nums = [101, 102, 103, 104]
        idx = 10
        result = torefs.any_item_num(item_nums, idx)
        self.assertEqual(result, {})

        # test case 3: empty list
        item_nums = []
        idx = 0
        result = torefs.any_item_num(item_nums, idx)
        self.assertEqual(result, {})

        # test case 4: negative index
        item_nums = [101, 102, 103, 104]
        idx = -1
        result = torefs.any_item_num(item_nums, idx)
        self.assertEqual(result, {'item_num': 104})

    def test_merge(self):
        """ test merge method """
        dict1 = {'key1': 'value1'}
        dict2 = {'key2': 'value2'}

        torefs = toREFs()
        merged_dict = torefs.merge(dict1, dict2)

        self.assertEqual(merged_dict, {'key1': 'value1', 'key2': 'value2'})


class TestTXTtoREFs(unittest.TestCase):

    def test_cleanup(self):
        """ test the cleanup method """
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/txt/arXiv/0/00000.raw')
        torefs = TXTtoREFs(filename=filename, buffer={}, parsername='arXiv')

        result = torefs.cleanup(reference="Some cleaned-up reference &amp;")
        self.assertEqual(result, "Some cleaned-up reference &")

        # test case where 'stacks.iop.org' is part of the reference
        result = torefs.cleanup(reference="This is a reference with stacks.iop.org and i=10.1088/xyz")

    def test_process_a_reference(self):
        """ test process_a_reference method """

        # test case 1: using filename
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/txt/arXiv/0/00000.raw')
        torefs = TXTtoREFs(filename=filename, buffer={}, parsername='arXiv')

        results, prev_reference, block_references = torefs.process_a_reference(
            is_enumerated=True,
            line='J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].',
            next_line='C. J. A. P. Martins, "The status of varying constants: A review of the physics, searches and implications", 1709.02923.',
            reference='J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].',
            prev_reference='',
            block_references=[]
        )

        self.assertEqual(results, 'J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514]. J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].')
        self.assertEqual(prev_reference, '')

        # test case 2: having it in the buffer
        buffer = {
            "source_filename": os.path.abspath(os.path.dirname(__file__) + "/stubdata/txt/arXiv/0/00000.raw"),
            "parser_name": "arXiv",
            "block_references": [
                {'source_bibcode': '0001arXiv.........Z',
                 'references': [{'item_num': '1',
                                 'refraw': 'J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].'},
                                {'item_num': '2',
                                 'refraw': 'C. J. A. P. Martins, "The status of varying constants: A review of the physics, searches and implications", 1709.02923.'}]
                },
                {'source_bibcode': '0002arXiv.........Z',
                 'references': [{'item_num': '1',
                                 'refraw': 'Alsubai, K. A., Parley, N. R., Bramich, D. M., et al. 2011,,, MNRAS, 417, 709.'},
                                {'item_num': '2',
                                 'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136'}]
                },
                {'source_bibcode': '0003arXiv.........Z',
                 'references': [{'item_num': '1',
                                 'refraw': 'Abellan, F. J., Indebetouw, R., Marcaide, J. M., et al. 2017, ApJL, 842, L24'},
                                {'item_num': '2',
                                 'refraw': 'Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71'}]
                }
            ]
        }
        torefs = TXTtoREFs(filename='', buffer=buffer, parsername='arXiv')

        results, prev_reference, block_references = torefs.process_a_reference(
            is_enumerated=True,
            line='J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].',
            next_line='C. J. A. P. Martins, "The status of varying constants: A review of the physics, searches and implications", 1709.02923.',
            reference='J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].',
            prev_reference='',
            block_references=[]
        )

        self.assertEqual(results, 'J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514]. J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].')
        self.assertEqual(prev_reference, '')

    @patch.object(TXTtoREFs, 'get_references')
    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_init_error(self, mock_logger, mock_get_references):
        """ test valid bibcode length when reading from file """

        mock_get_references.return_value = [
            ['000123456789012345678',
             ['J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].']],
            ['0002234567890123456',
             ['C. J. A. P. Martins, "The status of varying constants: A review of the physics, searches and implications", 1709.02923.']]
        ]

        torefs = TXTtoREFs(filename='testfile.txt', buffer={}, parsername='arXiv', cleanup=TXTtoREFs.block_cleanup)

        # make sure get_references was called correctly with keyword arguments
        mock_get_references.assert_called_once_with(filename='testfile.txt', encoding='UTF-8')

        self.assertEqual(len(torefs.raw_references), 1)
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0002234567890123456')
        self.assertEqual(torefs.raw_references[0]['block_references'],['C. J. A. P. Martins, "The status of varying constants: A review of the physics, searches and implications", 1709.02923.'])

        mock_logger.error.assert_called_with(
            'Error in getting a bibcode along with the reference strings from reference file testfile.txt. '
            'Returned 000123456789012345678 for bibcode. Skipping!')

    def test_process_enumeration(self):
        """ test process_enumeration method """
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/txt/arXiv/0/00000.raw')
        torefs = TXTtoREFs(filename=filename, buffer={}, parsername='arXiv')

        line = 'J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].'
        results = torefs.process_enumeration(line, [])

        self.assertEqual(results, ['J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].'])

    def test_get_references(self):
        """ test get_references method """
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/txt/arXiv/0/00000.raw')
        torefs = TXTtoREFs(filename='', buffer={}, parsername='arXiv')

        results = torefs.get_references(filename)

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0][0], '0001arXiv.........Z')
        self.assertEqual(results[0][1], ['J.-P. Uzan, "Varying constants, gravitation and cosmology", "Living Rev. Rel." 14 (2011) 2, [1009.5514].', 'C. J. A. P. Martins, "The status of varying constants: A review of the physics, searches and implications", 1709.02923.'])
        self.assertEqual(results[1][0], '0002arXiv.........Z')
        self.assertEqual(results[2][0], '0003arXiv.........Z')
        self.assertEqual(len(results[1][1]), 2)
        self.assertEqual(len(results[2][1]), 2)

        # case when there are multiple references on one line
        with patch("builtins.open", mock_open(read_data=(
                "%R 0001arXiv.........Z\n"
                "%Z\n"
                "(1) Smith, J. et al. (2020), Study on Astrophysics, Astrophys. J., 100, 123-126; (2) Doe, J. (2021), New Discoveries in Physics, Phys. Rev., 110, 456-459.\n"))):
            results = torefs.get_references(filename='test_file.txt', encoding='UTF-8')

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], '0001arXiv.........Z')
            self.assertEqual(results[0][1],['Smith, J. et al. (2020), Study on Astrophysics, Astrophys. J., 100, 123-126', 'Doe, J. (2021), New Discoveries in Physics, Phys. Rev., 110, 456-459.'])

        # case when there are multiple references on the line where the references start
        with patch("builtins.open", mock_open(read_data=(
                "%R 0001arXiv.........Z\n"
                "%Z (1) Smith, J. et al. (2020), Study on Astrophysics, Astrophys. J., 100, 123-126; (2) Doe, J. (2021), New Discoveries in Physics, Phys. Rev., 110, 456-459.\n"))):
            results = torefs.get_references(filename='test_file.txt', encoding='UTF-8')

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], '0001arXiv.........Z')
            self.assertEqual(results[0][1],['Smith, J. et al. (2020), Study on Astrophysics, Astrophys. J., 100, 123-126', 'Doe, J. (2021), New Discoveries in Physics, Phys. Rev., 110, 456-459.'])

        # case when there is no references in the file
        with patch("builtins.open", mock_open(read_data=(
                "%R 0001arXiv.........Z\n"
                "%Z\n"))):
            results = torefs.get_references(filename='test_file.txt', encoding='UTF-8')

            self.assertEqual(len(results), 0)

    def test_fix_inheritance(self):
        """ test fix_inheritance method """
        torefs = TXTtoREFs(filename='', buffer={}, parsername='arXiv')

        # test when both references are correctly formatted for author replacement fix
        result = torefs.fix_inheritance(cur_refstr="--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.",
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, "Smith, J., and Johnson, A. (2020), A study on astrophysics, Astrophys. J., 100, 123-126.")

        # test when unknown placeholder is included, no change to the reference
        current_reference = "***** (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, current_reference)

        # test when the anchor, the year, is missing from the current reference, this works
        result = torefs.fix_inheritance(cur_refstr="--- , A study on astrophysics, Astrophys. J., 100, 123-126.",
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, "Smith, J., and Johnson, A. , A study on astrophysics, Astrophys. J., 100, 123-126.")

        # test when the anchor, the year, is missing from the previous reference, no change to the reference
        current_reference = "--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105, 2019.")
        self.assertEqual(result, current_reference)

        # test when the anchor, the year, is at the end of the previous reference, no change to the reference
        current_reference = "--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105, (2019).")
        self.assertEqual(result, current_reference)

        # test when authors are missing in the current reference and need to be inherited
        result = torefs.fix_inheritance(cur_refstr="--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.",
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, "Smith, J., and Johnson, A. (2020), A study on astrophysics, Astrophys. J., 100, 123-126.")

        # test when no placeholder is included, no change to the reference
        current_reference = "Adams, R. (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, current_reference)

    def test_is_reference(self):
        """ Test is_reference method """
        torefs = TXTtoREFs(filename='', buffer={}, parsername='arXiv')

        # test with a valid reference
        self.assertTrue(torefs.is_reference("Smith, J., and Johnson, A. (2019), A study on astrophysics, Astrophys. J., 99, 100-105."))

        # test with a valid reference containing DOI
        self.assertTrue(torefs.is_reference("Doe, J. et al., New methods in quantum computing, Nature, 67, 111-120, doi:10.1038/s41586-020-2055-7"))

        # test with a valid reference containing arXiv ID
        self.assertTrue(torefs.is_reference("Adams, R., The future of AI, J. Machine Learn., 21(4), 101-115, arXiv:1802.05161"))

        # test with an invalid reference (ie, no author or year, etc.)
        self.assertFalse(torefs.is_reference("This is not a valid reference"))

        # test with missing essential year
        self.assertFalse(torefs.is_reference("Miller, L. et al. The role of black holes in galaxy formation"))

        # test with a reference that matches the author/volume/page regex
        valid_reference_with_author_and_volume = "Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105."
        self.assertTrue(torefs.is_reference(valid_reference_with_author_and_volume))


class TestXMLtoREFs(unittest.TestCase):

    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_get_references(self, mock_logger):
        """test get_references method"""

        # test case 1: using filename
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.agu.xml')
        torefs = XMLtoREFs(filename=filename, buffer={}, parsername='AGU', tag='citation')

        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000JGR.....0.....Z')
        self.assertEqual(len(torefs.raw_references[0]['block_references']), 5)

        # test case 2: having it in the buffer
        buffer = {
            "source_filename": os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.agu.xml'),
            "parser_name": "AGU",
            "block_references": [
                {
                    'source_bibcode': '0000JGR.....0.....Z',
                    'references': [
                        {
                            'item_num': '1',
                            'authors': 'Bouwman',
                            'journal': 'Global Biogeochem. Cycles',
                            'volume': '7',
                            'page': '557',
                            'year': '1993',
                            'refstr': 'Bouwman, 1993. Global Biogeochem. Cycles, 7, 557.',
                            'refraw': '<citation id="bouw93">\n    <journal_title>Global Biogeochem. Cycles</journal_title>\n    <first_author>Bouwman</first_author>\n    <reftitle>Global analysis of the potential for N<sub>2</sub>O production in natural soils</reftitle>\n    <volume>7</volume>\n    <firstPage>557</firstPage>\n    <CitationNumber>null</CitationNumber>\n    <year>1993</year>\n    <partOfCode>journal</partOfCode>\n    <paperType>article</paperType>\n</citation>',
                            'journal_title': 'Global Biogeochem. Cycles',
                            'first_author': 'Bouwman',
                            'reftitle': 'Global analysis of the potential for N2O production in natural soils',
                            'firstPage': '557',
                            'CitationNumber': 'null',
                            'paperType': 'article'
                        },
                        {
                            'item_num': '2',
                            'authors': 'Scheffer',
                            'journal': 'Lehrbuch der Bodenkunde',
                            'year': '1992',
                            'refstr': 'Scheffer, Lehrbuch der Bodenkunde, 1992, standalone, book',
                            'refraw': '<citation id="sche92">\n    <journal_title>null</journal_title>\n    <first_author>Scheffer</first_author>\n    <reftitle>Lehrbuch der Bodenkunde</reftitle>\n    <firstPage>null</firstPage>\n    <CitationNumber>null</CitationNumber>\n    <year>1992</year>\n    <partOfCode>standalone</partOfCode>\n    <paperType>book</paperType>\n</citation>',
                            'journal_title': 'null',
                            'first_author': 'Scheffer',
                            'reftitle': 'Lehrbuch der Bodenkunde',
                            'firstPage': 'null',
                            'CitationNumber': 'null',
                            'partOfCode': 'standalone',
                            'paperType': 'book'
                        },
                        {
                            'item_num': '3',
                            'authors': 'Yu',
                            'journal': 'J. Geophys. Res.',
                            'volume': '113',
                            'page': 'DD14S12',
                            'year': '2008',
                            'doi': '10.1029/2007JD009349',
                            'refstr': 'Yu, 2008. J. Geophys. Res., 113, DD14S12. doi:10.1029/2007JD009349',
                            'refraw': '<citation id="yu08">\n    <journal_title>J. Geophys. Res.</journal_title>\n    <first_author>Yu</first_author>\n    <reftitle>A satellite-based assessment of transpacific transport of pollution aerosol</reftitle>\n    <volume>113</volume>\n    <firstPage>null</firstPage>\n    <CitationNumber>D14S12</CitationNumber>\n    <year>2008</year>\n    <DOI>10.1029/2007JD009349</DOI>\n    <partOfCode>journal</partOfCode>\n    <paperType>article</paperType>\n</citation>',
                            'journal_title': 'J. Geophys. Res.',
                            'first_author': 'Yu',
                            'reftitle': 'A satellite-based assessment of transpacific transport of pollution aerosol',
                            'volume': '113',
                            'firstPage': 'null',
                            'CitationNumber': 'D14S12',
                            'year': '2008',
                            'DOI': '10.1029/2007JD009349',
                            'partOfCode': 'journal',
                            'paperType': 'article'
                        },
                        {
                            'item_num': '4',
                            'authors': 'Tjernström',
                            'journal': 'J. Geophys. Res.',
                            'volume': '101',
                            'issue': 'D14',
                            'year': '1996',
                            'refstr': 'Tjernström, 1996. J. Geophys. Res., 101, D14.',
                            'refraw': '<citation id="tjer">\n    <journal_title>J. Geophys Res.</journal_title>\n    <first_author>Tjernström</first_author>\n    <reftitle>Thermal mesoscale circulations on the Baltic coast, Part 1, A numerical case study</reftitle>\n    <volume>101</volume>\n    <issue>D14</issue>\n    <firstPage>null</firstPage>\n    <CitationNumber>null</CitationNumber>\n    <year>1996</year>\n    <partOfCode>journal</partOfCode>\n    <paperType>article</paperType>\n</citation>',
                            'journal_title': 'J. Geophys Res.',
                            'first_author': 'Tjernström',
                            'reftitle': 'Thermal mesoscale circulations on the Baltic coast, Part 1, A numerical case study',
                            'volume': '101',
                            'issue': 'D14',
                            'year': '1996',
                            'firstPage': 'null',
                            'CitationNumber': 'null',
                            'partOfCode': 'journal',
                            'paperType': 'article'
                        },
                        {
                            'item_num': '5',
                            'doi': '10.5194/acp-8-6117-2008',
                            'refstr': '10.5194/acp-8-6117-2008',
                            'refraw': '<citation id="zhan08">\n    <DOI>10.5194/acp-8-6117-2008</DOI>\n    <partOfCode>journal</partOfCode>\n    <paperType>article</paperType>\n</citation>',
                            'DOI': '10.5194/acp-8-6117-2008',
                            'partOfCode': 'journal',
                            'paperType': 'article'
                        }
                    ]
                }
            ]
        }
        torefs = XMLtoREFs(filename='', buffer=buffer, parsername='AGU')
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000JGR.....0.....Z')
        self.assertEqual(len(torefs.raw_references[0]['block_references']), 5)

        # test case 3: no data in the file
        with patch('builtins.open', mock_open(read_data='')):
            torefs = XMLtoREFs(filename='testfile.xml', buffer=None, parsername='AGU')
            self.assertEqual(len(torefs.raw_references), 0)
            mock_logger.error.assert_called_with("File testfile.xml is empty.")

    @patch.object(XMLtoREFs, 'get_references')
    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_init_error(self, mock_logger, mock_get_references):
        """ test valid bibcode length when reading from file """

        mock_get_references.return_value = [
            ['0000JGR.....0.....Z',
             '<citation id="bouw93">\n    <journal_title>Global Biogeochem. Cycles</journal_title>\n    <first_author>Bouwman</first_author>\n    <reftitle>Global analysis of the potential for N<sub>2</sub>O production in natural soils</reftitle>\n    <volume>7</volume>\n    <firstPage>557</firstPage>\n    <CitationNumber>null</CitationNumber>\n    <year>1993</year>\n    <partOfCode>journal</partOfCode>\n    <paperType>article</paperType>\n</citation>'],
            ['000223456789012345678',
             '<citation id="sche92">\n    <journal_title>null</journal_title>\n    <first_author>Scheffer</first_author>\n    <reftitle>Lehrbuch der Bodenkunde</reftitle>\n    <firstPage>null</firstPage>\n    <CitationNumber>null</CitationNumber>\n    <year>1992</year>\n    <partOfCode>standalone</partOfCode>\n    <paperType>book</paperType>\n</citation>']
        ]

        torefs = XMLtoREFs(filename='testfile.xml', buffer={}, parsername='AGU', tag='citation')

        # make sure get_references was called correctly with keyword arguments
        mock_get_references.assert_called_once_with(filename='testfile.xml')

        self.assertEqual(len(torefs.raw_references), 1)
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000JGR.....0.....Z')
        self.assertIn('citation id="bouw93"', torefs.raw_references[0]['block_references'][0])

        mock_logger.error.assert_called_with(
            'Error in getting a bibcode along with the reference strings from reference file testfile.xml. '
            'Returned 000223456789012345678 for bibcode. Skipping!')

    def test_detect_ref_format(self):
        """ test detect_ref_format method """
        torefs = XMLtoREFs(filename='', buffer={}, parsername='')

        # test with XML format
        self.assertEqual(torefs.detect_ref_format('<ADSBIBCODE><bibcode>0001arXiv.........Z</bibcode></ADSBIBCODE>'), 'xml')

        # test with LaTeX format
        self.assertEqual(torefs.detect_ref_format('\\adsbibcode{0001arXiv.........Z}'), 'tex')

        # test with tag format
        self.assertEqual(torefs.detect_ref_format('%R 0001arXiv.........Z '), 'tag')

        # test with an unknown format
        self.assertIsNone(torefs.detect_ref_format('This is just plain text.'))

    def test_strip_tag(self):
        """test strip_tag method"""
        torefs = XMLtoREFs(filename='', buffer={}, parsername='')
        refstr = "<reference><bibcode>0001arXiv0001</bibcode><title>Test Title</title></reference>"
        match = re.search(r'<reference>(.*?)</reference>', refstr)

        # test case 1: Left side
        result_left_strip = torefs.strip_tag(1, match, 'Left')
        self.assertEqual(result_left_strip, match.end())

        # test case 2: Right side
        result_right_no_strip = torefs.strip_tag(1, match, 'Right')
        self.assertEqual(result_right_no_strip, match.start())

    def test_extract_tag(self):
        """test extract_tag method"""
        torefs = XMLtoREFs(filename='', buffer={}, parsername='')
        refstr = "<reference><bibcode>0001arXiv0001</bibcode><title>Test Title</title></reference>"
        result, tag = torefs.extract_tag(refstr, 'reference', remove=1, keeptag=0, greedy=1, attr=1)
        self.assertEqual(result, '')
        self.assertEqual(tag, '<bibcode>0001arXiv0001</bibcode><title>Test Title</title>')


class TestOCRtoREFs(unittest.TestCase):

    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_get_references(self, mock_logger):
        """test get_references method"""

        # test case 1: using filename
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/ocr/ADS/0/0000ADSTEST.0.....Z.ref.ocr.txt')
        torefs = OCRtoREFs(filename=filename, buffer={}, parsername="ADSocr")

        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000ADSTEST.0.....Z')
        self.assertEqual(len(torefs.raw_references[0]['block_references']), 23)

        # test case 2: having it in the buffer
        buffer = {
            'source_filename': os.path.abspath(os.path.dirname(__file__) + '/stubdata/ocr/ADS/0/0000ADSTEST.0.....Z.ref.ocr.txt'),
            'parser_name': 'OCR',
            'block_references': [
                {
                    'source_bibcode': '0000ADSTEST.0.....Z',
                    'references': [
                        {'item_num': '1', 'refraw': 'Kambry M.A., Nishikawa J. 1990, Sol. Phys. 126, 89'},
                        {'item_num': '2', 'refraw': 'Kambry M.A., Nishikawa J., Sakurai T., Ichimoto K., Hiei E. 1991, Sol. Phys. 132, 41'},
                        {'item_num': '3', 'refraw': 'BESSELL, M. S. :1983, Publ. Astron. Soc. Pac. 95, 480.'},
                        {'item_num': '4', 'refraw': 'BESSELL, M. S., WEIS, E. W.:1987, Publ. Astron. Soc. Pac. 99, 642.'},
                        {'item_num': '5', 'refraw': 'CARNEY, B. W., LATHAM, D. W.:1987, Astron. J. 92, 116.'},
                        {'item_num': '6', 'refraw': 'DOMMANGET, S.: 1985, Proceedings of the Colloquium on the European Astrometry Satellite Hipparcos, Scientific Aspects of the Input Catalogue Preparation, Aussois (France). ESA SP-238, 153.'},
                        {'item_num': '7', 'refraw': 'EGRET, D., G0~Z, A.: 1985, Proceedings of the Colloquium on the European Astrometry Satellite Hipparcos, Scientific Aspects of the Input Catalogue Preparation, Aussois (France). ESA SP-238, 87.'},
                        {'item_num': '8', 'refraw': 'Davoust, E., and Pence, W. D. 1982, Astr. Ap. Suppi., 49, 631.'},
                        {'item_num': '9', 'refraw': 'de Vaucouleurs, G. 1977a, Ap. J. Suppi., 33, 211.'},
                        {'item_num': '10', 'refraw': 'de Vaucouleurs, G. 1977b, in The Evolution of Galaxies and Stellar Populations, ed. B. M. Tinsley and R. B. Larson (New Haven: Yale University Observatory), p. 43.'},
                        {'item_num': '11', 'refraw': 'de Vaucouleurs, G., and Agiiero, E. 1973, Pub. A.S.P., 85, 150.'},
                        {'item_num': '12', 'refraw': 'de Vaucouleurs, G., de Vaucouleurs, A., and Corwin, H. G., Jr. 1976, Second Reference Catalogue of Bright Galaxies (Austin: University of Texas Press) (RC2).'},
                        {'item_num': '13', 'refraw': 'Fraser, C. W. 1977, Astr. Ap. Suppl., 29, 161.'},
                        {'item_num': '14', 'refraw': 'Freeman, K. C. 1970, Ap. J., 160, 811.'},
                        {'item_num': '15', 'refraw': 'Hamabe, M. 1982, Pub. Astr. Soc. Japan, 34, 423.'},
                        {'item_num': '16', 'refraw': 'Kodaira, K., Okamura, S., and Watanabe, M. 1983, Ap. J. (Letters), 274, L49.'},
                        {'item_num': '17', 'refraw': 'Kormendy, J. 1977, Ap. J., 218, 333.'},
                        {'item_num': '18', 'refraw': 'Kormendy, J. 1980, Proc. ESO Workshop on Two-Dimensional Photometry, ed. P. Crane and K. Kjiir (Munich: European Southern Observatory), p. 191.'},
                        {'item_num': '19', 'refraw': 'Simien, F., and de Vaucouleurs, G. 1983, in IAU Symposium 100, Internal Kinematics and Dynamics of Galaxies, ed. E. Athanassoula (l)ordrecht: Reidel), p. 375.'},
                        {'item_num': '20', 'refraw': "Packer, Ch., Thomas, R. N.: 1962, Annales d'Astrophys. ~, 100."},
                        {'item_num': '21', 'refraw': "Rybansky', M., Ru~in, V.: 1983, Bull. Astron. inst. Czechosl. li~ 79"},
                        {'item_num': '22', 'refraw': 'Solar-Geophysical Data, 439 Part I * 450 Part 1, February 1981 * February 1982, U.S. Department of Commerce (Boulder, Colorado, USA 80303).'},
                        {'item_num': '23', 'refraw': 'Underwood, J. H., Broussard, R. M.: 1977, Aerospace Report, No. ATR * 77 (7405) - 2.'}
                    ]
                }
            ]
        }
        torefs = OCRtoREFs(filename='', buffer=buffer, parsername='ADSocr')
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000ADSTEST.0.....Z')
        self.assertEqual(len(torefs.raw_references[0]['block_references']), 23)

        # test case 3: no data in the file
        with patch('builtins.open', mock_open(read_data='')):
            torefs = OCRtoREFs(filename='testfile.ocr.txt', buffer=None, parsername='ADSocr')
            self.assertEqual(len(torefs.raw_references), 0)
            mock_logger.error.assert_called_with("No references found in reference file testfile.ocr.txt.")

    @patch.object(OCRtoREFs, 'get_references')
    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_init_error(self, mock_logger, mock_get_references):
        """ test valid bibcode length when reading from file """

        mock_get_references.return_value = [
            ['000123456789012345678',
             ['Kambry M.A., Nishikawa J. 1990, Sol. Phys. 126, 89']],
            ['0002234567890123456',
             ['Solomon, S.C. and Head, J.W.(1982) JGR, 87, 9236-9246']]
        ]

        torefs = OCRtoREFs(filename='testfile.ocr.txt', buffer={}, parsername='ADSocr', cleanup=OCRtoREFs.re_cleanup)

        mock_get_references.assert_called_once_with(filename='testfile.ocr.txt', encoding='UTF-8')

        self.assertEqual(len(torefs.raw_references), 1)
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0002234567890123456')
        self.assertEqual(torefs.raw_references[0]['block_references'], ['Solomon, S.C. and Head, J.W.(1982) JGR, 87, 9236-9246'])

        mock_logger.error.assert_called_with(
            'Error in getting a bibcode along with the reference strings from reference file testfile.ocr.txt. '
            'Returned 000123456789012345678 for bibcode. Skipping!')

    def test_process_with_header_line_exception(self):
        """ test process_with_header_line method for exception handling """

        # simulate a reader with only one line so that i + 1 will cause IndexError
        reader = ['References']

        torefs = OCRtoREFs(filename='', buffer={}, parsername='ADSocr')

        with patch.object(OCRtoREFs, 'merge_split_process') as mock_merge_split_process:
            mock_merge_split_process.return_value = []

            result = torefs.process_with_header_line(reader)

            mock_merge_split_process.assert_not_called()
            self.assertEqual(result, [])

    def test_remove_enumeration_exception(self):
        """ Test remove_enumeration method for exception handling when split throws an exception """
        torefs = OCRtoREFs(filename='', buffer={}, parsername='ADSocr')

        with patch.object(OCRtoREFs, 're_reference_start') as mock_re_reference_start:
            mock_re_reference_start.search.return_value = True

            with patch.object(OCRtoREFs, 're_remove_enumeration') as mock_re_remove_enumeration:
                mock_re_remove_enumeration.split.side_effect = Exception("Test exception")

                result_line, result_status = torefs.remove_enumeration("123 (2022) Some Journal Title, Volume 50, Pages 123-456", 1)

                self.assertEqual(result_status, 0)
                mock_re_remove_enumeration.split.assert_called_once()

    def test_fix_inheritance(self):
        """ test fix_inheritance method """
        torefs = OCRtoREFs(filename='', buffer={}, parsername='ADSocr')

        # test when both references are correctly formatted for author replacement fix
        result = torefs.fix_inheritance(cur_refstr="--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.",
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, "Smith, J., and Johnson, A. (2020), A study on astrophysics, Astrophys. J., 100, 123-126.")

        # test when unknown placeholder is included, no change to the reference
        current_reference = "***** (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, current_reference)

        # test when the anchor, the year, is missing from the current reference, this works
        result = torefs.fix_inheritance(cur_refstr="--- , A study on astrophysics, Astrophys. J., 100, 123-126.",
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, "Smith, J., and Johnson, A. , A study on astrophysics, Astrophys. J., 100, 123-126.")

        # test when the anchor, the year, is missing from the previous reference, no change to the reference
        current_reference = "--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105, 2019.")
        self.assertEqual(result, current_reference)

        # test when the anchor, the year, is at the end of the previous reference, no change to the reference
        current_reference = "--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105, (2019).")
        self.assertEqual(result, current_reference)

        # test when authors are missing in the current reference and need to be inherited
        result = torefs.fix_inheritance(cur_refstr="--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.",
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, "Smith, J., and Johnson, A. (2020), A study on astrophysics, Astrophys. J., 100, 123-126.")

        # test when no placeholder is included, no change to the reference
        current_reference = "Adams, R. (2020), A study on astrophysics, Astrophys. J., 100, 123-126."
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr="Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.")
        self.assertEqual(result, current_reference)

    def test_is_reference(self):
        """ Test is_reference method """
        torefs = OCRtoREFs(filename='', buffer={}, parsername='ADSocr')

        # test with a valid reference
        self.assertTrue(torefs.is_reference("Smith, J., and Johnson, A. (2019), A study on astrophysics, Astrophys. J., 99, 100-105."))

        # test with a valid reference containing DOI
        self.assertTrue(torefs.is_reference("Doe, J. et al., New methods in quantum computing, Nature, 67, 111-120, doi:10.1038/s41586-020-2055-7"))

        # test with a valid reference containing arXiv ID
        self.assertTrue(torefs.is_reference("Adams, R., The future of AI, J. Machine Learn., 21(4), 101-115, arXiv:1802.05161"))

        # test with an invalid reference (ie, no author or year, etc.)
        self.assertFalse(torefs.is_reference("This is not a valid reference"))

        # test with missing essential year
        self.assertFalse(torefs.is_reference("Miller, L. et al. The role of black holes in galaxy formation"))

        # test with a reference that matches the author/volume/page regex
        valid_reference_with_author_and_volume = "Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105."
        self.assertTrue(torefs.is_reference(valid_reference_with_author_and_volume))


class TestTEXtoREFs(unittest.TestCase):

    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_get_references(self, mock_logger):
        """test get_references method"""

        # test case 1: using filename
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/tex/ADS/0/iss0.tex')
        torefs = TEXtoREFs(filename=filename, buffer={}, parsername="ADStex")

        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000nla.....0.....Z')
        self.assertEqual(len(torefs.raw_references[0]['block_references']), 6)

        # test case 2: having it in the buffer
        buffer = {
            'source_filename': os.path.abspath(os.path.dirname(__file__) + '/stubdata/tex/ADS/0/iss0.tex'),
            'parser_name': 'ADStex',
            "block_references": [
                {
                    'source_bibcode': '0000nla.....0.....Z',
                    'references': [
                        {'item_num': '1', 'refraw': 'Braakman etal. 2005, 60th Int. Symp. Mol. Spectr., Ohio State University'},
                        {'item_num': '2', 'refraw': 'Halfen etal. 2006, apj, 639, 237'},
                        {'item_num': '3', 'refraw': 'Hollis etal. 2000, apj, 540, L107'},
                        {'item_num': '4', 'refraw': 'Ilyushin 2004, J. Mol. Spectr., 227, 140'},
                        {'item_num': '5', 'refraw': 'Kattija-Ari and Harmony 1980, Int. J. Quan. Chem., 14, 443'},
                        {'item_num': '6', 'refraw': 'Pszczokowski etal. 2005, J. Mol. Spectr., 234, 106'}
                    ]
                },
                {
                    'source_bibcode': '0000waas....0.....Z',
                    'references': [
                        {'item_num': '1', 'refraw': 'Bender, R., A&A, 229, 441 (1990).'},
                        {'item_num': '2', 'refraw': 'Bender, R., Saglia, R.P., and Gerhard, O.E., MNRAS, 269, 785 (1994).'},
                        {'item_num': '3', 'refraw': 'Bonfanti, P., Simien, F., Rampazzo, R. and Prugniel, Ph., 1999, A&AS, 139, 483 (1999).'},
                        {'item_num': '4', 'refraw': 'Combes, F., Rampazzo, R., Bonfanti, P.P., Prugniel, P. and Sulentic, J.W., A&A, 297, 37 (1995).'},
                        {'item_num': '5', 'refraw': 'Coziol, R., Ribeiro, A. L. B., De Carvalho, R. and Capelato, H. V., ApJ, 493, 563 (1998).'},
                        {'item_num': '6', 'refraw': 'Fukazawa, Y., Nakazawa, K., Isobe, N., Ohashi, T. and Kamae, T., ApJ, 546, 87 (2001).'},
                        {'item_num': '7', 'refraw': 'Fukugita, M., Shimasaku, K., Ichikawa T., Publications of the astronomical society of the Pacific, 107, 945 (1995).'},
                        {'item_num': '8', 'refraw': 'Mendes de Oliveira C., Aram, P., Plana, H. and Balkowski, C., AJ, 126, 2635 (2003).'},
                        {'item_num': '9', 'refraw': 'Nishiura, S., Shimada, M., Ohyama, Y., Murayama, T. and Taniguchi, Y., AJ, 120, 1691 (2000).'},
                        {'item_num': '10', 'refraw': 'Pildis, R. A., Evrard, A. E., and Bergman, J. N., AJ, 112, 378 (1996).'},
                        {'item_num': '11', 'refraw': 'Rubin V.C., Hunter D.A., Ford W.K.Jr., ApJS, 76, 153 (1991).'},
                        {'item_num': '12', 'refraw': 'Vennik, J., Richter, G. M. and Longo, G., AN, 314, 393 (1993).'},
                        {'item_num': '13', 'refraw': 'Vrtilek, J., M., Grego, L., David, L. P. etal., APS meeting, B17.107 (2002).'},
                        {'item_num': '14', 'refraw': 'Zepf, S. E., Whitmore, B. C., Levison, H. F., ApJ, 383, 524 (1991).'}
                    ]
                }
            ]
        }
        torefs = TEXtoREFs(filename='', buffer=buffer, parsername='ADStex')
        self.assertEqual(torefs.raw_references[1]['bibcode'], '0000waas....0.....Z')
        self.assertEqual(len(torefs.raw_references[1]['block_references']), 14)

        # test case 3: no data in the file
        with patch('builtins.open', mock_open(read_data='')):
            torefs = TEXtoREFs(filename='testfile.tex', buffer=None, parsername='ADStex')
            self.assertEqual(len(torefs.raw_references), 0)
            mock_logger.error.assert_called_with("No references found in reference file testfile.tex.")

        # test cas4: when exception is raised
        with patch('builtins.open', side_effect=Exception("Test exception")):
            torefs = TEXtoREFs(filename='testfile.tex', buffer=None, parsername="ADStex")
            result = torefs.get_references("testfile.tex", "utf-8")
            mock_logger.error.assert_called_with('Exception: Test exception')
            self.assertEqual(result, [])

    @patch.object(TEXtoREFs, 'get_references')
    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_init_error(self, mock_logger, mock_get_references):
        """ test valid bibcode length when reading from file """

        mock_get_references.return_value = [
            ['000123456789012345678',
             ['Kambry M.A., Nishikawa J. 1990, Sol. Phys. 126, 89']],
            ['0002234567890123456',
             ['Solomon, S.C. and Head, J.W.(1982) JGR, 87, 9236-9246']]
        ]

        torefs = TEXtoREFs(filename='testfile.tex', buffer={}, parsername='ADStex', cleanup=TEXtoREFs.re_cleanup)

        mock_get_references.assert_called_once_with(filename='testfile.tex', encoding='UTF-8')

        self.assertEqual(len(torefs.raw_references), 1)
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0002234567890123456')
        self.assertEqual(torefs.raw_references[0]['block_references'], ['Solomon, S.C. and Head, J.W.(1982) JGR, 87, 9236-9246'])

        mock_logger.error.assert_called_with(
            'Error in getting a bibcode along with the reference strings from reference file testfile.tex. '
            'Returned 000123456789012345678 for bibcode. Skipping!')


class TestHTMLtoREFs(unittest.TestCase):

    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_get_references(self, mock_logger):
        """test get_references method"""

        # test case 1: using filename
        filename = os.path.abspath(os.path.dirname(__file__) + '/stubdata/html/A+A/0/0000A&A.....0.....Z.ref.txt')
        tag = re.compile(r'(?:<LI>\s*)(.*?)(?=<LI>|</UL>)', (re.IGNORECASE | re.DOTALL))
        torefs = HTMLtoREFs(filename=filename, buffer={}, parsername='AnAhtml', tag=tag, file_type=HTMLtoREFs.single_bibcode)

        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000A&A.....0.....Z')
        self.assertEqual(len(torefs.raw_references[0]['block_references']), 10)

        # test case 2: having it in the buffer
        buffer = {
            'source_filename': os.path.abspath(os.path.dirname(__file__) + '/stubdata/html/A+A/0/0000A&A.....0.....Z.ref.txt'),
            'parser_name': 'AnAhtml',
            'block_references': [
                {
                    'source_bibcode': '0000A&A.....0.....Z',
                    'references': [
                        {'item_num': '1', 'refraw': 'Barucci, M. A., Romon, J., Doressoundiram, A., & Tholen, D. J. 2000, Astron. J., 120, 496'},
                        {'item_num': '2', 'refraw': 'Gutiérrez, P. J., Ortiz, J. L., Rodrigo, R., & López-Moreno, J. J. 2000, A&A, 355, 809'},
                        {'item_num': '3', 'refraw': 'Hahn, G., & Bailey, M. E. 1990, Nature, 348, 132'},
                        {'item_num': '4', 'refraw': 'Hainaut, O. R., Delahodde, C. E., Boehnhardt, H., et al. 2000, A&A, 356, 1076'},
                        {'item_num': '5', 'refraw': 'Jewitt, D., & Kalas, P. 1998, ApJ, 499, L103'},
                        {'item_num': '6', 'refraw': 'Landolt, A. U. 1992, AJ, 104, 340'},
                        {'item_num': '7', 'refraw': 'Lomb, N. R. 1976, Astroph. Space Sci., 39, 447'},
                        {'item_num': '8', 'refraw': 'Peixinho, N., Lacerda, P., Ortiz, J. L., et al. 2001, A&A, in press'},
                        {'item_num': '9', 'refraw': 'Press, W. H., Teukolsky, S. A., Vetterling, W. T., & Flannery, B. P. 1992, in Numerical Recipes in Fortran. 2nd ed. (Cambridge Univ. Press, London), 569'},
                        {'item_num': '10', 'refraw': 'Tholen, D. J., Hartmann, W. K., Cruikshank, D. P., et al. 1988, IAUC, 4554'}
                    ]
                }
            ]
        }
        torefs = HTMLtoREFs(filename='', buffer=buffer, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.single_bibcode)
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0000A&A.....0.....Z')
        self.assertEqual(len(torefs.raw_references[0]['block_references']), 10)

    @patch.object(HTMLtoREFs, 'get_references')
    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_init_error(self, mock_logger, mock_get_references):
        """ test valid bibcode length when reading from file """

        mock_get_references.return_value = [
            ['000123456789012345678',
             ["J.-P. Uzan, 'Varying constants, gravitation and cosmology', 'Living Rev. Rel.' 14 (2011) 2, [1009.5514]."]],
            ['0002234567890123456',
             ["C. J. A. P. Martins, 'The status of varying constants: A review of the physics, searches and implications', 1709.02923."]]
        ]

        torefs = HTMLtoREFs(filename='testfile.html', buffer={}, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.single_bibcode, cleanup=HTMLtoREFs.block_cleanup)

        mock_get_references.assert_called_once_with(filename='testfile.html', encoding='UTF-8', tag='', file_type=HTMLtoREFs.single_bibcode)

        self.assertEqual(len(torefs.raw_references), 1)
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0002234567890123456')
        self.assertEqual(torefs.raw_references[0]['block_references'], ["C. J. A. P. Martins, 'The status of varying constants: A review of the physics, searches and implications', 1709.02923."])

        mock_logger.error.assert_called_with(
            'Error in getting a bibcode along with the reference strings from reference file testfile.html. '
            'Returned 000123456789012345678 for bibcode. Skipping!')

    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_get_references_single_record_errors(self, mock_logger):
        """ test get_references_single_record when something goes wrong """

        # test case 1: when no bibcode is provided
        torefs = HTMLtoREFs(filename='testfile.html', buffer=None, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.single_bibcode)
        result = torefs.get_references_single_record('testfile.html', 'utf-8', 'html', bibcode='')
        mock_logger.error.assert_called_with('No bibcode extracted in reference file testfile.html.')
        self.assertEqual(result, [])

        # test case 2: when tag is not provided
        torefs = HTMLtoREFs(filename='testfile.html', buffer=None, parsername='AnAhtml', tag=None, file_type=HTMLtoREFs.single_bibcode)
        with patch('builtins.open', MagicMock(read=MagicMock(return_value='<html><body>Reference Content</body></html>'))):
            result = torefs.get_references_single_record('testfile.html', 'utf-8', None, bibcode='000123456789012345678')
            mock_logger.debug.assert_called_with('Unable to parse source file testfile.html, no tag was provided.')
            self.assertEqual(result, [])

        # test case 3: when no references are found
        torefs = HTMLtoREFs(filename='testfile.html', buffer=None, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.single_bibcode)
        with patch('builtins.open', mock_open(read_data='')):
            result = torefs.get_references_single_record('testfile.html', 'utf-8', tag='', bibcode='000123456789012345678')
            mock_logger.error.assert_called_with('No references found in reference file testfile.html.')
            self.assertEqual(result, [])

        # test case 4: when an exception is raised
        torefs = HTMLtoREFs(filename='testfile.html', buffer=None, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.single_bibcode)
        with patch('builtins.open', MagicMock(side_effect=Exception('File read error'))):
            result = torefs.get_references_single_record('testfile.html', 'utf-8', tag='', bibcode='000123456789012345678')
            mock_logger.error.assert_called_with('Exception: File read error')
            self.assertEqual(result, [])

    @patch('adsrefpipe.refparsers.toREFs.logger')
    def test_get_references_multi_records_errors(self, mock_logger):
        """ test get_references_multi_records when something goes wrong """

        # test case 1: when no references are found
        torefs = HTMLtoREFs(filename='testfile.html', buffer=None, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.multi_bibcode)
        with patch('builtins.open', mock_open(read_data='')):
            result = torefs.get_references_multi_records('testfile.html', 'utf-8', tag='')
            mock_logger.error.assert_called_with('No references found in reference file testfile.html.')
            self.assertEqual(result, [])

        # test case 2: when an exception is raised
        torefs = HTMLtoREFs(filename='testfile.html', buffer=None, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.multi_bibcode)
        with patch('builtins.open', MagicMock(side_effect=Exception('File read error'))):
            result = torefs.get_references_multi_records('testfile.html', 'utf-8', tag='')
            mock_logger.error.assert_called_with('Exception: File read error')
            self.assertEqual(result, [])

    def test_fix_inheritance(self):
        """ test fix_inheritance method """
        torefs = HTMLtoREFs(filename='', buffer={}, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.single_bibcode)

        # test when both references are correctly formatted for author replacement fix
        result = torefs.fix_inheritance(cur_refstr='--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.',
                                        prev_refstr='Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.')
        self.assertEqual(result, 'Smith, J., and Johnson, A. (2020), A study on astrophysics, Astrophys. J., 100, 123-126.')

        # test when unknown placeholder is included, no change to the reference
        current_reference = '***** (2020), A study on astrophysics, Astrophys. J., 100, 123-126.'
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr='Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.')
        self.assertEqual(result, current_reference)

        # test when the anchor, the year, is missing from the current reference, this works
        result = torefs.fix_inheritance(cur_refstr='--- , A study on astrophysics, Astrophys. J., 100, 123-126.',
                                        prev_refstr='Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.')
        self.assertEqual(result, 'Smith, J., and Johnson, A. , A study on astrophysics, Astrophys. J., 100, 123-126.')

        # test when the anchor, the year, is missing from the previous reference, no change to the reference
        current_reference = '--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.'
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr='Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105, 2019.')
        self.assertEqual(result, current_reference)

        # test when the anchor, the year, is at the end of the previous reference, no change to the reference
        current_reference = '--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.'
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr='Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105, (2019).')
        self.assertEqual(result, current_reference)

        # test when authors are missing in the current reference and need to be inherited
        result = torefs.fix_inheritance(cur_refstr='--- (2020), A study on astrophysics, Astrophys. J., 100, 123-126.',
                                        prev_refstr='Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.')
        self.assertEqual(result, 'Smith, J., and Johnson, A. (2020), A study on astrophysics, Astrophys. J., 100, 123-126.')

        # test when no placeholder is included, no change to the reference
        current_reference = 'Adams, R. (2020), A study on astrophysics, Astrophys. J., 100, 123-126.'
        result = torefs.fix_inheritance(cur_refstr=current_reference,
                                        prev_refstr='Smith, J., and Johnson, A. (2019), A previous study on astrophysics, Astrophys. J., 99, 100-105.')
        self.assertEqual(result, current_reference)

    def test_is_reference(self):
        """ Test is_reference method """
        torefs = HTMLtoREFs(filename='', buffer={}, parsername='AnAhtml', tag='', file_type=HTMLtoREFs.single_bibcode)

        # test with a valid reference
        self.assertTrue(torefs.is_reference('Smith, J., and Johnson, A. (2019), A study on astrophysics, Astrophys. J., 99, 100-105.'))

        # test with a valid reference containing DOI
        self.assertTrue(torefs.is_reference('Doe, J. et al., New methods in quantum computing, Nature, 67, 111-120, doi:10.1038/s41586-020-2055-7'))

        # test with a valid reference containing arXiv ID
        self.assertTrue(torefs.is_reference('Adams, R., The future of AI, J. Machine Learn., 21(4), 101-115, arXiv:1802.05161'))

        # test with an invalid reference (ie, no author or year, etc.)
        self.assertFalse(torefs.is_reference('This is not a valid reference'))

        # test with missing essential year
        self.assertFalse(torefs.is_reference('Miller, L. et al. The role of black holes in galaxy formation'))

        # test with a reference that matches the author/volume/page regex
        valid_reference_with_author_and_volume = 'Smith, J., and Johnson, A., A previous study on astrophysics, Astrophys. J., 99, 100-105.'
        self.assertTrue(torefs.is_reference(valid_reference_with_author_and_volume))


class TestReference(unittest.TestCase):

    @patch('adsrefpipe.refparsers.reference.logger')
    def test_errors(self, mock_logger):
        """ test couple of exceptions """

        # test initialization exception handling
        with self.assertRaises(ReferenceError) as context:
            Reference(reference_str='some reference string')
            self.assertEqual(str(context.exception), 'Parse method not defined.')
            mock_logger.assert_called_with('Error during Reference initialization: Parse method not defined.')

        # test when the parse method is not defined
        with self.assertRaises(Exception) as context:
            Reference(reference_str='some reference string').parse()
            self.assertEqual('Parse method not defined.', str(context.exception))

    def test_parse_volume(self):
        """ test parse_volume function """
        with patch.object(Reference, 'parse', MagicMock()):
            self.reference = Reference(reference_str='some reference string')

            # test ase #1: volume contains digits
            self.assertEqual(self.reference.parse_volume('12345'), '12345')

            # test case 2: volume contains roman numerals
            self.assertEqual(self.reference.parse_volume('XIV'), 'XIV')

            # test case 3: volume contains no digits or roman numerals
            self.assertEqual(self.reference.parse_volume('abc'), '')

            # test case 4: volume contains both digits and roman numerals, function returns the first found digits
            self.assertEqual(self.reference.parse_volume('123XIV'), '123')

            # test case 5: volume is an empty string
            self.assertEqual(self.reference.parse_volume(''), '')

            # test case 6: when volume contains leading zeroes, returns all digits including leading zeros
            self.assertEqual(self.reference.parse_volume('000123'), '000123')

            # test case 7: when volume contains an invalid roman numeral
            self.assertEqual(self.reference.parse_volume('IIII'), '')

    def test_url_decode(self):
        """ test the url_decode method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = Reference(reference_str='some reference string')

            # test case 1: URL with valid hex encoding, do the replacement
            self.assertEqual(reference.url_decode('https://example.com/file%20name%21'),
                             'https://example.com/file name!')

            # test case 2: URL with invalid hex encoding, no change
            url = 'https://example.com/file%G1name'
            self.assertEqual(reference.url_decode(url), url)

            # test case 3: URL with no hex encoding, no change
            url = 'https://example.com/file-name'
            self.assertEqual(reference.url_decode(url), url)

    def test_match_int(self):
        """ test the match_int method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = Reference(reference_str='some reference string')

            # test case 1: string with an integer
            self.assertEqual(reference.match_int('The reference is 12345.'), '12345')

            # test case 2: string with multiple integers, returns the first number
            self.assertEqual(reference.match_int('Volume 123, Issue 456.'), '123')

            # test case 3: string with no integers
            self.assertIsNone(reference.match_int('no numbers here'))

            # test case 4: a list containing a string with an integer
            self.assertEqual(reference.match_int(['Volume 789']), '789')

            # test case 5: alphanumeric string, starting with non-digit characters, returns the number
            self.assertEqual(reference.match_int('abc12345'), '12345')

    def test_int2roman(self):
        """ test the int2roman method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = Reference(reference_str='some reference string')

            # test case 1: conversion of integer to Roman numeral
            self.assertEqual(reference.int2roman(1), 'I')
            self.assertEqual(reference.int2roman(2025), 'MMXXV')

            # test case 2: conversion of the largest possible integer
            self.assertEqual(reference.int2roman(4000), 'MMMM')

            # test case 3: larger than it can convert, raises ReferenceError
            with self.assertRaises(ReferenceError):
                reference.int2roman(4001)

            # test case 4: smaller than it can convert, raises ReferenceError
            with self.assertRaises(ReferenceError):
                reference.int2roman(0)

    def test_roman2int(self):
        """ test the roman2int method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = Reference(reference_str='some reference string')

            # test case 1: conversion of Roman numeral to integer
            self.assertEqual(reference.roman2int('I'), 1)
            self.assertEqual(reference.roman2int('MMXXV'), 2025)
            self.assertEqual(reference.roman2int('CM'), 900)

            # test case 2: conversion of the largest possible Roman numeral
            self.assertEqual(reference.roman2int('MMMM'), 4000)

            # test case 3: invalid Roman numeral, raises ReferenceError
            with self.assertRaises(ReferenceError):
                reference.roman2int('roman')


class TestXMLreference(unittest.TestCase):

    def test_init_errors(self):
        """ test the XMLreference initialization when errors happen """

        with patch('adsrefpipe.refparsers.reference.XmlString') as mock_xmlstring:
            # test case 1: when reference_str is empty, raise ReferenceError
            with self.assertRaises(ReferenceError) as context:
                XMLreference('')
                self.assertEqual(str(context.exception), 'XMLReference must have a non-empty input reference')

            # test case 2: when XmlString parsing raises an exception
            mock_xmlstring.side_effect = Exception('XML parsing error')
            with self.assertRaises(ReferenceError) as context:
                XMLreference('Invalid XML string')
                self.assertTrue('XMLreference: error parsing string Invalid XML string' in str(context.exception))

            # test case 3: when KeyboardInterrupt is raised during XmlString parsing
            mock_xmlstring.side_effect = KeyboardInterrupt
            with self.assertRaises(KeyboardInterrupt):
                XMLreference('Invalid XML string')

    def test_str_invalid_object(self):
        """ test the __str__ method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = XMLreference(12345)  # Passing an integer (which doesn't have a `toxml()` method)

            # This should hit the except block because the integer doesn't have `toxml()`
            self.assertEqual(str(reference), '')  # Should return '' due to the exception being caught

    def test_str(self):
        """ test the __str__ method """
        with patch.object(Reference, 'parse', MagicMock()):
            with patch('adsrefpipe.refparsers.reference.UnicodeHandler') as mock_unicode_handler:

                mock_unicode = MagicMock()
                mock_unicode_handler.return_value = mock_unicode

                reference = XMLreference('<refstr>some reference string</refstr>')
                self.assertEqual(str(reference), '<refstr>some reference string</refstr>')

    def test_xmlnode_nodecontents(self):
        """ test xmlnode_nodecontents method """
        with patch.object(Reference, 'parse', MagicMock()):

            # test case 1: when empty string is passed to xmlnode_nodecontents
            reference = XMLreference('<refstr>some reference string</refstr>')

            results = reference.xmlnode_nodecontents('')

            self.assertEqual(results, 'some reference string')

            # test case 2: AttributeError exception
            with patch('adsrefpipe.refparsers.reference.unicode_handler') as mock_unicode_handler:
                reference = XMLreference('<refstr>some reference string</refstr>')

                mock_unicode_handler.ent2asc.side_effect = Exception('Unicode conversion error')
                mock_unicode_handler.cleanall.return_value = 'cleaned content'

                reference.reference_str = MagicMock()
                reference.reference_str.getElementsByTagName.side_effect = AttributeError('No childNodes')

                results = reference.xmlnode_nodecontents('some element')

                self.assertEqual(results, '')

            # test case 3: ent2asc exception
            with patch('adsrefpipe.refparsers.reference.unicode_handler') as mock_unicode_handler:
                with patch('adsrefpipe.refparsers.reference.XMLreference.re_remove_xml_tag') as mock_re_remove_xml_tag:
                    reference = XMLreference('<refstr>some reference string</refstr>')

                    reference.reference_str.getElementsByTagName = MagicMock(return_value=[])

                    mock_unicode_handler.ent2asc.side_effect = Exception('Unicode conversion error')
                    mock_unicode_handler.cleanall.return_value = 'cleaned content'
                    reference.re_remove_xml_tag = mock_re_remove_xml_tag

                    result = reference.xmlnode_nodecontents('some element')

                    self.assertEqual(result, 'cleaned content')

    def test_xmlnode_nodescontents(self):
        """ test xmlnode_nodecontents method """
        with patch.object(Reference, 'parse', MagicMock()):

            # test case 1: when empty string is passed to xmlnode_nodecontents
            reference = XMLreference('<refstr>some reference string</refstr>')

            results = reference.xmlnode_nodescontents('')

            self.assertEqual(results, 'some reference string')

            # test case 2: AttributeError exception
            with patch('adsrefpipe.refparsers.reference.unicode_handler') as mock_unicode_handler:
                reference = XMLreference('<refstr>some reference string</refstr>')

                mock_unicode_handler.ent2asc.side_effect = Exception('Unicode conversion error')
                mock_unicode_handler.cleanall.return_value = 'cleaned content'

                reference.reference_str = MagicMock()
                reference.reference_str.getElementsByTagName.side_effect = AttributeError('No childNodes')

                results = reference.xmlnode_nodescontents('some element')

                self.assertEqual(results, '')


            # test case 3: empty childNodes, should hit the `continue` statement
            reference = XMLreference('<refstr>some reference string</refstr>')

            reference.reference_str = MagicMock()
            mock_element = MagicMock()

            mock_element.childNodes = []
            reference.reference_str.getElementsByTagName = MagicMock(return_value=[mock_element])

            results = reference.xmlnode_nodescontents('element_name')

            self.assertEqual(results, [])

            # test case 4: ent2asc exception
            with patch('adsrefpipe.refparsers.reference.unicode_handler') as mock_unicode_handler:
                with patch('adsrefpipe.refparsers.reference.XMLreference.re_remove_xml_tag') as mock_re_remove_xml_tag:
                    reference = XMLreference('<refstr>some reference string</refstr>')

                    reference.reference_str.getElementsByTagName = MagicMock(
                        return_value=[MagicMock(childNodes=[MagicMock(toxml=MagicMock(return_value='some content'))])])

                    mock_unicode_handler.ent2asc.side_effect = Exception('Unicode conversion error')
                    mock_unicode_handler.cleanall.return_value = 'cleaned content'
                    reference.re_remove_xml_tag = mock_re_remove_xml_tag

                    result = reference.xmlnode_nodescontents('some element')

                    self.assertEqual(result, ['cleaned content'])

    def test_xmlnode_textcontents(self):
        """ test xmlnode_textcontents method """
        with patch.object(Reference, 'parse', MagicMock()):

            # test case 1: when empty string is passed to xmlnode_nodecontents
            reference = XMLreference('<refstr>some reference string</refstr>')

            results = reference.xmlnode_textcontents('')

            self.assertEqual(results, 'some reference string')

            # test case 2: empty childNodes, should hit the `continue` statement
            reference = XMLreference('<refstr>some reference string</refstr>')

            reference.reference_str = MagicMock()
            mock_element = MagicMock()

            mock_element.childNodes = []
            reference.reference_str.getElementsByTagName = MagicMock(return_value=[mock_element])

            results = reference.xmlnode_textcontents('element_name')

            self.assertEqual(results, '')

            # test case 3: extracting text content from sub-elements
            reference = XMLreference('<refstr>some reference string</refstr>')

            # create a mock parent element that has child nodes
            mock_element = MagicMock()
            mock_element.nodeName = 'parentElement'
            mock_element.nodeType = 1

            # create a mock sub-element with a TEXT_NODE as its child
            sub_element = MagicMock()
            sub_element.nodeName = 'subElement'
            sub_element.nodeType = 1
            sub_element.ELEMENT_NODE = 1

            # add a text node to the sub-element
            text_node = MagicMock()
            text_node.nodeType = 3
            text_node.TEXT_NODE = 3
            text_node.data = 'subelement content'

            sub_element.childNodes = [text_node]
            mock_element.childNodes = [sub_element]
            reference.reference_str = MagicMock()
            reference.reference_str.getElementsByTagName = MagicMock(return_value=[mock_element])

            result = reference.xmlnode_textcontents('parentElement', subels=['subElement'])

            self.assertEqual(result, 'subelement content')

    def test_xmlnode_attribute_(self):
        """ test xmlnode_attribute method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = XMLreference('<refstr>some reference string</refstr>')
            self.assertEqual(reference.xmlnode_attribute('', 'attribute'), '')

    def test_xmlnode_attributes(self):
        """ test xmlnode_attributes method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = XMLreference('<refstr>some reference string</refstr>')
            self.assertEqual(reference.xmlnode_attributes('', ''), {})

    def test_xmlnode_attribute_match_return(self):
        """ test xmlnode_attribute_match_return method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = XMLreference('<refstr>some reference string</refstr>')
            self.assertEqual(reference.xmlnode_attribute_match_return('', {}, ''), '')

    def test_extract_tag(self):
        """ test extract_tag method """
        with patch.object(Reference, 'parse', MagicMock()):
            reference = XMLreference('<refstr>some reference string</refstr>')
            results = reference.extract_tag(refstr='<refstr>some reference string</refstr>', tag='refstr', greedy=1, keeptag=1)
            self.assertEqual(results, ('', '<refstr>some reference string</refstr>'))


class TestLatexReference(unittest.TestCase):

    def test_str(self):
        """ test the __str__ method """
        with patch.object(Reference, 'parse', MagicMock()):
            with patch('adsrefpipe.refparsers.reference.LatexReference.cleanup') as mock_cleanup:
                reference = LatexReference('\\bibitem Some LaTeX reference with \\textit{italic} and \\textbf{bold}.')
                mock_cleanup.return_value = 'Some LaTeX reference with italic and bold.'
                self.assertEqual(str(reference), 'Some LaTeX reference with italic and bold.')

if __name__ == '__main__':
    unittest.main()
