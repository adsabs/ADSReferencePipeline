import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest
from unittest.mock import patch, MagicMock, mock_open
import xml.dom.minidom as dom
from xml.parsers.expat import ExpatError

from adsrefpipe.tests.unittests.stubdata import parsed_references
from adsrefpipe.refparsers.reference import ReferenceError
from adsrefpipe.refparsers.xmlFile import XmlList, XmlString
from adsrefpipe.refparsers.AASxml import AAStoREFs, AASreference
from adsrefpipe.refparsers.AGUxml import AGUtoREFs, AGUreference
from adsrefpipe.refparsers.APSxml import APStoREFs, APSreference
from adsrefpipe.refparsers.AnAxml import AnAtoREFs, AnAreference
from adsrefpipe.refparsers.AIPxml import AIPtoREFs, AIPreference
from adsrefpipe.refparsers.BlackwellXML import BLACKWELLtoREFs, BLACKWELLreference
from adsrefpipe.refparsers.CrossRefXML import CrossRefToREFs
from adsrefpipe.refparsers.CUPxml import CUPtoREFs, CUPreference
from adsrefpipe.refparsers.EDPxml import EDPtoREFs, EDPreference
from adsrefpipe.refparsers.EGUxml import EGUtoREFs
from adsrefpipe.refparsers.ElsevierXML import ELSEVIERtoREFs, ELSEVIERreference
from adsrefpipe.refparsers.IcarusXML import ICARUStoREFs, ICARUSreference
from adsrefpipe.refparsers.IOPFTxml import IOPFTtoREFs
from adsrefpipe.refparsers.IOPxml import IOPtoREFs, IOPreference
from adsrefpipe.refparsers.IPAPxml import IPAPtoREFs
from adsrefpipe.refparsers.JATSxml import JATStoREFs, JATSreference
from adsrefpipe.refparsers.JSTAGExml import JSTAGEtoREFs, JSTAGEreference
from adsrefpipe.refparsers.LivingReviewsXML import LivingReviewsToREFs
from adsrefpipe.refparsers.MDPIxml import MDPItoREFs, MDPIreference
from adsrefpipe.refparsers.NLM3xml import NLMtoREFs
from adsrefpipe.refparsers.NatureXML import NATUREtoREFs
from adsrefpipe.refparsers.ONCPxml import ONCPtoREFs
from adsrefpipe.refparsers.OUPxml import OUPtoREFs, OUPreference
from adsrefpipe.refparsers.PASAxml import PASAtoREFs
from adsrefpipe.refparsers.RSCxml import RSCtoREFs, RSCreference
from adsrefpipe.refparsers.SpringerXML import SPRINGERtoREFs, SPRINGERreference
from adsrefpipe.refparsers.SPIExml import SPIEtoREFs, SPIEreference
from adsrefpipe.refparsers.UCPxml import UCPtoREFs, UCPreference
from adsrefpipe.refparsers.VERSITAxml import VERSITAtoREFs, VERSITAreference
from adsrefpipe.refparsers.WileyXML import WILEYtoREFs, WILEYreference

class TestXmlList(unittest.TestCase):

    def test_init(self):
        """ test initialization """
        xml_list = XmlList()
        # make sure an empty list is assigned
        self.assertEqual(xml_list.data, [])

    def test_toxml(self):
        """ test toxml method """

        # test case 1: when XmlList is empty return empty string
        xml_list_empty = XmlList()
        xml_list_empty.noname = 1
        xml_list_empty.childNodes = []
        self.assertEqual(xml_list_empty.toxml(), '')

        # test case 2: XmlList with a child element
        doc = dom.Document()
        element = doc.createElement("TestElement")
        element.appendChild(doc.createTextNode("Some content"))
        xml_list = XmlList(elements=[element])
        self.assertEqual(xml_list.toxml(), element.toxml())

        # test case 3: XmlList behaves like a normal XML element
        xml_list_named = XmlList(elements=[element], name="RootElement")
        expected_xml = "<RootElement><TestElement>Some content</TestElement></RootElement>"
        self.assertEqual(xml_list_named.toxml(), expected_xml)

    def test_str(self):
        """ test __str__ method """

        # test case 1: empty XmlList returns an empty string
        xml_list_empty = XmlList()
        xml_list_empty.noname = 1
        xml_list_empty.childNodes = []
        self.assertEqual(str(xml_list_empty), '')

        # test case 2: XmlList with a child element pretty-prints the child
        doc = dom.Document()
        element = doc.createElement("TestElement")
        element.appendChild(doc.createTextNode("Some content"))
        xml_list = XmlList(elements=[element])
        xml_list.noname = 1
        self.assertEqual(len(xml_list.childNodes), 1)
        self.assertEqual(element.toprettyxml(indent='  ').strip(), str(xml_list).strip())

        # test case 3: named XmlList pretty-prints itself
        xml_list_named = XmlList(elements=[element], name="RootElement")
        expected_named = xml_list_named.toprettyxml(indent='  ').strip()
        self.assertEqual(str(xml_list_named).strip(), expected_named)


class TestXmlString(unittest.TestCase):

    @patch("xml.dom.minidom.parseString")
    def test_init(self, mock_parse_string):
        """ test initialization """

        # simulate ExpatError only on the first call with a properly formatted error message
        parse_attempts = []
        def side_effect(xml_str):
            parse_attempts.append(xml_str)
            if len(parse_attempts) == 1:  # Fail only on the first attempt
                raise ExpatError("not well-formed (invalid token): line 1, column 5")
            return MagicMock()
        mock_parse_string.side_effect = side_effect

        # create XmlString instance with invalid XML containing an untagged element
        XmlString(buffer="<root><883::AID-MASY883>ValidContent</root>")

        self.assertGreater(len(parse_attempts), 1)
        self.assertIn("ValidContent", parse_attempts[-1])
        self.assertNotEqual(parse_attempts[0], parse_attempts[-1])

    @patch("xml.dom.minidom.parseString")
    def test_init_exception(self, mock_parse_string):
        """ test initialization when there is an exception """

        # simulate ExpatError only on the first call with a properly formatted error message
        parse_attempts = []
        def side_effect(xml_str):
            parse_attempts.append(xml_str)
            if len(parse_attempts) == 1:  # Fail only on the first attempt
                raise ExpatError("not well-formed (invalid token): line 1, column 5")
            return MagicMock()
        mock_parse_string.side_effect = side_effect

        # create XmlString instance with invalid XML to trigger exception
        xml_string = XmlString(buffer="InvalidTag<no-match-here>ValidContent")

        self.assertEqual(len(parse_attempts), 1)
        self.assertIn("ValidContent", parse_attempts[-1])


class TestAASreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        reference = AASreference('<CITATION ID="rf2" URI="2000Sci...290..953A" URI-STATUS="NORESOLVE" BIBCODE="2000Sci...290..953A" BIB-STATUS="LINKS">---. 2000b, Science, 290, 953 DOI: 10.1234/doi_for_testing </CITATION>')
        self.assertEqual(reference['bibcode'], '2000Sci...290..953A')
        self.assertEqual(reference.get('doi'), '10.1234/doi_for_testing')
        self.assertIsNone(reference.get('eprint', None))
        self.assertEqual(reference.get('refplaintext', ''), '---. 2000b, Science, 290, 953 DOI: 10.1234/doi_for_testing')
        self.assertEqual(reference.parsed, 1)


class TestAAStoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aas.raw')
        references = AAStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_aas)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.AASxml.AASreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.AASxml.logger') as mock_logger:
                torefs = AAStoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("AASxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestAGUreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        reference = AGUreference('<citation id="bouw93"><journal_title>Global Biogeochem. Cycles</journal_title><first_author>Bouwman</first_author><reftitle>Global analysis of the potential for N<sub>2</sub>O production in natural soils</reftitle><volume>7</volume><firstPage>557</firstPage><CitationNumber>null</CitationNumber><year>1993</year><partOfCode>journal</partOfCode><paperType>article</paperType><doi>10.1234/doi_for_testing</doi></citation>')
        self.assertEqual(reference.get('authors'), 'Bouwman')
        self.assertEqual(reference.get('doi'), '10.1234/doi_for_testing')
        self.assertIsNone(reference.get('eprint', None))
        self.assertEqual(reference.get('refstr', ''), 'Bouwman, 1993. Global Biogeochem. Cycles, 7, 557. doi:10.1234/doi_for_testing')
        self.assertEqual(reference.parsed, 1)

    def test_pages(self):
        """ test parse pages method of reference class"""
        reference = AGUreference('<empty/>')
        self.assertEqual(reference.parse_pages(None), ('', None))
        self.assertEqual(reference.parse_pages('L23'), ('23', 'L'))
        self.assertEqual(reference.parse_pages('T2', ignore='RSTU'), ('2', None))
        self.assertEqual(reference.parse_pages('T2', letters='RSTU'), ('2', 'T'))
        self.assertEqual(reference.parse_pages('23S'), ('23', 'S'))
        self.assertEqual(reference.parse_pages('S23'), ('23', 'S'))

    def test_url(self):
        """ test calling url decode method  of reference class """
        reference = AGUreference('<empty/>')
        self.assertEqual(reference.url_decode('%AF'), '¯')

    def test_parse_refplaintext(self):
        """ test parse_refplaintext method """

        reference = AGUreference('<unstructured_citation>Bouwman, 1993. Global Biogeochem. Cycles, 7, 557.</unstructured_citation>')
        self.assertEqual(reference.parse_refplaintext(), 'Bouwman, 1993. Global Biogeochem. Cycles, 7, 557.')


class TestAGUtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.agu.xml')
        references = AGUtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_agu)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.AGUxml.AGUreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.AGUxml.logger') as mock_logger:
                torefs = AGUtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("AGUxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestAIPreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: when there is a doi
        reference = AIPreference('<citation id="r1"><ref><biother>    <biaugrp>        <biauth>            <bifname>G. A.</bifname>            <punct></punct>            <bilname>Smolenskii</bilname>        </biauth>        <punct>and</punct>        <biauth>            <bifname>V. A.</bifname>            <punct></punct>            <bilname>Ioffe</bilname>        </biauth>    </biaugrp>    <punct>,</punct>    <emph type="1">Communications de Colloque International de Magnetism de Grenoble</emph>    <punct>(</punct>    <othinfo>France), 2-6 Jullet, Communication No.1</othinfo>    <punct>(</punct>    <year>1958</year>    <punct>).</punct></biother><doi>10.1234/doi_for_testing</doi></ref></citation>')
        self.assertEqual(reference.get('authors'), 'Smolenskii, G., Ioffe, V.')
        self.assertEqual(reference.get('doi'), '10.1234/doi_for_testing')
        self.assertIsNone(reference.get('eprint', None))
        self.assertEqual(reference.get('refstr', ''), 'Smolenskii, G., Ioffe, V., 1958. doi:10.1234/doi_for_testing')
        self.assertEqual(reference.parsed, 1)

        # test case 2: when there is an arXiv id with tag isskey_xxx
        reference = AIPreference('<citation id="r1"><ref><biother>    <biaugrp>        <biauth>            <bifname>G. A.</bifname>            <punct></punct>            <bilname>Smolenskii</bilname>        </biauth>        <punct>and</punct>        <biauth>            <bifname>V. A.</bifname>            <punct></punct>            <bilname>Ioffe</bilname>        </biauth>    </biaugrp>    <punct>,</punct>    <emph type="1">Communications de Colloque International de Magnetism de Grenoble</emph>    <punct>(</punct>    <othinfo>France), 2-6 Jullet, Communication No.1</othinfo>    <punct>(</punct>    <year>1958</year>    <punct>).</punct></biother><isskey_xxx>arXiv:1234.56789</isskey_xxx></ref></citation>')
        expected_parsed_reference = {'authors': 'Smolenskii, G., Ioffe, V.',
                                     'year': '1958',
                                     'arxiv': 'arXiv:1234.56789',
                                     'refstr': 'Smolenskii, G., Ioffe, V., 1958. arXiv:1234.56789'}
        self.assertEqual(reference.get_parsed_reference(), expected_parsed_reference)

        # test case 3: when there is an arXiv id in reference string
        with patch('adsrefpipe.refparsers.AIPxml.AIPreference.match_arxiv_id') as mock_match_arxiv_id:
            mock_match_arxiv_id.return_value = 'arXiv:1234.56789'
            reference = AIPreference('<citation id="r1"><ref><biother>    <biaugrp>        <biauth>            <bifname>G. A.</bifname>            <punct></punct>            <bilname>Smolenskii</bilname>        </biauth>        <punct>and</punct>        <biauth>            <bifname>V. A.</bifname>            <punct></punct>            <bilname>Ioffe</bilname>        </biauth>    </biaugrp>    <punct>,</punct>    <emph type="1">Communications de Colloque International de Magnetism de Grenoble</emph>    <punct>(</punct>    <othinfo>France), 2-6 Jullet, Communication No.1</othinfo>    <punct>(</punct>    <year>1958</year>    <punct>).</punct></biother></ref></citation>')
            expected_parsed_reference = {'authors': 'Smolenskii, G., Ioffe, V.',
                                         'year': '1958',
                                         'arxiv': 'arXiv:1234.56789',
                                         'refstr': 'Smolenskii, G., Ioffe, V., 1958. arXiv:1234.56789'}
            self.assertEqual(reference.get_parsed_reference(), expected_parsed_reference)


class TestAIPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aip.xml')
        references = AIPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_aip)


    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.AIPxml.AIPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.AIPxml.logger') as mock_logger:
                torefs = AIPtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("AIPxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestAnAreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: when there is an eprint
        reference = AnAreference('<bibliomixed><ulink Type="arXiv">2103.12345</ulink></bibliomixed>')
        self.assertEqual(reference.get('eprint'), "arXiv:2103.12345")

        # test case 2: when there are no tags
        reference = AnAreference('<bibliomixed XRefLabel="bara02" N="4"><biblioset></biblioset></bibliomixed>')
        self.assertEqual(reference.get_parsed_reference(), {})


class TestAnAtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.ana.xml')
        references = AnAtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_ana)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.AnAxml.AnAreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.AnAxml.logger') as mock_logger:
                torefs = AnAtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("AnAxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestAPSreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: when there is no journal name, but series
        reference = APSreference("<ref id='c2'> <label>[2]</label> <mixed-citation publication-type='journal'><object-id>2</object-id><person-group person-group-type='author'><string-name name-style='western'>M. N. Ali</string-name><etal/></person-group>, <source>Nature</source> <series>Nature Ser. Test No. 0</series> <volume>514</volume>, <page-range>205</page-range> (<year>2014</year>).</mixed-citation> </ref>")
        self.assertEqual(reference.get('jrlstr'), 'Nature Ser. Test No. 0')
        self.assertEqual(reference.get('refplaintext'), '2 M. N. Ali , Nature Nature Ser. Test No. 0 514 , 205 ( 2014 ).')

        # test case 2: when there is an eprint but without a tag
        reference = APSreference('<ref citid="c88"><eprint><refauth>T. N. Truong</refauth>, arXiv:hep-ph/0102300</ref>')
        self.assertEqual(reference.get('eprint'), 'arXiv:hep-ph/0102300')
        self.assertEqual(reference.get('refstr'), 'arXiv:hep-ph/0102300')

        # test case 3: when there is an doi but without a tag
        reference = APSreference('<ref citid="c88"><eprint><refauth>T. N. Truong</refauth>, doi:10.1234/doi_for_testing</ref>')
        self.assertEqual(reference.get('doi'), 'doi:10.1234/doi_for_testing')
        self.assertEqual(reference.get('refstr'), 'doi:10.1234/doi_for_testing')

    def test_parse_authors(self):
        """ test parse_authors method """

        # test case 1: when the list of authors appear in one tag
        reference = APSreference('<ref citid="c36"><jcite><refauth>M. Golterman, and S. Peris</refauth>, <jtitle>J. High Energy Phys.</jtitle><coden>JHEPFG</coden><issn>1029-8479</issn><issue>01</issue> (<volume>2001</volume>) <pages>028</pages>.<doi>10.1088/1126-6708/2001/01/028</doi></jcite></ref>')
        self.assertEqual(reference.get('authors'), 'M. Golterman, and S. Peris')

        # test case 2: when there is only one author and the last name only
        reference = APSreference('<ref citid="c36"><jcite><refauth>Golterman</refauth><jtitle>J. High Energy Phys.</jtitle><coden>JHEPFG</coden><issn>1029-8479</issn><issue>01</issue> (<volume>2001</volume>) <pages>028</pages>.<doi>10.1088/1126-6708/2001/01/028</doi></jcite></ref>')
        self.assertEqual(reference.get('authors'), 'Golterman')


class TestAPStoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aps.xml')
        references = APStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_aps)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.APSxml.APSreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.APSxml.logger') as mock_logger:
                torefs = APStoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("APSxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])

    def test_cleanup(self):
        """ Test cleanup method """

        torefs = APStoREFs(filename='testfile.xml', buffer={})
        cleaned_reference, _ = torefs.cleanup('<ref><ibid><journal>Some Journal</journal></ref>', 'Previous Reference Authors')
        self.assertEqual(cleaned_reference, '<ref>Previous Reference Authors<journal>Some Journal</journal></ref>')


class TestBLACKWELLreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: having a bibcocde for type 1 reference type
        reference = BLACKWELLreference('<reference type="journal" id="b26"><namegroup type="author"><name type="author"><surname>Steiger</surname>, <forenames>R.H.</forenames></name><miscellaneoustext>&amp;</miscellaneoustext><name type="author"><surname>J&auml;ger</surname>, <forenames>E.</forenames></name></namegroup>, <date date="1977">1977</date>. <title type="document">Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology</title>, <title type="abbreviated">Earth planet. Sci. Lett.</title>, <volume>36</volume>, <page type="first">359</page>- <page type="last">362</page>. <externallink type="ads" id="1977E&PSL..36..359S"/></reference>')
        self.assertEqual(reference.get('bibcode'), '1977E&PSL..36..359S')
        self.assertEqual(reference.get('refstr', ''), 'Steiger, R.H., Jager, E., 1977. Earth planet. Sci. Lett., Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology, 36.')
        self.assertEqual(reference.parsed, 1)

        # test case 2: having a bibcocde for type 2 reference type
        reference = BLACKWELLreference('<bb id="b15"><jnlref><au><snm>Wei</snm><x>, </x><fnms>M.</fnms></au><x></x>&amp;<x></x><au><snm>Schwarz</snm><x>, </x><fnms>K.P.</fnms></au><x>, </x><cd year="1998">1998</cd><x>. </x><tl>Flight test results from a strapdown airborne gravity system</tl><x>, </x>J. Geodesy<x>, </x><vid>72</vid><x>, </x><ppf>323</ppf><x>-</x><ppl>332</ppl><x>.</x><extlink linktype="ads" linkid="1998JGeod..72..323W"/></jnlref></bb>')
        self.assertEqual(reference.get('bibcode'), '1998JGeod..72..323W')
        self.assertEqual(reference.get('refstr', ''), 'Wei, M., Schwarz, K.P., 1998. Flight test results from a strapdown airborne gravity system, 72, 323.')
        self.assertEqual(reference.parsed, 1)

        # test case 3: having a doi for type 1 reference type
        reference = BLACKWELLreference('<reference type="journal" id="b26"><namegroup type="author"><name type="author"><surname>Steiger</surname>, <forenames>R.H.</forenames></name><miscellaneoustext>&amp;</miscellaneoustext><name type="author"><surname>J&auml;ger</surname>, <forenames>E.</forenames></name></namegroup>, <date date="1977">1977</date>. <title type="document">Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology</title>, <title type="abbreviated">Earth planet. Sci. Lett.</title>, <volume>36</volume>, <page type="first">359</page>- <page type="last">362</page>. <externallink type="doi" id="10.1016/0012-821X(77)90060-7"/></reference>')
        self.assertEqual(reference.get('doi'), '10.1016/0012-821X(77)90060-7')
        self.assertEqual(reference.get('refstr', ''), 'Steiger, R.H., Jager, E., 1977. Earth planet. Sci. Lett., Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology, 36. doi:10.1016/0012-821X(77)90060-7')
        self.assertEqual(reference.parsed, 1)

        # test case 4: having a doi or type 1 reference type
        reference = BLACKWELLreference('<bb id="b15"><jnlref><au><snm>Wei</snm><x>, </x><fnms>M.</fnms></au><x></x>&amp;<x></x><au><snm>Schwarz</snm><x>, </x><fnms>K.P.</fnms></au><x>, </x><cd year="1998">1998</cd><x>. </x><tl>Flight test results from a strapdown airborne gravity system</tl><x>, </x>J. Geodesy<x>, </x><vid>72</vid><x>, </x><ppf>323</ppf><x>-</x><ppl>332</ppl><x>.</x><extlink linktype="doi" linkid="10.1007/s001900050171"/></jnlref></bb>')
        self.assertEqual(reference.get('doi'), '10.1007/s001900050171')
        self.assertEqual(reference.get('refstr', ''), 'Wei, M., Schwarz, K.P., 1998. Flight test results from a strapdown airborne gravity system, 72, 323. doi:10.1007/s001900050171')
        self.assertEqual(reference.parsed, 1)

    def test_parse_authors(self):

        # test case 1: no surname/forenames tags instead have fullname
        reference = BLACKWELLreference('<reference type="journal" id="b26"><namegroup type="author"><name type="author"><fullname>Steiger, R.H.</fullname></name></namegroup>, <date date="1977">1977</date>. <title type="document">Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology</title>, <title type="abbreviated">Earth planet. Sci. Lett.</title>, <volume>36</volume>, <page type="first">359</page>- <page type="last">362</page>.</reference>')
        self.assertEqual(reference.parse_authors(), 'Steiger, R.H.')

        # test case 2: no snm/fnms tag, just au
        reference = BLACKWELLreference('<bb id="b7"><bookref><au>Jekeli, C.</au><x>, </x><cd year="2000">2000</cd><x>. </x><tl>The inefficacy of stochastic gravity models in airborne IMU/GPS vector gravimetry</tl><x>, </x><refmisc>Paper presented at the IAG-Symp. GGG2000</refmisc><x>, </x><loc>Banff<x>, </x>Canada</loc><x>, </x><refmisc>August 2000</refmisc><x>.</x></bookref></bb>')
        self.assertEqual(reference.parse_authors(), 'Jekeli, C.')

        # test case 3: with collaboration tag corporatename
        reference = BLACKWELLreference('<reference type="journal" id="b26"><namegroup type="author"><name type="author"><surname>Steiger</surname>, <forenames>R.H.</forenames></name><miscellaneoustext>&amp;</miscellaneoustext><name type="author"><surname>J&auml;ger</surname>, <forenames>E.</forenames></name></namegroup>, <corporatename>Collaboration Name</corporatename>, <date date="1977">1977</date>. <title type="document">Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology</title>, <title type="abbreviated">Earth planet. Sci. Lett.</title>, <volume>36</volume>, <page type="first">359</page>- <page type="last">362</page>.</reference>')
        self.assertEqual(reference.parse_authors(), 'Collaboration Name, Steiger, R.H., Jager, E.')

        # test case 4: with collaboration tag collab
        reference = BLACKWELLreference("<reference><otherref><au><snm>Hoshiba</snm><x>, </x><fnms>M</fnms></au><x>, </x><cd year='1991'>1991.</cd><refmisc> Simulation of multiple-scattered coda wave excitation based on the energy conservation law, </refmisc><collab>Phys. Earth planet. Inter.</collab><refmisc>, 67, </refmisc><ppf>123</ppf><x>-</x><ppl>136</ppl><x>.</x></otherref></reference>")
        self.assertEqual(reference.parse_authors(), 'Phys. Earth planet. Inter., Hoshiba, M')

    def test_parse_eprint(self):
        """ test parse_eprint method """

        # test case 1: having a externallink type="url"
        reference = BLACKWELLreference('<reference type="journal" id="b26"><namegroup type="author"><name type="author"><surname>Steiger</surname>, <forenames>R.H.</forenames></name><miscellaneoustext>&amp;</miscellaneoustext><name type="author"><surname>J&auml;ger</surname>, <forenames>E.</forenames></name></namegroup>, <date date="1977">1977</date>. <title type="document">Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology</title>, <title type="abbreviated">Earth planet. Sci. Lett.</title>, <volume>36</volume>, <page type="first">359</page>- <page type="last">362</page>. <externallink type="url">http://arxiv.org/abs/nlin.CD/0123456</externallink></reference>')
        self.assertEqual(reference.parse_eprint(), 'nlin/0123456')

        # test case 2: having a externallink type=arxiv_class
        reference = BLACKWELLreference('<reference type="journal" id="b26"><namegroup type="author"><name type="author"><surname>Steiger</surname>, <forenames>R.H.</forenames></name><miscellaneoustext>&amp;</miscellaneoustext><name type="author"><surname>J&auml;ger</surname>, <forenames>E.</forenames></name></namegroup>, <date date="1977">1977</date>. <title type="document">Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology</title>, <title type="abbreviated">Earth planet. Sci. Lett.</title>, <volume>36</volume>, <page type="first">359</page>- <page type="last">362</page>. <externallink type="nlin">nlin/0123456</externallink></reference>')
        self.assertEqual(reference.parse_eprint(), 'nlin/0123456')

        # test case 3: having a miscellaneoustext
        reference = BLACKWELLreference('<reference type="journal" id="b26"><namegroup type="author"><name type="author"><surname>Steiger</surname>, <forenames>R.H.</forenames></name><name type="author"><surname>J&auml;ger</surname>, <forenames>E.</forenames></name></namegroup>, <date date="1977">1977</date>. <title type="document">Subcommission on Geochronology: convention on the use of decay constants in Geo and Cosmochronology</title>, <title type="abbreviated">Earth planet. Sci. Lett.</title>, <volume>36</volume>, <page type="first">359</page>- <page type="last">362</page>. <miscellaneoustext>arXiv:nlin/0123456 [nlin.CD]</miscellaneoustext></reference>')
        self.assertEqual(reference.parse_eprint(), 'nlin/0123456')


class TestBLACKWELLtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        testing = [
            ('/stubdata/test.blackwell.xml', parsed_references.parsed_blackwell),
            ('/stubdata/test.mnras.xml', parsed_references.parsed_mnras),
        ]
        for (file, expected) in testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + file)
            references = BLACKWELLtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.BlackwellXML.BLACKWELLreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.BlackwellXML.logger') as mock_logger:
                torefs = BLACKWELLtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("BLACKWELLxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestCrossRefToREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.xref.xml')
        references = CrossRefToREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_crossref)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.CrossRefXML.CrossRefreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.CrossRefXML.logger') as mock_logger:
                torefs = CrossRefToREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("CrossRefxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestCUPreference(unittest.TestCase):

    def test_parse(self):
        """ test the parse method"""

        # test case 1: when the citation type is "thesis" but no journal (title/university) can be extracted
        reference = CUPreference('<citation citation-type="thesis" id="ref017"><name><surname>Derra</surname><given-names>G.</given-names></name><year>1986</year>.</citation>')
        reference.parse()
        self.assertEqual(reference['jrlstr'], 'Thesis')

        # test case 2: when the citation type is "confproc" and there is a series tag
        reference = CUPreference('<citation citation-type="confproc" id="ref004"><name><surname>Campbell</surname><given-names>D. J.</given-names></name><etal/><year>1985</year> <series>Controlled Fusion and Plasma Physics</series>.</citation>')
        reference.parse()
        self.assertEqual(reference['jrlstr'], 'Controlled Fusion and Plasma Physics')

        # test case 3: when there is a DOI
        reference = CUPreference('<citation citation-type="journal" id="ref4"><name><surname>Rao</surname><given-names>S.</given-names></name><year>2003</year><source>ApJ</source><volume>595</volume><fpage>94</fpage><pub-id pub-id-type="doi">10.1086/377964</pub-id></citation>')
        reference.parse()
        self.assertEqual(reference['doi'], '10.1086/377964')

    def test_parse_authors(self):
        """ test parse_authors method """

        # test case 1: when authors tag name is missing, so use authors from `string-name` tag
        reference = CUPreference('<citation id="ref16" publication-type="other"><year>1967</year>, in: <string-name name-style="western"><given-names>J.</given-names><surname>Dommanget</surname></string-name> (ed.), <italic>On the Evolution of Double Stars</italic>, Communications Serie B, No. 17 Computes Rondus, p. 105</citation>')
        authors = reference.parse_authors()
        self.assertEqual(authors, 'Dommanget, J.')

        # test case 2: when authors are not found but collab exists
        reference = CUPreference('<citation citation-type="journal" id="ref2"><collab>Collaborative Group</collab><year>2006</year><source>ApJ</source><volume>630</volume><fpage>108</fpage></citation>')
        authors = reference.parse_authors()
        self.assertEqual(authors, 'Collaborative Group')


class TestCUPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.cup.xml')
        references = CUPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_cup)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.CUPxml.CUPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.CUPxml.logger') as mock_logger:
                torefs = CUPtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("CUPxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestEDPreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: when there is an arXiv link
        reference = EDPreference('<bibliomixed XRefLabel="AlBa02" N="1"> R. Albert, A.-L. Barabási, Rev. Mod. Phys. 74, 47 (2002)<biblioset><Title>Rev. Mod. Phys.</Title><Date>2002</Date><VolumeNum>74</VolumeNum><ArtPageNums>47</ArtPageNums><bibliomisc id="epjb||Rev. Mod. Phys.| R. Albert, A.-L. Barabási|74||47|2002||AlBa02[1]|"/></biblioset><ulink Type="arXiv">https://arxiv.org/abs/1234.5678</ulink> </bibliomixed>')
        reference.parse()
        self.assertEqual(reference['eprint'], 'https://arxiv.org/abs/1234.5678')  # eprint assigned from ulink

        # test case 2: when there is no bibliomixed tag, but other tags are there, so the reference is be parsed correctly
        reference = EDPreference('<biblioset><Title>Rev. Mod. Phys.</Title><Date>2002</Date><VolumeNum>74</VolumeNum><ArtPageNums>47</ArtPageNums><bibliomisc id="epjb||Rev. Mod. Phys.| R. Albert, A.-L. Barabási|74||47|2002||AlBa02[1]|"/></biblioset>')
        reference.parse()
        expected_parsed_reference = {'authors': 'R. Albert, A.-L. Barabási',
                                     'year': '2002',
                                     'journal': 'Rev. Mod. Phys.',
                                     'volume': '74',
                                     'page': '47',
                                     'refstr': 'R. Albert, A.-L. Barabási, 2002. Rev. Mod. Phys., 74, 47.'}
        self.assertEqual(reference.get_parsed_reference(), expected_parsed_reference)


class TestEDPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.edp.xml')
        references = EDPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_edp)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.EDPxml.EDPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.EDPxml.logger') as mock_logger:
                torefs = EDPtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("EDPxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestEGUtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.egu.xml')
        references = EGUtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_egu)
        
    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.EGUxml.EGUreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.EGUxml.logger') as mock_logger:
                torefs = EGUtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("EGUxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestELSEVIERreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: when journal is J. geophys. Res.
        reference = ELSEVIERreference("<?xml version='1.0' encoding='ISO-8859-1' standalone='yes' ?>\n<reference>\n    <textref>\n        Cassidy W. A., B. P. Glass and\n        B. C. Heezen (in press) Physical and chemical properties of the Australasian microtekites.\n        J. geophys. Res.\n    </textref>\n    <host>\n        <series>\n            <title>\n                <maintitle>Geophys. Res. Lett.</maintitle>\n            </title>\n        </series>\n    </host>\n    <issue-nr>ABC123</issue-nr>\n    <first-page>1</first-page>\n    <volume-nr>10</volume-nr>\n</reference>")
        self.assertEqual(reference.get('page'), 'BC123')

        # test case 2: when textref is missing, refplaintext will be marked as incomplete
        reference = ELSEVIERreference("<?xml version='1.0' encoding='ISO-8859-1' standalone='yes' ?>\n<reference>\n    Cassidy W. A., B. P. Glass and\n        B. C. Heezen (in press) Physical and chemical properties of the Australasian microtekites.\n        J. geophys. Res.\n    <host>\n        <series>\n            <title>\n                <maintitle>Geophys. Res. Lett.</maintitle>\n            </title>\n        </series>\n    </host>\n    <issue-nr>ABC123</issue-nr>\n    <first-page>1</first-page>\n    <volume-nr>10</volume-nr>\n</reference>")
        self.assertEqual(reference.get('refplaintext'), 'Cassidy W. A., B. P. Glass and B. C. Heezen (in press) Physical and chemical properties of the Australasian microtekites. J. geophys. Res. Geophys. Res. Lett. ABC123 1 10 --- Incomplete')

        # test case 3: when there is a collaboration tag
        reference = ELSEVIERreference("<?xml version='1.0' encoding='ISO-8859-1' standalone='yes' ?><reference id='sbref0001'><contribution langtype='en'><authors><author><given-name>M.A.</given-name><surname>Vicente</surname></author><author><given-name>J.</given-name><surname>Minguez</surname></author><author><given-name>D.C.</given-name><surname>Gonzalez</surname></author></authors></contribution><collaboration>Collaboration Name</collaboration><host><edited-book><editors><editor><given-name>A.M.</given-name><surname>Halefoglu</surname></editor></editors><date>2017</date><publisher><name>IntechOpen</name><location>Rijeka</location></publisher></edited-book></host></reference>")
        self.assertEqual(reference.parse_authors(), 'Collaboration Name, Vicente, M.A., Minguez, J., Gonzalez, D.C.')

        # test case 3: when there is are no authors)
        reference = ELSEVIERreference("<?xml version='1.0' encoding='ISO-8859-1' standalone='yes' ?><reference id='sbref0001'><contribution langtype='en'><editors><editor><given-name>A.M.</given-name><surname>Halefoglu</surname></editor></editors></contribution><host><edited-book><date>2017</date><publisher><name>IntechOpen</name><location>Rijeka</location></publisher></edited-book></host></reference>")
        self.assertEqual(reference.parse_authors(), 'Halefoglu, A.M.')


class TestELSEVIERtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.elsevier.xml')
        references = ELSEVIERtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_elsevier)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.ElsevierXML.ELSEVIERreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.ElsevierXML.logger') as mock_logger:
                torefs = ELSEVIERtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("ELSEVIERxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestICARUSreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: when there is an EDITION tag
        reference = ICARUSreference("<CITATION ID='icar.1998.6075RF9' TYPE='BK'><AUTHOR><SURNAME>Danby</SURNAME><FNAME>J.</FNAME><INIT>M. A.</INIT></AUTHOR> <DATE>1988</DATE>. <TITLE>Fundamentals of Celestial Mechanics</TITLE><EDITION>, 2nd edition</EDITION>, <PUBNAME><ORGNAME>Willmann&ndash;Bell</ORGNAME><CITY>Richmond</CITY><SBD>VA</SBD></PUBNAME>.<ARTICLEREF></ARTICLEREF></CITATION>")
        self.assertEqual(reference.get('ttlstr'), 'Fundamentals of Celestial Mechanics, 2nd edition Ed.')

        # test case 2: where there is a collabration tag, but no author list
        reference = ICARUSreference("<CITATION ID='IS985941RF3' TYPE='REP'><AUTHOR><SURNAME>Cremonese</SURNAME><FNAME>G.</FNAME></AUTHOR>, and the <CORPAUTH><ORGNAME>European Hale-Bopp Team</ORGNAME></CORPAUTH> <DATE>1997a</DATE>. <SERIESTITLE>IAU Circular</SERIESTITLE> <REPID>6631</REPID>.<ARTICLEREF></ARTICLEREF></CITATION>")
        self.assertEqual(reference.get('authors'), 'European Hale-Bopp Team, Cremonese, G.')


class TestICARUStoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.icarus.raw')
        references = ICARUStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_icarus)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.IcarusXML.ICARUSreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.IcarusXML.logger') as mock_logger:
                torefs = ICARUStoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("ICARUSxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestIOPFTtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.iopft.xml')
        references = IOPFTtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_iopft)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.IOPFTxml.IOPFTreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.IOPFTxml.logger') as mock_logger:
                torefs = IOPFTtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("IOPFTxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestIOPreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test when volume and year are equal and there is an issue number, issue is assigned to volume
        reference = IOPreference("<?xml version='1.0' encoding='ISO-8859-1' standalone='yes' ?><reference type='journal'> <ref_label>[7]</ref_label> <ref_citation>Ma X. 2017 Modelling and Simulation Based on ADAMS for Elevator Wire Rope System Journal of Hunan Institute of Engineering: Natural Science Edition 3 32-36</ref_citation> <ref_item_title>Modelling and Simulation Based on ADAMS for Elevator Wire Rope System</ref_item_title> <ref_authors>Ma X.</ref_authors> <ref_journal>Journal of Hunan Institute of Engineering: Natural Science Edition</ref_journal> <ref_issn>1671-119X</ref_issn> <ref_volume>2017</ref_volume> <ref_year>2017</ref_year> <ref_start_page>32</ref_start_page> <ref_end_page>36</ref_end_page> <ref_issue>2</ref_issue></reference>")
        self.assertEqual(reference.get('volume'), '2')


class TestIOPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        testing = [
            ('/stubdata/test.iop.xml', parsed_references.parsed_iop),
            ('/stubdata/test.edporiop.xml', parsed_references.parsed_edporiop),
        ]
        for (file, expected) in testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + file)
            references = IOPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.IOPxml.IOPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.IOPxml.logger') as mock_logger:
                torefs = IOPtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("IOPxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestIPAPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.ipap.xml')
        references = IPAPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_ipap)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.IPAPxml.IPAPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.IPAPxml.logger') as mock_logger:
                with patch.object(IPAPtoREFs, 'cleanup', return_value=['invalid reference']):
                    torefs = IPAPtoREFs(filename='testfile.ipap.xml', buffer={})
                    torefs.raw_references = raw_references
                    result = torefs.process_and_dispatch()

                    mock_logger.error.assert_called_with("IPAPxml: error parsing reference: ReferenceError")
                    self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])

    def test_get_references(self):
        """ test get_references methode """
        testfile_content = '''
                     <ADSBIBCODE>0000JaJAP...0.....Z</ADSBIBCODE><BibUnstructured>M. Sheik-Bahae, A. A. Said, T. H. Wei, D. J. Hagan, and E. W. Van Stryland: IEEE J. Quantum Electron. 26 (1990) 760.</BibUnstructured>
                     <ADSBIBCODE>0000JaJAP...1.....Z</ADSBIBCODE><BibUnstructured>T. D. Krauss and F. W. Wise: Appl. Phys. Lett. 65 (1994) 1739.</BibUnstructured>
        '''
        expected_results = [
            ['0000JaJAP...0.....Z', '<BibUnstructured>M. Sheik-Bahae, A. A. Said, T. H. Wei, D. J. Hagan, and E. W. Van Stryland: IEEE J. Quantum Electron. 26 (1990) 760.</BibUnstructured>\n                     '],
            ['0000JaJAP...1.....Z', '<BibUnstructured>T. D. Krauss and F. W. Wise: Appl. Phys. Lett. 65 (1994) 1739.</BibUnstructured>\n        ']
        ]

        with patch('builtins.open', mock_open(read_data=testfile_content)):
            results = IPAPtoREFs("testfile.ipap.xml", None).get_references("testfile.ipap.xml")

            self.assertEqual(results, expected_results)


    def test_cleanup(self):
        """ test cleanup method """
        torefs = IPAPtoREFs(filename='testfile.ipap.xml', buffer={})

        # test case 1: when there's an empty reference after splitting, also two references are merged, this causes confusion for the parser
        results = torefs.cleanup("<BibUnstructured>Author1 et al.: Journal1 (2021), Author2 et al.: Journal2 (2022);   </BibUnstructured>")
        self.assertEqual(results, ['<BibUnstructured>Author1 et al. 2022  Journal1 (2021), Author2 et al.: Journal2  </BibUnstructured>'])

        # test case 2: when reference does not match expected syntax pattern, missing year, which is rejected
        with patch('adsrefpipe.refparsers.IPAPxml.logger') as mock_logger:
            results = torefs.cleanup("<BibUnstructured>Author1 et al. Journal1</BibUnstructured>")
            self.assertEqual(results, [])
            mock_logger.error.assert_called_with("IPAPxml: reference string does not match expected syntax: Author1 et al. Journal1")


class TestJATSreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: when publication type is missing, set type to other and proceed
        reference = JATSreference('<mixed-citation publication-type="other">(1) L. A. Barroso: &#x201C;The Price of Performance&#x201D;, Queue, Vol. 3, No. 7, pp. 48-53 (2005)</mixed-citation>')
        reference.parse()
        self.assertEqual(reference.parsed, 1)

        # test case 2: when multiple 'conf-name' tags (conference names in separate tags)
        reference = JATSreference('<mixed-citation publication-type="confproc"> <string-name> <given-names>E.</given-names> <surname>Schiassi</surname> </string-name>, <string-name> <given-names>A.</given-names> <surname>D"Ambrosio</surname> </string-name>, <string-name> <given-names>H.</given-names> <surname>Johnston</surname> </string-name>, <string-name> <given-names>R.</given-names> <surname>Furfaro</surname> </string-name>, <string-name> <given-names>F.</given-names> <surname>Curti</surname> </string-name>, and <string-name> <given-names>D.</given-names> <surname>Mortari</surname> </string-name>, &ldquo; <article-title>Complete energy optimal landing on planetary bodies via theory of functional connections</article-title>,&rdquo; in <conf-name>Proceedings of the Astrodynamics Specialist Conference, AAS</conf-name> <conf-name>Advances in Spacecraft Navigation and Control</conf-name>, pp. <fpage>20</fpage>&ndash;<lpage>557</lpage>. </mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('ttlstr'), 'Proceedings of the Astrodynamics Specialist Conference, AAS')
        self.assertEqual(reference.get('jrlstr'), 'Advances in Spacecraft Navigation and Control')

        # test case 3: parse title and journal from unstructured field
        reference = JATSreference('<mixed-citation publication-type="other"> <person-group person-group-type="author"> <string-name> <given-names>K.</given-names> <x xml:space="preserve"> </x> <surname>McGrattan</surname> </string-name> <x xml:space="preserve"> and </x> <string-name> <given-names>S.</given-names> <x xml:space="preserve"> </x> <surname>Miles</surname> </string-name> </person-group> <x xml:space="preserve">, </x> <comment> Modeling fires using computational fluid dynamics (CFD): in <italic>Society of Fire Protection Engineering, SFPE Handbook Fire Prot. Eng.</italic>, 5th Ed., 2016, pp. 1034–1065 </comment> <x xml:space="preserve">.</x> </mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('ttlstr'), 'Modeling fires using computational fluid dynamics')
        self.assertEqual(reference.get('jrlstr'), 'Society of Fire Protection Engineering, SFPE Handbook Fire Prot. Eng.')

        # test case 4: parse journal from unstructured field
        reference = JATSreference('<mixed-citation publication-type="other"> <person-group person-group-type="author"> <string-name> <given-names>K.</given-names> <x xml:space="preserve"> </x> <surname>McGrattan</surname> </string-name> <x xml:space="preserve"> and </x> <string-name> <given-names>S.</given-names> <x xml:space="preserve"> </x> <surname>Miles</surname> </string-name> </person-group> <x xml:space="preserve">, </x> <comment> in <italic>Society of Fire Protection Engineering, SFPE Handbook Fire Prot. Eng.</italic>, 5th Ed., 2016, pp. 1034–1065 </comment> <x xml:space="preserve">.</x> </mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('ttlstr', ''), '')
        self.assertEqual(reference.get('jrlstr'), 'Society of Fire Protection Engineering, SFPE Handbook Fire Prot. Eng.')

        # test case 5: when eprint is in an external link tag
        reference = JATSreference('<mixed-citation publication-type="other">D.M. McAvity and H. Osborn, <italic>Conformal field theories near a boundary in general dimensions</italic>, <italic>Nucl. Phys. B</italic><bold>455</bold> (1995) 522 [<ext-link href="https://inspirehep.net/search?p=find%2BEPRINT%2Bcond-mat%2F9505127" ext-link-type="url">INSPIRE</ext-link>].</mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('eprint'), 'arXiv:cond-mat/9505127')

        # test case 6: when the only indication to identify the title are quotes around it
        reference = JATSreference('<mixed-citation publication-type="other">__amp__ldquo;American Association of Textile Chemists and Colorists, AATCC 61---Test Method for Colorfastness to Laundering__amp__rdquo;, Accelerated, 2013.</mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('ttlstr'), 'American Association of Textile Chemists and Colorists, AATCC 61---Test Method for Colorfastness to Laundering')

    def test_parse_authors(self):
        """ test parse_authors method """

        # test case 1: when IndexError exception is thrown since author surname or given-names are missing
        reference = JATSreference('<mixed-citation publication-type="journal"> <name> <surname>Doe</surname> </name><name> <given-names>Jane</given-names> <surname>Doe</surname> </name> <article-title>A study on computational methods</article-title> <source>Journal of Computational Science</source> <year>2021</year> </mixed-citation>')
        self.assertEqual(reference.parse_authors(), 'Doe')

        # test case 2: when an author element has no child nodes
        reference = JATSreference('<mixed-citation publication-type="journal"> <person-group person-group-type="author"/>  <article-title>Sample Article</article-title> <source>Sample Journal</source> <year>2021</year> </mixed-citation>')
        self.assertEqual(reference.parse_authors(), '')

        # test case 3: when an author element type is not included
        reference = JATSreference('<mixed-citation publication-type="journal"> <person-group> <name> <given-names>John</given-names> <surname>Doe</surname> </name> <name>no child element here</name> </person-group> <article-title>Sample Article</article-title> <source>Sample Journal</source> <year>2021</year> </mixed-citation>')
        self.assertEqual(reference.parse_authors(), '')

        # test case 4: when <name> tag is missing for an author
        reference = JATSreference('<mixed-citation publication-type="journal"> <person-group person-group-type="author"> <name> <given-names>John</given-names> <surname>Doe</surname> </name> <!-- missing tagName, simulate AttributeError --> <given-names>Jane</given-names> <surname>Smith</surname> </person-group> <article-title>Sample Article</article-title> <source>Sample Journal</source> <year>2021</year> </mixed-citation>')
        self.assertEqual(reference.parse_authors(), 'Doe, John')

        # test case 5: when author lastname is  not provided, throw an exception and return None
        reference = JATSreference('<mixed-citation publication-type="journal"> <person-group person-group-type="author"> <name> <given-names>John</given-names> <!-- missing surname to simulate error --> </name> <name> <given-names>Jane</given-names> <!-- missing surname to simulate error --> </name> </person-group> </mixed-citation>')
        self.assertIsNone(reference.parse_authors())

        # test case 6: malformed XML with an empty <string-name> element (no child nodes)
        reference = JATSreference('<mixed-citation publication-type="journal"> <person-group person-group-type="author"> <string-name> <given-names>J.</given-names> <surname>Doe</surname> </string-name> <string-name></string-name> <!-- this <string-name> has no child nodes --> </person-group> <article-title>Sample Article</article-title> <source>Sample Journal</source> <year>2021</year> </mixed-citation>')
        self.assertEqual(reference.parse_authors(), 'Doe, J.')


class TestJATStoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.jats.xml')
        references = JATStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_jats)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.JATSxml.JATSreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.JATSxml.logger') as mock_logger:
                torefs = JATStoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("JATSxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestJSTAGEreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test when there is a volume
        reference = JSTAGEreference("<Citation order='5'> <CitAuthor>[in Japanese]</CitAuthor> <CitJournalName>[in Japanese]</CitJournalName> <CitVol>47</CitVol> <CitFirstPage>65</CitFirstPage> <CitLastPage>80</CitLastPage> <CitYear>2005</CitYear> <Original lang='ja'>&#x67cf;&#x6728;&#x6d0b;&#x5f66;&#xff0c;&#x9e7f;&#x5712;&#x76f4;&#x5efa;(2005)&#xff1a;&#x65e5;&#x672c;&#x306e;&#x5806;&#x7a4d;&#x76c6;&#x3092;&#x60f3;&#x5b9a;&#x3057;&#x305f;&#x4e8c;&#x9178;&#x5316;&#x70ad;&#x7d20;&#x5730;&#x4e2d;&#x8caf;&#x7559;&#x306b;&#x304a;&#x3051;&#x308b;&#x6c34;-&#x5ca9;&#x77f3;&#x53cd;&#x5fdc;&#x306e;&#x691c;&#x8a0e;&#xff1a;&#x5343;&#x8449;&#x770c;&#x623f;&#x7dcf;&#x534a;&#x5cf6;&#x306e;&#x4f8b;&#xff0e;&#x65e5;&#x672c;&#x5730;&#x4e0b;&#x6c34;&#x5b66;&#x4f1a;&#x8a8c;&#xff0c;47, 65-80.</Original> <Original lang='en'/> </Citation>")
        self.assertEqual(reference.get('volume'), '47')


class TestJSTAGEtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.jst.xml')
        references = JSTAGEtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_jst)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.JSTAGExml.JSTAGEreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.JSTAGExml.logger') as mock_logger:
                torefs = JSTAGEtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("JSTAGExml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestLivingReviewstoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        xml_testing = [
            ('/stubdata/lrr-2014-6.living.xml', parsed_references.parsed_livingreviews_llr),
            ('/stubdata/lrsp-2007-2.living.xml', parsed_references.parsed_livingreviews_lrsp)
        ]
        for (filename, expected_results) in xml_testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + filename)
            references = LivingReviewsToREFs(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected_results)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.LivingReviewsXML.LivingReviewsreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.LivingReviewsXML.logger') as mock_logger:
                torefs = LivingReviewsToREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("LivingReviewsXML: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestMDPIreference(unittest.TestCase):

    def test_parse(self):
        """ test the parse method """

        # test case 1: when type is thesis with no 'conf-name' and 'source', but a 'comment' field containing 'thesis'
        reference = MDPIreference("<mixed-citation publication-type='journal'> <person-group person-group-type='author'> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> <year>2013</year> <!-- missing 'source' and 'conf-name' --> <comment>Thesis: Some interesting title on advanced physics</comment> </mixed-citation>")
        reference.parse()
        self.assertEqual(reference.get('jrlstr'), 'Thesis: Some interesting title on advanced physics')

        # test case 2: when comment contains only volume or pages but not both, so it cannot tell which is which
        reference = MDPIreference("<citation citation-type='journal'> <comment>123</comment> <!-- this comment contains only one numeric, needed two volume and pages --> <year>2021</year> </citation>")
        reference.parse()
        self.assertEqual(reference.get('volume'), '')
        self.assertEqual(reference.get('pages'), '')

        # test case 3: when DOI is included in the 'comment' field
        reference = MDPIreference("<mixed-citation publication-type='journal'> <comment>DOI: 10.1234/example.doi</comment> <year>2021</year> <article-title>Sample Article</article-title> <source>Sample Journal</source> </mixed-citation>")
        reference.parse()
        self.assertEqual(reference.get('doi'), '10.1234/example.doi')

        # test case 4: when DOI is included with no tag, parsed from refstr
        reference = MDPIreference("<mixed-citation publication-type='journal'> <year>2021</year> <article-title>Sample Article</article-title> <source>Sample Journal</source> DOI: 10.5678/another.doi <!-- included with no tag !--> </mixed-citation>")
        reference.parse()
        self.assertEqual(reference.get('doi'), '10.5678/another.doi')

        # test case 5: when eprint is parsed from <pub-id> with pub-id-type='arxiv'
        reference = MDPIreference('<mixed-citation publication-type="journal"> <pub-id pub-id-type="arxiv">arXiv:2312.08579</pub-id> <year>2021</year> <article-title>Sample Article</article-title> <source>Sample Journal</source> </mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('eprint'), 'arXiv:2312.08579')

        # test case 6: when eprint is parsed from <elocation-id> with content-type='arxiv'
        reference = MDPIreference('<mixed-citation publication-type="journal"> <elocation-id content-type="arxiv">arXiv:2312.08579</elocation-id> <year>2021</year> <article-title>Sample Article</article-title> <source>Sample Journal</source> </mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('eprint'), 'arXiv:2312.08579')


    def test_parse_authors(self):
        """ test parse_authors method """

        # test case: when there is no tag lastname and first name tags
        reference = MDPIreference("<mixed-citation publication-type='journal'> <person-group person-group-type='author'>Schultheis M.<etal>et al</etal> </person-group> <collab>University Collaborators</collab> </mixed-citation>")
        self.assertEqual(reference.parse_authors(), 'University Collaborators, Schultheis M. et. al')


class TestMDPItoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.mdpi.xml')
        references = MDPItoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_mdpi)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.MDPIxml.MDPIreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.MDPIxml.logger') as mock_logger:
                torefs = MDPItoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("MDPIxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])

    def test_missing_authors(self):
        """ test missing_authors method """

        torefs = MDPItoREFs(filename='', buffer=None)
        result = torefs.missing_authors(prev_reference='<person-group person-group-type="author"> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group>',
                                        cur_reference='--- the rest of the reference')
        self.assertEqual(result, '<person-group person-group-type="author"> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> the rest of the reference')


class TestNLM3toREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.nlm3.xml')
        references = NLMtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_nlm3)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.NLM3xml.NLMreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.NLM3xml.logger') as mock_logger:
                torefs = NLMtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("NLMxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestNaturetoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.nature.xml')
        references = NATUREtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_nature)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.NatureXML.NATUREreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.NatureXML.logger') as mock_logger:
                torefs = NATUREtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("NatureXML: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestONCPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.meta.xml')
        references = ONCPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_oncp)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.ONCPxml.ONCPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.ONCPxml.logger') as mock_logger:
                torefs = ONCPtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("ONCPxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])

    def test_get_references(self):
        """ test get_references methode """

        # test when there are multiple bibcodes in the file
        testfile_content = '''
                     <ADSBIBCODE>0000AcPSl...0.....Z</ADSBIBCODE><bibtext seqNum="1"> R. Hofstadter, F. Bumiller, M. R. Yearian, &lt;i&gt;Rev. Mod. Phys.&lt;/i&gt; 30 (1958) 483.</bibtext>
                     <ADSBIBCODE>0000AcPSl...1.....Z</ADSBIBCODE><bibtext seqNum="1"> J. J. Sakurai, Currents and Mesons, Univ. of Chicago Press, 1967.</bibtext>
        '''
        expected_results = [
            ['0000AcPSl...0.....Z', '<bibtext seqNum="1"> R. Hofstadter, F. Bumiller, M. R. Yearian, &lt;i&gt;Rev. Mod. Phys.&lt;/i&gt; 30 (1958) 483.</bibtext>\n                     '],
            ['0000AcPSl...1.....Z', '<bibtext seqNum="1"> J. J. Sakurai, Currents and Mesons, Univ. of Chicago Press, 1967.</bibtext>\n        ']
        ]

        with patch('builtins.open', mock_open(read_data=testfile_content)):
            results = ONCPtoREFs("testfile.meta.xml", None).get_references("testfile.meta.xml")

            self.assertEqual(results, expected_results)


class TestOUPreference(unittest.TestCase):

    def test_parse(self):
        """ test the parse method """

        # test case 1: when comment contains only volume or pages but not both, so it cannot tell which is which
        reference = OUPreference("<?xml version='1.0' encoding='ISO-8859-1' standalone='yes' ?><ref id='bib56'> <citation citation-type='journal'> <comment>123</comment> <!-- this comment contains only one numeric, needed two volume and pages --> <year>2021</year></citation> </ref>")
        reference.parse()
        self.assertEqual(reference.get('volume'), '')
        self.assertEqual(reference.get('pages'), '')

        # test case 2: when eprint is parsed from <elocation-id> with content-type='arxiv'
        reference = OUPreference('<mixed-citation publication-type="journal"> arXiv:2312.08579 <year>2021</year> <article-title>Sample Article</article-title> <source>Sample Journal</source> </mixed-citation>')
        reference.parse()
        self.assertEqual(reference.get('eprint'), 'arXiv:2312.08579')

    def test_parse_authors(self):
        """ test parse_authors method """

        # test case: when there is no tag lastname and first name tags
        reference = OUPreference("<mixed-citation publication-type='journal'> <person-group person-group-type='author'>Schultheis M.<etal>et al</etal> </person-group> <collab>University Collaborators</collab> </mixed-citation>")
        self.assertEqual(reference.parse_authors(), 'University Collaborators, Schultheis M. et. al')


class TestOUPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.oup.xml')
        references = OUPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_oup)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.OUPxml.OUPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.OUPxml.logger') as mock_logger:
                torefs = OUPtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("OUPxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])

    def test_missing_authors(self):
        """ test missing_authors method """

        torefs = OUPtoREFs(filename='', buffer=None)
        result = torefs.missing_authors(prev_reference='<person-group person-group-type="author"> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group>',
                                        cur_reference='--- the rest of the reference')
        self.assertEqual(result, '<person-group person-group-type="author"> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> the rest of the reference')


class TestPASAtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.pasa.xml')
        references = PASAtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_pasa)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.PASAxml.PASAreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.PASAxml.logger') as mock_logger:
                torefs = PASAtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("PASAxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestRSCReference(unittest.TestCase):

    def test_parse(self):
        """Test the parse method for DOI and eprint extraction"""

        # test case 1: when there is a DOI
        reference = RSCreference('<citgroup id="cit1"> <journalcit> <citauth><fname>L.-S.</fname><surname>Li</surname></citauth> <citauth><fname>J.</fname><surname>Hu</surname></citauth> <citauth><fname>W.</fname><surname>Yang</surname></citauth> <citauth><fname>A. P.</fname><surname>Alivisatos</surname></citauth> <title>Nano Lett.</title> <year>2001</year> <volumeno>1</volumeno> <pages><fpage>349</fpage><lpage>351</lpage></pages> <link type="doi">10.1021/nl010040z</link> </journalcit></citgroup>')
        reference.parse()
        self.assertEqual(reference['doi'], '10.1021/nl010040z')

        # test case 2: when there is an eprint
        reference = RSCreference('<citgroup id="cit3"> <journalcit> <citauth><fname>L.</fname><surname>Cademartiri</surname></citauth> <citauth><fname>G. A.</fname><surname>Ozin</surname></citauth> <title>Adv. Mater.</title> <year>2008</year> <volumeno>20</volumeno> <pages><fpage>A1</fpage><lpage>8</lpage></pages> <link type="arxiv">https://arxiv.org/abs/0807.2314</link> </journalcit> </citgroup>')
        reference.parse()
        self.assertEqual(reference['eprint'], 'arXiv:0807.2314')


class TestRSCtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.rsc.xml')
        references = RSCtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_rsc)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.RSCxml.RSCreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.RSCxml.logger') as mock_logger:
                torefs = RSCtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("RSCxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestSPIEreference(unittest.TestCase):

    def test_parse(self):
        """ test he parse method """

        # test case 1: when type is unknown
        with patch('adsrefpipe.refparsers.SPIExml.logger') as mock_logger:
            reference = SPIEreference("<ref id='r1'> <label>1.</label> <mixed-citation publication-type='unknown_type'> <collab>E. National Academies of Science and Medicine</collab>, <source>Exoplanet Science Strategy</source>, <publisher-name>The National Academies Press</publisher-name>, <publisher-loc>Washington DC</publisher-loc> (<year>2018</year>).</mixed-citation> </ref>")
            reference.parse()
            mock_logger.error.assert_called_with("SPIExml: found unknown reference type 'unknown_type'")

        # test case 2: when there is only a chapter title assign it to title
        spie_ref = SPIEreference("<ref id='r6'> <label>6.</label> <mixed-citation publication-type='book'> <person-group person-group-type='author'> <string-name> <given-names>P. F.</given-names> <surname>McManamon</surname> </string-name> </person-group>, <chapter-title>Chapter Title Example</chapter-title>, <publisher-name>SPIE</publisher-name>, <publisher-loc>Bellingham, WA</publisher-loc>.</mixed-citation> </ref>")
        spie_ref.parse()
        self.assertEqual(spie_ref.get('jrlstr', ''), '')
        self.assertEqual(spie_ref.get('ttlstr'), 'Chapter Title Example')

        # test case 3: when there is only a chapter title and also title assign chapter title to journal
        reference = SPIEreference("<ref id='r2'> <label>2.</label> <mixed-citation publication-type='other'><article-title>Non-contact vibration measurement</article-title>, <chapter-title>Chapter Title Example</chapter-title>, <ext-link ext-link-type='uri' href='https://www.polytec.com/uk/vibrometry/'>https://www.polytec.com/uk/vibrometry/</ext-link>.</mixed-citation> </ref>")
        reference.parse()
        self.assertEqual(reference.get('jrlstr'), 'Chapter Title Example')
        self.assertEqual(reference.get('ttlstr'), 'Non-contact vibration measurement')

        # test case 4: when type is 'thesis' and journal/title tags are missing
        reference = SPIEreference("<ref id='r9'> <label>9.</label> <mixed-citation publication-type='thesis'> <person-group person-group-type='author'> <string-name> <given-names>K. A.</given-names> <surname>Menard</surname> </string-name> </person-group>, Master's Thesis, UCF (<year>1995</year>).</mixed-citation> </ref>")
        reference.parse()
        self.assertEqual(reference.get('jrlstr'), "Master's Thesis")


class TestSPIEtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.spie.xml')
        references = SPIEtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_spie)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.SPIExml.SPIEreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.SPIExml.logger') as mock_logger:
                torefs = SPIEtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("SPIExml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestSPRINGERreference(unittest.TestCase):

    def test_parse(self):
        """ test the parse method """

        # test case 1: no chapter title, so booktitle is assigned to title
        reference = SPRINGERreference('<Citation ID="CR1"> <BibChapter> <ChapterTitle/> <BookTitle>Book Title</BookTitle> <SeriesTitle>Series Title</SeriesTitle> <Year>2022</Year> </BibChapter> <BibUnstructured>Book Title, Series Title, 2022</BibUnstructured> </Citation>')
        reference.parse()
        self.assertEqual(reference.get('ttlstr'), 'Book Title')
        self.assertEqual(reference.get('jrlstr'), 'Series Title')

        # test case 2: reference type BibIssue, journal set, title is empty

        reference = SPRINGERreference('<Citation ID="CR17"> <BibIssue> <JournalTitle>Journal Title</JournalTitle> <Year>2017</Year> <VolumeID>1893</VolumeID> <IssueID>1</IssueID> </BibIssue> <BibUnstructured>Numerical Analysis of Wave Propagation, AIP Conf. Proc. 1893 (1), 030130 (2017).</BibUnstructured> </Citation>')
        reference.parse()
        self.assertEqual(reference.get('jrlstr'), 'Journal Title')
        self.assertEqual(reference.get('ttlstr', ''), '')

        # test case 3: no title and no year, should parse title and year from refstr
        reference = SPRINGERreference('<Citation ID="CR2"> <BibUnstructured>Smith, J., Doe, A., and Johnson, R. Some Title of the Paper, Journal of Research, 2021</BibUnstructured> </Citation>')
        reference.parse()
        self.assertEqual(reference.get('ttlstr'), 'Some Title of the Paper')
        self.assertEqual(reference.get('year'), '2021')
        self.assertEqual(reference.get('refstr'), None)
        self.assertEqual(reference.get('refplaintext'), 'Smith, J., Doe, A., and Johnson, R. Some Title of the Paper, Journal of Research, 2021')

    def test_parse_authors(self):
        """ test parse_authors method"""

        # test parse authors with institutional authors if no authors exist
        reference = SPRINGERreference('<Citation ID="CR4"> <BibArticle> <InstitutionalAuthorName>Some Institutional Author</InstitutionalAuthorName> </BibArticle> </Citation>')
        authors = reference.parse_authors()
        self.assertEqual(authors, 'Some Institutional Author')

    def test_parse_doi(self):
        """ test parse_doi method"""

        # test DOI extraction when an exception happen
        doi = SPRINGERreference('<Citation ID="CR5"> <BibArticle> <Occurrence Type="DOI"> <Handle></Handle>  <!-- empty Handle to trigger the exception --> </Occurrence> </BibArticle> </Citation>').parse_doi()
        self.assertEqual(doi, None)


class TestSpringertoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.springer.xml')
        references = SPRINGERtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_springer)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.SpringerXML.SPRINGERreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.SpringerXML.logger') as mock_logger:
                torefs = SPRINGERtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("SPRINGERxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])

    def test_cleanup_(self):
        """ Test cleanup method """

        cleaned_reference = SPRINGERtoREFs('', {}).cleanup('<Occurrence Type="DOI"><Handle><https://doi.org/10.1000/xyz></Handle></Occurrence>')
        # assert that the DOI has been cleaned, i.e., < and > have been replaced
        self.assertIn('<Handle>&lt;', cleaned_reference)
        self.assertIn('&gt;</Handle>', cleaned_reference)
        self.assertNotIn('<Handle><', cleaned_reference)
        self.assertNotIn('></Handle>', cleaned_reference)


class TestUCPreference(unittest.TestCase):

    def test_parse(self):
        """ test the parse method """

        # test case 1: when tag for year is missing, but it is pased correctly
        reference = UCPreference("<citation> <person-group person-group-type='author'> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> <article-title>Astrophysics Study</article-title> <source>Astrophys. J.</source> <volume>343</volume> <fpage>481</fpage> <lpage>488</lpage> 2020 <!-- year without any tag --> </citation>")
        reference.parse()
        self.assertEqual(reference.get('year'), "2020")

        # test case 2: when type is thesis, but no other information is provided, 'Thesis' is assigned to journal
        reference = UCPreference("<citation citation-type='thesis'> <person-group person-group-type='author'> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> <year>2020</year> </citation>")
        reference.parse()  # Invoke the parsing method
        self.assertEqual(reference.get('jrlstr'), 'Thesis')

        # test case 3: extract jounal from comment
        reference = UCPreference("<citation> <person-group person-group-type='author'> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> <year>2020</year> <article-title>Astrophysics Study</article-title> <comment>, Technical Instrument Report <pub-id pub-id-type='art-access-id'>WFPC2 98-01</pub-id></comment> </citation>")
        reference.parse()
        self.assertEqual(reference.get('jrlstr'), "Technical Instrument Report  WFPC2 98-01")

        # test case 4: parsing doi
        reference = UCPreference('<citation> <person-group person-group-type="author"> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> <year>2020</year> <article-title>Astrophysics Study</article-title> <source>Astrophys. J.</source> <volume>343</volume> <fpage>481</fpage> <lpage>488</lpage> <pub-id pub-id-type="doi">10.1109/XYZ2020</pub-id> </citation>')
        reference.parse()
        self.assertEqual(reference.get('doi'), "10.1109/XYZ2020")

        # test case 5: parsing eprint
        reference = UCPreference('<citation> <person-group person-group-type="author"> <name><surname>Makarenko</surname><given-names>A.N.</given-names></name> <name><surname>Obukhov</surname><given-names>V.V.</given-names></name> <name><surname>Kirnos</surname><given-names>I.V.</given-names></name> </person-group> <year>2020</year> <article-title>Astrophysics Study</article-title> <source>Astrophys. J.</source> <volume>343</volume> <fpage>481</fpage> <lpage>488</lpage> <pub-id pub-id-type="arxiv">arXiv:2001.01234</pub-id> </citation>')
        reference.parse()
        self.assertEqual(reference.get('eprint'), "arXiv:2001.01234")

        # test case 6: parsing doi when there is no doi tag
        reference = UCPreference('<citation> <person-group person-group-type="author"> <name><surname>Smith</surname><given-names>J.</given-names></name> </person-group> <year>2021</year> <article-title>Scientific Discoveries in Astrophysics</article-title> <source>Astrophysical Journal</source> DOI: 10.1234/astrophys.2021.123456</citation>')
        reference.parse()
        self.assertEqual(reference.get('doi'), '10.1234/astrophys.2021.123456')

        # test case 7: parsing eprint when there is no tag
        reference = UCPreference('<citation> <person-group person-group-type="author"> <name><surname>Smith</surname><given-names>J.</given-names></name> </person-group> <year>2021</year> <article-title>Scientific Discoveries in Astrophysics</article-title> <source>Astrophysical Journal</source> arXiv:2101.12345 </citation>')
        reference.parse()  # Invoke the parsing method
        self.assertEqual(reference.get('eprint'), 'arXiv:2101.12345')

    def test_parse_authors(self):
        """ test parse_authors method """

        # test case, when there is a colab
        reference = UCPreference('<citation> <person-group person-group-type="author"> <name><surname>Smith</surname><given-names>J.</given-names></name> <name><surname>Johnson</surname><given-names>A.</given-names></name> </person-group> <collab>Global Collaboration Team</collab> <year>2021</year> <article-title>Scientific Discoveries in Astrophysics</article-title> <source>Astrophysical Journal</source> </citation>')
        reference.parse()
        self.assertEqual(reference.get('authors'), "Global Collaboration Team, Smith, J.")


class TestUCPtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.ucp.xml')
        references = UCPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_ucp)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.UCPxml.UCPreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.UCPxml.logger') as mock_logger:
                torefs = UCPtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("UCPxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


class TestVERSITAreference(unittest.TestCase):

    def test_parse(self):
        """ test parse method when volume and pages indicators are missing """

        expected_parsed_reference = {'authors': 'Li, H, Ni, JP, Yang, XD, Dong, QF',
                                     'journal': 'Open Phys',
                                     'title': 'Test influence of screen thickness on Double-N Six-light-screen sky screen target',
                                     'volume': '2022',
                                     'page': '18',
                                     'year': '2022',
                                     'refstr': 'Li, H, Ni, JP, Yang, XD, Dong, QF, 2022. Open Phys, Test influence of screen thickness on Double-N Six-light-screen sky screen target, 2022, 18.'}

        # test Case 1: when both volume and pages tags are missing, with mixed_citation available
        reference_string = '<ref id="j_phys-2022-0222_ref_003"> <label>[3]</label> <mixed-citation>Li H, Ni JP, Yang XD, Dong QF. "Test influence of screen thickness on Double-N Six-light-screen sky screen target." Open Phys. id. 202218, 2022.</mixed-citation> <element-citation publication-type="journal"> <name> <surname>Li</surname> <given-names>H</given-names> </name> <name> <surname>Ni</surname> <given-names>JP</given-names> </name> <name> <surname>Yang</surname> <given-names>XD</given-names> </name> <name> <surname>Dong</surname> <given-names>QF</given-names> </name> <article-title>Test influence of screen thickness on Double-N Six-light-screen sky screen target</article-title> <source>Open Phys</source> <year>2022</year> <issue>1</issue> </element-citation></ref>'
        reference = VERSITAreference(reference_string)
        self.assertEqual(reference.get_parsed_reference(), expected_parsed_reference)

        # test Case 2: when volume and pages tags are missing, in addition to mixed_citation tag
        reference_string = '<ref id="j_phys-2022-0222_ref_003"> <label>[3]</label> Li H, Ni JP, Yang XD, Dong QF. "Test influence of screen thickness on Double-N Six-light-screen sky screen target." Open Phys. id. 202218, 2022. <element-citation publication-type="journal"> <name> <surname>Li</surname> <given-names>H</given-names> </name> <name> <surname>Ni</surname> <given-names>JP</given-names> </name> <name> <surname>Yang</surname> <given-names>XD</given-names> </name> <name> <surname>Dong</surname> <given-names>QF</given-names> </name> <article-title>Test influence of screen thickness on Double-N Six-light-screen sky screen target</article-title> <source>Open Phys</source> <year>2022</year> <issue>1</issue> </element-citation></ref>'
        reference = VERSITAreference(reference_string)
        self.assertEqual(reference.get_parsed_reference(), expected_parsed_reference)

    def test_parse_authors(self):
        """ test parse_authors method """
        # when there is no author
        reference_string = "<ref id='j_phys-2022-0006_ref_026'> <label>[26]</label> <mixed-citation>OV2710-1E. 1080p/720p HD color CMOS image sensor with OmniPixel&reg;3-HS technology. Available from: https://www.ovt.com/sensors/OV2710-1E (accessed on November 9th, 2019).</mixed-citation> <element-citation publication-type='other'> <source>OV2710-1E. 1080p/720p HD color CMOS image sensor with OmniPixel&reg;3-HS technology</source> <comment>Available from: https://www.ovt.com/sensors/OV2710-1E (accessed on November 9th, 2019).</comment> </element-citation> </ref>"
        reference = VERSITAreference(reference_string)
        self.assertEqual(reference.parse_authors(), '')


class TestVERSITAtoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """

        # test case 1: using filename
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.versita.xml')
        references = VERSITAtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_versita)

        # test case 2: having it in the buffer
        buffer = {
            "source_filename": os.path.abspath(os.path.dirname(__file__) + "/stubdata/test.versita.xml"),
            "parser_name": "VERSITA",
            "block_references": [
                {'source_bibcode': '0000OPhy....0.....Z',
                 'references': [{'item_num': '1',
                                 'refraw':'<ref id="j_phys-2022-0222_ref_002"> <label>[2]</label> <mixed-citation>Astakhov SA, Biriukov VI. Problems of ensuring the acceleration dynamics of aircraft during track tests at a speed of 1600 m/s. Incas Bull. 2020;12(S):33&ndash;42.</mixed-citation> <element-citation publication-type="journal"> <name> <surname>Astakhov</surname> <given-names>SA</given-names> </name> <name> <surname>Biriukov</surname> <given-names>VI</given-names> </name> <article-title>Problems of ensuring the acceleration dynamics of aircraft during track tests at a speed of 1600 m/s</article-title> <source>Incas Bull</source> <year>2020</year> <volume>12</volume> <issue>S</issue> <fpage>33</fpage> <lpage>42</lpage> </element-citation></ref>'},
                                {'item_num': '2',
                                 'refraw': '<ref id="j_phys-2022-0222_ref_003"> <label>[3]</label> <mixed-citation>Li H, Ni JP, Yang XD, Dong QF. Test influence of screen thickness on Double-N Six-light-screen sky screen target. Open Phys. 2022;19(1):1&ndash;8.</mixed-citation> <element-citation publication-type="journal"> <name> <surname>Li</surname> <given-names>H</given-names> </name> <name> <surname>Ni</surname> <given-names>JP</given-names> </name> <name> <surname>Yang</surname> <given-names>XD</given-names> </name> <name> <surname>Dong</surname> <given-names>QF</given-names> </name> <article-title>Test influence of screen thickness on Double-N Six-light-screen sky screen target</article-title> <source>Open Phys</source> <year>2022</year> <volume>19</volume> <issue>1</issue> <fpage>1</fpage> <lpage>8</lpage> </element-citation></ref>'}]
                },
            ]
        }
        references = VERSITAtoREFs(filename='', buffer=buffer).process_and_dispatch()
        expected_references = [{'bibcode': '0000OPhy....0.....Z',
                                'references': [{'authors': 'Astakhov, SA, Biriukov, VI',
                                                'journal': 'Incas Bull',
                                                'title': 'Problems of ensuring the acceleration dynamics of aircraft during track tests at a speed of 1600 m/s',
                                                'volume': '12',
                                                'page': '33',
                                                'year': '2020',
                                                'refstr': 'Astakhov, SA, Biriukov, VI, 2020. Incas Bull, Problems of ensuring the acceleration dynamics of aircraft during track tests at a speed of 1600 m/s, 12, 33.',
                                                'refraw': '<ref id="j_phys-2022-0222_ref_002"> <label>[2]</label> <mixed-citation>Astakhov SA, Biriukov VI. Problems of ensuring the acceleration dynamics of aircraft during track tests at a speed of 1600 m/s. Incas Bull. 2020;12(S):33&ndash;42.</mixed-citation> <element-citation publication-type="journal"> <name> <surname>Astakhov</surname> <given-names>SA</given-names> </name> <name> <surname>Biriukov</surname> <given-names>VI</given-names> </name> <article-title>Problems of ensuring the acceleration dynamics of aircraft during track tests at a speed of 1600 m/s</article-title> <source>Incas Bull</source> <year>2020</year> <volume>12</volume> <issue>S</issue> <fpage>33</fpage> <lpage>42</lpage> </element-citation></ref>', 'item_num': '1'},
                                               {'authors': 'Li, H, Ni, JP, Yang, XD, Dong, QF',
                                                'journal': 'Open Phys',
                                                'title': 'Test influence of screen thickness on Double-N Six-light-screen sky screen target',
                                                'volume': '19',
                                                'page': '1',
                                                'year': '2022',
                                                'refstr': 'Li, H, Ni, JP, Yang, XD, Dong, QF, 2022. Open Phys, Test influence of screen thickness on Double-N Six-light-screen sky screen target, 19, 1.',
                                                'refraw': '<ref id="j_phys-2022-0222_ref_003"> <label>[3]</label> <mixed-citation>Li H, Ni JP, Yang XD, Dong QF. Test influence of screen thickness on Double-N Six-light-screen sky screen target. Open Phys. 2022;19(1):1&ndash;8.</mixed-citation> <element-citation publication-type="journal"> <name> <surname>Li</surname> <given-names>H</given-names> </name> <name> <surname>Ni</surname> <given-names>JP</given-names> </name> <name> <surname>Yang</surname> <given-names>XD</given-names> </name> <name> <surname>Dong</surname> <given-names>QF</given-names> </name> <article-title>Test influence of screen thickness on Double-N Six-light-screen sky screen target</article-title> <source>Open Phys</source> <year>2022</year> <volume>19</volume> <issue>1</issue> <fpage>1</fpage> <lpage>8</lpage> </element-citation></ref>', 'item_num': '2'}
                                               ]
                                }]
        self.assertEqual(references, expected_references)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.VERSITAxml.VERSITAreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.VERSITAxml.logger') as mock_logger:
                torefs = VERSITAtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("VERSITAxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])

    @patch.object(VERSITAtoREFs, 'get_references')
    @patch('adsrefpipe.refparsers.VERSITAxml.logger')
    def test_init_error(self, mock_logger, mock_get_references):
        """ test valid bibcode length when reading from file """

        mock_get_references.return_value = [
            ['000123456789012345678',
             'Huang C, Liu Z, Liu ZX, Hao CL, Li DJ, Luo K. Motion characteristics of high-speed supercavitating projectiles including structural deformation. Energies. 2022;15(1):1&ndash;7.'],
            ['0002234567890123456',
             'Astakhov SA, Biriukov VI. Problems of ensuring the acceleration dynamics of aircraft during track tests at a speed of 1600 m/s. Incas Bull. 2020;12(S):33&ndash;42.']
        ]

        torefs = VERSITAtoREFs(filename='testfile.versita.xml', buffer={})

        mock_get_references.assert_called_once_with(filename='testfile.versita.xml')

        self.assertEqual(len(torefs.raw_references), 1)
        self.assertEqual(torefs.raw_references[0]['bibcode'], '0002234567890123456')

        mock_logger.error.assert_called_with(
            'Error in getting a bibcode along with the reference strings from reference file testfile.versita.xml. '
            'Returned 000123456789012345678 for bibcode. Skipping!'
        )


class TestWileyReference(unittest.TestCase):

    def test_parse(self):
        """ test parse method """

        # test case 1: exception in xmlnode_attribute when no type is included in the reference
        with patch('adsrefpipe.refparsers.reference.XMLreference.xmlnode_attribute') as mock_xmlnode_attribute:
            mock_xmlnode_attribute.side_effect = Exception("AttributeError")
            reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <otherTitle>Learning nonlinear dynamical systems using an eM algorithm</otherTitle>. In <bookTitle>Advances in neural information processing systems</bookTitle> (pp. <pageFirst>431</pageFirst>-<pageLast>437</pageLast>).</citation></bib>')

            # when type is missing or wrong, it treats it as `other`
            expected_parsed_reference = {'authors': 'Ghahramani, Z., Roweis, S. T.',
                                         'journal': 'Advances in neural information processing systems',
                                         'title': 'Learning nonlinear dynamical systems using an eM algorithm',
                                         'page': '431',
                                         'year': '1999',
                                         'refstr': 'Ghahramani, Z., Roweis, S. T., 1999. Advances in neural information processing systems, Learning nonlinear dynamical systems using an eM algorithm, 431.'}

            self.assertEqual(reference.get_parsed_reference(), expected_parsed_reference)

        # test case 2: when an unknown type is included in the reference
        with patch('adsrefpipe.refparsers.WileyXML.logger') as mock_logger:
            reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="unknown_type"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <otherTitle>Learning nonlinear dynamical systems using an eM algorithm</otherTitle>. In <bookTitle>Advances in neural information processing systems</bookTitle> (pp. <pageFirst>431</pageFirst>-<pageLast>437</pageLast>).</citation></bib>')
            mock_logger.error.assert_called_with("WILEY2xml: found unknown reference type 'unknown_type'")

        # test case 3: having an arXiv id
        with patch('adsrefpipe.refparsers.WileyXML.WILEYreference.match_arxiv_id') as mock_match_arxiv_id:
            mock_match_arxiv_id.return_value = 'arXiv:1234.56789'
            reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <otherTitle>Learning nonlinear dynamical systems using an eM algorithm</otherTitle>. In <bookTitle>Advances in neural information processing systems</bookTitle> (pp. <pageFirst>431</pageFirst>-<pageLast>437</pageLast>).</citation></bib>')

            expected_parsed_reference = {'authors': 'Ghahramani, Z., Roweis, S. T.',
                                         'journal': 'Advances in neural information processing systems',
                                         'title': 'Learning nonlinear dynamical systems using an eM algorithm',
                                         'page': '431',
                                         'year': '1999',
                                         'arxiv': 'arXiv:1234.56789',
                                         'refstr': 'Ghahramani, Z., Roweis, S. T., 1999. Advances in neural information processing systems, Learning nonlinear dynamical systems using an eM algorithm, 431. arXiv:1234.56789'}

            self.assertEqual(reference.get_parsed_reference(), expected_parsed_reference)

    def test_nodecontents(self):
        """ test nodecontents method when there is an exception """
        with patch('adsrefpipe.refparsers.reference.XMLreference.xmlnode_nodecontents') as mock_xmlnode_nodecontents:
            mock_xmlnode_nodecontents.side_effect = Exception("Exception")

            reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation_error xml:id="wrcr24970-cit-0043"></citation_error></bib>')

            result = reference.nodecontents('some_node')
            self.assertEqual(result, '')

    def test_parse_authors(self):
        """ test parse_authors method """

        # test case 1: when there is a group
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author>, <groupName>Research Group</groupName> (<pubYear year="1999">1999</pubYear>). <otherTitle>Learning nonlinear dynamical systems using an eM algorithm</otherTitle>. In <bookTitle>Advances in neural information processing systems</bookTitle> (pp. <pageFirst>431</pageFirst>-<pageLast>437</pageLast>).</citation></bib>')
        self.assertEqual(reference.parse_authors(), 'Research Group, Ghahramani, Z., Roweis, S. T.')

        # test case 2: when the authors list is not divided into first name and last name, remove initials and keep last names only
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><author><familyName>Z. Ghahramani</familyName></author>, &amp; <author><familyName>S. T. Roweis</familyName></author> (<pubYear year="1999">1999</pubYear>). <otherTitle>Learning nonlinear dynamical systems using an eM algorithm</otherTitle>. In <bookTitle>Advances in neural information processing systems</bookTitle> (pp. <pageFirst>431</pageFirst>-<pageLast>437</pageLast>).</citation></bib>')
        self.assertEqual(reference.parse_authors(), 'Ghahramani, Roweis')

    def test_parse_pub_type(self):
        """ test parse_pub_type_other method """

        # test case 1: only one title field (articleTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <articleTitle>Learning nonlinear dynamical systems using an eM algorithm</articleTitle>.</citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Learning nonlinear dynamical systems using an eM algorithm")
        self.assertIsNone(journal)
        self.assertIsNone(series)

        # test case 2: only one title field (chapterTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><chapterTitle>Learning nonlinear dynamical systems using an eM algorithm</chapterTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Learning nonlinear dynamical systems using an eM algorithm")
        self.assertIsNone(journal)
        self.assertIsNone(series)

        # test case 3: only one title field (journalTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><journalTitle>Journal of Computational Science</journalTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Journal of Computational Science")
        self.assertIsNone(journal)
        self.assertIsNone(series)

        # test case 4: only one title field (bookTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><bookTitle>Advances in Neural Information Processing</bookTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertIsNone(title)
        self.assertEqual(journal, "Advances in Neural Information Processing")
        self.assertIsNone(series)

        # test case 5: only one title field (bookSeriesTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><bookSeriesTitle>Information Science Series</bookSeriesTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertIsNone(title)
        self.assertEqual(journal, "Information Science Series")
        self.assertIsNone(series)

        # test case 6: only one title field (otherTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><otherTitle>Understanding Artificial Intelligence</otherTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertIsNone(title)
        self.assertEqual(journal, "Understanding Artificial Intelligence")
        self.assertIsNone(series)

        # test case 7: two title fields (otherTitle and articleTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <otherTitle>Learning nonlinear dynamical systems using an eM algorithm</otherTitle>. <articleTitle>Nonlinear systems analysis</articleTitle>.</citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Learning nonlinear dynamical systems using an eM algorithm")
        self.assertEqual(journal, "Nonlinear systems analysis")
        self.assertIsNone(series)

        # test case 8: two title fields (bookTitle and bookSeriesTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><bookTitle>Advances in neural information processing</bookTitle><bookSeriesTitle>Information Science Series</bookSeriesTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Advances in neural information processing")
        self.assertEqual(journal, "Information Science Series")
        self.assertIsNone(series)

        # test case 9: two title fields (chapterTitle and bookSeriesTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><chapterTitle>Chapter 3: Data Analysis</chapterTitle><bookSeriesTitle>Springer Texts in Computational Science</bookSeriesTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Chapter 3: Data Analysis")
        self.assertEqual(journal, "Springer Texts in Computational Science")
        self.assertIsNone(series)

        # test case 10: two title fields (otherTitle and bookSeriesTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><otherTitle>Learning Deep Neural Networks</otherTitle><bookSeriesTitle>AI and Machine Learning</bookSeriesTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Learning Deep Neural Networks")
        self.assertEqual(journal, "AI and Machine Learning")
        self.assertIsNone(series)

        # test case 11: two title fields (chapterTitle and bookTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><chapterTitle>Advanced Machine Learning</chapterTitle><bookTitle>Introduction to AI</bookTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Advanced Machine Learning")
        self.assertEqual(journal, "Introduction to AI")
        self.assertIsNone(series)

        # test case 12: two title fields (otherTitle and bookTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><otherTitle>Exploring Deep Learning</otherTitle><bookTitle>Modern AI Techniques</bookTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Exploring Deep Learning")
        self.assertEqual(journal, "Modern AI Techniques")
        self.assertIsNone(series)

        # test case 13: two title fields (chapterTitle and otherTitle)
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><chapterTitle>Chapter 4: AI Algorithms</chapterTitle><otherTitle>Deep Neural Networks for AI</otherTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Chapter 4: AI Algorithms")
        self.assertEqual(journal, "Deep Neural Networks for AI")
        self.assertIsNone(series)

        # test case 14: error case for two titles (unknown combination), return the titles anyway
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <articleTitle>Learning nonlinear dynamical systems using an eM algorithm</articleTitle>. <bookSeriesTitle>Advanced Machine Learning</bookSeriesTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Learning nonlinear dynamical systems using an eM algorithm")
        self.assertEqual(journal, "Advanced Machine Learning")
        self.assertIsNone(series)

        # test case 15: error case for three titles (unknown combination), return the titles anyway
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <chapterTitle>Machine Learning Chapter</chapterTitle> <otherTitle>Advanced Neural Networks</otherTitle> <bookSeriesTitle>Data Science Series</bookSeriesTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertEqual(title, "Machine Learning Chapter")
        self.assertEqual(journal, "Data Science Series")
        self.assertEqual(series, "Advanced Neural Networks")

        # test case 16: no title field
        reference = WILEYreference('<bib xml:id="jgrd4543-bib-0056"><citation xml:id="wrcr24970-cit-0043" type="other"><author><familyName>Ghahramani</familyName>, <givenNames>Z.</givenNames></author>, &amp; <author><familyName>Roweis</familyName>, <givenNames>S. T.</givenNames></author> (<pubYear year="1999">1999</pubYear>). <otherTitle></otherTitle></citation></bib>')
        title, journal, series = reference.parse_pub_type_other()
        self.assertIsNone(title)
        self.assertIsNone(journal)
        self.assertIsNone(series)


class TestWileytoREFs(unittest.TestCase):

    def test_init(self):
        """ test init """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.wiley2.xml')
        references = WILEYtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_wiley)

    def test_process_and_dispatch_exception(self):
        """ test exception in process_and_dispatch """

        # data for raw references
        raw_references = [{
            'bibcode': '0000TEST..........Z',
            'block_references': ['invalid reference'],
            'item_nums': []
        }]

        with patch('adsrefpipe.refparsers.WileyXML.WILEYreference', side_effect=ReferenceError("ReferenceError")):
            with patch('adsrefpipe.refparsers.WileyXML.logger') as mock_logger:
                torefs = WILEYtoREFs(filename='testfile.xml', buffer={})
                torefs.raw_references = raw_references
                result = torefs.process_and_dispatch()

                mock_logger.error.assert_called_with("WILEYxml: error parsing reference: ReferenceError")
                self.assertEqual(result, [{'bibcode': '0000TEST..........Z', 'references': []}])


if __name__ == '__main__':
    unittest.main()