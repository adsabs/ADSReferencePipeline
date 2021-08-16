import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest

from adsrefpipe.tests.unittests.stubdata import parsed_references
from adsrefpipe.xmlparsers.CrossRefXML import CrossReftoREFs
from adsrefpipe.xmlparsers.ElsevierXML import ELSEVIERtoREFs
from adsrefpipe.xmlparsers.JATSxml import JATStoREFs
from adsrefpipe.xmlparsers.IOPxml import IOPtoREFs
from adsrefpipe.xmlparsers.SpringerXML import SPRINGERtoREFs
from adsrefpipe.xmlparsers.APSxml import APStoREFs
from adsrefpipe.xmlparsers.NatureXML import NATUREtoREFs
from adsrefpipe.xmlparsers.AIPxml import AIPtoREFs
from adsrefpipe.xmlparsers.WileyXML import WILEYtoREFs
from adsrefpipe.xmlparsers.NLM3xml import NLMtoREFs
from adsrefpipe.xmlparsers.AGUxml import AGUtoREFs, AGUreference

from adsrefpipe.xmlparsers.reference import Reference, ReferenceError, XMLreference


class TestXMLParsers(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def test_crossrefxml_parser(self):
        """ test parser for crossrefxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.ref.xml')
        references = CrossReftoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_crossref)

    def test_elsevierxml_parser(self):
        """ test parser for elsevierxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.elsevier.xml')
        references = ELSEVIERtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_elsevier)

    def test_jatsxml_parser(self):
        """ test parser for jatsxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.jats.xml')
        references = JATStoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_jats)

    def test_iopxml_parser(self):
        """ test parser for iopxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.iop.xml')
        references = IOPtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_iop)

    def test_springerxml_parser(self):
        """ test parser for springerxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.springer.xml')
        references = SPRINGERtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_springer)

    def test_apsxml_parser(self):
        """ test parser for apsxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aps.xml')
        references = APStoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_aps)

    def test_naturexml_parser(self):
        """ test parser for naturexml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.nature.xml')
        references = NATUREtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_nature)

    def test_aipxml_parser(self):
        """ test parser for aipxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aip.xml')
        references = AIPtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_aip)

    def test_wileyxml_parser(self):
        """ test parser for wileyxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.wiley2.xml')
        references = WILEYtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_wiley)

    def test_nlm3xml_parser(self):
        """ test parser for nlm3xml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.nlm3.xml')
        references = NLMtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_nlm3)

    def test_aguxml_parser(self):
        """ test parser for aguxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.agu.xml')
        references = AGUtoREFs(filename=reference_source)
        self.assertEqual(references, parsed_references.parsed_agu)

    def test_reference_init(self):
        """ test Reference class init """
        with self.assertRaises(Exception) as context:
            Reference({'authors': "Pipeline, R", 'jrlstr': "For Testing", 'year': 2020})
        self.assertEqual('Parse method not defined.', str(context.exception))

    def test_reference_pages(self):
        """ test calling parse pages method of reference class"""
        reference = AGUreference('<empty/>')
        self.assertEqual(reference.parse_pages(None), ('', None))
        self.assertEqual(reference.parse_pages('L23'), ('23', 'L'))
        self.assertEqual(reference.parse_pages('T2', ignore='RSTU'), ('2', None))
        self.assertEqual(reference.parse_pages('T2', letters='RSTU'), ('2', 'T'))
        self.assertEqual(reference.parse_pages('23S'), ('23', 'S'))
        self.assertEqual(reference.parse_pages('S23'), ('23', 'S'))

    def test_reference_url(self):
        """ test calling url decode method of XMLreference"""
        reference = AGUreference('<empty/>')
        self.assertEqual(reference.url_decode('%AF'), '¯')


if __name__ == '__main__':
    unittest.main()