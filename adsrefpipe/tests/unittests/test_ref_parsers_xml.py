import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest

from adsrefpipe.tests.unittests.stubdata import parsed_references
from adsrefpipe.refparsers.AASxml import AAStoREFs
from adsrefpipe.refparsers.AGUxml import AGUtoREFs, AGUreference
from adsrefpipe.refparsers.APSxml import APStoREFs
from adsrefpipe.refparsers.AnAxml import AnAtoREFs
from adsrefpipe.refparsers.AIPxml import AIPtoREFs
from adsrefpipe.refparsers.BlackwellXML import BLACKWELLtoREFs
from adsrefpipe.refparsers.CrossRefXML import CrossRefToREFs
from adsrefpipe.refparsers.CUPxml import CUPtoREFs
from adsrefpipe.refparsers.EDPxml import EDPtoREFs
from adsrefpipe.refparsers.EGUxml import EGUtoREFs
from adsrefpipe.refparsers.ElsevierXML import ELSEVIERtoREFs
from adsrefpipe.refparsers.IcarusXML import ICARUStoREFs
from adsrefpipe.refparsers.IOPFTxml import IOPFTtoREFs
from adsrefpipe.refparsers.IOPxml import IOPtoREFs
from adsrefpipe.refparsers.IPAPxml import IPAPtoREFs
from adsrefpipe.refparsers.JATSxml import JATStoREFs
from adsrefpipe.refparsers.JSTAGExml import JSTAGEtoREFs
from adsrefpipe.refparsers.LivingReviewsXML import LivingReviewsToREFs
from adsrefpipe.refparsers.MDPIxml import MDPItoREFs
from adsrefpipe.refparsers.NLM3xml import NLMtoREFs
from adsrefpipe.refparsers.NatureXML import NATUREtoREFs
from adsrefpipe.refparsers.ONCPxml import ONCPtoREFs
from adsrefpipe.refparsers.OUPxml import OUPtoREFs
from adsrefpipe.refparsers.PASAxml import PASAtoREFs
from adsrefpipe.refparsers.RSCxml import RSCtoREFs
from adsrefpipe.refparsers.SpringerXML import SPRINGERtoREFs
from adsrefpipe.refparsers.SPIExml import SPIEtoREFs
from adsrefpipe.refparsers.UCPxml import UCPtoREFs
from adsrefpipe.refparsers.VERSITAxml import VERSITAtoREFs
from adsrefpipe.refparsers.WileyXML import WILEYtoREFs

class TestReferenceParsersXML(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def test_aasxml_parser(self):
        """ test parser for anaxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aas.raw')
        references = AAStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_aas)

    def test_aguxml_parser(self):
        """ test parser for aguxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.agu.xml')
        references = AGUtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_agu)

    def test_aipxml_parser(self):
        """ test parser for aipxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aip.xml')
        references = AIPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_aip)

    def test_anaxml_parser(self):
        """ test parser for anaxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.ana.xml')
        references = AnAtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_ana)

    def test_apsxml_parser(self):
        """ test parser for apsxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.aps.xml')
        references = APStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_aps)

    def test_blackwellxml_parser(self):
        """ test parser for blackwellxml """
        testing = [
            ('/stubdata/test.blackwell.xml', parsed_references.parsed_blackwell),
            ('/stubdata/test.mnras.xml', parsed_references.parsed_mnras),
        ]
        for (file, expected) in testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + file)
            references = BLACKWELLtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected)

    def test_crossrefxml_parser(self):
        """ test parser for crossrefxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.xref.xml')
        references = CrossRefToREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_crossref)

    def test_cupxml_parser(self):
        """ test parser for cupxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.cup.xml')
        references = CUPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_cup)

    def test_edpxml_parser(self):
        """ test parser for edpxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.edp.xml')
        references = EDPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_edp)

    def test_eguxml_parser(self):
        """ test parser for eguxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.egu.xml')
        references = EGUtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_egu)

    def test_elsevierxml_parser(self):
        """ test parser for elsevierxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.elsevier.xml')
        references = ELSEVIERtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_elsevier)

    def test_icarusxml_parser(self):
        """ test parser for icarusxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.icarus.raw')
        references = ICARUStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_icarus)

    def test_iopftxml_parser(self):
        """ test parser for iopftxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.iopft.xml')
        references = IOPFTtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_iopft)

    def test_iopxml_parser(self):
        """ test parser for iopxml """
        testing = [
            ('/stubdata/test.iop.xml', parsed_references.parsed_iop),
            ('/stubdata/test.edporiop.xml', parsed_references.parsed_edporiop),
        ]
        for (file, expected) in testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + file)
            references = IOPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected)

    def test_ipapxml_parser(self):
        """ test parser for ipapxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.ipap.xml')
        references = IPAPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_ipap)

    def test_jatsxml_parser(self):
        """ test parser for jatsxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.jats.xml')
        references = JATStoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_jats)

    def test_jstxml_parser(self):
        """ test parser for jstxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.jst.xml')
        references = JSTAGEtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_jst)

    def test_livingreviewsxml_parser(self):
        """ test parser for livingreviewsxml """
        xml_testing = [
            ('/stubdata/lrr-2014-6.living.xml', parsed_references.parsed_livingreviews_llr),
            ('/stubdata/lrsp-2007-2.living.xml', parsed_references.parsed_livingreviews_lrsp)
        ]
        for (filename, expected_results) in xml_testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + filename)
            references = LivingReviewsToREFs(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected_results)

    def test_mdpixml_parser(self):
        """ test parser for mdpixml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.mdpi.xml')
        references = MDPItoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_mdpi)

    def test_nlm3xml_parser(self):
        """ test parser for nlm3xml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.nlm3.xml')
        references = NLMtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_nlm3)

    def test_naturexml_parser(self):
        """ test parser for naturexml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.nature.xml')
        references = NATUREtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_nature)

    def test_oncpxml_parser(self):
        """ test parser for oncpxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.meta.xml')
        references = ONCPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_oncp)

    def test_oupxml_parser(self):
        """ test parser for oupxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.oup.xml')
        references = OUPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_oup)

    def test_pasaxml_parser(self):
        """ test parser for pasaxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.pasa.xml')
        references = PASAtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_pasa)

    def test_rscxml_parser(self):
        """ test parser for rscxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.rsc.xml')
        references = RSCtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_rsc)

    def test_spiexml_parser(self):
        """ test parser for spiexml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.spie.xml')
        references = SPIEtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_spie)

    def test_springerxml_parser(self):
        """ test parser for springerxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.springer.xml')
        references = SPRINGERtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_springer)

    def test_ucpxml_parser(self):
        """ test parser for ucpxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.ucp.xml')
        references = UCPtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_ucp)

    def test_versitaxml_parser(self):
        """ test parser for wileyxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.versita.xml')
        references = VERSITAtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_versita)

    def test_wileyxml_parser(self):
        """ test parser for wileyxml """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/test.wiley2.xml')
        references = WILEYtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_wiley)

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