import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest
import mock
import json

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

from adsrefpipe.refparsers.arXivTXT import ARXIVtoREFs

from adsrefpipe.refparsers.reference import Reference, ReferenceError, XMLreference
from adsrefpipe.refparsers.handler import verify
from adsrefpipe.utils import get_bibcode, verify_bibcode, get_resolved_references


class TestReferenceParsers(unittest.TestCase):

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

    def test_reference_init(self):
        """ test Reference class init """
        with self.assertRaises(Exception) as context:
            Reference({'authors': "Pipeline, R", 'jrlstr': "For Testing", 'year': 2020}).parse()
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

    def test_html_parser(self):
        """ test parsers for html references """
        html_testing = [
            (verify('AnAhtml'), '/stubdata/html/A+A/0/0000A&A.....0.....Z.ref.txt', parsed_references.parsed_AnA_html, ''),
            (verify('AnAShtml'), '/stubdata/html/A+AS/0/0000A&AS....0.....Z.ref.txt', parsed_references.parsed_AnAS_html, ''),
            (verify('AEdRvHTML'), '/stubdata/html/AEdRv/0/0000.html', parsed_references.parsed_AEdRv_html, ''),
            (verify('AnRFMhtml'), '/stubdata/html/AnRFM/0/annurev.fluid.00.html', parsed_references.parsed_AnRFM_html, '0000AnRFM...0.....Z'),
            (verify('ARAnAhtml'), '/stubdata/html/ARA+A/0/annurev.astro.00.html', parsed_references.parsed_ARAnA_html, '0000ARA&A...0.....Z'),
            (verify('AREPShtml'), '/stubdata/html/AREPS/0/0000AREPS...0.....Z.refs.html', parsed_references.parsed_AREPS_html_1, ''),
            (verify('AREPShtml'), '/stubdata/html/AREPS/0/annurev.earth.00.html', parsed_references.parsed_AREPS_html_2, '0001AREPS...0.....Z'),
            (verify('JLVEnHTML'), '/stubdata/html/JLVEn/0/0000JLVEn...0.....Z.raw', parsed_references.parsed_JLVEn, ''),
            (verify('PASJhtml'), '/stubdata/html/PASJ/0/iss0.raw', parsed_references.parsed_PASJ, ''),
            (verify('PASPhtml'), '/stubdata/html/PASP/0/iss0.raw', parsed_references.parsed_PASP, ''),
        ]
        annrev_response = {
          "responseHeader":{"status":0, "QTime":1, "params":{"q":"doi:\"10.1146/annurev\"", "fl":"bibcode", "start":"0", "rows":"10", "wt":"json"}},
          "response":{"numFound":1,"start":0,"docs":[{ "bibcode":""}]}
        }
        for (parser, filename, expected_results, bibcode) in html_testing:
            with mock.patch('requests.get') as get_mock:
                get_mock.return_value = mock_response = mock.Mock()
                mock_response.status_code = 200
                annrev_response['response']['docs'][0]['bibcode'] = bibcode
                mock_response.text = json.dumps(annrev_response)

                reference_source = os.path.abspath(os.path.dirname(__file__) + filename)
                references = parser(filename=reference_source, buffer=None).process_and_dispatch()
                self.assertEqual(references, expected_results)

    def test_ocr_parser(self):
        """ test parsers for ocr references """
        ocr_testing = [
            (verify('ObsOCR'), '/stubdata/ocr/Obs/0/0000ObsTEST.0.....Z.ref.ocr.txt', parsed_references.parsed_obs_ocr),
            (verify('ADSocr'), '/stubdata/ocr/ADS/0/0000ADSTEST.0.....Z.ref.ocr.txt', parsed_references.parsed_ads_ocr_0),
            (verify('ADSocr'), '/stubdata/ocr/ADS/0/0001ADSTEST.0.....Z.ref.ocr.txt', parsed_references.parsed_ads_ocr_1),
            (verify('ADSocr'), '/stubdata/ocr/ADS/0/0002ADSTEST.0.....Z.ref.ocr.txt', parsed_references.parsed_ads_ocr_2),
            (verify('ADSocr'), '/stubdata/ocr/ADS/0/0003ADSTEST.0.....Z.ref.ocr.txt', parsed_references.parsed_ads_ocr_3),
            (verify('ADSocr'), '/stubdata/ocr/ADS/0/0004ADSTEST.0.....Z.ref.ocr.txt', parsed_references.parsed_ads_ocr_4),
        ]
        for (parser, filename, expected_results) in ocr_testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + filename)
            references = parser(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected_results)

    def test_tex_parser(self):
        """ test the parser for latex references """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/tex/ADS/0/iss0.tex')
        references = verify('ADStex')(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_ads_tex)

    def test_txt_parser(self):
        """ test parsers for text references """
        txt_testing = [
            (verify('ThreeBibsTxt'), '/stubdata/txt/ARA+A/0/0000ADSTEST.0.....Z.ref.raw', parsed_references.parsed_ARAnA_txt_0),
            (verify('ThreeBibsTxt'), '/stubdata/txt/ARA+A/0/0001ARA+A...0.....Z.ref.refs', parsed_references.parsed_ARAnA_txt_1),
            (verify('ThreeBibsTxt'), '/stubdata/txt/ARNPS/0/0000ADSTEST.0.....Z.ref.raw', parsed_references.parsed_ARNPS_txt_0),
            (verify('ThreeBibsTxt'), '/stubdata/txt/ARNPS/0/0001ARNPS...0.....Z.ref.txt', parsed_references.parsed_ARNPS_txt_1),
            (verify('ThreeBibsTxt'), '/stubdata/txt/AnRFM/0/0000ADSTEST.0.....Z.ref.raw', parsed_references.parsed_AnRFM_txt_0),
            (verify('ThreeBibsTxt'), '/stubdata/txt/AnRFM/0/0001AnRFM...0.....Z.ref.txt', parsed_references.parsed_AnRFM_txt_1),
            (verify('PThPhTXT'), '/stubdata/txt/PThPh/0/iss0.raw', parsed_references.parsed_PThPh_txt),
            (verify('PThPhTXT'), '/stubdata/txt/PThPS/0/editor.raw', parsed_references.parsed_PThPS_txt),
            (verify('ADStxt'), '/stubdata/txt/ADS/0/0000ADSTEST.0.....Z.raw', parsed_references.parsed_ads_txt),
            (verify('PairsTXT'), '/stubdata/txt/AUTHOR/0/0000.pairs', parsed_references.parsed_pairs_txt_0),
            (verify('PairsTXT'), '/stubdata/txt/ATel/0/0000.pairs', parsed_references.parsed_pairs_txt_1),

        ]
        for (parser, filename, expected_results) in txt_testing:
            reference_source = os.path.abspath(os.path.dirname(__file__) + filename)
            references = parser(filename=reference_source, buffer=None).process_and_dispatch()
            self.assertEqual(references, expected_results)

    def test_arxivtxt_parser(self):
        """ test parser for arxivtxt """
        reference_source = os.path.abspath(os.path.dirname(__file__) + '/stubdata/txt/arXiv/0/00000.raw')
        references = ARXIVtoREFs(filename=reference_source, buffer=None).process_and_dispatch()
        self.assertEqual(references, parsed_references.parsed_arxiv)

    def test_get_bibcode(self):
        """ some reference files provide doi, and bibcode needs to be infered from doi """
        return_value = {
            u'responseHeader': {u'status': 0, u'QTime': 13},
            u'response': {
                u'start': 0,
                u'numFound': 1,
                u'docs': [{u'bibcode': u'2023arXiv230317899C'}]
            }
        }
        with mock.patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.text = json.dumps(return_value)
            bibcode = get_bibcode(doi='10.48550/arXiv.2303.17899')
            self.assertEqual(bibcode, '2023arXiv230317899C')

    def test_get_bibcode_error(self):
        """ some reference files provide doi, and bibcode needs to be infered from doi when solr returns error"""
        with mock.patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = mock.Mock()
            mock_response.status_code = 502
            bibcode = get_bibcode(doi='10.48550/arXiv.2303.17899')
            self.assertEqual(bibcode, None)

    def test_verify_bibcode(self):
        """ test calling solr to verify a bibcode """
        return_value = {
            u'responseHeader': {u'status': 0, u'QTime': 13},
            u'response': {
                u'start': 0,
                u'numFound': 1,
                u'docs': [{u'bibcode': u'2023arXiv230317899C'}]
            }
        }
        with mock.patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.text = json.dumps(return_value)
            bibcode = verify_bibcode(bibcode='2023arXiv230317899C')
            self.assertEqual(bibcode, '2023arXiv230317899C')

    def test_verify_bibcode_error(self):
        """ test calling solr to verify a bibcode when error is returned """
        with mock.patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = mock.Mock()
            mock_response.status_code = 502
            bibcode = verify_bibcode(bibcode='2023arXiv230317899C')
            self.assertEqual(bibcode, None)

    def test_get_resolved_references_error(self):
        """ test calling get_resolved_references with wrong end point """
        references = [{'item_num': 2,
                       'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                       'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]
        self.assertEqual(get_resolved_references(references, 'wrong_url'), None)

        with mock.patch('requests.post') as get_mock:
            get_mock.return_value = mock_response = mock.Mock()
            mock_response.status_code = 502
            self.assertEqual(get_resolved_references(references, 'xml'), None)


if __name__ == '__main__':
    unittest.main()