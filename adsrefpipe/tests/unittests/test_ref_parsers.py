import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import json
import re

from adsrefpipe.tests.unittests.stubdata import parsed_references
from adsrefpipe.refparsers.arXivTXT import ARXIVtoREFs

from adsrefpipe.refparsers.handler import verify
from adsrefpipe.refparsers.unicode import tostr, UnicodeHandler, UnicodeHandlerError


class TestReferenceParsers(unittest.TestCase):

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
            with patch('requests.get') as get_mock:
                get_mock.return_value = mock_response = Mock()
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

    ##### unicode module's unittests #####

    def test_unicode_tostr_exception(self):
        """ test unicode's tostr when ValueError is raised """
        mock_value = Mock(spec=str)
        mock_value.encode.side_effect = ValueError("Encoding error")
        self.assertEqual(tostr(mock_value), "")

    def test_unicode_handler_init_exception(self):
        """ test UnicodeHandler's init when ValueError is raised """

        # Invalid code (not an int)
        mock_data = 'invalid_entry "entity" "ascii" "latex"\n'
        with patch("builtins.open", mock_open(read_data=mock_data)):
            handler = UnicodeHandler("dummy_path")
            self.assertNotIn("entity", handler)

    def test_unicode_handler_sub_numasc_entity_exception(self):
        """ test UnicodeHandler's __sub_numasc_entity when IndexError and OverflowError are raised """

        # mock file reading to prevent FileNotFoundError
        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        handler.unicode = MagicMock()
        handler.unicode.__getitem__.side_effect = IndexError  # Simulate IndexError

        # test IndexError handling (falls back to unicodedata.normalize)
        match = re.match(r'&#(?P<number>\d+);', "&#999999;")
        if match:
            with patch("unicodedata.normalize", return_value="normalized_value"):
                self.assertEqual(handler._UnicodeHandler__sub_numasc_entity(match), "normalized_value")

        # test OverflowError handling (raises UnicodeHandlerError)
        match = re.match(r'&#(?P<number>\d+);', "&#9999999999;")
        if match:
            with patch("unicodedata.normalize", side_effect=OverflowError):
                with self.assertRaises(UnicodeHandlerError) as context:
                    handler._UnicodeHandler__sub_numasc_entity(match)
                self.assertEqual(str(context.exception), "Unknown numeric entity: &#9999999999;")

    def test_unicode_handler_sub_hexnumasc_entity(self):
        """ test UnicodeHandler's __sub_hexnumasc_entity method """

        # mock file reading to prevent FileNotFoundError
        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        # ensure no entry exists for entno so that the elif branch executes
        handler.unicode = [None] * 65536

        # hex for £ (163) to trigger the elif branch
        match = re.match(r'&#x(?P<hexnum>[0-9A-Fa-f]+);', "&#x00A3;")
        if match:
            # mock u2asc to return a known value
            with patch.object(handler, "u2asc", return_value="converted_ascii") as mock_u2asc:
                self.assertEqual(handler._UnicodeHandler__sub_hexnumasc_entity(match), "converted_ascii")
                # ensure u2asc is called with the correct character
                mock_u2asc.assert_called_once_with("£")

        # mock unicode lookup to raise IndexError
        handler.unicode = MagicMock()
        handler.unicode.__getitem__.side_effect = IndexError

        # large invalid hex value to trigger the IndexError exception
        match = re.match(r'&#x(?P<hexnum>[0-9A-Fa-f]+);', "&#x99999;")
        if match:
            # check that the correct exception is raised
            with self.assertRaises(UnicodeHandlerError) as context:
                handler._UnicodeHandler__sub_hexnumasc_entity(match)
            # ensure the exception message is correct
            self.assertEqual(str(context.exception), "Unknown hexadecimal entity: &#x99999;")

    def test_unicode_handler_sub_hexnum_toent(self):
        """ test UnicodeHandler's __sub_hexnum_toent method """

        # mock file reading to prevent FileNotFoundError
        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        # test ValueError exception, should return escaped unicode representation
        match = re.match(r'&#x(?P<number>[G-Z]+);', "&#xGHI;")
        if match:
            self.assertEqual(handler._UnicodeHandler__sub_hexnum_toent(match), r"\uGHI")
        # ensure unicode list is large enough and contains a valid entity
        handler.unicode = [None] * 70000
        handler.unicode[163] = MagicMock(entity="pound")

        # test valid conversion to named entity
        match = re.match(r'&#x(?P<number>[0-9A-Fa-f]+);', "&#x00A3;")
        if match:
            self.assertEqual(handler._UnicodeHandler__sub_hexnum_toent(match), "&pound;")

        # test UnicodeHandlerError for unknown entity by ensuring index is in range but has no entity
        handler.unicode = MagicMock()
        handler.unicode.__getitem__.return_value = None
        match = re.match(r'&#x(?P<number>[0-9A-Fa-f]+);', "&#x99999;")
        if match:
            with self.assertRaises(UnicodeHandlerError) as context:
                handler._UnicodeHandler__sub_hexnum_toent(match)
            # ensure the exception message is correct
            self.assertEqual(str(context.exception), "Unknown hexadecimal entity: 629145")

    def test_unicode_handler_toentity(self):
        """ test UnicodeHandler's __toentity method """

        # mock file reading to prevent FileNotFoundError
        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        # ensure unicode list is large enough
        handler.unicode = [None] * 70000

        # mock a named entity for character £ (ascii_value 163)
        handler.unicode[163] = Mock(entity="pound")

        # test named entity conversion
        self.assertEqual(handler._UnicodeHandler__toentity("£"), "&pound;")

        # test numeric entity conversion when no named entity exists
        # Ʃ (mathematical summation, ascii_value 425)
        self.assertEqual(handler._UnicodeHandler__toentity("Ʃ"), "&#425;")

    def test_unicode_handler_cleanall(self):
        """ test UnicodeHandler's cleanall method """

        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        # mock regex substitutions for unrelated operations
        handler.re_accent = Mock(sub=lambda func, text: text)
        handler.re_missent = Mock(sub=lambda func, text: text)
        handler.re_morenum = Mock(sub=lambda func, text: text)
        handler.re_rsquo = Mock(sub=lambda repl, text: text)

        # mock re_backslash and re_lower_upper_ls substitutions to test cleanslash is true
        handler.re_backslash = Mock(sub=lambda repl, text: text.replace("\\", ""))
        handler.re_lower_upper_ls = Mock(sub=lambda repl, text: text.replace("l/a", "&lstrok;a"))
        input_text = "l/a and back\\slash"
        expected_output = "&lstrok;a and backslash"
        self.assertEqual(handler.cleanall(input_text, cleanslash=1), expected_output)

    def test_unicode_handler_sub_accent(self):
        """ test UnicodeHandler's __sub_accent method """

        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        # correct mapping: accent symbols -> entity suffixes
        handler.accents = {"`": "grave", "'": "acute", "^": "circ"}
        # create a mock match object
        match = Mock()
        match.group.side_effect = lambda x: "e" if x == 1 else "`"

        self.assertEqual(handler._UnicodeHandler__sub_accent(match), "&egrave;")

    def test_unicode_handler_sub_missent(self):
        """ test UnicodeHandler's __sub_missent method """

        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        # mock missent dictionary with correct mapping
        handler.missent = {"b4": "acute", "caron": "scaron"}
        # mock keys() method to simulate known and unknown entities
        handler.keys = Mock(side_effect=lambda: {"sacute", "egrave"})
        # create a mock match object for an entity that exists in keys()
        match_existing = Mock()
        match_existing.group.side_effect = lambda x: "s" if x == 1 else "b4"

        # test correction when entity exists
        self.assertEqual(handler._UnicodeHandler__sub_missent(match_existing), "&sacute;")

        # create a mock match object for an entity that does not exist in keys()
        match_non_existing = Mock()
        match_non_existing.group.side_effect = lambda x: "e" if x == 1 else "caron"

        # test correction when entity does not exist
        self.assertEqual(handler._UnicodeHandler__sub_missent(match_non_existing), "e&scaron;")

    def test_unicode_handler_sub_morenum(self):
        """ test UnicodeHandler's __sub_morenum method """

        with patch("builtins.open", mock_open(read_data="")):
            handler = UnicodeHandler("dummy_path")

        # mock morenum dictionary with a valid numeric entity mapping
        handler.morenum = {"34": "quot", "169": "copy"}
        # create a mock match object for a valid numeric entity
        match_valid = Mock()
        match_valid.group.side_effect = lambda x: "34" if x == 1 else None

        # test valid numeric entity conversion
        self.assertEqual(handler._UnicodeHandler__sub_morenum(match_valid), "&quot;")

        # create a mock match object for an unknown numeric entity
        match_invalid = Mock()
        match_invalid.group.side_effect = lambda x: "9999" if x == 1 else None

        # test KeyError handling (should raise KeyError)
        with self.assertRaises(KeyError):
            handler._UnicodeHandler__sub_morenum(match_invalid)


if __name__ == '__main__':
    unittest.main()
