
import sys, os
import regex as re
import argparse
from typing import List, Dict

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.reference import unicode_handler


class IPAPreference(XMLreference):
    """
    This class handles parsing IPAP references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match XML tags and extract reference strings
    re_xml_to_text = re.compile(r'<([A-Za-z_]*)\b[^>]*>(?P<ref_str>.*?)</\1>')

    def parse(self):
        """
        parse the IPAP reference and extract citation information such as authors, year, title, and DOI

        :return:
        """
        self.parsed = 0

        match = self.re_xml_to_text.search(self.reference_str.toxml())
        if match:
            self['refplaintext'] = match.group('ref_str').strip()

        self.parsed = 1

class IPAPtoREFs(XMLtoREFs):
    """
    This class converts IPAP XML references to a standardized reference format. It processes raw IPAP references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to match and extract citation data from BibUnstructured tags
    re_parse_line = re.compile(r'(?i)<BibUnstructured.*?>(?P<citation>.*?)</BibUnstructured>')
    citation_format = '<BibUnstructured>%s</BibUnstructured>'
    # to match author, journal, and year pattern in citations
    re_ref_inline = re.compile(r'(?P<authors>.*?):(?P<journal>.+)\((?P<year>\d{4})[a-zA-Z]?\)(?P<rest>.*)')
    # to match years in citation strings
    re_match_years = re.compile(r'\(\d{4}\)')

    def __init__(self, filename: str, buffer: str):
        """
        initialize the IPAPtoREFs object to process IPAP references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=IPAPtoREFs, tag='BibUnstructured')

    def get_references(self, filename: str, encoding: str = "utf8") -> List[Dict]:
        """
        *.ipap.xml source files are not true tagged xml files,
        so overwriting the generic read method to read this kind of files correctly

        this is a comment from the classic file, note that moving the year should not be neccesary
        for the new resolver, however, decided to keep the move:
        
        Extracts references from IPAP (*.ipap.xml) which are just
        plain strings wrapped by <BibUnstructured> tags.
        The only additional formatting we have to do is to move the
        year after the author list, e.g. going from this:
            N. Yu. Reshetikhin and V. G. Turaev: Commun. Math. Phys. 127 (1990) 1
        to this:
            N. Yu. Reshetikhin and V. G. Turaev 1990 Commun. Math. Phys. 127  1

        See, e.g. /proj/ads/references/sources/JPSJ/0077/iss3.ipap.xml

        returns an array of bibcode and reference text blobs
        parsed from the input file

        :param filename: the filename to read from
        :param encoding: the encoding to use for reading the file
        :return: list of parsed references
        """
        if filename:
            try:
                buffer = open(filename, encoding=encoding, errors='ignore').read()

                result = []

                match = self.re_format_xml.search(buffer)
                while match:
                    bibcode = match.group('bibcode')
                    block_start = match.end()

                    match = self.re_format_xml.search(buffer, block_start)
                    if match:
                        block_end = match.start()
                        block = buffer[block_start:block_end]
                    else:
                        block = buffer[block_start:]

                    result.append([bibcode, block])

                return result
            except Exception as error:
                logger.error("Unable to open file %s. Exception %s." % (filename, error))
                return []

    def cleanup(self, reference: str) -> List[str]:
        """
        cleans up and reformats a reference string into a canonical format

        :param reference: reference string to clean up
        :return: list of cleaned references
        """
        cleaned_references = []
        match = self.re_parse_line.search(reference.replace('\r', ' ').replace('\n', ' ').strip())
        if match:
            citation = unicode_handler.ent2asc(match.group('citation')).strip()

            # now try to figure out if this string contains the concatenation of
            # two or more references, which would be in the form:
            #    ref1; ref2; ref3
            # for example <BibUnstructured>G. M. Chernov et al.; Nucl. Phys. A <b>280</b> (1977) 478; D. Ghosh et al.: Nucl. Phys. A 468 (1987) 719.</BibUnstructured>
            # we do this by comparing how many times the year regexp appears
            # as opposed to how many instance of the separator character ';'
            # we find in the string (should be one more)
            years = self.re_match_years.split(citation)
            citations = [x.strip() for x in citation.split(';')]
            if len(years) != len(citations) + 1:
                # either one string or a mismatch: play it safe and
                # treat this as a single refstring
                citations = [citation]

            for ref in citations:
                if not ref:
                    continue
                match = self.re_ref_inline.match(ref)
                if match:
                    # reformat string so that it is in our canonical form:
                    # authors year journal rest
                    tagged_reference = self.citation_format%(match.group('authors') + ' ' + match.group('year') + ' ' + match.group('journal') + ' ' + match.group('rest'))
                    cleaned_references.append(tagged_reference)
                else:
                    logger.error("IPAPxml: reference string does not match expected syntax: %s" %ref)

        return cleaned_references

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and parsing, then dispatch the parsed references

        :return: a list of dictionaries containing bibcodes and parsed references
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, raw_reference in enumerate(block_references):
                cleaned_references = self.cleanup(raw_reference)

                logger.debug("IPAPxml: parsing %s" % cleaned_references)
                try:
                    for reference in cleaned_references:
                        ipap_reference = IPAPreference(reference)
                        parsed_references.append(self.merge({**ipap_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("IPAPxml: error parsing reference: %s" %error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of IPAPXML references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse IPAP references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(IPAPtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(IPAPtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.ipap.xml')
        result = IPAPtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_ipap:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
