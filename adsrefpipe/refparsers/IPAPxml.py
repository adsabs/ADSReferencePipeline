
import sys, os
import regex as re
import argparse

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs
from adsrefpipe.refparsers.reference import unicode_handler

class IPAPreference(XMLreference):

    re_xml_to_text = re.compile(r'<([A-Za-z_]*)\b[^>]*>(?P<ref_str>.*?)</\1>')

    def parse(self):
        """

        :return:
        """
        self.parsed = 0

        match = self.re_xml_to_text.search(self.reference_str.toxml())
        if match:
            self['refplaintext'] = match.group('ref_str').strip()

        self.parsed = 1

class IPAPtoREFs(XMLtoREFs):

    re_parse_line = re.compile(r'(?i)<BibUnstructured.*?>(?P<citation>.*?)</BibUnstructured>')
    citation_format = '<BibUnstructured>%s</BibUnstructured>'

    re_ref_inline = re.compile(r'(?P<authors>.*?):(?P<journal>.+)\((?P<year>\d{4})[a-zA-Z]?\)(?P<rest>.*)')
    re_match_years = re.compile(r'\(\d{4}\)')


    def __init__(self, filename, buffer):
        """

        :param filename:
        :param buffer:
        :param unicode:
        :param tag:
        """
        XMLtoREFs.__init__(self, filename, buffer, parsername=IPAPtoREFs, tag='BibUnstructured')

    def get_references(self, filename, encoding="utf8"):
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

        :param filename:
        :param encoding:
        :return:
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

    def cleanup(self, reference):
        """

        :param reference:
        :return:
        """
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

            cleaned_references = []
            for ref in citations:
                # reformat string so that it is in our canonical form:
                # Authors year journal rest
                if not ref:
                    continue
                match = self.re_ref_inline.match(ref)
                if match:
                    tagged_reference = self.citation_format%(match.group('authors') + ' ' + match.group('year') + ' ' + match.group('journal') + ' ' + match.group('rest'))
                    cleaned_references.append(tagged_reference)
                else:
                    logger.error("IPAPxml: reference string does not match expected syntax: %s" %ref)

            return cleaned_references

    def process_and_dispatch(self):
        """

        :return:
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']

            parsed_references = []
            for raw_reference in block_references:
                cleaned_references = self.cleanup(raw_reference)

                logger.debug("IPAPxml: parsing %s" % cleaned_references)
                try:
                    for reference in cleaned_references:
                        ipap_reference = IPAPreference(reference)
                        parsed_references.append({**ipap_reference.get_parsed_reference(), 'refraw': raw_reference})
                except ReferenceError as error_desc:
                    logger.error("IPAPxml: error parsing reference: %s" %error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


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
