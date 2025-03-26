
import sys, os
import regex as re
import argparse
from typing import List, Dict

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.reference import XMLreference, ReferenceError
from adsrefpipe.refparsers.toREFs import XMLtoREFs, toREFs
from adsrefpipe.refparsers.unicode import tostr


class VERSITAreference(XMLreference):
    """
    This class handles parsing VERSITA references in XML format. It extracts citation information such as authors,
    year, journal, title, volume, pages, DOI, and eprint, and stores the parsed details.
    """

    # to match the raw reference string
    re_from_raw = re.compile(r'(?:<[^>]*>)(.*?)(?:<)')
    # to match the publication ID in the reference string
    re_match_id = re.compile(r'\sid\.\s+(\d{4})(.{2})')

    def parse(self):
        """
        parse the VERSITA reference

        :return:
        """
        self.parsed = 0

        refstr = self.xmlnode_nodecontents('mixed-citation')

        if self.xmlnode_nodecontents('element-citation'):
            authors = self.parse_authors()
            year = self.xmlnode_nodecontents('year')
            journal = self.xmlnode_nodecontents('source')
            title = self.xmlnode_nodecontents('article-title') or self.xmlnode_nodecontents('chapter-title')
            volume = self.xmlnode_nodecontents('volume')
            pages = self.xmlnode_nodecontents('fpage')

            doi = ''
            pub_id = self.xmlnode_nodecontents('pub-id')
            if pub_id:
                doi = self.match_doi(pub_id)
            if not doi:
                # attempt to extract doi from refstr
                doi = self.match_doi(refstr)
            eprint = self.match_arxiv_id(refstr)

            # these fields are already formatted the way we expect them
            self['authors'] = authors.strip()
            self['year'] = year.strip()
            self['jrlstr'] = journal.strip()
            self['ttlstr'] = title.strip()

            if not volume or not pages:
                if not refstr:
                    match = self.re_from_raw.findall(str(self))
                    if match:
                        the_ref = match[1].strip()
                else:
                    the_ref = refstr
                match = self.re_match_id.search(the_ref)
                if match:
                    if not volume:
                        volume = match.group(1)
                    if not pages:
                        pages = 'E' + match.group(2)

            self['volume'] = self.parse_volume(volume)
            self['page'], self['qualifier'] = self.parse_pages(pages)
            self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])

            if doi:
                self['doi'] = doi
            if eprint:
                self['eprint'] = eprint

        self['refstr'] = self.get_reference_str()
        if not self['refstr']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(refstr))

        self.parsed = 1

    def parse_authors(self) -> str:
        """
        parse the authors from the reference string and format them accordingly

        :return: a formatted string of authors
        """
        authors = self.xmlnode_nodescontents('person-group', attrs={'person-group-type': 'author'}, keepxml=1) or \
                  self.xmlnode_nodescontents('name', keepxml=1) or \
                  self.xmlnode_nodescontents('string-name', keepxml=1)

        collab = self.xmlnode_nodescontents('collab')

        if not authors or len(authors) == 0:
            # see if there are editors
            authors = self.xmlnode_nodescontents('person-group', attrs={'person-group-type': 'editor'}, keepxml=1)
            if (not authors or len(authors) == 0) and not collab:
                return ''

        author_list = []
        for author in authors:
            an_author = ''
            author, lastname = self.extract_tag(author, 'surname')
            author, givennames = self.extract_tag(author, 'given-names')
            if lastname: an_author = tostr(lastname)
            if an_author and givennames: an_author += ', ' + tostr(givennames)
            # some references have both first and last name in the givennames tag
            if not an_author and givennames: an_author = tostr(givennames)
            if an_author: author_list.append(an_author)

        if collab:
            author_list = collab + author_list

        authors = ", ".join(author_list)
        return authors


class VERSITAtoREFs(XMLtoREFs):
    """
    This class converts VERSITA XML references to a standardized reference format. It processes raw VERSITA references from
    either a file or a buffer and outputs parsed references, including bibcodes, authors, volume, pages, and DOI.
    """

    # to clean up XML blocks by removing certain tags
    block_cleanup = [
        (re.compile(r'</?uri.*?>'), ''),
        (re.compile(r'\(<comment>.*?</comment>\)'), ''),
        (re.compile(r'</?ext-link.*?>'), ''),
        (re.compile(r'\s+xlink:href='), ' href='),
        (re.compile(r'<inline-formula>.*?</inline-formula>'), ''),
        (re.compile(r'\smixed-citation>', flags=re.IGNORECASE), r'<mixed-citation>')
    ]
    # to clean up references by replacing certain patterns
    reference_cleanup = [
        (re.compile(r'</?ext-link.*?>'), ''),
        (re.compile(r'^(.*?):.*(<italic>.*)'), r'\1 \2'),
        (re.compile(r'</?italic>'), r''),
    ]
    # to clean up the start of the reference string
    re_clean_start = re.compile('(^.*\/title\>\s+)')

    def __init__(self, filename: str, buffer: str):
        """
        initialize the VERSITAtoREFs object to process VERSITA references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        # XMLtoREFs.__init__(self, filename, buffer, parsername=VERSITAtoREFs, tag='ref', cleanup=self.block_cleanup)

        # some of the reference files do not have the top level tag ref and hence are not being parsed by xml
        # duplicate XMLtoREFs.__init__ here to be able see if tag ref is not included, and added it in
        # also none of the references have a terminating top tag, so add them in too

        self.raw_references = []

        if buffer:
            self.filename = buffer['source_filename']
            self.parsername = buffer['parser_name']

            for buf in buffer['block_references']:
                block_references, item_nums = [[ref['refraw'] for ref in buf['references']], [ref['item_num'] for ref in buf['references']]]
                self.raw_references.append({'bibcode': buf['source_bibcode'], 'block_references': block_references, 'item_nums': item_nums})
        else:
            self.filename = filename
            self.parsername = VERSITAtoREFs

            pairs = self.get_references(filename=filename)
            for pair in pairs:
                bibcode = pair[0]
                buffer = self.re_clean_start.sub('', pair[1])

                if len(bibcode) != 19:
                    logger.error("Error in getting a bibcode along with the reference strings from reference file %s. Returned %s for bibcode. Skipping!" % (filename, bibcode))
                    continue

                buffer = list(filter(None, [ref.strip() for ref in buffer.split('\n')]))
                buffer = ' '.join(['%s </ref>'%ref if ref.startswith('<ref') else '<ref id="no id"> %s </ref>'%ref for ref in buffer])
                if self.block_cleanup:
                    for (compiled_re, replace_str) in self.block_cleanup:
                        buffer = compiled_re.sub(replace_str, buffer)

                block_references = self.get_xml_block(buffer, tag='ref')
                self.raw_references.append({'bibcode':bibcode, 'block_references':block_references})

    def cleanup(self, reference: str) -> str:
        """
        clean up the input reference by replacing specific patterns

        :param reference: the raw reference string to clean up
        :return: the cleaned reference string
        """
        for (compiled_re, replace_str) in self.reference_cleanup:
            reference = compiled_re.sub(replace_str, reference)
        return reference

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and then parse the references

        :return: list of dictionaries, each containing bibcodes and parsed references
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, raw_reference in enumerate(block_references):
                reference = self.cleanup(raw_reference)

                logger.debug("VERSITAxml: parsing %s" % reference)
                try:
                    versita_reference = VERSITAreference(reference)
                    parsed_references.append(self.merge({**versita_reference.get_parsed_reference(), 'refraw': raw_reference}, self.any_item_num(item_nums, i)))
                except ReferenceError as error_desc:
                    logger.error("VERSITAxml: error parsing reference: %s" % error_desc)

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of VERSOTAxml references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse VERSITA references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(VERSITAtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(VERSITAtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.versita.xml')
        result = VERSITAtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_versita:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
