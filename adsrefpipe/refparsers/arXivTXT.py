
import sys, os
import regex as re
import argparse
from typing import List, Dict

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.toREFs import TXTtoREFs
from adsrefpipe.refparsers.reference import unicode_handler


class ARXIVtoREFs(TXTtoREFs):
    """
    This class converts ARXIV text references to a standardized reference format. It processes raw ARXIV references from
    either a file or a buffer and outputs parsed references, including bibcodes and formatted reference strings.
    """

    # to clean up reference string issues (such as unwanted tags and characters)
    reference_cleanup_1 = [
        (re.compile(r'[\{\}]'), ''),
        (re.compile(r'\\(it|bf)'), ''),
        (re.compile(r'<A HREF=.*?>'), ''),
        (re.compile(r'</A>'), ''),
        (re.compile(r'&amp;'), r'&'),
        (re.compile(r'&nbsp;'), ' '),
        (re.compile('(&#65533;)+'), ''),
    ]
    # to clean up reference string issues (like formatting and specific terms)
    reference_cleanup_2 = [
        (re.compile(r'\ ApJ,'), r', ApJ,'),
        (re.compile(r'\ ApJS,'), r', ApJS,'),
        (re.compile(r'\ MNRAS,'), r', MNRAS,'),
        (re.compile(r'\ Nature,'), ', Nature,'),
        (re.compile(r'BIBLIOGRAPHY,\ +p\.\ +\d+\ +'), ''),
        (re.compile(r'^\d+\.\ '), ''),
        (re.compile(r'\|+\.'), r'---'),
        (re.compile(r'^[A-Z\ ]+\ \d+'), ''),
        (re.compile(r'^\s*\[\d*\]\s*'), ''),
        (re.compile(r'astroph'), r'astro-ph'),
        (re.compile(r'astro-\ ph'), r'astro-ph'),
        (re.compile(r'astro-ph/\s+'), r'astro-ph/'),
        (re.compile(r'"et al."'), r'et al.'),
    ]


    def __init__(self, filename: str, buffer: str):
        """
        initialize the ARXIVtoREFs object to process ARXIV references

        :param filename: the path to the source file
        :param buffer: the XML references as a buffer
        """
        TXTtoREFs.__init__(self, filename, buffer, ARXIVtoREFs)

    def cleanup(self, reference: str) -> str:
        """
        clean up the input reference by replacing specific patterns and applying unicode handling

        :param reference: the raw reference string to clean up
        :return: the cleaned reference string
        """
        for (compiled_re, replace_str) in self.reference_cleanup_1:
            reference = compiled_re.sub(replace_str, reference)
        reference = unicode_handler.ent2asc(reference)
        for (compiled_re, replace_str) in self.reference_cleanup_2:
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

                logger.debug("arXivTXT: parsing %s" % reference)
                parsed_references.append(self.merge({'refstr': reference, 'refraw': raw_reference}, self.any_item_num(item_nums, i)))

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


# This is the main program used for manual testing and verification of arXivTXT references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
from adsrefpipe.tests.unittests.stubdata import parsed_references
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse arXiv references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='text reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(ARXIVtoREFs(filename=args.filename, buffer=None).process_and_dispatch())
    elif args.buffer:
        print(ARXIVtoREFs(buffer=args.buffer, filename=None).process_and_dispatch())
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        filename = os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/txt/arXiv/0/00000.raw')
        result = ARXIVtoREFs(filename=filename, buffer=None).process_and_dispatch()
        if result == parsed_references.parsed_arxiv:
            print('Test passed!')
        else:
            print('Test failed!')
    sys.exit(0)
