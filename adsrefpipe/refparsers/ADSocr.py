
import sys, os
import argparse
import re
from typing import List, Dict

from adsputils import setup_logging, load_config
logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.toREFs import OCRtoREFs
from adsrefpipe.refparsers.reference import unicode_handler


class ADSocrToREFs(OCRtoREFs):
    """
    Class for handling OCR to references conversion for ADS documents.
    """

    def __init__(self, filename: str, buffer: str, parsername: str = None):
        """
        initialize the ADSocrToREFs parser

        :param filename: path to the source file
        :param buffer: the latex references in buffer form
        :param parsername: the name of the parser (optional)
        """
        if not parsername:
            parsername = ADSocrToREFs
        OCRtoREFs.__init__(self, filename, buffer, parsername)

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and parsing, then dispatch the parsed references

        :return: a list of dictionaries containing bibcodes and parsed references
        """
        # method implementation
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, reference in enumerate(block_references):
                reference = unicode_handler.ent2asc(reference)
                logger.debug("ADSocr: parsing %s" % reference)
                parsed_references.append(self.merge({'refstr': reference, 'refraw': reference}, self.any_item_num(item_nums, i)))

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references

class ObsOCRtoREFs(ADSocrToREFs):
    """
    Class for handling OCR to references conversion for OBS documents.
    """

    # all punctuation characters used in reference parsing
    punctuations = r'!\"#\$%&\'\(\)\*\+,-\./:;<=>\?@\[\]\^_`{\|}~\\'
    # to match enumerations in references
    enumeration = r'^\s*[%s]*[\dOoIiSsta]{1,3}[a-z]{0,1}[%s\s]+' % (punctuations, punctuations)
    # to look ahead for patterns in enumerations
    enumeration_lookahead = r'(?=.*[A-Z]{1}[\.\s]+)(?=.*[12]\d\d\d[a-z]*)?'
    # to match the start of a reference based on enumeration
    re_reference_start = re.compile(r'(%s)%s' % (enumeration, enumeration_lookahead))
    # to match enumeration from reference lines
    re_remove_enumeration = re.compile(r'%s%s' % (enumeration, enumeration_lookahead))

    def __init__(self, filename: str, buffer: str):
        """
        initialize the ObsOCRtoREFs parser

        :param filename: path to the source file
        :param buffer: the latex references in buffer form
        """
        ADSocrToREFs.__init__(self  , filename, buffer, parsername=ObsOCRtoREFs)

    def get_references(self, filename: str, encoding: str = "ISO-8859-1") -> List[Dict[str, List[str]]]:
        """
        read and extract references from the specified file

        :param filename: path to the reference file
        :param encoding: encoding used for reading the file (default is ISO-8859-1)
        :return: list of references found in the file
        """
        try:
            references = []

            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                reader = f.readlines()

                bibcode = None

                match = self.re_bibcode.match(os.path.basename(filename))
                if match:
                    bibcode = match.group(1)

                    block_references = []
                    prev_reference = ''
                    reference = ''
                    for i, line in enumerate(reader):
                        if not line.strip():
                            continue
                        if self.re_reference_start.search(line):
                            # add previous reference if any, since current line is the start of reference
                            if reference:
                                block_references, reference, prev_reference = self.verify_accept(block_references, reference, prev_reference)
                            # remove the enumeration first
                            line = list(filter(None, self.re_remove_enumeration.split(line)))[0]
                            # now start the new multi-line reference
                            reference = line.replace('\n', ' ').replace('\r', ' ').strip()
                        else:
                            # now continue with the multi-line reference
                            reference += (' ' + line.replace('\n', ' ').replace('\r', ' ').strip()).strip()

                    # add the last multi-line reference here
                    if reference:
                        block_references, _, _ = self.verify_accept(block_references, reference, prev_reference)

                    if bibcode and block_references:
                        references.append([bibcode, block_references])
                else:
                    logger.error("Error in getting the bibcode from the reference file name %s. Skipping!" % (filename))

            if len(references) > 0:
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []


def toREFs(filename: str, buffer: str):      # pragma: no cover
    """
    this is a local function, called from main, for testing purposes

    :param filename:
    :param buffer:
    :return:
    """
    reference_type = filename.split('/')[-3]
    if reference_type == 'Obs':
        results = ObsOCRtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    else:
        results = ADSocrToREFs(filename=filename, buffer=buffer).process_and_dispatch()
    for result in results:
        print(result['bibcode'])
        for i, reference in enumerate(result['references']):
            print(i + 1, reference['refstr'])



# This is the main program used for manual testing and verification of OCR references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse latex references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='latex reference(s)')
    args = parser.parse_args()
    if args.filename:
        toREFs(args.filename, buffer=None)
    elif args.buffer:
        toREFs(filename=None, buffer=args.buffer)
    # if no reference source is provided, just run the test files
    elif not args.filename and not args.buffer:
        # testing the ocr references
        ocr_testing = [
            (ADSocrToREFs, '/../tests/unittests/stubdata/ocr/ADS/0/0000ADSTEST.0.....Z.ref.ocr.txt'),
            (ADSocrToREFs, '/../tests/unittests/stubdata/ocr/ADS/0/0001ADSTEST.0.....Z.ref.ocr.txt'),
            (ADSocrToREFs, '/../tests/unittests/stubdata/ocr/ADS/0/0002ADSTEST.0.....Z.ref.ocr.txt'),
            (ADSocrToREFs, '/../tests/unittests/stubdata/ocr/ADS/0/0003ADSTEST.0.....Z.ref.ocr.txt'),
            (ADSocrToREFs, '/../tests/unittests/stubdata/ocr/ADS/0/0004ADSTEST.0.....Z.ref.ocr.txt'),
            (ObsOCRtoREFs, '/../tests/unittests/stubdata/ocr/Obs/0/0000ObsTEST.0.....Z.ref.ocr.txt'),
        ]
        for (parser, filename) in ocr_testing:
            filename = os.path.abspath(os.path.dirname(__file__) + filename)
            compare = ''
            for i,one in enumerate(parser(filename=filename, buffer=None).process_and_dispatch()):
                compare += '---<%s>---\n'%one['bibcode']
                for ref in one['references']:
                    compare += '%s\n'%ref['refstr'].strip()
            with open(os.path.abspath(filename + '.result'), 'r', encoding='utf-8', errors='ignore') as f:
                from_file = f.read()
                if from_file == compare.strip():
                    print('Test `%s` passed!'%filename)
                else:
                    print('Test `%s` failed!'%filename)
    sys.exit(0)
