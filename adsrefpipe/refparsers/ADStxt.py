
import sys, os
import regex as re
import argparse
from typing import List, Dict, Tuple

from adsputils import setup_logging, load_config

logger = setup_logging('refparsers')
config = {}
config.update(load_config())

from adsrefpipe.refparsers.toREFs import TXTtoREFs
from adsrefpipe.refparsers.reference import unicode_handler, LatexReference


class ADStxtToREFs(TXTtoREFs):
    """
    This class is responsible for parsing and processing text references in ADS format.
    """

    # some text references have latex code, remove them
    latex_reference = LatexReference("")

    def __init__(self, filename: str, buffer: str, parsername: str = None):
        """
        initialize the ADStxtToREFs object

        :param filename: the reference file to parse
        :param buffer: the content of the reference file
        :param parsername: optional name for the parser
        """
        if not parsername:
            parsername = ADStxtToREFs
        TXTtoREFs.__init__(self, filename, buffer, parsername)

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and call the parser

        :return: list of processed references
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, reference in enumerate(block_references):
                reference = self.latex_reference.cleanup(unicode_handler.ent2asc(reference))
                logger.debug("ADStxt: parsing %s" % reference)
                parsed_references.append(self.merge({'refstr': reference, 'refraw': reference}, self.any_item_num(item_nums, i)))

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references

class ARAnATXTtoREFs(ADStxtToREFs):
    """
    This class processes references from ARAnA format text files.
    """

    def __init__(self, filename: str, buffer: str):
        """
        initialize the ARAnATXTtoREFs object

        :param filename: the reference file to parse
        :param buffer: the content of the reference file
        """
        ADStxtToREFs.__init__(self, filename, buffer, parsername=ARAnATXTtoREFs)

    def get_references(self, filename: str, encoding: str = "ISO-8859-1") -> List[Dict]:
        """
        read reference file for ARAnA text format

        :param filename: the reference file to parse
        :param encoding: the file encoding to use
        :return: list of parsed references
        """
        try:
            references = []

            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                reader = f.readlines()
                for i in range(len((reader))):
                    for (compiled_re, replace_str) in self.block_cleanup:
                        reader[i] = compiled_re.sub(replace_str, reader[i])

                bibcode = None
                match = self.re_bibcode.match(os.path.basename(filename))
                if match:
                    bibcode = match.group(1)

                if bibcode:
                    is_enumerated = False
                    if len(reader) > 1:
                        if self.re_enumeration.search(reader[0]) or self.re_enumeration.search(reader[1]):
                            is_enumerated = True

                    block_references = []
                    prev_reference = ''
                    reference = ''
                    for i, line in enumerate(reader):
                        if line.startswith('%'):
                            continue
                        line = line.strip()
                        next_line = reader[i + 1] if (i + 1) < len(reader) else ''
                        if not line:
                            continue
                        reference, prev_reference, block_references = self.process_a_reference(is_enumerated, line, next_line, reference, prev_reference, block_references)
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

class PThPhTXTtoREFs(ADStxtToREFs):
    """
    This class processes references from PThPh text files, handling author inheritance and other formatting.
    """

    # to match "ibid." with optional preceding punctuation or whitespace
    re_author_list_placeholder = re.compile(r'(?:^|[;,.\s]+)ibid.')
    # to match the author list placeholder pattern "; ibid."
    author_list_placeholder_pattern = '; ibid.'
    # to match prior volume and year information in references
    re_prior_volume_year = re.compile(r'(.*)(?=[\s\.,]+\d+[\s\(]+[12]+\d\d\d[a-z]*)')

    def __init__(self, filename: str, buffer: str):
        """
        initialize the PThPhTXTtoREFs object

        :param filename: the reference file to parse
        :param buffer: the content of the reference file
        """
        ADStxtToREFs.__init__(self, filename, buffer, parsername=PThPhTXTtoREFs)

    def verify_accept(self, block_references: List[str], cur_reference: str, prev_reference: str) -> Tuple[List[str], str]:
        """
        verify and process a reference, fixing author inheritance if necessary

        :param block_references: the current block of references
        :param cur_reference: the current reference string
        :param prev_reference: the previous reference string
        :return: updated block references and previous reference
        """
        multi_reference = self.re_author_list_placeholder.split(self.cleanup(cur_reference))
        multi_reference = [multi_reference[0]] + [self.author_list_placeholder_pattern + single_reference for single_reference in multi_reference[1:]]
        for single_reference in multi_reference:
            if self.is_reference(single_reference):
                reference = self.fix_inheritance(single_reference, prev_reference)
                block_references.append(reference.strip())
                prev_reference = reference
        return block_references, prev_reference

    def get_references(self, filename: str, encoding: str = "ISO-8859-1") -> List[Dict]:
        """
        read reference file for PThPh text format

        :param filename: the reference file to parse
        :param encoding: the file encoding to use
        :return: list of parsed references
        """
        try:
            references = []
            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                reader = f.readlines()
                bibcode = None
                block_references = []
                prev_reference = ''
                ref_block = False
                for line in reader:
                    if not line.strip():
                        continue
                    elif line.startswith('%R'):
                        if bibcode and block_references:
                            references.append([bibcode, block_references])
                            block_references = []
                        bibcode = line.split('%R ')[1].strip()
                        ref_block = False
                    elif bibcode and line.startswith('%Z'):
                        ref_block = True
                        reference = re.sub(r'^\d+ ', '', line[3:])
                        block_references, prev_reference = self.verify_accept(block_references, reference, prev_reference)
                    elif ref_block:
                        reference = re.sub(r'^\d+ ', '', line)
                        block_references, prev_reference = self.verify_accept(block_references, reference, prev_reference)
                # last block
                if bibcode and block_references:
                    references.append([bibcode, block_references])
            if len(references) > 0:
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

    def fix_inheritance(self, cur_refstr: str, prev_refstr: str) -> str:
        """
        if author list is the same as the reference above it, for this publication the place holder
        is ibid followed by volume and then year, hence
        get the list of authors from the previous reference and add it to the current one

        :param cur_refstr: the current reference string
        :param prev_refstr: the previous reference string
        :return: updated reference string with inherited authors
        """
        match = self.re_author_list_placeholder.match(cur_refstr)
        if match and prev_refstr and len(prev_refstr) > 1:
            try:
                # find the volume/year and return everything that came before it
                prev_authors = self.re_prior_volume_year.match(prev_refstr)
                if prev_authors:
                    cur_refstr = prev_authors.group().strip() + " " + cur_refstr[match.end():].strip()
            except TypeError:
                pass
        return cur_refstr

class FlatTXTtoREFs(ADStxtToREFs):
    """
    This class processes references from a flat text format without block indicators.
    """

    def __init__(self, filename: str, buffer: str):
        """
        initialize the FlatTXTtoREFs object

        :param filename: the reference file to parse
        :param buffer: the content of the reference file
        """
        ADStxtToREFs.__init__(self, filename, buffer, parsername=FlatTXTtoREFs)

    def get_references(self, filename: str, encoding: str = "ISO-8859-1") -> List[Dict]:
        """
        read reference file for flat text format

        :param filename: the reference file to parse
        :param encoding: the file encoding to use
        :return: list of parsed references
        """
        try:
            references = []

            match = self.re_bibcode.match(os.path.basename(filename))
            if match:
                bibcode = match.group(1)

                with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                    reader = f.readlines()
                    block_references = []
                    prev_reference = ''
                    for line in reader:
                        if not line.strip():
                            continue
                        if self.is_reference(line):
                            reference = self.fix_inheritance(line, prev_reference)
                            block_references.append(reference.strip())
                            prev_reference = reference
                    references.append([bibcode, block_references])
            if len(references) > 0:
                logger.debug("Read source file %s, and got %d references to resolve for bibcode %s." % (filename, len(references), bibcode))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

class ThreeBibstemsTXTtoREFs(TXTtoREFs):
    """
    This class processes multiple flavors of the three bibstems: ARA+A, ARNPS, and AnRFM.
    Some references have flags like %R and %Z, while others do not, relying on the bibstem extracted from the filename.
    """

    # to match block indicators like %R and %Z
    re_block_indicator = re.compile(r'(%R|%Z)')

    def __init__(self, filename: str, buffer: str):
        """
        initialize the ThreeBibstemsTXTtoREFs object

        :param filename: the reference file to parse
        :param buffer: the content of the reference file
        """
        # see which flavor of the reference file to process, are there flag indicators in the file
        with open(filename, 'r', encoding="ISO-8859-1", errors='ignore') as f:
            content = ' '.join(f.readlines())
            num_flags = len(self.re_block_indicator.findall(content))

        reference_type = filename.split('/')[-3]
        # yes, there are block identifier, then if it is ARA&A bibstem direct the process to its own class,
        # otherwise for the two bibstems direct the process to the generic text parser
        if num_flags > 1:
            if reference_type == 'ARA+A':
                self.parser = ARAnATXTtoREFs(filename=filename, buffer=buffer)
            elif reference_type in ['ARNPS', 'AnRFM']:
                self.parser = ADStxtToREFs(filename=filename, buffer=buffer)
        # there is no block indicator, it is a flatfile format
        else:
            self.parser = FlatTXTtoREFs(filename=filename, buffer=buffer)

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        dispatch the reference processing to the appropriate parser

        :return: list of processed references
        """
        return self.parser.process_and_dispatch()

class PairsTXTtoREFs(ADStxtToREFs):
    """
    This class processes references from a pairs text format, which can be separated by semicolons or tabs.
    """

    def __init__(self, filename: str, buffer: str):
        """
        initialize the PairsTXTtoREFs object

        :param filename: the reference file to parse
        :param buffer: the content of the reference file
        """
        ADStxtToREFs.__init__(self, filename, buffer, parsername=PairsTXTtoREFs)


    def get_references(self, filename: str, encoding: str = "ISO-8859-1") -> List[Dict]:
        """
        read reference file for pairs text format

        :param filename: the reference file to parse
        :param encoding: the file encoding to use
        :return: list of parsed references
        """
        try:
            references = []

            accumulator = {}
            with open(filename, 'r', encoding=encoding, errors='ignore') as f:
                reader = f.readlines()
                for line in reader:
                    if not line.strip():
                        continue
                    if line.startswith('#'):
                        continue
                    line = line.replace('\r', '').replace('\n', '')
                    # there are two flavors of pairs file, separated by semicolon or separated by tabs
                    if ';' in line:
                        fields = line.split(';')
                        # for this flavor, the order is: manuscript bibcode, citation bibcode, and possible refstr
                        if not accumulator.get(fields[0], None):
                            accumulator[fields[0]] = []
                        accumulator[fields[0]].append((fields[1], fields[2]))
                    else:
                        fields = line.split('\t')
                        # for this flavor, the order is: citation bibcode, and manuscript bibcode
                        if not accumulator.get(fields[1], None):
                            accumulator[fields[1]] = []
                        accumulator[fields[1]].append((fields[0]))

            for bibcode,block_references in accumulator.items():
                references.append([bibcode, block_references])

            if len(references) > 0:
                logger.debug("Read source file %s, and got %d records." % (filename, len(references)))
            elif len(references) == 0:
                logger.error('No references found in reference file %s.' % (filename))
            return references
        except Exception as e:
            logger.error('Exception: %s' % (str(e)))
            return []

    def process_and_dispatch(self) -> List[Dict[str, List[Dict[str, str]]]]:
        """
        perform reference cleaning and then call the parser

        :return: list of processed references
        """
        references = []
        for raw_block_references in self.raw_references:
            bibcode = raw_block_references['bibcode']
            block_references = raw_block_references['block_references']
            item_nums = raw_block_references.get('item_nums', [])

            parsed_references = []
            for i, reference in enumerate(block_references):
                if type(reference) == str:
                    logger.debug("ADSpairs: parsing %s" % reference)
                    parsed_references.append(self.merge({'refstr': reference, 'bibcode': reference}, self.any_item_num(item_nums, i)))
                elif type(reference) == tuple:
                    logger.debug("ADSpairs: parsing %s" % ';'.join(reference))
                    parsed_references.append(self.merge({'refstr': ';'.join(reference), 'bibcode': reference[0]}, self.any_item_num(item_nums, i)))

            references.append({'bibcode': bibcode, 'references': parsed_references})
            logger.debug("%s: parsed %d references" % (bibcode, len(references)))

        return references


def toREFs(filename: str, buffer: str):      # pragma: no cover
    """
    this is a local function, called from main, for testing purposes

    :param filename:
    :param buffer:
    :return:
    """
    reference_type = filename.split('/')[-3]
    extension = filename.split('.')[-1]
    if reference_type in ['ARA+A', 'ARNPS', 'AnRFM']:
        results = ThreeBibstemsTXTtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif reference_type in ['PThPh', 'PThPS']:
        results = PThPhTXTtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    elif extension == 'pairs':
        results = PairsTXTtoREFs(filename=filename, buffer=buffer).process_and_dispatch()
    else:
        results = ADStxtToREFs(filename=filename, buffer=buffer).process_and_dispatch()
    for result in results:
        print(result['bibcode'])
        for i, reference in enumerate(result['references']):
            print(i + 1, reference['refstr'])


# This is the main program used for manual testing and verification of text references.
# It allows parsing references from either a file or a buffer, and if no input is provided,
# it runs a source test file to verify the functionality against expected parsed results.
# The test results are printed to indicate whether the parsing is successful or not.
if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse AcASn Text references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='text reference(s)')
    args = parser.parse_args()
    if args.filename:
        toREFs(args.filename, buffer=None)
    elif args.buffer:
        toREFs(filename=None, buffer=args.buffer)
    # if no reference source is provided, just run the source test file
    elif not args.filename and not args.buffer:
        txt_testing = [
            (ThreeBibstemsTXTtoREFs, '/../tests/unittests/stubdata/txt/ARA+A/0/0000ADSTEST.0.....Z.ref.raw'),
            (ThreeBibstemsTXTtoREFs, '/../tests/unittests/stubdata/txt/ARA+A/0/0001ARA+A...0.....Z.ref.refs'),
            (ThreeBibstemsTXTtoREFs, '/../tests/unittests/stubdata/txt/ARNPS/0/0000ADSTEST.0.....Z.ref.raw'),
            (ThreeBibstemsTXTtoREFs, '/../tests/unittests/stubdata/txt/ARNPS/0/0001ARNPS...0.....Z.ref.txt'),
            (ThreeBibstemsTXTtoREFs, '/../tests/unittests/stubdata/txt/AnRFM/0/0000ADSTEST.0.....Z.ref.raw'),
            (ThreeBibstemsTXTtoREFs, '/../tests/unittests/stubdata/txt/AnRFM/0/0001AnRFM...0.....Z.ref.txt'),
            (PThPhTXTtoREFs, '/../tests/unittests/stubdata/txt/PThPh/0/iss0.raw'),
            (PThPhTXTtoREFs, '/../tests/unittests/stubdata/txt/PThPS/0/editor.raw'),
            (ADStxtToREFs, '/../tests/unittests/stubdata/txt/ADS/0/0000ADSTEST.0.....Z.raw'),
            (PairsTXTtoREFs, '/../tests/unittests/stubdata/txt/AUTHOR/0/0000.pairs'),
            (PairsTXTtoREFs, '/../tests/unittests/stubdata/txt/ATel/0/0000.pairs'),
        ]
        for (parser, file) in txt_testing:
            filename = os.path.abspath(os.path.dirname(__file__) + file)
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
