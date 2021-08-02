import sys, os
import re
import argparse
import urllib.parse

from adsrefpipe.xmlparsers.reference import XMLreference, ReferenceError
from adsrefpipe.xmlparsers.common import get_references, get_xml_block, match_doi, match_arxiv_id, match_year

from adsputils import setup_logging, load_config
logger = setup_logging('reference-xml')
config = {}
config.update(load_config())


class JATSreference(XMLreference):

    re_title_in_quote = re.compile(r"__amp__ldquo;(?P<INQUOTES>[^__amp__rdquo;].*)__amp__rdquo;", re.MULTILINE)
    re_ext_link = re.compile(r'<ext-link.*href="(?P<EXT_LINK>[^"]*)"', re.MULTILINE)
    re_ext_link_inspire_J = re.compile(r'https:\/\/inspirehep.net\/search\?p=find\+J\s*"(?P<METADATA>.*)"')
    re_ext_link_inspire_doi = re.compile(r'https:\/\/inspirehep.net\/search\?p=find\+doi\s*"(?P<DOI>.*)"')
    re_ext_link_inspire_EPRINT = re.compile(r'https:\/\/inspirehep.net\/search\?p=find\+EPRINT\+(?P<ARXIV_ID>.*)')

    re_ampersand = re.compile(r"&amp;", re.IGNORECASE)

    re_unstructured_title_journal = re.compile(r"([:,]+\s*(?P<title>[^.(,]*)[.(,]+)([^<]*)(<italic.*>(?P<journal>.*)</italic>)", re.MULTILINE)
    re_unstructured_title = re.compile(r"(:\s*(?P<title>[^.(<_/][A-Za-z.\s]+)[.,(<_/]+)([^<_/]*)", re.MULTILINE)
    re_unstructured_journal = re.compile(r"[Ii]?[Nn]?[:,]?\s+(<italic.*>(?P<journal>.*)</italic>)", re.MULTILINE)

    def parse(self, prevref=None):
        """
        
        :param prevref: 
        :return: 
        """

        self.parsed = 0

        authors = self.parse_authors()
        year = self.xmlnode_nodecontents('year').strip()
        # see if year can be extracted from reference string
        if not year:
            year = match_year(str(self.reference_str))
        issn = self.xmlnode_nodecontents('issn').strip()
        ext_link = self.parse_external_links()

        try:
            # 5/4/2021 types I found in JATs xmls are as follows
            # journal, book, confproc, other, patent, thesis, eprint, report, website, and supplementary-material
            type = self.xmlnode_attribute('mixed-citation', 'publication-type')
        except:
            # 5/14/2021 can have something like <mixed-citation publication-type=''>2 ...
            type = "other"

        if type in ["book", "proc"]:
            source = self.xmlnode_nodecontents('source').strip()
            chapter_title = self.xmlnode_nodecontents('chapter-title').strip()
            if chapter_title and source:
                journal = source
                title = chapter_title
            elif source:
                title = source
                journal = ""
            elif chapter_title:
                title = chapter_title
                journal = ""
            else:
                # even though it is a book, some references have been tagged with article-title
                title = self.xmlnode_nodecontents('article-title').strip()
                journal = ''
        elif type == "confproc":
            conf = self.xmlnode_nodescontents('conf-name')
            if len(conf) == 1:
                title = ""
                journal = conf[0].strip()
            # outlier: title and journal both in conf-name
            elif len(conf) > 1:
                title = conf[0].strip()
                journal = conf[1].strip()
            else:
                journal = title = ''
            # outlier: type confproc can have article-title and source
            if not journal:
                journal = self.xmlnode_nodecontents('source').strip()
            if not title:
                title = self.xmlnode_nodecontents('article-title').strip()
            # outlier: another tag for year
            # and actually can include month, so grab the year only
            if not year:
                year = match_year(self.xmlnode_nodecontents('conf-date').strip())
        else:
            title = self.xmlnode_nodecontents('article-title').strip()
            journal = self.xmlnode_nodecontents('source')

        if not journal and not title:
            # outlier: some references have title in between colon and dot, and the journal inside italic tag
            match = self.re_unstructured_title_journal.search(str(self.reference_str))
            if match:
                journal = match.group('journal')
                title = match.group('title')
            else:
                # outlier: do we have only title
                match = self.re_unstructured_title.search(str(self.reference_str))
                if match:
                    title = match.group('title')
                # outlier: do we have only journal inside italic tag
                match = self.re_unstructured_journal.search(str(self.reference_str))
                if match:
                    journal = match.group('journal')

        volume = self.xmlnode_nodecontents('volume')
        pages = self.xmlnode_nodecontents('fpage').strip()
        if len(pages) == 0:
            pages = self.xmlnode_nodecontents('page-range')

        doi = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'doi'}).strip()
        if len(doi) > 0:
            self['doi'] = doi.strip()
        else:
            # attempt to extract it from refstr
            doi = match_doi(str(self.reference_str))
            if doi:
                self['doi'] = doi
            # see if there is a inspire link for eprint
            elif ext_link:
                for link in ext_link:
                    match = self.re_ext_link_inspire_doi.match(urllib.parse.unquote(link))
                    if match:
                        doi = match_doi(match.group('DOI'))
                        if doi:
                            self['doi'] = doi
        eprint = self.xmlnode_nodecontents('pub-id', attrs={'pub-id-type': 'arxiv'}).strip()
        if len(eprint) > 0:
            self['eprint'] = eprint.strip()
        else:
            # attempt to extract arxiv id from refstr
            eprint = match_arxiv_id(str(self.reference_str))
            if eprint:
                self['eprint'] = eprint
            # see if there is a inspire link for eprint
            elif ext_link:
                for link in ext_link:
                    match = self.re_ext_link_inspire_EPRINT.match(urllib.parse.unquote(link))
                    if match:
                        eprint = match_arxiv_id(match.group('ARXIV_ID'))
                        if eprint:
                            self['eprint'] = eprint

        if not title:
            match = self.re_title_in_quote.search(str(self.reference_str))
            if match:
                title = match.group('INQUOTES').rstrip(',')

        # if there are missing fields, see if we have inspire link to be able to extract these information out
        if (not journal or not volume or not pages) and ext_link:
            for link in ext_link:
                match = self.re_ext_link_inspire_J.match(urllib.parse.unquote(link))
                if match:
                    metadata = match.group('METADATA').split(',')
                    journal = metadata[0].replace('.', '. ').strip()
                    volume = metadata[1]
                    pages = metadata[2]

        # these fields are already formatted the way we expect them
        self['authors'] = authors
        self['year'] = year
        self['jrlstr'] = journal.strip()
        self['ttlstr'] = title.strip()
        self['volume'] = self.parse_volume(volume.strip())
        self['page'], self['qualifier'] = self.parse_pages(pages)
        self['pages'] = self.combine_page_qualifier(self['page'], self['qualifier'])
        self['issn'] = issn
        self['ext_link'] = ext_link

        self['refstr'] = self.get_reference_str()
        if not self['refstr'] and type not in ['website', 'supplementary-material']:
            self['refplaintext'] = self.get_reference_plain_text(self.to_ascii(self.xmlnode_nodecontents('mixed-citation')))

        self.parsed = 1

    def parse_authors(self):
        """
        xml_reference = "<mixed-citation publication-type='journal'> <string-name> <given-names>F.</given-names> <surname>Mesa</surname> </string-name>, <string-name> <given-names>G.</given-names> <surname>Valerio</surname> </string-name>, <string-name> <given-names>R.</given-names> <surname>Rodriguez-Berral</surname> </string-name>, and <string-name> <given-names>O.</given-names> <surname>Quevedo-Teruel</surname> </string-name>, &ldquo;Simulation-assisted efficient computation of the dispersion diagram of periodic structures: A comprehensive overview with applications to filters, leaky-wave antennas and metasurfaces,&rdquo; <source>IEEE Antennas Propag. Mag.</source> (published online, 2021).<pub-id pub-id-type='doi' specific-use='metadata'>10.1109/MAP.2020.3003210</pub-id></mixed-citation>"
        xml_reference = "<mixed-citation publication-type='journal'><person-group person-group-type='author'><name><surname>Aminu</surname><given-names>Mohammed D.</given-names></name><name><surname>Nabavi</surname><given-names>Seyed Ali</given-names></name><name><surname>Rochelle</surname><given-names>Christopher A.</given-names></name><name><surname>Manovic</surname><given-names>Vasilije</given-names></name></person-group><article-title xml:lang='en'>A review of developments in carbon dioxide storage</article-title><source>Applied Energy</source><year>2017</year><volume>208</volume><fpage>1389</fpage><lpage>1419</lpage></mixed-citation>"

        there are two sets of tags for author list
        1- <string-name> <given-names> given-name-here </given-names> <surname> surname-here </surname> </string-name>
        2- <person-group person-group-type='author'><name><surname> surname-here </surname><given-names> given-name-here </given-names></name></person-group>
        plus tag collab

        :return:
        """
        try:
            authors = []

            # if tag is <string-name>
            elements = self.reference_str.getElementsByTagName('string-name')
            if not elements or len(elements) == 0:
                # is it <person-group person-group-type='author'>
                elements = self.reference_str.getElementsByTagName('person-group')
                if not elements or len(elements) == 0:
                    # see if we have collaborators
                    authors_contents = self.xmlnode_nodescontents('collab')
                    if len(authors_contents) > 0:
                        for author in authors_contents:
                            authors.append(self.to_ascii(author.strip()))
                    else:
                        elements = self.reference_str.getElementsByTagName('name')
                        for name in elements:
                            try:
                                lastname = name.getElementsByTagName('surname')[0].firstChild.data
                                firstname = name.getElementsByTagName('given-names')[0].firstChild.data
                                author = "%s, %s" % (lastname.strip(), firstname.strip().replace('-', ' '))
                                authors.append(self.to_ascii(author))
                            except IndexError:
                                # there is firstname with out the last name or the other way around
                                # if it is the first author and we have the lastname return that only
                                if len(authors) == 0 and lastname:
                                    authors.append(self.to_ascii(lastname.strip()))
                                    break
                else:
                    # <person-group person-group-type='author'>
                    for element in elements:
                        if not element.childNodes:
                            continue
                        if element.attributes.items() != [('person-group-type', 'author')]:
                            continue
                        for name in element.childNodes:
                            try:
                                if (name.tagName == 'name'):
                                    try:
                                        lastname = name.getElementsByTagName('surname')[0].firstChild.data
                                        firstname = name.getElementsByTagName('given-names')[0].firstChild.data
                                        author = "%s, %s" % (lastname.strip(), firstname.strip().replace('-',' '))
                                        authors.append(self.to_ascii(author))
                                    except IndexError:
                                        # there is firstname with out the last name or the other way around
                                        # if it is the first author and we have the lastname return that only
                                        if len(authors) == 0 and lastname:
                                            authors.append(self.to_ascii(lastname.strip()))
                                            break
                                elif (name.tagName == 'collab'):
                                    author = self.xmlnode_nodecontents('collab').strip()
                                    if author:
                                        authors.append(self.to_ascii(author))
                                elif (name.tagName == 'anonymous'):
                                    author = self.xmlnode_nodecontents('anonymous').replace(', editors','').strip()
                                    if author:
                                        authors.append(self.to_ascii(author))
                            except AttributeError:
                                # tag object has no attribute 'tagName', skip
                                pass
            else:
                # <string-name>
                for element in elements:
                    if not element.childNodes:
                        continue
                    try:
                        lastname = element.getElementsByTagName('surname')[0].firstChild.data
                        firstname = element.getElementsByTagName('given-names')[0].firstChild.data
                        author = "%s, %s" % (lastname.strip(), firstname.strip().replace('-', ' '))
                        authors.append(self.to_ascii(author))
                    except IndexError:
                        try:
                            # there is firstname with out the last name or the other way around
                            # if it is the first author and we have the lastname return that only
                            if len(authors) == 0 and lastname:
                                authors.append(self.to_ascii(lastname.strip()))
                                break
                        except UnboundLocalError:
                            authors.append(self.to_ascii(element.firstChild.data))
            return ', '.join(authors)
        except:
            return None


    def parse_external_links(self):
        """

        :return:
        """
        match = self.re_ext_link.findall(str(self.reference_str))
        if match:
            return match
        return None


re_cleanup_buffer = [
    (re.compile(r'</?uri.*?>'), ''),
    (re.compile(r'\(<comment>.*?</comment>\)'), ''),
    (re.compile(r'<inline-formula[\w\s="\']*>.*?</inline-formula>', re.DOTALL), ''),
    (re.compile(r'<label>.*?</label>'), ''),
    (re.compile(r'<name><surname>(.*)</surname></name><name><surname>(.*)</surname>'), r'<name><surname>\1 \2</surname>')
]
re_cleanup_block = [
    # (re.compile(r'</?ext-link.*?>'), ''),
    (re.compile(r'<object-id>.*?</object-id>'), ''),
    (re.compile(r'<inline-formula>.*?</inline-formula>'), ''),
    (re.compile(r'<disp-formula>.*?</disp-formula>'), ''),
    # (re.compile(r'<\w*\:?math>.*?</\w*\:?math>'), ''),
    (re.compile(r'ext-link.*href='), 'ext-link href='),
    (re.compile(r'\s+xlink:href='), ' href=')
]

def JATStoREFs(filename=None, buffer=None, unicode=None):
    """

    :param filename:
    :param buffer:
    :param unicode: 
    :return: 
    """
    references = []
    pairs = get_references(filename=filename, buffer=buffer)

    for pair in pairs:
        bibcode = pair[0]
        buffer = pair[1]

        for one_set in re_cleanup_buffer:
            buffer = one_set[0].sub(one_set[1], buffer)

        references_bibcode = {'bibcode':bibcode, 'references':[]}

        # We have to split on 'mixed-citation' to capture entries
        # with multiple references!
        block_references = get_xml_block(buffer, 'mixed-citation')

        for reference in block_references:
            # these are found in APS references
            for one_set in re_cleanup_block:
                reference = one_set[0].sub(one_set[1], reference)

            logger.debug("JATSxml: parsing %s" % reference)
            try:
                jats_reference = JATSreference(reference)
                references_bibcode['references'].append(jats_reference.get_parsed_reference())
            except ReferenceError as error_desc:
                logger.error("JATSxml: error parsing reference: %s" %error_desc)

        references.append(references_bibcode)
        logger.debug("%s: parsed %d references" % (bibcode, len(references)))

    return references

if __name__ == '__main__':      # pragma: no cover
    parser = argparse.ArgumentParser(description='Parse JATS references')
    parser.add_argument('-f', '--filename', help='the path to source file')
    parser.add_argument('-b', '--buffer', help='xml reference(s)')
    args = parser.parse_args()
    if args.filename:
        print(JATStoREFs(filename=args.filename))
    if args.buffer:
        print(JATStoREFs(buffer=args.buffer))
    # if no reference source is provided, just run the source test file
    if not args.filename and not args.buffer:
        print(JATStoREFs(os.path.abspath(os.path.dirname(__file__) + '/../tests/unittests/stubdata/test.jats.xml')))
    sys.exit(0)
    # /proj/ads/references/sources/JAP/0127/iss24.jats.xml
