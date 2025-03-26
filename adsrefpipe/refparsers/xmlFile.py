#!/usr/bin/env python
#
#   File:  XmlFile.py
#

import xml.dom.minidom as dom
from xml.parsers.expat import ExpatError
import regex as re
from collections import UserList


class XmlList(dom.Element, UserList):
    """
    represents an XML list structure, combining functionalities of an XML element and a list of child elements
    """

    def __init__(self, elements: list = None, name: str = None):
        """
        initializes an XmlList object

        :param elements: list of XML elements
        :param name: name of the XML list element
        """
        if not elements:
            elements = []

        UserList.__init__(self, elements)

        if not name:
            self.noname = 1
            name = 'XmlList'
        else:
            self.noname = 0

        dom.Element.__init__(self, name)

        self.childNodes = elements
        self.__name = name

    def toxml(self) -> str:
        """
        converts the XML structure to a string

        :return: XML string representation of the object
        """
        # empty XmlList
        if not self.childNodes:
            return ''
        elif self.noname:
            return self.childNodes[0].toxml()
        else:
            return dom.Element.toxml(self)

    def __str__(self) -> str:
        """
        returns a pretty-printed XML string representation of the object

        :return: formatted XML string
        """
        if not self.childNodes:
            return ''
        elif self.noname and self.childNodes:
            return self.childNodes[0].toprettyxml(indent='  ')
        else:
            return self.toprettyxml(indent='  ')


class XmlString(XmlList):
    """
    represents an XML string, with methods to clean up and parse the XML content
    """

    # regular expressions used for XML cleanup and parsing
    re_cleanup = [
        (re.compile(r'\s\s+'), r' '),  # replaces multiple spaces with a single space
        (re.compile(r'> <'), r'><'),   # removes spaces between adjacent XML tags
        (re.compile(r'&'), '__amp__')  # replaces ampersands with '__amp__' to prevent parsing issues
    ]
    # matches and removes all XML tags from a string
    re_remove_all_tags = re.compile(r'<[^<]+>')
    # matches the last opening tag in a string
    re_match_open_tag = re.compile(r'<(?!.*<)')
    # matches text that appears between XML tags
    re_match_text_between_tags = re.compile(r'[^<>]*')

    def __init__(self, buffer: str = None, doctype: str = None):
        """
        initializes an XmlString object by parsing an XML buffer and applying cleanup rules

        :param buffer: XML string input
        :param doctype: document type identifier for the XML structure
        """
        # use dummy string if nothing no input is specified
        if not buffer: buffer = '<xmldoc />'

        buffer = buffer.replace('\n', ' ')

        for one_set in self.re_cleanup:
            buffer = one_set[0].sub(one_set[1], buffer)

        # up to three attempt to fix the reference, remove untag tags (ie, <883::AID-MASY883>)
        # but there is also less than and equal that at this point I can not do anything about
        # unless at the end, turn xml into text and replacing them
        # not sure why range does not work here!!
        for _ in [0,1,2,3]:
            try:
                xml = dom.parseString(buffer)
                self.__doctype = doctype
                XmlList.__init__(self, elements=xml.childNodes, name=doctype)
                return
            except ExpatError as e:
                try:
                    match = re.findall(r'(\d+)', str(e))
                    if len(match) == 2:
                        start = int(match[1])
                        range = [self.re_match_open_tag.search(buffer[:start]).span()[0],
                                 self.re_match_text_between_tags.search(buffer[start:]).span()[1]+start+1]
                        remove_text = buffer[range[0]:range[1]]
                        if remove_text.count('<') == 1 and not (remove_text.startswith('</') or remove_text.startswith('< ')):
                            buffer = buffer.replace(remove_text,'')
                    continue
                except AttributeError:
                    return

        # no success, so turn xml into text, remove < and > if any, and then put one tag around it
        # to be able to extract it as text from this structure
        top_tag = buffer.split(' ',1)[0][1:]
        the_buffer = self.re_remove_all_tags.sub(' ', buffer).replace('<','&lt;').replace('>','&gt;')
        buffer_transform = "<%s> %s </%s>"%(top_tag, the_buffer, top_tag)
        xml = dom.parseString(buffer_transform)
        self.__doctype = doctype
        print('...xml', xml.toprettyxml())
        XmlList.__init__(self, elements=xml.childNodes, name=doctype)
