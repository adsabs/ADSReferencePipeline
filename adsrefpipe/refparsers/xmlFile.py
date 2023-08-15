#!/usr/bin/env python
#
#   File:  XmlFile.py
#

import xml.dom.minidom as dom
from xml.parsers.expat import ExpatError
import regex as re
from collections import UserList


class XmlList(dom.Element, UserList):
    def __init__(self, elements=None, name=None):
        """

        :param elements:
        :param name:
        """
        if not elements:
            elements = []

        UserList.__init__(self, elements)

        if not name:
            self.noname = 1
            name = 'XmlList'

        dom.Element.__init__(self, name)

        self.childNodes = elements
        self.__name = name

    def toxml(self):
        """

        :return:
        """
        if not self:
            return ''
        elif self.noname:
            return self.childNodes[0].toxml()
        else:
            return dom.Element.toxml(self)

    def __str__(self):
        """

        :return:
        """
        if not self and self.__name == 'XmlList':
            return ''
        elif self.noname:
            return self.childNodes[0].toprettyxml(indent='  ')
        else:
            return self.toprettyxml(indent='  ')


class XmlString(XmlList):

    re_cleanup = [
        (re.compile(r'\s\s+'), r' '),
        (re.compile(r'> <'), r'><'),
        (re.compile(r'&'), '__amp__')
    ]
    re_remove_all_tags = re.compile(r'<[^<]+>')
    re_match_open_tag = re.compile(r'<(?!.*<)')
    re_match_text_between_tags = re.compile(r'[^<>]*')

    def __init__(self, buffer=None, doctype=None):
        """

        :param buffer:
        :param doctype:
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
        XmlList.__init__(self, elements=xml.childNodes, name=doctype)
