#!/usr/bin/env python
#
#   File:  XmlFile.py
#

import xml.dom.minidom as dom
import re
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

    def __init__(self, buffer=None, doctype=None):
        """

        :param buffer:
        :param doctype:
        """
        # use dummy string if nothing no input is specified
        if not buffer: buffer = '<xmldoc />'

        buffer = buffer.replace('\n', ' ')
        buffer = re.sub(r'\s\s+', ' ', buffer)
        buffer = re.sub(r'> <', '><', buffer)
        buffer = re.sub(r'&', '__amp__', buffer)
        xml = dom.parseString(buffer)
        self.__doctype = doctype
        XmlList.__init__(self, elements=xml.childNodes, name=doctype)
