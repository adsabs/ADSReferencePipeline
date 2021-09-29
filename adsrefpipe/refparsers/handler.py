# -*- coding: utf-8 -*-

from adsrefpipe.refparsers.CrossRefXML import CrossReftoREFs
from adsrefpipe.refparsers.ElsevierXML import ELSEVIERtoREFs
from adsrefpipe.refparsers.JATSxml import JATStoREFs
from adsrefpipe.refparsers.IOPxml import IOPtoREFs
from adsrefpipe.refparsers.SpringerXML import SPRINGERtoREFs
from adsrefpipe.refparsers.APSxml import APStoREFs
from adsrefpipe.refparsers.NatureXML import NATUREtoREFs
from adsrefpipe.refparsers.AIPxml import AIPtoREFs
from adsrefpipe.refparsers.WileyXML import WILEYtoREFs
from adsrefpipe.refparsers.NLM3xml import NLMtoREFs
from adsrefpipe.refparsers.AGUxml import AGUtoREFs
from adsrefpipe.refparsers.arXivTXT import ARXIVtoREFs

def verify(parser_name):
    """

    :param parser_name: parser name from db
    :return:
    """
    # based on parser name return the parser class, if it is an xml
    if parser_name == 'CrossRef':
        return CrossReftoREFs
    if parser_name == 'ELSEVIER':
        return ELSEVIERtoREFs
    if parser_name == 'JATS':
        return JATStoREFs
    if parser_name == 'IOP':
        return IOPtoREFs
    if parser_name == 'SPRINGER':
        return SPRINGERtoREFs
    if parser_name == 'APS':
        return APStoREFs
    if parser_name == 'NATURE':
        return NATUREtoREFs
    if  parser_name == 'AIP':
        return AIPtoREFs
    if parser_name == 'WILEY':
        return WILEYtoREFs
    if parser_name == 'NLM':
        return NLMtoREFs
    if parser_name == 'AGU':
        return AGUtoREFs
    if parser_name == 'arXiv':
        return ARXIVtoREFs
    return None
