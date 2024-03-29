
from adsrefpipe.models import Action, Parser

actions_records = [
    Action(status='initial'),
    Action(status='retry'),
    Action(status='delete'),
]
parsers_records = [
    Parser(name='arXiv', extension_pattern='.raw', reference_service_endpoint='/text', matches=[
        {"journal": "arXiv", "all_volume": True}, {"journal": "OTHER", "volume_string": "Astro2020"},
        {"journal": "ESASP", "all_volume": True}, {"journal": "ICRC", "all_volume": True},
        {"journal": "adap-org", "all_volume": True}, {"journal": "alg-geom", "all_volume": True},
        {"journal": "astro-ph", "all_volume": True}, {"journal": "chao-dyn", "all_volume": True},
        {"journal": "cmp-lg", "all_volume": True}, {"journal": "comp-gas", "all_volume": True},
        {"journal": "cond-mat", "all_volume": True}, {"journal": "cs", "all_volume": True},
        {"journal": "dg-ga", "all_volume": True}, {"journal": "funct-an", "all_volume": True},
        {"journal": "gr-qc", "all_volume": True}, {"journal": "hep-ex", "all_volume": True},
        {"journal": "hep-lat", "all_volume": True}, {"journal": "hep-ph", "all_volume": True},
        {"journal": "hep-th", "all_volume": True}, {"journal": "nlin", "all_volume": True},
        {"journal": "mat-ph", "all_volume": True}, {"journal": "math-ph", "all_volume": True},
        {"journal": "math", "all_volume": True}, {"journal": "nuc-ex", "all_volume": True},
        {"journal": "nucl-ex", "all_volume": True}, {"journal": "nucl-th", "all_volume": True},
        {"journal": "patt-sol", "all_volume": True}, {"journal": "physics", "all_volume": True},
        {"journal": "q-alg", "all_volume": True}, {"journal": "q-bio", "all_volume": True},
        {"journal": "quant-ph", "all_volume": True}, {"journal": "solv-int", "all_volume": True}
    ]),
    # top 10 most used xml parsers
    Parser(name='AGU', extension_pattern='.agu.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='AIP', extension_pattern='.aip.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='APS', extension_pattern='.ref.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='CrossRef', extension_pattern='.xref.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='ELSEVIER', extension_pattern='.elsevier.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='IOP', extension_pattern='.iop.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='JATS', extension_pattern='.jats.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='NATURE', extension_pattern='.nature.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='NLM', extension_pattern='.nlm3.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='OCR', extension_pattern='.ocr.txt', reference_service_endpoint='/text', matches=[]),
    Parser(name='SPRINGER', extension_pattern='.springer.xml', reference_service_endpoint='/xml', matches=[]),
    Parser(name='WILEY', extension_pattern='.wiley2.xml', reference_service_endpoint='/xml', matches=[]),
    # html parsers
    Parser(name='AEdRvHTML', extension_pattern='.html', reference_service_endpoint='/text', matches=[
        {"journal": "AEdRv", "volume_begin": 1, "volume_end": 5}
    ]),
    Parser(name='AnAhtml', extension_pattern='.ref.txt', reference_service_endpoint='/xml', matches=[
        {"journal": "A+A", "all_volume": True}
    ]),
    Parser(name='AnAShtml', extension_pattern='.txt', reference_service_endpoint='/text', matches=[
        {"journal": "A+AS", "volume_begin": 121, "volume_end": 126}
    ]),
    Parser(name='AnRFMhtml', extension_pattern='.html', reference_service_endpoint='/text', matches=[
        {"journal": "AnRFM", "volume_begin": 38, "volume_end": 39}
    ]),
    Parser(name='ARAnAhtml', extension_pattern='.html', reference_service_endpoint='/text', matches=[
        {"journal": "ARA+A", "volume_begin": 44, "volume_end": 45}
    ]),
    Parser(name='AREPShtml', extension_pattern='.html', reference_service_endpoint='/text', matches=[
        {"journal": "AREPS", "volume_begin": 24, "volume_end": 35}
    ]),
    Parser(name='JLVEnHTML', extension_pattern='.raw', reference_service_endpoint='/text', matches=[
        {"journal": "JLVEn", "all_volume": True}
    ]),
    Parser(name='PASJhtml', extension_pattern='.raw', reference_service_endpoint='/text', matches=[
        {"journal": "PASJ", "volume_begin": 51, "volume_end": 53}
    ]),
    Parser(name='PASPhtml', extension_pattern='.raw', reference_service_endpoint='/text', matches=[
        {"journal": "PASP", "all_volume": True}
    ]),
    Parser(name='ADStxt', extension_pattern='.raw', reference_service_endpoint='/text', matches=[
        {"journal":"A+ARv", "volume_begin":1996, "volume_end":2004},{"journal":"A+AT", "all_volume":True},
        {"journal":"AANv", "all_volume":True},{"journal":"AASP", "all_volume":True},
        {"journal":"AcA", "volume_begin":49, "volume_end":58},{"journal":"AcASn", "all_volume":True},
        {"journal":"Apei", "all_volume":True},{"journal":"ARI", "all_volume":True},
        {"journal":"ASD", "all_volume":True},{"journal":"ASInC", "all_volume":True},
        {"journal":"BlAJ", "all_volume":True},{"journal":"BOTT", "all_volume":True},
        {"journal":"BSRSL", "all_volume":True},  {"journal":"CoAst", "all_volume":True},
        {"journal":"CONF", "partial_bibcode":"2001cksa.conf"},{"journal":"CONF", "partial_bibcode":"2001defi.conf"},
        {"journal":"CONF", "partial_bibcode":"2001gge..conf"},{"journal":"CONF", "partial_bibcode":"2000prpl.conf"},
        {"journal":"CONF", "partial_bibcode":"2001IAUS..200"},{"journal":"CONF", "partial_bibcode":"2001pao..conf"},
        {"journal":"CONF", "partial_bibcode":"2001pimo.conf"},{"journal":"CONF", "partial_bibcode":"2001psrd.rept"},
        {"journal":"CONF", "partial_bibcode":"2001qarr.conf"},{"journal":"CONF", "partial_bibcode":"2001ragt.meet"},
        {"journal":"CONF", "partial_bibcode":"2002adass..11"},{"journal":"CONF", "partial_bibcode":"2002dgkd.conf"},
        {"journal":"CONF", "partial_bibcode":"2002evn..conf"},{"journal":"CONF", "partial_bibcode":"2002IAUS..207"},
        {"journal":"CONF", "partial_bibcode":"2002luml.conf"},{"journal":"CONF", "partial_bibcode":"2002nla..work"},
        {"journal":"CONF", "partial_bibcode":"2002osp..conf"},{"journal":"CONF", "partial_bibcode":"2002psrd.rept"},
        {"journal":"CONF", "partial_bibcode":"2002sdef.conf"},{"journal":"CONF", "partial_bibcode":"2003acfp.conf"},
        {"journal":"CONF", "partial_bibcode":"2003adass..12"},{"journal":"CONF", "partial_bibcode":"2003csss...12"},
        {"journal":"CONF", "partial_bibcode":"2003egcs.conf"},{"journal":"CONF", "partial_bibcode":"2003fthp.conf"},
        {"journal":"CONF", "partial_bibcode":"2003mglh.conf"},{"journal":"CONF", "partial_bibcode":"2003mpse.conf"},
        {"journal":"CONF", "partial_bibcode":"2003psrd.rept"},{"journal":"CONF", "partial_bibcode":"2003tdse.conf"},
        {"journal":"CONF", "partial_bibcode":"2003trt..work"},{"journal":"CONF", "partial_bibcode":"2003whdw.conf"},
        {"journal":"CONF", "partial_bibcode":"2004bdmh.conf"},{"journal":"CONF", "partial_bibcode":"2004mast.conf"},
        {"journal":"CONF", "partial_bibcode":"2004otp..work"},{"journal":"CONF", "partial_bibcode":"2004psrd.rept"},
        {"journal":"CONF", "partial_bibcode":"2004sdab.conf"},{"journal":"CONF", "partial_bibcode":"2004ssrc.conf"},
        {"journal":"CONF", "partial_bibcode":"2004tivo.conf"},{"journal":"CONF", "partial_bibcode":"2005AIPC..761"},
        {"journal":"CONF", "partial_bibcode":"2005daas.conf"},{"journal":"CONF", "partial_bibcode":"2005prpl.conf"},
        {"journal":"CONF", "partial_bibcode":"2005psrd.rept"},{"journal":"CONF", "partial_bibcode":"2005ragt.meet"},
        {"journal":"CONF", "partial_bibcode":"2005smp..conf"},{"journal":"CONF", "partial_bibcode":"2005vopc.conf"},
        {"journal":"CONF", "partial_bibcode":"2005yosc.conf"},{"journal":"CONF", "partial_bibcode":"2006amos.conf"},
        {"journal":"CONF", "partial_bibcode":"2006nla..conf"},{"journal":"CONF", "partial_bibcode":"2006psrd.rept"},
        {"journal":"CONF", "partial_bibcode":"2006tafp.conf"},{"journal":"CONF", "partial_bibcode":"2006yosc.conf"},
        {"journal":"CONF", "partial_bibcode":"2007amos.conf"},{"journal":"CONF", "partial_bibcode":"2007msfa.conf"},
        {"journal":"CONF", "partial_bibcode":"2007pms..conf"},{"journal":"CONF", "partial_bibcode":"2007psrd.rept"},
        {"journal":"CONF", "partial_bibcode":"2007ragt.meet"},{"journal":"CONF", "partial_bibcode":"2007soch.conf"},
        {"journal":"CONF", "partial_bibcode":"2007waas.work"},{"journal":"CONF", "partial_bibcode":"2008ysc..conf"},
        {"journal":"CONF", "partial_bibcode":"2009eimw.conf"},{"journal":"CONF", "partial_bibcode":"2010pim8.conf"},
        {"journal":"CONF", "partial_bibcode":"2010pim9.conf"},{"journal":"CONF", "partial_bibcode":"2010sf2a.conf"},
        {"journal":"CONF", "partial_bibcode":"2010ttt..work"},{"journal":"CONF", "partial_bibcode":"2011mast.conf"},
        {"journal":"CONF", "partial_bibcode":"2011pimo.conf"},{"journal":"CONF", "partial_bibcode":"2011sf2a.conf"},
        {"journal":"CONF", "partial_bibcode":"2011tfa..conf"},{"journal":"CONF", "partial_bibcode":"2012pimo.conf"},
        {"journal":"CONF", "partial_bibcode":"2012sf2a.conf"},{"journal":"CONF", "partial_bibcode":"2013pimo.conf"},
        {"journal":"CONF", "partial_bibcode":"2014apn6.conf"},
        {"journal":"CoSka", "all_volume":True},{"journal":"EJTP", "all_volume":True},
        {"journal":"eMetN", "all_volume":True},{"journal":"ErNW", "all_volume":True},
        {"journal":"GeIss", "all_volume":True},  {"journal":"IAUIn", "all_volume":True},
        {"journal":"ISSIR", "all_volume":True},  {"journal":"JAD", "all_volume":True},
        {"journal":"JApA", "all_volume":True},  {"journal":"JASS", "all_volume":True},
        {"journal":"JAVSO", "all_volume":True},{"journal":"JIMO", "all_volume":True},
        {"journal":"JIntS", "all_volume":True},  {"journal":"JSARA", "all_volume":True},
        {"journal":"KFNT", "all_volume":True},  {"journal":"Level5", "all_volume":True},
        {"journal":"LPI", "all_volume":True},{"journal":"LRR", "all_volume":True},
        {"journal":"LRSP", "all_volume":True},{"journal":"MAA", "all_volume":True},
        {"journal":"Msngr", "all_volume":True},  {"journal":"OTHER", "partial_bibcode":"1964MSRSL..26..476K"},
        {"journal":"OTHER", "partial_bibcode":"1983ASPRv...2..189P"},{"journal":"OTHER", "partial_bibcode":"1992cdtp.book..161T"},
        {"journal":"OTHER", "partial_bibcode":"1992CoSys...6..137T"},{"journal":"OTHER", "partial_bibcode":"1992qcqm.book..187T"},
        {"journal":"OTHER", "partial_bibcode":"1994dcgr.book..251T"},{"journal":"OTHER", "partial_bibcode":"1994fnas.book..431T"},
        {"journal":"OTHER", "partial_bibcode":"1995dsc..cof..324T"},{"journal":"OTHER", "partial_bibcode":"1995dsc..conf..324T"},
        {"journal":"OTHER", "partial_bibcode":"1995gtmp.book..479T"},{"journal":"OTHER", "partial_bibcode":"1996vbps.conf...21E"},
        {"journal":"OTHER", "partial_bibcode":"1996vbps.conf...23E"},{"journal":"OTHER", "partial_bibcode":"1997gbas.confE...1E"},
        {"journal":"OTHER", "partial_bibcode":"2002AdAMP..48..263F"},{"journal":"OTHER", "partial_bibcode":"2003PhDT.......177P"},
        {"journal":"OTHER", "partial_bibcode":"2004CSF....20..713T"},{"journal":"OTHER", "partial_bibcode":"2005JDDE...17...85V"},
        {"journal":"OTHER", "partial_bibcode":"2006csxs.book..623T"},{"journal":"OTHER", "partial_bibcode":"2006PhDT........15F"},
        {"journal":"OTHER", "partial_bibcode":"2007AIPC..948..357M"},{"journal":"OTHER", "partial_bibcode":"2007ASPC..378..251W"},
        {"journal":"OTHER", "partial_bibcode":"2007ASPC..378..450M"},{"journal":"OTHER", "partial_bibcode":"2007HiA....14...63G"},
        {"journal":"OTHER", "partial_bibcode":"2007NewA...12..479S"},{"journal":"OTHER", "partial_bibcode":"2007PhDT........35H"},
        {"journal":"OTHER", "partial_bibcode":"2008AIPC.1016..383M"},{"journal":"OTHER", "partial_bibcode":"2008CQGra..25d5014P"},
        {"journal":"OTHER", "partial_bibcode":"2008evn..confE..63E"},{"journal":"OTHER", "partial_bibcode":"2008IAUS..251..197M"},
        {"journal":"OTHER", "partial_bibcode":"2008JPCA..112.2339H"},{"journal":"OTHER", "partial_bibcode":"2008PCCP...10.2374H"},
        {"journal":"OTHER", "partial_bibcode":"2009arXiv0909.4971M"},{"journal":"OTHER", "partial_bibcode":"2009arXiv0909.4972M"},
        {"journal":"OTHER", "partial_bibcode":"2009ESAST.257.....P"},{"journal":"OTHER", "partial_bibcode":"2009JQSRT.110..533R"},
        {"journal":"OTHER", "partial_bibcode":"2009PhDT.........1M"},{"journal":"OTHER", "partial_bibcode":"2009sfch.book..459C"},
        {"journal":"OTHER", "partial_bibcode":"2010AdAst2010E..21W"},{"journal":"OTHER", "partial_bibcode":"2010BrJPh..40...38R"},
        {"journal":"OTHER", "partial_bibcode":"2010ChSBu..55.3847W"},{"journal":"OTHER", "partial_bibcode":"2010JVGR..193...82D"},
        {"journal":"OTHER", "partial_bibcode":"2010KPCB...26....1Z"},{"journal":"OTHER", "partial_bibcode":"2010pdac.book..230A"},
        {"journal":"OTHER", "partial_bibcode":"2010Sci...327.1470C"},{"journal":"OTHER", "partial_bibcode":"2011PhDT.........1B"},
        {"journal":"OTHER", "partial_bibcode":"2011PhDT.........1R"},{"journal":"OTHER", "partial_bibcode":"2011PhDT........68T"},
        {"journal":"OTHER", "partial_bibcode":"2011PhDT........85L"},{"journal":"OTHER", "partial_bibcode":"2011PhDT.......324K"},
        {"journal":"OTHER", "partial_bibcode":"2011sf2a.conf"},{"journal":"OTHER", "partial_bibcode":"2011sswh.book...31C"},
        {"journal":"OTHER", "partial_bibcode":"2011wdac.book..117F"},{"journal":"OTHER", "partial_bibcode":"2012amld.book"},
        {"journal":"OTHER", "partial_bibcode":"2012IJFPS..37...47M"},{"journal":"OTHER", "partial_bibcode":"2012IJRSG...1....1T"},
        {"journal":"OTHER", "partial_bibcode":"2013BSRSL..82...33D"},{"journal":"OTHER", "partial_bibcode":"2013IJAP....4b...6R"},
        {"journal":"OTHER", "partial_bibcode":"2013IJAP....4d...8R"},{"journal":"OTHER", "partial_bibcode":"2013PhDT.........2I"},
        {"journal":"OTHER", "partial_bibcode":"2014JApS...41..164R"},{"journal":"OTHER", "partial_bibcode":"2014PhDT.......131M"},
        {"journal":"OTHER", "partial_bibcode":"2014PhDT.......337H"},{"journal":"OTHER", "partial_bibcode":"2015pust.book.....C"},
        {"journal":"OTHER", "partial_bibcode":"2016CoSka..46...15K"},{"journal":"OTHER", "partial_bibcode":"2016ESASP1313.....R"},
        {"journal":"OTHER", "partial_bibcode":"2017pcos.book.....J"},{"journal":"OTHER", "partial_bibcode":"2017PhDT.........2N"},
        {"journal":"OTHER", "volume_string":"AIPC"},{"journal":"OTHER", "volume_string":"ASSL"},
        {"journal":"OTHER", "volume_string":"BUEOP"},  {"journal":"OTHER", "volume_string":"FrP"},
        {"journal":"OTHER", "volume_string":"JHEP"},{"journal":"PASJ", "volume_begin":54, "volume_end":9999},
        {"journal":"PNAS", "all_volume":True},{"journal":"POBeo", "all_volume":True},
        {"journal":"PZP", "all_volume":True},  {"journal":"RMxAA", "all_volume":True},
        {"journal":"RMxAC", "all_volume":True},{"journal":"RoAJ", "all_volume":True},
        {"journal":"SerAJ", "all_volume":True},  {"journal":"SpWea", "all_volume":True},
        {"journal":"SunGe", "all_volume":True},  {"journal":"ZGlGl", "all_volume":True},
    ]),
]

