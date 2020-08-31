
import os, fnmatch

from adsputils import setup_logging, get_date

import celery
import argparse
import threading

# for temporary function `populate_tmp_arxiv_table`
import csv
from adsrefpipe.models import arXiv
# update state from diff to match after verifiying from solr
# this is temporary for test purposes only
from adsrefpipe.models import Compare

from adsrefpipe import tasks
from adsrefpipe.utils import get_date_modified_struct_time

app = tasks.app
logger = setup_logging('run.py')

def diagnostics(bibcodes, source_filenames):
    """
    Show information about what we have in our storage.
    :param: bibcodes - list of bibcodes
    :param: source_filenames - list of source filenames
    """
    # TODO: ask Steve if this is sufficient
    print app.query_reference_tbl(bibcodes, source_filenames)
    # TODO: queue some data to tables which should veified manually


def resolve(cutoff_date, source_file_path):
    """
    :param cutoff_date: if modified date is after this date
    :param source_file_path:
    :return: list of files in the directory with modified date after the cutoff, if any
    """
    list_files = []
    for root, dirs, files in os.walk(source_file_path):
        for basename in files:
            if fnmatch.fnmatch(basename, '*.raw'):
                filename = os.path.join(root, basename)
                if get_date_modified_struct_time(filename) >= cutoff_date:
                    list_files.append(filename)
    return list_files

def check_queue(task_id_list):
    """
    every few seconds check the task_id_list
    if there has been a failure, requeue

    :param task_id_list:
    :return:
    """
    # verify that all tasks got processed successfully
    # if not, attempt max_retry before quiting
    for file, task_id, num_retry in task_id_list[:]:
        # grab the AsyncResult
        result = app.AsyncResult(task_id)
        print '......result=', file, result.state, result.result
        if result.state == "SUCCESS":
            if result.result:
                # successfully processed
                task_id_list.remove((file, task_id, num_retry))
            else:
                # retry
                print '......try again=', file, num_retry
                task_id_list.remove((file, task_id, num_retry))
                if num_retry > 1:
                    # generate a new task id, since if the same id is used celery gets confused
                    task_id = celery.uuid()
                    task_id_list.append((file, task_id, num_retry - 1))
                    tasks.task_process_reference_file.apply_async(args=[file], task_id=task_id)
    if len(task_id_list) > 0:
        threading.Timer(10, check_queue, (task_id_list,)).start()

def queue_files(files):
    """
    queue all the requested files

    :param files:
    :return:
    """
    task_id_list = []
    max_retry = 3

    for file in files:
        task_id = celery.uuid()
        tasks.task_process_reference_file.apply_async(args=[file], task_id=task_id)
        task_id_list.append((file, task_id, max_retry))

    check_queue(task_id_list)

def populate_tmp_arxiv_table():
    """
    this function reads the csv file containing list of bibcode and arxiv category
    and populates temporary arxiv table

    :return:
    """
    filename = os.getcwd() + "/arxiv_classes_2.csv"
    with open(filename) as f:
        reader = csv.reader(f, delimiter=",")
        next(reader, None)
        arxiv_list = []
        for line in reader:
            arxiv_record = arXiv(bibcode=line[0], category=line[1])
            arxiv_list.append(arxiv_record)
        if len(arxiv_list) > 0:
            with app.session_scope() as session:
                try:
                    session.bulk_save_objects(arxiv_list)
                    session.flush()
                    logger.debug("Added `arXiv` records successfully.")
                    return True
                except:
                    logger.error("Attempt to add `arXiv` records failed.")
                    return False

# def update_compare_tmp_diff_to_match():
#     """
#
#     :return:
#     """
#     filename = os.getcwd() + "/diff_to_match.csv"
#     with open(filename) as f:
#         reader = csv.reader(f, delimiter=",")
#         next(reader, None)
#         with app.session_scope() as session:
#             record = session.query(Compare).filter(and_(Compare.bibcode == bibcode)).all()
#                 user.firstname, user.lastname = user.name.split(' ')
#
#             session.execute(update(Compare).where(
#                 Status.c.id == st.id).values(resource_root=rn))
#             result = session.execute(Compare.update().where(Compare.c.ID == 20).values(USERNAME='k9'))
#
#             session.update().where(account.c.name == op.inline_literal('account 1')).\ \
#                 values({'name': op.inline_literal('account 2')})
#         )
#         for line in reader:
#             bibcode_duo.append(())
#             arxiv_list.append(arxiv_record)
#         if len(arxiv_list) > 0:
#             with app.session_scope() as session:
#                 try:
#                     session.bulk_save_objects(arxiv_list)
#                     session.flush()
#                     logger.debug("Added `arXiv` records successfully.")
#                     return True
#                 except:
#                     logger.error("Attempt to add `arXiv` records failed.")
#                     return False

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-d',
                        '--diagnostics',
                        dest='diagnostics',
                        action='store_true',
                        help='Show diagnostic message')

    parser.add_argument('-r',
                        '--resolve',
                        dest='resolve',
                        action='store_true',
                        help='Resolve reference files (specify source files top directory and (optional) cutoff date of modified)')

    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        help='List of bibcodes separated by spaces for diagnostic')

    parser.add_argument('-f',
                        '--filenames',
                        dest='source_filenames',
                        action='store',
                        help='List of source filenames separated by spaces')

    parser.add_argument('-s',
                        '--since',
                        dest='since',
                        action='store',
                        default=None,
                        help='Starting date for resolving')

    parser.add_argument('-p',
                        '--path',
                        dest='path',
                        action='store',
                        default=None,
                        help='Path of source files for resolving')

    parser.add_argument('-t',
                        '--test',
                        dest='test',
                        action='store_true',
                        help='Just for testing')

    #TODO: add more command for querying db

    args = parser.parse_args()

    # either pass in the list of bibcodes, or list of filenames to query db on
    # if neither is supplied 10 random records are returned
    if args.diagnostics:
        if args.bibcodes:
            args.bibcodes = args.bibcodes.split(' ')
            diagnostics(args.bibcodes, None)

        elif args.source_filenames:
            args.source_filenames = args.source_filenames.split()
            diagnostics(None, args.source_filenames)

        else:
            diagnostics(None, None)

    # either pass in the path where the files to be processed are located, or list of filenames
    # if path is passed, date can be passed in to eliminate the files passed cutoff modified date
    # TODO: implement --force option
    if args.resolve:
        cutoff_date = get_date('1972')
        # if date has been specified, read it, otherwise we are going with everything, init above
        if args.since:
            cutoff_date = get_date(args.since)
        # if path is given find all files in that directory that satisfy date cutoff
        if args.path:
            files = resolve(cutoff_date.timetuple(), args.path)
        # if specific file names are giving
        elif args.source_filenames:
            files = args.source_filenames.split()
        else:
            files = []

        if len(files) > 0:
            queue_files(files)

    # this is to test the path of execution bypassing the queue
    if args.test:
        # populate_tmp_arxiv_table()

        april_arxiv_references = [11,12,13,14,16,18,19,20,21,22,23,25,30,31,36,37,39,43,51,65,66,72,79,98,102,114,122,126,146,158,164,168,170,203,208,224,229,242,254,257,322,339,346,355,400,401,439,441,454,456,477,511,519,610,617,618,619,625,627,628,629,632,633,634,636,637,641,643,644,645,647,649,650,656,664,671,672,676,678,680,718,727,736,747,791,815,836,846,847,848,863,864,870,874,888,890,902,916,934,947,997,1005,1021,1039,1042,1050,1063,1069,1102,1104,1109,1135,1139,1151,1152,1161,1188,1191,1193,1195,1196,1197,1198,1199,1204,1205,1210,1233,1253,1263,1273,1308,1338,1340,1342,1393,1400,1424,1427,1432,1439,1440,1448,1470,1488,1501,1503,1517,1538,1564,1593,1594,1621,1622,1633,1642,1645,1662,1666,1669,1676,1695,1711,1717,
                                  1721,1724,1726,1727,1745,1751,1756,1758,1786,1795,1796,1811,1827,1829,1892,1914,1933,1960,1982,1988,2024,2033,2039,2045,2054,2055,2058,2079,2091,2155,2204,2225,2237,2250,2259,2305,2332,2341,2343,2364,2408,2436,2447,2486,2496,2506,2508,2512,2527,2550,2602,2638,2650,2654,2663,2675,2698,2703,2713,2715,2728,2733,2761,2765,2775,2815,2840,2862,2883,2884,2887,2889,2891,2893,2894,2895,2896,2897,2898,2901,2904,2906,2910,2916,2918,2921,2923,2926,2927,2935,2937,2960,2963,2969,2993,3007,3009,3022,3039,3043,3051,3052,3065,3094,3128,3135,3140,3154,3157,3161,3175,3182,3192,3195,3201,3203,3211,3232,3241,3246,3247,3275,3279,3305,3306,3310,3410,3425,3435,3463,3467,3471,3475,3492,3504,3511,3514,3518,3551,
                                  3562,3570,3574,3600,3601,3603,3606,3608,3611,3612,3615,3616,3626,3629,3631,3668,3676,3679,3689,3701,3748,3772,3777,3790,3792,3817,3824,3855,3872,3886,3912,3918,3931,3936,3940,3941,3956,3957,3964,3968,3986,3987,3993,4009,4012,4016,4035,4039,4063,4074,4083,4115,4132,4158,4160,4165,4166,4167,4171,4176,4177,4179,4185,4187,4195,4202,4210,4230,4236,4241,4248,4253,4263,4266,4269,4277,4280,4304,4335,4337,4350,4360,4363,4382,4395,4399,4401,4429,4430,4437,4461,4506,4516,4527,4530,4531,4540,4542,4547,4568,4590,4600,4601,4615,4701,4702,4708,4740,4747,4748,4749,4753,4754,4756,4757,4762,4764,4773,4774,4808,4833,4847,4886,4887,4904,4922,4930,4942,4952,4964,4991,4997,5000,5016,5019,5049,5075,5121,5135,
                                  5149,5158,5165,5168,5169,5170,5171,5178,5179,5180,5187,5201,5238,5257,5270,5271,5296,5342,5347,5354,5379,5447,5449,5453,5475,5480,5489,5514,5562,5584,5590,5600,5603,5604,5605,5615,5618,5620,5627,5701,5724,5765,5787,5797,5818,5840,5888,5905,5941,5947,5952,5957,5974,5982,6008,6027,6041,6058,6078,6082,6083,6086,6102,6105,6106,6113,6114,6116,6118,6121,6123,6124,6126,6128,6131,6132,6137,6140,6155,6157,6170,6186,6210,6214,6218,6226,6228,6234,6245,6253,6256,6265,6270,6276,6287,6352,6359,6365,6396,6399,6416,6419,6437,6449,6452,6457,6466,6470,6476,6478,6486,6495,6501,6526,6528,6540,6549,6556,6570,6589,6600,6601,6603,6605,6606,6616,6619,6622,6624,6631,6681,6682,6685,6707,6709,6713,6723,6724,
                                  6725,6728,6729,6730,6731,6732,6733,6734,6743,6771,6779,6781,6802,6808,6812,6815,6829,6837,6841,6844,6845,6859,6884,6914,6926,6936,6974,6978,6979,6984,6992,6995,7021,7026,7074,7075,7092,7134,7185,7187,7196,7199,7218,7230,7232,7239,7240,7244,7246,7250,7252,7255,7256,7257,7258,7261,7263,7269,7273,7274,7279,7283,7289,7291,7305,7315,7327,7328,7345,7366,7382,7410,7412,7431,7449,7461,7465,7468,7481,7488,7491,7504,7515,7522,7523,7535,7556,7586,7589,7591,7595,7599,7604,7621,7627,7647,7654,7655,7669,7670,7687,7695,7709,7727,7744,7768,7783,7789,7792,7805,7811,7819,7834,7842,7844,7846,7847,7848,7854,7855,7863,7865,7866,7867,7885,7892,7901,7907,7908,7909,7913,7914,7916,7951,7963,7971,7974,7979,7990,
                                  8006,8011,8012,8014,8026,8048,8050,8111,8121,8137,8139,8143,8157,8168,8171,8182,8203,8218,8229,8230,8242,8254,8280,8293,8321,8327,8331,8334,8342,8363,8369,8392,8393,8394,8395,8397,8399,8400,8401,8403,8404,8405,8406,8408,8414,8415,8455,8463,8464,8477,8479,8482,8484,8502,8521,8536,8537,8542,8560,8577,8590,8598,8632,8722,8736,8785,8804,8835,8839,8866,8880,8880,8884,8923,8928,8929,8970,8972,8979,9023,9029,9030,9053,9067,9076,9076,9077,9109,9113,9118,9128,9142,9149,9150,9176,9176,9206,9213,9288,9291,9328,9339,9356,9359,9361,9369,9371,9378,9382,9384,9390,9391,9394,9396,9401,9415,9431,9433,9438,9482,9487,9497,9510,9511,9513,9515,9516,9518,9519,9520,9524,9525,9526,9528,9530,9532,9533,9534,9535,
                                  9539,9540,9541,9542,9555,9566,9567,9572,9586,9587,9597,9606,9606,9609,9636,9648,9658,9670,9686,9698,9700,9706,9716,9771,9772,9785,9818,9830,9840,9841,9871,9872,9873,9916,9956,9961,9979,9991,10015,10025,10028,10032,10036,10056,10094,10095,10097,10104,10105,10106,10123,10133,10148,10155,10160,10164,10193,10204,10205,10206,10207,10209,10210,10213,10214,10214,10218,10223,10230,10238,10244,10260,10266,10294,10296,10309,10372,10406,10407,10429,10452,10500,10504,10508,10510,10540,10542,10552,10579,10591,10607,10611,10644,10665,10668,10678,10683,10684,10702,10709,10735,10743,10752,10754,10755,10757,10760,10763,10764,10765,10767,10770,10771,10772,10778,10787,10803,10817,10834,10855,10868,10869,
                                  10877,10902,10944,10945,10974,10986,10989,10990,11006,11006,11008,11008,11027,11028,11061,11096,11104,11108,11109,11161,11175,11176,11209,11210,11214,11215,11223,11224,11225,11230,11236,11247,11263,11276,11295,11298,11303,11312,11314,11326,11328,11335,11351,11359,11366,11378,11379,11382,11384,11387,11388,11389,11391,11392,11394,11396,11397,11402,11418,11442,11444,11458,11474,11507,11518,11522,11542,11550,11550,11557,11560,11585,11601,11619,11620,11643,11669,11681,11681,11688,11740,11747,11758,11764,11775,11786,11808,11821,11845,11866,11873,11881,11884,11895,11911,11912,11913,11914,11917,11921,11927,11929,11936,11981,12033,12050,12060,12096,12105,12128,12132,12136,12176,12189,12215,12218,
                                  12229,12267,12270,12272,12310,12326,12328,12372,12382,12396,12419,12421,12434,12439,12448,12460,12475,12499,12509,12510,12516,12547,12548,12549,12579,12589,12597,12612,12622,12641,12649,12659,12663,12664,12666,12688,12708,12718,12748,12766,12808,12812,12829,12872,12874,12878,12882,12899,12900,12907,12924,12938,12946,12955,12981,12985,12996,12999,13001,13025,13028,13029,13032,13033,13037,13038,13039,13040,13042,13044,13045,13046,13048,13051,13053,13054,13056,13061,13065,13069,13079,13100,13113,13125,13155,13156,13163,13186,13189,13199,13200,13209,13212,13241,13252,13280,13296,13299,13307,13308,13355,13392,13418,13422,13426,13430,13435,13436,13461,13498,13502,13534,13548,13551,13552,13561,
                                  13589,13594,13596,13597,13600,13616,13623,13646,13648,13660,13662,13666,13666,13677,13679,13698,13706,13707,13720,13722,13724,13728,13729,13730,13731,13732,13733,13734,13736,13737,13739,13740,13741,13742,13743,13750,13752,13754,13755,13756,13757,13757,13767,13768,13782,13792,13794,13811,13877,13911,13913,13915,13917,13920,13924,13946,13951,13966,13976,13987,13996,14011,14019,14029,14040,14041,14042,14050,14058,14083,14099,14121,14122,14136,14156,14192,14207,14232,14234,14242,14260,14347,14349,14350,14354,14377,14380,14383,14389,14390,14392,14394,14396,14399,14402,14403,14419,14433,14435,14458,14472,14493,14496,14499,14508,14508,14536,14544,14562,14573,14576,14587,14618,14630,14643,14645,
                                  14653,14661,14668,14673,14712,14725,14757,14773,14806,14812,14849,14857,14869,14888,14889,14893,14894,14909,14913,14937,14972,14976,14980,14985,14988,14988,15000]
        april_arxiv_references = [11,21,31,43,168,224,254,346,637,864,1102,1233,1342,1593,1633,1642,1645,1662,1756,1829,2155,2343,2527,2728,2927,3052,3161,3275,3435,3886,3912,4009,4016,4035,4506,4547,4702,4773,5480,5562,5590,5947,6113,6114,6155,6170,6270,6287,6359,6419,6495,6540,6549,6603,6771,6815,6984,7556,7595,7654,7695,7792,7805,7842,7909,7951,8139,8334,8403,8464,8482,8484,8866,8928,8972,9497,9511,9524,9533,9871,10015,10218,10683,10772,10817,11006,11328,11392,11601,11845,11929,12060,12270,12326,12382,13039,13461,13561,13589,13648,13755,13811,14156,14347,14980,15000]
        april_arxiv_references = [6155]
        april_arxiv_references = [254,864,1233,1593,1642,1645,1829,2155,2343,2728,2927,3052,3161,3435,3912,4009,4035,4547,4702,5590,5947,6113,6170,6270,6359,6419,6495,6540,6771,6815,7556,7595,7654,7695,7792,7842,7909,8139,8334,8403,8464,8482,8484,8866,8928,9497,9511,9524,9533,9871,10015,10218,10683,10772,11006,11328,11392,11601,12060,12270,12326,12382,13039,13755,13811,14156,14347,14980]
        for i in april_arxiv_references:
            filename = '/proj/ads/references/sources/arXiv/2004/%05d.raw'%i
            print filename
            tasks.task_process_reference_file(filename)

        # if args.source_filenames:
        #     tasks.task_process_reference_file(args.source_filenames)
