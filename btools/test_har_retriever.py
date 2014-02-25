__author__ = 'guandalf'
from unittest import TestCase
from mock import patch

from multiprocessing import Process
from flask import Flask, request
from datetime import date, timedelta

from gotestnew import HARRetriever
from acq2har import Acq2HAR, CATStatsReport

OUROWNUA = 'Our-own-personal-UA'
results_filename = 'got_headers.txt'

import logging
logger = logging.getLogger('acq2hartest')

headers_catcher = Flask(__name__)
harstorage_mock = Flask(__name__)

headers_catcher.config['SERVER_NAME'] = '127.0.0.1:5000'
harstorage_mock.config['SERVER_NAME'] = '127.0.0.1:5001'

@headers_catcher.route('/')
def capture_headers():
    with open(results_filename, 'wt') as f:
        f.write(str(request.headers))
    #my_request_headers = request.headers
    return str(request.headers)

@harstorage_mock.route('/results/upload', methods=['GET', 'POST'])
def mock_harstorage():
    return 'Success'


class Acq2HARTest(TestCase):
    @classmethod
    def setUpClass(cls):
        # Override settings
        cls.harstorage_mock_server_process = Process(target=harstorage_mock.run)
        cls.harstorage_mock_server_process.start()
        super(Acq2HARTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        cls.harstorage_mock_server_process.terminate()
        cls.harstorage_mock_server_process.join()
        super(Acq2HARTest, cls).tearDownClass()

    def test_base_call(self):
        label = 'IT_CAT+_Giochissimo'
        indirfile = 'urlfiledir'
        #real = HARRetriever('bmpexe_test', hsuri='hsuti_test')
        #real.get_hars = MagicMock(name='get_hars')

        with patch('acq2har.HARRetriever') as mock:
            instance = mock.return_value
            instance.get_hars.return_value = {'url': {'harpost': 'Successful', 'element': True}}
            instance.bmpexe = ''
            instance.hsuri = ''

            t = Acq2HAR()
            t.do_dirty_job(bmpexe='bmpexe_test',
                           hsuri='http://127.0.0.1:5001',
                           indirfile=indirfile,
                           outdirfile=indirfile,
                           label=label)
            # date of previous monday to do the select and to check if file exists

            today = date.today()
            dateselect = str((today-timedelta(days=7+today.weekday())))

            ### if the file with url is not present, create it
            # name of file is label_datepreviusmonday.
            urlfilename = "%s_%s" % (label, dateselect)
            urlfilename = urlfilename.replace(' ', '')
            urlfile = "%s/%s" % (indirfile, urlfilename)

            logging.debug(urlfile)

            # read the file and add "&noTrack=true" if necessary
            with open(urlfile, "r") as urlsfile:
                URLS = []
                u = urlsfile.readlines()
                map(lambda x: URLS.append("%s%s" % (x[:-1], "&noTrack=true")), u)

            instance.get_hars.assert_called_once_with(label, URLS)
            logging.debug(type(instance.bmpexe))
            #self.assertEqual(instance.bmpexe, 'bmpexe_test')
            #self.assertTrue(False)

    def test_PT_CATPlus(self):
        label = 'PT_CAT+'
        indirfile = 'urlfiledir'
        #real = HARRetriever('bmpexe_test', hsuri='hsuti_test')
        #real.get_hars = MagicMock(name='get_hars')

        with patch('acq2har.HARRetriever') as mock:
            instance = mock.return_value
            instance.get_hars.return_value = {'url': {'harpost': 'Successful', 'element': True}}
            instance.bmpexe = ''
            instance.hsuri = ''

            t = Acq2HAR()
            t.do_dirty_job(bmpexe='bmpexe_test',
                           hsuri='http://127.0.0.1:5001',
                           indirfile=indirfile,
                           outdirfile=indirfile,
                           label=label)
            # date of previous monday to do the select and to check if file exists

            today = date.today()
            dateselect = str((today-timedelta(days=7+today.weekday())))

            ### if the file with url is not present, create it
            # name of file is label_datepreviusmonday.
            urlfilename = "%s_%s" % (label, dateselect)
            urlfilename = urlfilename.replace(' ', '')
            urlfile = "%s/%s" % (indirfile, urlfilename)

            logging.debug(urlfile)

            # read the file and add "&noTrack=true" if necessary
            with open(urlfile, "r") as urlsfile:
                URLS = []
                u = urlsfile.readlines()
                map(lambda x: URLS.append("%s%s" % (x[:-1], "&noTrack=true")), u)

            instance.get_hars.assert_called_once_with(label, URLS)
            logging.debug(type(instance.bmpexe))
            #self.assertEqual(instance.bmpexe, 'bmpexe_test')

    def test_ZA_CAT(self):
        label = 'ZA_CAT'
        indirfile = 'urlfiledir'
        #real = HARRetriever('bmpexe_test', hsuri='hsuti_test')
        #real.get_hars = MagicMock(name='get_hars')

        with patch('acq2har.HARRetriever') as mock:
            instance = mock.return_value
            instance.get_hars.return_value = {'url': {'harpost': 'Successful', 'element': True}}
            instance.bmpexe = ''
            instance.hsuri = ''

            t = Acq2HAR()
            t.set_db02uri('mysql://tech:mf15mdj@10.1.16.66:3301')
            t.do_dirty_job(bmpexe='bmpexe_test',
                           hsuri='http://127.0.0.1:5001',
                           indirfile=indirfile,
                           outdirfile=indirfile,
                           label=label)
            # date of previous monday to do the select and to check if file exists

            today = date.today()
            dateselect = str((today-timedelta(days=7+today.weekday())))

            ### if the file with url is not present, create it
            # name of file is label_datepreviusmonday.
            urlfilename = "%s_%s" % (label, dateselect)
            urlfilename = urlfilename.replace(' ', '')
            urlfile = "%s/%s" % (indirfile, urlfilename)

            logging.debug(urlfile)

            # read the file and add "&noTrack=true" if necessary
            with open(urlfile, "r") as urlsfile:
                URLS = []
                u = urlsfile.readlines()
                map(lambda x: URLS.append("%s%s" % (x[:-1], "&noTrack=true")), u)

            instance.get_hars.assert_called_once_with(label, URLS)
            logging.debug(type(instance.bmpexe))
            #self.assertEqual(instance.bmpexe, 'bmpexe_test')
            
    def test_CATStatsReport(self):
      from sqlalchemy import create_engine
      from sqlalchemy.orm import sessionmaker

      some_engine = create_engine('sqlite://')
      Session = sessionmaker(bind=some_engine)
      session = Session()
      
      c = 'pp'
      dateselect = '2013-09-11'
      fT = ['pp_mobile', 'pp_mobile2']
      sessions = {'cat_stats_reporting':session}
      sr = CATStatsReport()
      q = sr.get_capid_query(c, dateselect, fT, sessions)
      ### doesn't work
      self.assertEqual(str(q.statement),'SELECT "CAT_Tracking_Data_Weekly"."CAPID", sum("CAT_Tracking_Data_Weekly".total) AS capid_total, sum(CASE WHEN ("CAT_Tracking_Data_Weekly"."kpiID" = 1) THEN "CAT_Tracking_Data_Weekly".total ELSE 0 END) AS subs FROM "CAT_Tracking_Data_Weekly" WHERE "CAT_Tracking_Data_Weekly".date = "2013-09-11" AND "CAT_Tracking_Data_Weekly".country = "pp" AND "CAT_Tracking_Data_Weekly"."flowType" IN ("pp_mobile", "pp_mobile2") GROUP BY CAPId ORDER BY capid_total desc LIMIT 5')


class HARRetrieverTest(TestCase):
    hsuri = 'http://127.0.0.1:5000'

    @classmethod
    def setUpClass(cls):
        # Override settings
        #cls.headers_catcher_server_process = Process(target=headers_catcher.run)
        #cls.headers_catcher_server_process.start()
        super(HARRetrieverTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        #cls.headers_catcher_server_process.terminate()
        #cls.headers_catcher_server_process.join()
        super(HARRetrieverTest, cls).tearDownClass()

    def test_oldstyle_har_retriever(self):
        h = HARRetriever('/Users/guandalf/MyApps/browsermob-proxy-2.0-beta-6/bin/browsermob-proxy',
                         hsuri=self.hsuri,
                         uastring=OUROWNUA)
        self.assertIsInstance(h, HARRetriever)

        r = h.get_hars('test_label', ['http://127.0.0.1:5000/'])

        self.assertIsInstance(r, dict)
        with open("pippo.txt", "rt") as f:
            hs = f.read().splitlines()
            all_headers = {}
            for l in hs:
                h = l.split(': ', 2)
                try:
                    all_headers[h[0]] = h[1]
                except IndexError:
                    all_headers[h[0]] = ''

        logger.debug(all_headers)
        self.assertIn('User-Agent', all_headers.keys())
        self.assertEqual(all_headers['User-Agent'], OUROWNUA)

    def test_har_retriever(self):
        h = HARRetriever('/Users/guandalf/MyApps/browsermob-proxy-2.0-beta-6/bin/browsermob-proxy',
                         hsuri=self.hsuri,
                         uastring=OUROWNUA,
                         headers={'X-Forwarded-For': '111.222.333.444',
                                  'X-Up-Calling-Line-Id': '1234567890'})
        self.assertIsInstance(h, HARRetriever)

        r = h.get_hars('test_label', ['http://127.0.0.1:5000/'])

        self.assertIsInstance(r, dict)
        with open("pippo.txt", "rt") as f:
            hs = f.read().splitlines()
            all_headers = {}
            for l in hs:
                h = l.split(': ', 2)
                try:
                    all_headers[h[0]] = h[1]
                except IndexError:
                    all_headers[h[0]] = ''

        logger.debug(all_headers)
        self.assertIn('User-Agent', all_headers.keys())
        self.assertEqual(all_headers['User-Agent'], OUROWNUA)

        self.assertIn('X-Forwarded-For', all_headers.keys())
        self.assertEqual(all_headers['X-Forwarded-For'], '111.222.333.444')

        self.assertIn('X-Up-Calling-Line-Id', all_headers.keys())
        self.assertEqual(all_headers['X-Up-Calling-Line-Id'], '1234567890')

    def test_har_retriever_cb(self):
        h = HARRetriever('/Users/guandalf/MyApps/browsermob-proxy-2.0-beta-6/bin/browsermob-proxy',
                         hsuri=self.hsuri,
                         uastring=OUROWNUA,
                         headers={'X-Forwarded-For': '111.222.333.444',
                                  'X-Up-Calling-Line-Id': '1234567890'})
        self.assertIsInstance(h, HARRetriever)

        totest = ['http://www.google.it', 'http://www.duckduckgo.com', 'http://httpbin.org/delay/12']
        #totest = ['http://www.google.it']
        r = h.get_hars('ES_CAT+', totest)
        for tt in totest:
            self.assertEqual(r[tt]['harpost'], 'Successful')
        #does nothing, I just need to send data to Couchbase
        self.assertTrue(False)

    def test_har_retriever_noua(self):
        h = HARRetriever('/Users/guandalf/MyApps/browsermob-proxy-2.0-beta-6/bin/browsermob-proxy',
                         hsuri=self.hsuri,
                         headers={'X-Forwarded-For': '111.222.333.444',
                                  'X-Up-Calling-Line-Id': '1234567890'})
        self.assertIsInstance(h, HARRetriever)

        r = h.get_hars('test_label', ['http://127.0.0.1:5000/'])

        self.assertIsInstance(r, dict)
        with open("pippo.txt", "rt") as f:
            hs = f.read().splitlines()
            all_headers = {}
            for l in hs:
                h = l.split(': ', 2)
                try:
                    all_headers[h[0]] = h[1]
                except IndexError:
                    all_headers[h[0]] = ''

        logger.debug(all_headers)
        self.assertIn('User-Agent', all_headers.keys())
        self.assertNotEqual(all_headers['User-Agent'], OUROWNUA)

        self.assertIn('X-Forwarded-For', all_headers.keys())
        self.assertEqual(all_headers['X-Forwarded-For'], '111.222.333.444')

        self.assertIn('X-Up-Calling-Line-Id', all_headers.keys())
        self.assertEqual(all_headers['X-Up-Calling-Line-Id'], '1234567890')
