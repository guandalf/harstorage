#!/usr/bin/python

__author__ = 'guandalf'
import sys
import json
import requests
import urllib
import shutil
from browsermobproxy import Server, Client
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from time import sleep, gmtime, time
from optparse import OptionParser
from pyvirtualdisplay import Display
from pprint import pprint
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait # available since 2.4.0
from selenium.webdriver.support import expected_conditions as EC # available since 2.26.0
import logging
from couchbase import Couchbase


class HARRetriever():
    def __init__(self,
                 bmpexe,
                 hsuri='http://127.0.0.1:8080',
                 uastring='Mozilla/5.0 (Linux; U; Android 4.0.3; de-ch; HTC Sensation Build/IML74K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
                 headers={}):
        self.port = 19090
        self.hsuri = hsuri
        self.headers = headers
        if uastring != '':
            self.headers.update({'User-Agent': uastring})
        self.x = Display()
        self.x.start()

        # Browsermob proxy setup
        # we try to create a new connection to the client
        try:
            self.proxy = Client('http://localhost:%s' % self.port)
        except requests.exceptions.ConnectionError:
            self.server = Server(bmpexe, options={'port': self.port})
            self.server.start()
            self.proxy = self.server.create_proxy()
        self.proxy.headers(self.headers)

    def setup_driver(self):
        # Webdriver setup
        self.profile = webdriver.FirefoxProfile()
        #if self.uastring:
        #    self.profile.set_preference('general.useragent.override', self.uastring)
        self.profile.set_proxy(self.proxy.selenium_proxy())
        self.binary = FirefoxBinary()
        self.binary._firefox_env["DISPLAY"] = ":%s" % self.x.display
        self.driver = webdriver.Firefox(firefox_profile=self.profile, firefox_binary=self.binary)

    def get_hars(self, label, urls):
        ## start loop Url
        headers = {}

        class my_js_runner(object):
            def __init__(self, condition):
                self.condition = condition

            def __call__(self, driver):
                return driver.execute_script(self.condition)

        r = {}
        for url in urls:
            self.setup_driver()
            self.proxy.new_har(label)
            self.driver.get(url)
            try:
                #element = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.TAG_NAME, "body")))
                element = WebDriverWait(self.driver, 10).until(my_js_runner('return document.readyState == "complete"'))
            except:
                element = False
                pass
            # seems like browsermod needs some time to generate har info
            sleep(5)

            self.driver.quit()

            har = json.dumps(self.proxy.har)

            path = "/results/upload"
            headers = {"Content-type": "application/x-www-form-urlencoded", "Automated": "true"}
            payload = {"file": har}
            try:
                harpost = requests.post('%s%s' % (self.hsuri, path), data=payload, headers=headers)
            except:
                harpost = None
            # print of result of each url
            r[url] = {'element': element, 'harpost': harpost.content}

        self.proxy.close()
        #let's update servicemanagement
        if label[-1] == '+':
            t = gmtime()
            #convert label to SM format
            sm_l_tmp = label.replace('+', '').split('_')
            sm_label = sm_l_tmp[1] + '-' + sm_l_tmp[0]
            logging.log(logging.DEBUG, sm_label)
            cb = Couchbase.connect(host='http://10.1.20.55', bucket='harstorage')
            peaks = cb.query('peaks', 'by_hour', key=[sm_label, t[0], t[1], t[2], t[3]], stale=False)
            logging.log(logging.DEBUG, peaks)
            for peak in peaks:
                logging.log(logging.DEBUG, peak)
                to_send = []
                if peak.value[0] > 0:
                    to_send.append(('Harstorage peak',  peak.value[1]))
                    to_send.append(('Harstorage green', peak.value[2]))
                    to_send.append(('Harstorage purple', peak.value[3]))
                logging.log(logging.DEBUG, to_send)
                smgmt_url = 'http://servicemanagement.cat.buongiorno.com/ServiceManagement/rest/nagios/store'
                time_sent = int(time() * 1000)
                for t in to_send:
                    smgmt_data = {"hostName": sm_label,
                                  "serviceName": t[0],
                                  "value": t[1],
                                  "warning": 1,
                                  "error": 1,
                                  "date": time_sent
                                  }
                    logging.log(logging.DEBUG, smgmt_data)
                    smgmt_r = requests.post(smgmt_url, data=json.dumps(smgmt_data), headers={'content-type': 'application/json'})
                    logging.log(logging.DEBUG, smgmt_r)
        return r

    def __del__(self):
        # we do not stop server anymore (leave there for others)
        #self.server.stop()
        #if self.profile:
        #    shutil.rmtree(self.profile.profile_dir)
        if self.x:
            self.x.stop()

if __name__ == '__main__':
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage=usage)

    parser.add_option('-o', '--outfile', action='store', type='string', dest='outfile', default=None, help ='Write HAR to <outfile>')
    parser.add_option('-u', '--url', action='store', type='string', dest='url', default='http://www.buongiorno.com', help='URL to check')
    parser.add_option('-H', '--harstorageuri', action='store', type='string', dest='HARSTORAGE_URI', default='http://harstorage.buongiorno.com:8080', help='Harstorage URI')
    parser.add_option('-l', '--harstoragelabel', action='store', type='string', dest='label', default='Default test', help='HAR file label')
    parser.add_option('-b', '--browsermobexe', action='store', type='string', dest='BMPEXE', default='/usr/local/browsermob-proxy/bin/browsermob-proxy', help='Browsermob Proxy executable full path')
    parser.add_option('-a', '--useragent', action='store', type='string', dest='UASTRING', default=None, help='User Agent String to setup firefox')
    parser.add_option('-d', '--debug', action='store_true', dest='debug', default=False, help='Overrides a lot of variables for debug purposes (DO NOT USE)')

    #parser.add_option('-p', '--port', action='store', type='int', dest='port', default=4448, help='Port to connect to')
    #parser.add_option('-s', '--site', action='store', type='string', dest='site', default='42', help='Site')
    #parser.add_option('-e', '--engine', action='store', type='string', dest='engine', default='*chrome', help='Site')
    #parser.add_option('-v', '--verbose', action='store_true', dest='verbose', default=False, help='Be more verbose on output')
    #parser.add_option('-n', '--nagios', action='store_true', dest='nagios', default=False, help='Send data to nagios for charting')
    #parser.add_option('-c', '--phonecompany', action='store', type='string', dest='pcompany', default='Sprint', help='Phone company Name')

    (options, args) = parser.parse_args(sys.argv[1:])

    if options.debug:
        print "Hola! You are in debug mode!!!"
        #options.HARSTORAGE_URI = 'http://harstorage.buongiorno.com:8080'
        #options.url = "http://wwfcatptmfr.buongiorno.com/wwf-splashTool/st-dispatch?capid=90"
        #options.label = 'Sergio Test'
        #options.UASTRING = 'Mozilla/5.0 (Linux; U; Android 2.0; en-us; Droid Build/ESD20) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17'
        #options.url = "http://ifortune.playmobile.it"
        #options.label = 'iFortune IT app'

    h = HARRetriever(options.BMPEXE, hsuri=options.HARSTORAGE_URI, headers={'User-Agent': options.UASTRING})
    save = h.get_hars(options.label, [options.url])

    if save[options.url].get('harpost',0) != 'Successful':
        if options.debug:
            print save[options.url].get('harpost', 0)
        exit(-1)

    labelencoded =  urllib.quote(options.label)
    print "OK - <a href=http://harstorage.buongiorno.com:8080/results/details?label=%s>RESULT</a>" % labelencoded
