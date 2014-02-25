#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'guandalf'
import sys
import datetime
import os
from datetime import timedelta
from optparse import OptionParser
from gotestnew import HARRetriever
from sqlalchemy import Boolean, Column, Integer, String, Date, create_engine, func, case
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import send_nsca
from ConfigParser import ConfigParser
import yaml
import logging

logger = logging.getLogger('acq2har')

CURRENT_DIR=os.path.dirname(os.path.abspath(__file__))

### load generic configurations
configuration = ConfigParser()
configuration.read(os.path.join(CURRENT_DIR, 'acq2har.cfg'))
config_nagios_path = configuration.defaults()['config_nagios_path']
nagiosip =  configuration.defaults()['nagiosip']

Base = declarative_base()

class CATStatsReport(Base):
    __tablename__ = 'CAT_Tracking_Data_Weekly'
    dbname = 'cat_stats_reporting'

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    country = Column(String)
    flowType = Column(String)
    CAPId = Column(Integer)
    total = Column(Integer)
    kpiID = Column(Integer)
    productLandingid = Column(String)

    def get_capid_query(self, c, dateselect, fT, sessions):        
        return sessions[self.dbname].query(CATStatsReport.CAPId,
                                           func.sum(CATStatsReport.total).label('capid_total'), 
                                           func.sum(case([(CATStatsReport.kpiID==1, CATStatsReport.total)], else_=0)).label('subs'))\
                                  .filter_by(country=c, date=dateselect)\
                                  .filter(CATStatsReport.flowType.in_(fT))\
                                  .group_by('CAPId')\
                                  .having('subs > 5')\
                                  .order_by('capid_total desc')\
                                  .limit(5)
                                  
    def get_capid(self, c, dateselect, fT, sessions):
        r = self.get_capid_query(c, dateselect, fT, sessions).all()
        logger.debug("First query of: %s" % (self.__class__))
        logger.debug(self.get_capid_query(c, dateselect, fT, sessions))
        logger.debug(r)
        return r
    

class CATUrl(object):

    def get_urls(self, country, capids, sessions):
        urls = []

        for capid in capids:
            urls.append('http://%s.cat.buongiorno.com/wwf-splashTool/st-dispatch?capid=%s' % (country, capid))    
        return urls


class CATProd(Base):
    __tablename__ = 'CAT_CAP'

    dbname = NotImplemented
    id = Column(Integer, primary_key=True)
    creativityId = Column(Integer)
    enabled = Column(Boolean)
    moaUrl = Column(String)
    
    def get_urls(self, country, capids, sessions):
        urls = []

        db2rows_q = sessions[self.dbname].query(self.__class__.moaUrl)\
            .filter(self.__class__.id.in_(capids))\
            .filter(self.__class__.creativityId!=None)\
            .filter_by(enabled=True)
        logger.debug("Second query of: %s" % (self.__class__))
        logger.debug(db2rows_q)
        db2rows = db2rows_q.all()
        map(lambda x: urls.append(x[0]), db2rows)
        urls = filter(None, urls)

        return urls


class CATStatsReportZa(CATStatsReport):

    def get_capid_query(self, c, dateselect, fT, sessions):        
        return sessions[self.dbname].query(CATStatsReport.CAPId,
                                           func.sum(CATStatsReport.total).label('capid_total'), 
                                           func.sum(case([(CATStatsReport.kpiID==1, CATStatsReport.total)], else_=0)).label('subs'))\
                                  .filter_by(country=c, date=dateselect)\
                                  .filter(~CATStatsReport.flowType.in_(fT))\
                                  .group_by('CAPId')\
                                  .having('subs > 5')\
                                  .order_by('capid_total desc')\
                                  .limit(5)
                                  

class CATStatsReportPlus(CATStatsReport):

    def get_capid_query(self, c, dateselect, fT, sessions):        
        return sessions[self.dbname].query(CATStatsReport.CAPId,
                                           func.sum(CATStatsReport.total).label('capid_total'), 
                                           func.sum(case([(CATStatsReport.kpiID==1, CATStatsReport.total)], else_=0)).label('subs'))\
                                  .filter_by(country=c, date=dateselect)\
                                  .filter(CATStatsReport.productLandingid != '')\
                                  .group_by('CAPId')\
                                  .having('subs > 5')\
                                  .order_by('capid_total desc')\
                                  .limit(5)
                                  

class CATUrlDummy(object):
    
    def get_capid(self, c, dateselect, fT, sessions):
        return []

    def get_urls(self, country, capid, sessions):
        urls = []
        urls.append("http://www.google.com/favicon.ico")
        return urls


class CATUrlItPlusGiochissimo(object):
    
    def get_capid(self, c, dateselect, fT, sessions):
        return []

    def get_urls(self, country, capid, sessions):
        urls = []
        urls.append("http://www.giochissimo.it/subscribe/?cr=17985")
        urls.append("http://www.giochissimo.it/subscribe/?cr=18487")
        urls.append("http://www.giochissimo.it/subscribe/?cr=22497")
        return urls


class CATProdFrPlus(CATProd):
    dbname = 'cat_prod_fr'


class CATProdItPlus(CATProd):
    dbname = 'cat_prod_it'


class CATProdUkPlus(CATProd):
    dbname = 'cat_prod_uk'


class CATProdDePlus(CATProd):
    dbname = 'cat_prod_de'


class CATProdChPlus(CATProd):
    dbname = 'wwfcat_b2c_ch'


class CATProdMxPlus(CATProd):
    dbname = 'cat_prod_mex'


class CATProdBrPlus(CATProd):
    dbname = 'cat_prod_bra'


class CATProdEsPlus(CATProd):
    dbname = 'wwfcat_b2c_es'


class CATProdRuPlus(CATProd):
    dbname = 'wwfcat_b2c_ru'


class CATProdGrPlus(CATProd):
    dbname = 'wwfcat_b2c_gr'


class CATProdTrPlus(CATProd):
    dbname = 'wwfcat_b2c_tr'


class CATProdNlPlus(CATProd):
    dbname = 'wwfcat_b2c_nl'


class CATProdNoPlus(CATProd):
    dbname = 'wwfcat_b2c_no'


class CATProdAtPlus(CATProd):
    dbname = 'wwfcat_b2c_at'


class CATProdZaPlus(CATProd):
    dbname = 'wwfcat_b2c_za'


class CATProdZa(CATProd):
    dbname = 'wwfcat_b2c_za'

    def get_urls(self, country, capids, sessions):
        urls = []
        db2rows_q = sessions[self.dbname].query(self.__class__.moaUrl)\
            .filter(self.__class__.id.in_(capids))\
            .filter(self.__class__.creativityId==None)\
            .filter_by(enabled=True)
        logger.debug("Second query of: %s" % (self.__class__))
        logger.debug(db2rows_q)
        db2rows = db2rows_q.all()
        map(lambda x: urls.append(x[0]), db2rows)
        urls = filter(None, urls)
        
        return urls

class CATProdUsPlus(CATProd):
    dbname = 'wwfcat_b2c_usa'


class CATProdUs(CATProd):
    dbname = 'wwfcat_b2c_usa'


class CATProdPtPlus(CATProd):
    dbname = 'wwfcat_b2c_pt'


class CATProdInPlus(CATProd):
    dbname = 'wwfcat_b2c_in'


class CATProdArPlus(CATProd):
    dbname = 'cat_prod_ar'


class CATUrlKko(object):
    
    def get_capid(self, c, dateselect, fT, sessions):
        return []

    def get_urls(self, country, capid, sessions):
        urls = []
        urls.append("http://lpm.kko-store.fr/list/list-html5?utm_source=gdnsem&utm_campaign=list-html5_search_top&utm_medium=im&utm_term=%2Bjeux%20%2Bmobile&ctm_offer=kko299&ctm_country=fr&ctm_bu=b2c&ctm_template=multi-game-redblue&gclid=CJ6l0vqrurwCFa-WtAodbS8AQg")
        return urls


class CATUrlDve(object):
    
    def get_capid(self, c, dateselect, fT, sessions):
        return []

    def get_urls(self, country, capid, sessions):
        urls = []
        urls.append("http://m.playweez.com/contexteabonnes?ext_code=Google-m-s-jeux+à+télécharger+gratuitement-b-43994057745-&mode=google")
        return urls


class Acq2HAR(object):
  
    def __init__(self):
        self.db01uri = 'mysql://ptm:wh3dZZrk@10.1.16.56:3301'
        self.db02uri = 'mysql://tech:santalucia@10.1.16.66:3301'
        self.db03uri = 'mysql://tech:mf15mdj@10.1.16.79:3301'
    
    def set_db01uri(self, uri):
        self.db01uri = uri

    def set_db02uri(self, uri):
        self.db02uri = uri
        
    def set_db03uri(self, uri):
        self.db03uri = uri
    
    def do_dirty_job(self,
                     bmpexe='',
                     hsuri='',
                     label='',
                     outdirfile='',
                     indirfile=''):
        
        dbconf = {
            self.db02uri: ['cat_prod_bra', 'cat_prod_it', 'cat_prod_mex', 'wwfcat_b2c_ch',
                      'wwfcat_b2c_ru', 'wwfcat_b2c_gr', 'wwfcat_b2c_tr', 'wwfcat_b2c_za', 'wwfcat_b2c_in', ],
            self.db03uri: ['cat_prod_de', 'cat_prod_uk', 'cat_prod_fr', 'wwfcat_b2c_es', 'wwfcat_b2c_nl', 'wwfcat_b2c_no', 'wwfcat_b2c_at', 'wwfcat_b2c_usa', 'cat_prod_ar', 'wwfcat_b2c_pt', ],
            self.db01uri: ['cat_stats_reporting'],
        }

        ### load infolabels structure from configuration file
        stream = file(os.path.join(CURRENT_DIR, 'labels.yaml'), 'r')
        infolabel = yaml.load(stream)
        logger.info('the infolabel structure is: %s' % infolabel)

        # date of previus monday to do the select and to check if file exists
        today = datetime.date.today()
        dateselect = str((today-timedelta(days=7+today.weekday())))

        ### if the file with url is not present, create it
        # name of file is label_datepreviusmonday.
        urlfilename = "%s_%s" % (label, dateselect)
        urlfilename = urlfilename.replace(' ', '')
        urlfile = "%s/%s" % (indirfile, urlfilename)

        if not(os.path.exists(indirfile)):
            exit("The inputdirfile doesn't exist!")

        if not(os.path.exists(urlfile)):
            engines = {}
            sessionmakers = {}
            sessions = {}
            for k in dbconf.keys():
                for db in dbconf[k]:
                    engines[k, db] = create_engine('%s/%s' %(k, db))
                    sessionmakers[k, db] = sessionmaker(bind=engines[k, db])
                    sessions[db] = sessionmakers[k, db]()

            db2 = getattr(sys.modules[__name__], infolabel[label][2])()
            db1 = getattr(sys.modules[__name__], infolabel[label][3])()

            logger.info("db1 is: %s" % db1)
            logger.info("db2 is: %s" % db2)

            ### first query
            country = infolabel[label][0]
            flowType = infolabel[label][1]

            stats_report = db1.get_capid(country, dateselect, flowType, sessions)

            capids = []
            map(lambda x: capids.append(x[0]), stats_report)

            logger.debug("date: %s" % dateselect)
            logger.debug("Country: %s" % country)
            logger.debug("FlowType: %s" % flowType)
            logger.debug("capids: %s" % capids)

            ### second query
            ## create second query (if is necessary)

            URLS = db2.get_urls(country, capids, sessions)

            # insert the Urls into a file
            urlsfile = open(urlfile, "w")
            for url in URLS:
                try:
                    urlsfile.write(url + "\n")
                except TypeError:
                    pass
            urlsfile.close()

        # read the file and add "&noTrack=true" if necessary
        urlsfile = open(urlfile, "r")
        URLS = []
        u = urlsfile.readlines()
        if label in ["WW_CHECK", "KKO", "DVE"]:
            map(lambda x: URLS.append("%s" % (x[:-1])), u)
        else:
            map(lambda x: URLS.append("%s%s" % (x[:-1], "&noTrack=true")), u)
        urlsfile.close()

        logger.debug("URLS: %s" % URLS)
        logger.debug("harstorage uri: %s" % hsuri)
        logger.debug("label: %s" % label)
        logger.debug("bmpexe: %s" % bmpexe)

        h = HARRetriever(bmpexe, hsuri=hsuri, headers=infolabel[label][4])
        result = h.get_hars(label, URLS)

        ### Check u (url) and r (harpost and element)
        res = 1
        nagiosmess = "There aren't urls for this label!"
        for u, r in result.iteritems():
            nagiosmess = ''
            if r['harpost'] <> "Successful":
                nagiosmess += 'Post to harstorage failed'
                res = 1
            if not r['element']:
                nagiosmess += 'Load time of a url over 10 seconds'
                res = 1
            if res == 2:
                break
            res = 0
            nagiosmess = "OK: Test was successful."

        ### insert the result into a file (one file for each label
        ### if the file with results is not present, create it
        # name of file is results_<label>.
        resfilename = "results_%s" % (label)
        resfilename = resfilename.replace(' ', '')
        resfile = "%s/%s" % (outdirfile, resfilename)

        if not(os.path.exists(outdirfile)):
            exit("The outputdirfile doesn't exist!")

        resultfile = open(resfile, "w")
        resultfile.write('')
        for u, r in result.iteritems():
            resultfile.write("%s - %s \n" % (u,r))
        resultfile.close()

        nagioshost = "mid1-s005dada-2"
        nagiosservice = infolabel[label][5]

        logger.info('Nagios service name: \'%s\'' % nagiosservice)
        logger.info('Nagios host name: \'%s\'' % nagioshost)

        status = [send_nsca.STATE_OK, send_nsca.STATE_WARNING, send_nsca.STATE_CRITICAL, send_nsca.STATE_UNKNOWN]
        nagiosres = status[res]

        ### call to nagios
        notif = send_nsca.send_nsca(nagiosres, nagioshost, nagiosservice, nagiosmess, nagiosip, config_path=config_nagios_path)


if __name__ == "__main__":
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage=usage)

    parser.add_option('-o', '--outfile', action='store', type='string', dest='outfile', default=None, help ='Write HAR to <outfile>')
    parser.add_option('-u', '--url', action='store', type='string', dest='url', default='http://www.buongiorno.com', help='URL to check')
    parser.add_option('-H', '--harstorageuri', action='store', type='string', dest='HARSTORAGE_URI', default='http://mid1-s005dada-2.buongiorno.com:8080', help='Harstorage URI')
    parser.add_option('-l', '--harstoragelabel', action='store', type='string', dest='label', default='Default test', help='HAR file label')
    parser.add_option('-b', '--browsermobexe', action='store', type='string', dest='BMPEXE', default='/usr/local/browsermob-proxy/bin/browsermob-proxy', help='Browsermob Proxy executable full path')
    parser.add_option('-a', '--useragent', action='store', type='string', dest='UASTRING', default=None, help='User Agent String to setup firefox')
    parser.add_option('-i', '--inputdirfile', action='store', type='string', dest='indirfile', default='/home/nagios/acq2harurls', help='Directory where store the urls')
    parser.add_option('-O', '--outputdirfile', action='store', type='string', dest='outdirfile', default='/home/nagios/acq2harres', help='Directory where store results')
    parser.add_option('-d', '--debug', action='store_true', dest='debug', default=False, help='Overrides a lot of variables for debug purposes (DO NOT USE)')

    (options, args) = parser.parse_args(sys.argv[1:])

    if options.debug:
        logging.basicConfig(filename='/tmp/acq2har.log', level=logging.DEBUG)
        logger.debug("Hola! You are in debug mode!!!")
        #options.HARSTORAGE_URI = 'http://harstorage.buongiorno.com:8080'
        #options.url = "http://wwfcatptmfr.buongiorno.com/wwf-splashTool/st-dispatch?capid=90"
        #options.label = 'Sergio Test'
        #options.UASTRING = 'Mozilla/5.0 (Linux; U; Android 2.0; en-us; Droid Build/ESD20) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17'
        #options.url = "http://ifortune.playmobile.it"
        #options.label = 'iFortune IT app'

    a2h = Acq2HAR()
    a2h.do_dirty_job(bmpexe=options.BMPEXE,
                     hsuri=options.HARSTORAGE_URI,
                     label=options.label,
                     outdirfile=options.outdirfile,
                     indirfile=options.indirfile)
