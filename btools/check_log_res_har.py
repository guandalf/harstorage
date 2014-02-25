#!/usr/bin/python

__author__ = 'maddabini'
import sys
import json
import requests
import time
import datetime
import tempfile
import os
import re
from optparse import OptionParser

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)

parser.add_option('-f', '--resultfile', action='store', type='string', dest='resultfile', default='/home/nagios/acq2harres/results_IT_CAT', help ='The file with result to check')

(options, args) = parser.parse_args(sys.argv[1:])

urlfile = options.resultfile

### alarm Warning if result file doesn't exist or is older than 2 hours
if not(os.path.exists(urlfile)):
    print ('The result file \"%s\" doesn\'t exist!!!' %(urlfile))
    exit(1)
elif (datetime.datetime.fromtimestamp(os.stat(urlfile).st_mtime) < datetime.datetime.now() - datetime.timedelta(seconds = 7200)):
    print ('The result file \"%s\" is older than 2 hours!!!' %(urlfile))
    exit(1)
   
resultfile = open(urlfile, "r")
text = resultfile.read()
resultfile.close()

regexpr1 = re.compile("'harpost': '<html>")
regexpr2 = re.compile("'element': False")

### alarm Warning if result file is empty and Critical if test (test and post) is KO
if text == '':
    print ('The result file \"%s\" is empty!!!' %(urlfile))
    exit(1) 
elif re.search(regexpr1, text) or re.search(regexpr2, text):
    print ('KO\n')
    print text
    exit(2)
else:
    print ('OK: Test was successful.\nHar post was successful.\nThe last test is not older than 2 hours.')
    exit()
