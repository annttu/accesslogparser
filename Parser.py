#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import sys
import re
import logging
import gzip

from datetime import datetime
from bz2 import BZ2File

logger = logging.getLogger("Parser")

# 123.123.123.1 - - [15/Nov/2009:06:50:06 +0200] "GET / HTTP/1.1" 404 995 "http://www.kapsi.fi/" "USER AGENT"
# XXXXX tämä matchataan XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX --------ei väliä-------------------
LOG_ENTRY_PATTERN = re.compile('^(?P<ip>[^ ]+) [^ ]+ [^ ]+ \[(?P<date>[^\]]+)\] "[A-Z]+ (https?:\/\/\S+)?\/(?P<file>.*\.[a-zA-Z0-9]+)?.* .*(?<!\\\)" (?P<status>\d+) (?P<size>\d+|-)')

MONTH_NUMBERS = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05',
          'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09',
          'Oct': '10', 'Nov': '11', 'Dec': '12'}


def parse_line(line):
    result = re.match(LOG_ENTRY_PATTERN, line)
    try:
        ip = result.group('ip')
        time = result.group('date')
        status = result.group('status')
        size = result.group('size')
        filename = result.group('file')
        return { 'ip': ip, 'date': time[0:11], 'status': status, 'size': size, 'file' : filename}
    except:
        print(line)


def date_convert(date):
    try:
        logger.debug("date_convert: " + date)
        day, month, year = date.split("/")
        return "%s-%s-%s" % (year, MONTH_NUMBERS[month], day)
    except:
        return "0000-00-00"


class Parser(object):
    '''
    Class for parsing Apache access log
    
    Parses files line by line and keeps track of the size and number of all
    requests for each day.
    '''
    def __init__(self, files=False):
        self.date = {}
        self.files = files

    def parse(self, line):
        parsed = parse_line(line)
        size = parsed['size']
        filename = parsed['file']
        date = date_convert(parsed['date'])
        if not self.date.get(date):
            self.date[date] = {'all' : [0, 0]} # bandwidth, hits
        if not size == '-':
            self.date[date]['all'][0] += int(size)
            self.date[date]['all'][1] += 1
        if filename and self.files:
            if filename not in self.date[date]:
                self.date[date][filename] = [0, 0]
            if not size == '-':
                self.date[date][filename][0] += int(size)
                self.date[date][filename][1] += 1

    def parse_file(self, filename):
        f = None
        try:
            # Open (possibly compressed) log file
            logger.debug(filename)
            if filename.endswith('.gz'):
                f = gzip.open(filename, 'r')
            elif filename.endswith('.bz2'):
                f = BZ2File(filename, 'r')
            else:
                f = open(filename, 'r')

            for line in f:
                self.parse(line)
        except Exception, e:
            logger.warn(unicode(e))
        finally:
            if f:
                f.close()


    def valid_data(self):
        '''Discard values for the first and last date'''
        retval = {}
        for key in self.date.keys():
            dates = sorted(self.date[key].keys(), key=lambda x: x[0])
            #try:
            #    del dates[0]
            #    del dates[-1]
            #except:
            #    return {}

            ret = {}
            for date in dates:
                ret[date] = self.date[key][date]
            retval[key] = ret
        return retval


def main():
    filelist = sys.argv[1:]
    print >> sys.stderr, "# Parsing files: %s" % ', '.join(filelist)

    p = Parser()
    for f in filelist:
        p.parse_file(f)

    for key, value in sorted(p.date.items()):
        print "%s %8.0f MiB %8d reqs" % (key, value[0]/1024/1024, value[1])


if __name__ == '__main__':
    main()
