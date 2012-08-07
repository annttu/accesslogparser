#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import os.path
import datetime
import logging

from socket import gethostname

from sqlobject import connectionForURI, sqlhub

from Parser import Parser
from model import Usage, UsageFiles

logging.basicConfig(level=logging.ERROR,
    format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('parselogs')

def main(log_dir, server_name, database_spec, only=None, files=False):
    sqlhub.processConnection = connectionForURI(database_spec)
    if only:
        domainlist = [only]
    else:
        domainlist = os.listdir(log_dir)
    for domain in domainlist:
        logger.info("Parsing logs for domain " + domain)
        logfile = os.path.join(log_dir, domain, 'access.log')
        p = Parser(files=files)
        p.parse_file(logfile)
        p.parse_file(logfile+'.1')
        for date, d in p.valid_data().items():
            if 'all' not in d:
                continue
            bytes, hits = d['all']
            date_d = datetime.date(*[int(x) for x in date.split("-")])
            try:
                u = Usage.selectBy(date=date_d, domain=domain, server=server_name)[0]
            except:
                Usage(server=server_name, domain=domain, date=date,
                      bytes=bytes, hits=hits)
                logger.debug("Added %s for domain %s" % (date, domain))
            if date_d.strftime("%d.%m.%Y") == datetime.datetime.now().strftime("%d.%m.%Y"):
                try:
                    u = Usage.selectBy(date=date_d, domain=domain, server=server_name)
                    for i in u:
                        u.delete(i.id)
                except:
                    pass
            d.pop('all')
            for filename, e in d.items():
                bytes, hits = e
                date_d = datetime.date(*[int(x) for x in date.split("-")])
                try:
                    u = UsageFiles.selectBy(date=date_d, domain=domain, server=server_name, filename=filename)[0]
                    continue
                except:
                    UsageFiles(server=server_name, domain=domain, date=date,
                                bytes=bytes, hits=hits, filename = filename)
                    logger.debug("Added %s for domain %s file %s" % (date, domain, filename))
                if date_d.strftime("%d.%m.%Y") == datetime.datetime.now().strftime("%d.%m.%Y"):
                    UsageFiles(server=server_name, domain=domain, date=date,
                                bytes=bytes, hits=hits, filename = filename)
                    logger.debug("Added %s for domain %s file %s" % (date, domain, filename))

if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser()

    parser.add_option("-l", "--log-dir", dest="log_dir",
                      help="read logs from DIRECTORY with vhost log dirs", metavar="DIRECTORY",
                      default='/var/log/apache2/vhosts')
    parser.add_option("-s", "--server-name", dest="server_name",
                      help="override the canonical hostname", metavar="SERVER-NAME",
                      default=gethostname())
    parser.add_option("-d", "--database-spec", dest="db_spec",
                      help="database to connect to", metavar="DB-SPEC",
                      default='sqlite:' + os.path.join(os.getcwd(), 'www.sqlite'))
    parser.add_option("-o", "--only", dest="only",
                      help="parse on only this domain", metavar="DOMAIN", default=None)
    parser.add_option("-f", "--files", dest="files",action="store_true",
                      help="parse also filenames", metavar="DOMAIN", default=False)

    opts, args = parser.parse_args()
    main(opts.log_dir, opts.server_name, opts.db_spec,opts.only, opts.files)
