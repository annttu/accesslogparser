#!/usr/bin/env python
# encoding: utf-8
from sqlobject import SQLObject, connectionForURI, sqlhub
from sqlobject import StringCol, IntCol, DateCol, ForeignKey
from datetime import datetime
    
class Usage(SQLObject):
    server = StringCol()
    domain = StringCol()
    date = DateCol(default=datetime.now())
    bytes = IntCol()
    hits = IntCol()
    
    def __unicode__(self):
        return '''%(date)s %(domain)s: %(kb).0f kB / %(hits)d requests''' % {
        'date': self.date, 'kb': self.bytes/1000, 'hits': self.hits,
        'domain': self.domain}

class UsageFiles(SQLObject):
    server = StringCol()
    domain = StringCol()
    date = DateCol(default=datetime.now())
    bytes = IntCol()
    hits = IntCol()
    filename = StringCol()

    def __unicode__(self):
        return '''%(date)s %(domain)s %(filename)s: %(kb).0f kB / %(hits)d requests''' % {
        'date': self.date, 'kb': self.bytes/1000, 'hits': self.hits,
        'domain': self.domain, 'filename' : self.filename}

def main():
    sqlhub.processConnection = connectionForURI('sqlite:/:memory:')
    SERVER='kirsikka'
    DOMAIN='mirror.kapsi.fi'
    Usage.createTable()
    for i in Usage.select():
        print unicode(i)

if __name__ == '__main__':
    main()
