import sys
if len(sys.argv) > 1 and '://' in sys.argv[1]:
    dburl = sys.argv[1]
    del sys.argv[1]
else:
    dburl = "sqlite:///:memory:"

try: sys.argv.remove( 'echo')
except: echo = False
else:   echo = True

import unittest
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import create_session
import aggregator as a

class TestBase(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Quick
        return super(TestBase, self).__init__(arg)

    class EasyInit(object):
        def __init__(self, **kwargs):
            for k,v in kwargs.iteritems():
                setattr( self, k, v)

    def setUp(self):
        meta = self.meta = MetaData(bind=dburl)
        meta.bind.echo = echo
        self.session = create_session()

    def tearDown(self):
        for v in self.__dict__.values():
            if type(v) is Table:
                v.drop()
        self.session.close()

    def save(self, *objs):
        for ob in objs:
            self.session.save_or_update(ob)
        self.session.flush()

    def refresh(self, *objs):
        for ob in objs:
            self.session.refresh(ob)

class TestAccurateMixin(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Accurate
        return super(TestAccurateMixin, self).__init__(arg)
