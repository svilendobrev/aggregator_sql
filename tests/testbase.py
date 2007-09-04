import sys
if len(sys.argv) > 1 and '://' in sys.argv[1]:
    dburl = sys.argv[1]
    del sys.argv[1]
else:
    dburl = "sqlite:///:memory:"

import unittest
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import create_session
import aggregator as a

class TestBase(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Quick
        return super(TestBase, self).__init__(arg)

    def setUp(self):
        meta = self.meta = MetaData(bind=dburl)
        self.session = create_session()

    def tearDown(self):
        for v in self.__dict__.values():
            if type(v) is Table:
                v.drop()

    def save(self, ob):
        self.session.save(ob)
        self.session.flush()

class TestAccurateMixin(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Accurate
        return super(TestAccurateMixin, self).__init__(arg)
