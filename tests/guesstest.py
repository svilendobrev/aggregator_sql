import testbase
import aggregator as a
import unittest
import simpletest
from sqlalchemy import *
from sqlalchemy.orm import create_session, mapper, relation

class SimpleTest3(simpletest.SimpleTest):
    def setUp(self):
        testbase.TestBase.setUp(self)
        blocks = Table('blocks', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('lines', Integer),
            Column('lastline', Integer),
            Column('length', Integer),
            Column('avg_sum', Integer),
            Column('avg_cnt', Integer),
            Column('avg', Float),
            )
        lines = Table('lines', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('block', Integer),
            Column('length', Integer, default=10),
            )
        class Block(object):
            pass
        class Line(object):
            pass
        self.Block = Block
        self.Line = Line
        self.blocks = blocks
        self.lines = lines
        blocks.create()
        lines.create()
        mapper(Block, blocks)
        c = lines.c.block == blocks.c.id
        mapper(Line, lines,
            extension=self.aggregator_class(
                a.Max( blocks.c.lastline, lines.c.id, c),
                a.Count( blocks.c.lines, c),
                a.Sum( blocks.c.length, lines.c.length, c),
                a.Average1( blocks.c.avg, lines.c.length, c),
                *a.AverageSimple( blocks.c.avg_sum, lines.c.length, blocks.c.avg_cnt, c)
            ))

class SimpleTest4(SimpleTest3, testbase.TestAccurateMixin):
    pass

class TestMove(testbase.TestBase):
    def setUp(self):
        pass

if __name__ == '__main__':
    unittest.main()
