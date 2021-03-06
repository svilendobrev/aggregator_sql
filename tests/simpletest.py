import testbase
import unittest
import aggregator as a
from sqlalchemy import *
from sqlalchemy.orm import create_session, mapper, relation
import sys

class SimpleTest(testbase.TestBase):

    def setUp(self):
        super(SimpleTest, self).setUp()
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
            Column('block', Integer, ForeignKey(blocks.c.id)),
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
        mapper(Line, lines,
            extension=self.aggregator_class(
                a.Max( blocks.c.lastline, lines.c.id),
                a.Count( blocks.c.lines),
                a.Sum( blocks.c.length, lines.c.length),
                a.Average1( blocks.c.avg, lines.c.length),
                #a.Average( blocks.c.avg_sum, lines.c.length, blocks.c.avg_cnt),
                *a.AverageSimple( blocks.c.avg_sum, lines.c.length, blocks.c.avg_cnt)
            ))

    def avg( self, b):
        self.assertEquals( b.avg_sum, b.length)
        self.assertEquals( b.avg_cnt, b.lines)
        self.assertAlmostEqual( float(b.avg_sum)/b.avg_cnt, b.avg, 5)

    def testSimpleCreate(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        l = self.Line()
        l.block = b.id
        self.save(l)

    def testAddLines(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        l = self.Line()
        l.block = b.id
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.lines, 1)
        self.assertEquals(b.lastline, l.id)
        self.assertEquals(b.length, 10)
        self.avg(b)

    def testAddMoreLines(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b.id
            l.length = i
            self.session.save(l)
        self.session.flush()
        self.session.refresh(b)
        self.assertEquals(b.lastline, l.id)
        self.assertEquals(b.length, 45)
        self.avg(b)

    def testUpdate(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b.id
            l.length = i
            self.session.save(l)
        l = self.Line()
        l.block = b.id
        l.length = 15
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.lines, 11)
        self.assertEquals(b.length, 60)
        self.avg(b)
        l.length = 25
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.lines, 11)
        self.assertEquals(b.length, 70)
        self.avg(b)

    def testNULL(self):
        b = self.Block()
        b.lines = None
        self.save(b)
        l = self.Line()
        l.block = b.id
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.lines, 1)
        self.assertEquals(b.lastline, l.id)
        self.avg(b)

    def testDeleteLines(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b.id
            l.length = i
            self.session.save(l)
        self.session.flush()
        l = self.session.query(self.Line).offset(2).limit(1).all()[0]
        self.session.delete(l)
        self.session.flush()
        self.session.refresh(b)
        self.assertEquals(b.lines, 9)
        self.assertNotEquals(b.lastline, l.id)
        self.assertEquals(b.length, 45 - l.length)
        self.avg(b)

    def testUpdateDelete(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b.id
            l.length = i
            self.session.save(l)
        self.session.flush()
        l = self.session.query(self.Line).offset(2).limit(1).all()[0]
        oldlen = l.length
        l.length = 100
        self.session.delete(l)
        self.session.flush()
        self.session.refresh(b)
        self.assertEquals(b.lines, 9)
        self.assertNotEquals(b.lastline, l.id)
        self.assertEquals(b.length, 45 - oldlen)
        self.avg(b)

    def testDeleteTwice(self):
        b1 = self.Block()
        b1.lines = 0
        self.save(b1)
        b2 = self.Block()
        b2.lines = 0
        self.save(b2)
        for i in range(10):
            l = self.Line()
            l.block = b1.id
            self.session.save(l)
            last1 = l
            l = self.Line()
            l.block = b2.id
            self.session.save(l)
            last2 = l
        self.session.flush()
        self.session.delete(last1)
        self.session.delete(last2)
        self.session.flush()
        self.session.refresh(b1)
        self.session.refresh(b2)
        self.assertEquals(b1.lines, 9)
        self.assertEquals(b2.lines, 9)
        self.assertNotEquals(b1.lastline, last1.id)
        self.assertNotEquals(b2.lastline, last2.id)
        self.avg(b1)
        self.avg(b2)

    def testMoveLine(self):
        b1 = self.Block()
        b1.lines = 0
        self.save(b1)
        b2 = self.Block()
        b2.lines = 0
        self.save(b2)
        for i in range(10):
            l = self.Line()
            l.length = i
            l.block = b1.id
            self.session.save(l)
            last1 = l
            l = self.Line()
            l.block = b2.id
            l.length = i
            self.session.save(l)
            last2 = l
        self.session.flush()
        self.session.refresh(b1)
        self.session.refresh(b2)
        self.assertEquals(b1.lines, 10)
        self.assertEquals(b2.lines, 10)
        self.assertEquals(b1.length, 45)
        self.assertEquals(b2.length, 45)
        self.assertEquals(b1.lastline, last1.id)
        self.assertEquals(b2.lastline, last2.id)
        last1.length = 18 # was9
        last2.length = 5   # was9
        self.session.flush()
        self.session.refresh(b1)
        self.session.refresh(b2)
        self.assertEquals(b1.length, 54)
        self.assertEquals(b2.length, 41)
        last1.block = b2.id
        self.session.flush()
        self.session.refresh(b1)
        self.session.refresh(b2)
        self.assertEquals(b1.lines, 9)
        self.assertEquals(b2.lines, 11)
        self.assertEquals(b1.length, 36)
        self.assertEquals(b2.length, 59)
        self.assertNotEquals(b1.lastline, last1.id)
        self.assertEquals(b2.lastline, last2.id)
        last1.block = b1.id
        last2.block = b1.id
        self.session.flush()
        self.session.refresh(b1)
        self.session.refresh(b2)
        self.assertEquals(b1.lines, 11)
        self.assertEquals(b2.lines, 9)
        self.assertEquals(b1.length, 59)
        self.assertEquals(b2.length, 36)
        self.assertEquals(b1.lastline, last2.id)
        self.assertNotEquals(b2.lastline, last2.id)


class ComplexTest(testbase.TestBase):

    def setUp(self):
        super(ComplexTest, self).setUp()
        users = Table('users', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('name', String(30)),
            Column('blocks', Integer),
            Column('lines', Integer),
            )
        blocks = Table('blocks', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('author', Integer, ForeignKey(users.c.id)),
            Column('lines', Integer),
            Column('lastline', Integer),
            )
        lines = Table('lines', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('block', Integer, ForeignKey(blocks.c.id)),
            Column('author', Integer, ForeignKey(users.c.id)),
            )
        class Block(object):
            pass
        class Line(object):
            pass
        class User(object):
            def __init__(self, name):
                self.name = name
        self.Block = Block
        self.Line = Line
        self.User = User
        self.blocks = blocks
        self.lines = lines
        self.users = users
        users.create()
        blocks.create()
        lines.create()
        mapper(Block, blocks,
            extension=self.aggregator_class(
                a.Count(users.c.blocks),
            ))
        mapper(User, users)
        mapper(Line, lines,
            extension=self.aggregator_class(
                a.Max(blocks.c.lastline, lines.c.id),
                a.Count(blocks.c.lines),
                a.Count(users.c.lines),
            ))

    def testSimpleCreate(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        l = self.Line()
        l.block = b.id
        self.save(l)

    def testAddLines(self):
        u = self.User('john')
        self.save(u)
        b = self.Block()
        b.author = u.id
        self.save(b)
        l = self.Line()
        l.block = b.id
        l.author = u.id
        self.save(l)
        self.session.refresh(b)
        self.session.refresh(u)
        self.assertEquals(b.lines, 1)
        self.assertEquals(b.lastline, l.id)
        self.assertEquals(u.lines, 1)
        self.assertEquals(u.blocks, 1)

    def testAddMoreLines(self):
        u = self.User('john')
        self.save(u)
        b = self.Block()
        b.lines = 0
        b.author = u.id
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b.id
            l.author = u.id
            self.session.save(l)
        self.session.flush()
        self.session.refresh(b)
        self.session.refresh(u)
        self.assertEquals(b.lines, 10)
        self.assertEquals(b.lastline, l.id)
        self.assertEquals(u.lines, 10)
        self.assertEquals(u.blocks, 1)

    def testDeleteLines(self):
        u = self.User('john')
        self.save(u)
        b = self.Block()
        b.lines = 0
        b.author = u.id
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b.id
            l.author = u.id
            self.session.save(l)
        self.session.flush()
        l = self.session.query(self.Line).filter_by(block=b.id).first()
        self.session.delete(l)
        self.session.flush()
        self.session.refresh(b)
        self.session.refresh(u)
        self.assertEquals(b.lines, 9)
        self.assertNotEquals(b.lastline, l.id)
        self.assertEquals(u.lines, 9)
        self.assertEquals(u.blocks, 1)

    def testTwoUsers(self):
        j = self.User('john')
        self.save(j)
        m = self.User('mike')
        self.save(m)
        b1 = self.Block()
        b1.lines = 0
        b1.author = j.id
        self.save(b1)
        b2 = self.Block()
        b2.lines = 0
        b2.author = m.id
        self.save(b2)
        b3 = self.Block()
        b3.lines = 0
        b3.author = m.id
        self.save(b3)
        for i in range(20):
            l = self.Line()
            l.block = [b1,b2,b3][i%3].id
            l.author = ((i % 3) and j or m).id
            self.session.save(l)
        self.session.flush()
        self.session.refresh(b1)
        self.session.refresh(b2)
        self.session.refresh(b3)
        self.session.refresh(j)
        self.session.refresh(m)
        self.assertEquals(b1.lines, 7)
        self.assertEquals(b2.lines, 7)
        self.assertEquals(b3.lines, 6)
        self.assertEquals(j.blocks, 1)
        self.assertEquals(m.blocks, 2)
        self.assertEquals(j.lines, 13)
        self.assertEquals(m.lines, 7)

class RelationsTest(testbase.TestBase):

    def setUp(self):
        super(RelationsTest, self).setUp()
        users = Table('users', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('name', String(30)),
            Column('blocks', Integer),
            Column('lines', Integer),
            )
        blocks = Table('blocks', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('author', Integer, ForeignKey(users.c.id)),
            Column('lines', Integer),
            Column('lastline', Integer),
            )
        lines = Table('lines', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('block', Integer, ForeignKey(blocks.c.id)),
            Column('author', Integer, ForeignKey(users.c.id)),
            )
        class Block(object):
            pass
        class Line(object):
            pass
        class User(object):
            def __init__(self, name):
                self.name = name
        self.Block = Block
        self.Line = Line
        self.User = User
        self.blocks = blocks
        self.lines = lines
        self.users = users
        users.create()
        blocks.create()
        lines.create()
        self.blockmapper = mapper(Block, blocks,
            extension=self.aggregator_class(
                a.Count(users.c.blocks),
            ), properties = {
                '_author': blocks.c.author,
                'author': relation(User),
            })
        mapper(User, users)
        mapper(Line, lines,
            extension=self.aggregator_class(
                a.Max(blocks.c.lastline, lines.c.id),
                a.Count(blocks.c.lines),
                a.Count(users.c.lines),
            ), properties = {
                '_author': lines.c.author,
                'author': relation(User),
                '_block': lines.c.block,
                'block': relation(Block),
            })

    def testSimpleCreate(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        l = self.Line()
        l.block = b
        self.save(l)

    def testAddLines(self):
        u = self.User('john')
        b = self.Block()
        b.author = u
        l = self.Line()
        l.block = b
        l.author = u
        self.session.save(u)
        self.session.save(b)
        self.session.save(l)
        self.session.flush()
        self.session.refresh(b)
        self.session.refresh(u)
        self.assertEquals(b.lines, 1)
        self.assertEquals(b.lastline, l.id)
        self.assertEquals(u.lines, 1)
        self.assertEquals(u.blocks, 1)

    def testAddMoreLines(self):
        u = self.User('john')
        self.save(u)
        b = self.Block()
        b.lines = 0
        b.author = u
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b
            l.author = u
            self.session.save(l)
        self.session.flush()
        self.session.refresh(b)
        self.session.refresh(u)
        self.assertEquals(b.lines, 10)
        self.assertEquals(b.lastline, l.id)
        self.assertEquals(u.lines, 10)
        self.assertEquals(u.blocks, 1)

    def testDeleteLines(self):
        u = self.User('john')
        self.save(u)
        b = self.Block()
        b.lines = 0
        b.author = u
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b
            l.author = u
            self.session.save(l)
        self.session.flush()
        l = self.session.query(self.Line).filter_by(block=b).first()
        self.session.delete(l)
        self.session.flush()
        self.session.refresh(b)
        self.session.refresh(u)
        self.assertEquals(b.lines, 9)
        self.assertNotEquals(b.lastline, l.id)
        self.assertEquals(u.lines, 9)
        self.assertEquals(u.blocks, 1)

    def testTwoUsers(self):
        j = self.User('john')
        self.save(j)
        m = self.User('mike')
        self.save(m)
        b1 = self.Block()
        b1.lines = 0
        b1.author = j
        self.save(b1)
        b2 = self.Block()
        b2.lines = 0
        b2.author = m
        self.save(b2)
        b3 = self.Block()
        b3.lines = 0
        b3.author = m
        self.save(b3)
        for i in range(20):
            l = self.Line()
            l.block = [b1,b2,b3][i%3]
            l.author = ((i % 3) and j or m)
            self.session.save(l)
        self.session.flush()
        self.session.refresh(b1)
        self.session.refresh(b2)
        self.session.refresh(b3)
        self.session.refresh(j)
        self.session.refresh(m)
        self.assertEquals(b1.lines, 7)
        self.assertEquals(b2.lines, 7)
        self.assertEquals(b3.lines, 6)
        self.assertEquals(j.blocks, 1)
        self.assertEquals(m.blocks, 2)
        self.assertEquals(j.lines, 13)
        self.assertEquals(m.lines, 7)

class TestBigValue(testbase.TestBase):

    def setUp(self):
        super(TestBigValue, self).setUp()
        blocks = Table('blocks', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('firstline', Numeric(len(str(sys.maxint))+1, 0)),
            )
        lines = Table('lines', self.meta,
            Column('id', Numeric(len(str(sys.maxint))+1, 0), primary_key=True, autoincrement=True),
            Column('block', Integer, ForeignKey(blocks.c.id)),
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
        mapper(Line, lines,
            extension=self.aggregator_class(
                a.Min(blocks.c.firstline, lines.c.id),
            ))

    def testBigValue(self):
        b = self.Block()
        b.firstline = None
        self.save(b)
        l = self.Line()
        l.id = sys.maxint + 2
        l.block = b.id
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.firstline, sys.maxint+2)

    def testSmallValue(self):
        b = self.Block()
        b.firstline = None
        self.save(b)
        l = self.Line()
        l.id = -1
        l.block = b.id
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.firstline, -1)

class TestUpdates(testbase.TestBase):

    def setUp(self):
        super(TestUpdates, self).setUp()
        blocks = Table('blocks', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('minlength', Integer),
            Column('maxlength', Integer),
            )
        lines = Table('lines', self.meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('block', Integer, ForeignKey(blocks.c.id)),
            Column('length', Integer),
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
        mapper(Line, lines,
            extension=self.aggregator_class(
                a.Min(blocks.c.minlength, lines.c.length),
                a.Max(blocks.c.maxlength, lines.c.length),
            ))

    def testAddValue(self):
        b = self.Block()
        b.lines = 0
        self.save(b)
        for i in range(10):
            l = self.Line()
            l.block = b.id
            l.length = i
            self.session.save(l)
        l = self.Line()
        l.block = b.id
        l.length = 15
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.minlength, 0)
        self.assertEquals(b.maxlength, 15)
        l.length = 25
        self.session.flush()
        self.session.refresh(b)
        self.assertEquals(b.minlength, 0)
        self.assertEquals(b.maxlength, 25)
        l.length = -10
        self.session.flush()
        self.session.refresh(b)
        self.assertEquals(b.minlength, -10)
        self.assertEquals(b.maxlength, 9)
        l.length = 5
        self.session.flush()
        self.session.refresh(b)
        self.assertEquals(b.minlength, 0)
        self.assertEquals(b.maxlength, 9)

class SimpleTest2(SimpleTest, testbase.TestAccurateMixin):
    pass

class ComplexTest2(ComplexTest,testbase.TestAccurateMixin):
    pass

class RelationsTest2(RelationsTest, testbase.TestAccurateMixin):
    pass

class TestBigValue2(TestBigValue, testbase.TestAccurateMixin):
    pass

class TestUpdates2(TestUpdates, testbase.TestAccurateMixin):
    pass

if __name__ == '__main__':
    unittest.main()
