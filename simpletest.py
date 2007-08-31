import sys
sys.path.insert(0, '.')
if len(sys.argv) > 1 and '://' in sys.argv[1]:
    dburl = sys.argv[1]
    del sys.argv[1]
else:
    dburl = "sqlite:///:memory:"

import unittest
import aggregator as a
from sqlalchemy import (
    Column, ForeignKey,
    Integer, String, Numeric,
    Table, MetaData,
    )
from sqlalchemy.orm import (
    create_session, mapper, relation,
    )

class SimpleTest(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Quick
        return super(SimpleTest, self).__init__(arg)

    def setUp(self):
        meta = self.meta = MetaData(bind=dburl)
        self.session = create_session()
        blocks = Table('blocks', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('lines', Integer),
            Column('lastline', Integer),
            Column('length', Integer),
            )
        lines = Table('lines', meta,
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
                a.Max(blocks.c.lastline, lines.c.id),
                a.Count(blocks.c.lines),
                a.Sum(blocks.c.length, lines.c.length),
            ))

    def tearDown(self):
        self.lines.drop()
        self.blocks.drop()
        
    def save(self, ob):
        self.session.save(ob)
        self.session.flush()

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
        l.length = 25
        self.save(l)
        self.session.refresh(b)
        self.assertEquals(b.lines, 11)
        self.assertEquals(b.length, 70)

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
        

class ComplexTest(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Quick
        return super(ComplexTest, self).__init__(arg)

    def setUp(self):
        meta = self.meta = MetaData(bind=dburl)
        self.session = create_session()
        users = Table('users', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('name', String),
            Column('blocks', Integer),
            Column('lines', Integer),
            )
        blocks = Table('blocks', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('author', Integer, ForeignKey(users.c.id)),
            Column('lines', Integer),
            Column('lastline', Integer),
            )
        lines = Table('lines', meta,
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
        blocks.create()
        lines.create()
        users.create()
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

    def tearDown(self):
        self.lines.drop()
        self.blocks.drop()
        self.users.drop()
        
    def save(self, ob):
        self.session.save(ob)
        self.session.flush()

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
        l = self.session.query(self.Line).get_by(block = b.id)
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

class RelationsTest(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Quick
        return super(RelationsTest, self).__init__(arg)

    def setUp(self):
        meta = self.meta = MetaData(bind=dburl)
        self.session = create_session()
        users = Table('users', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('name', String),
            Column('blocks', Integer),
            Column('lines', Integer),
            )
        blocks = Table('blocks', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('author', Integer, ForeignKey(users.c.id)),
            Column('lines', Integer),
            Column('lastline', Integer),
            )
        lines = Table('lines', meta,
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
        blocks.create()
        lines.create()
        users.create()
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

    def tearDown(self):
        self.lines.drop()
        self.blocks.drop()
        self.users.drop()
        
    def save(self, ob):
        self.session.save(ob)
        self.session.flush()

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
        l = self.session.query(self.Line).get_by(block = b)
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
        
class TestBigValue(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Quick
        return super(TestBigValue, self).__init__(arg)

    def setUp(self):
        meta = self.meta = MetaData(bind=dburl)
        self.session = create_session()
        blocks = Table('blocks', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('firstline', Numeric(len(str(sys.maxint))+1, 0)),
            )
        lines = Table('lines', meta,
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

    def tearDown(self):
        self.lines.drop()
        self.blocks.drop()
        
    def save(self, ob):
        self.session.save(ob)
        self.session.flush()

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

class TestUpdates(unittest.TestCase):
    def __init__(self, arg):
        self.aggregator_class = a.Quick
        return super(TestUpdates, self).__init__(arg)

    def setUp(self):
        meta = self.meta = MetaData(bind=dburl)
        self.session = create_session()
        blocks = Table('blocks', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('minlength', Integer),
            Column('maxlength', Integer),
            )
        lines = Table('lines', meta,
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

    def tearDown(self):
        self.lines.drop()
        self.blocks.drop()
        
    def save(self, ob):
        self.session.save(ob)
        self.session.flush()
    
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
        

class SimpleTest2(SimpleTest):
    def __init__(self, arg):
        self.aggregator_class = a.Accurate
        return unittest.TestCase.__init__(self, arg)

class ComplexTest2(ComplexTest):
    def __init__(self, arg):
        self.aggregator_class = a.Accurate
        return unittest.TestCase.__init__(self, arg)

class RelationsTest2(ComplexTest):
    def __init__(self, arg):
        self.aggregator_class = a.Accurate
        return unittest.TestCase.__init__(self, arg)

class TestBigValue2(TestBigValue):
    def __init__(self, arg):
        self.aggregator_class = a.Accurate
        return unittest.TestCase.__init__(self, arg)

class TestUpdates2(TestUpdates):
    def __init__(self, arg):
        self.aggregator_class = a.Accurate
        return unittest.TestCase.__init__(self, arg)

if __name__ == '__main__':
    unittest.main()
