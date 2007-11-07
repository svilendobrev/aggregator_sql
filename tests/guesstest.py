import testbase
import aggregator as a
import unittest
import simpletest
from sqlalchemy import *
from sqlalchemy.orm import create_session, mapper, relation

class SimpleTest3(simpletest.SimpleTest):
    def setUp(self):
        super(simpletest.SimpleTest, self).setUp() # Skip SimpleTest's setup
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

class TagsPerMovie(testbase.TestBase):
    def setUp(self):
        super(TagsPerMovie, self).setUp()
        tagging = self.tagging = Table('tagging', self.meta,
            Column('name', String(50), primary_key=True),
            Column('tbl', String(50), primary_key=True),
            Column('object_id', Integer, primary_key=True),
            )
        movies = self.movies = Table('movies', self.meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('tag_count', Integer),
            )
        events = self.events = Table('events', self.meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('tag_count', Integer),
            )
        self.meta.create_all()

        class Tagging(self.EasyInit):
            pass
        class Movie(self.EasyInit):
            pass
        class Event(self.EasyInit):
            pass

        self.Tagging = Tagging
        self.Movie = Movie
        self.Event = Event

        self.tagging_mapper = mapper(Tagging, tagging,
            extension=self.aggregator_class(
                a.Count(movies.c.tag_count,
                    (movies.c.id == tagging.c.object_id) & (tagging.c.tbl == 'movies')),
                a.Count(events.c.tag_count,
                    (events.c.id == tagging.c.object_id) & (tagging.c.tbl == 'events')),
                ))
        self.movie_mapper = mapper(Movie, movies)
        self.event_mapper = mapper(Event, events)
    def testInsert(self):
        m1 = self.Movie(name="The Bourne Ultimatum")
        m2 = self.Movie(name="Slip")
        e1 = self.Event(name="Suicide")
        e2 = self.Event(name="Celebration")
        self.save(m1, m2, e1, e2)
        self.save(
            self.Tagging(name="Triller", tbl="movies", object_id=m1.id),
            self.Tagging(name="Fiction", tbl="movies", object_id=m1.id),
            self.Tagging(name="Triller", tbl="movies", object_id=m2.id),
            self.Tagging(name="Triller", tbl="events", object_id=e1.id),
            self.Tagging(name="ForAdults", tbl="events", object_id=e1.id),
            self.Tagging(name="Happy", tbl="events", object_id=e2.id),
            self.Tagging(name="Smile", tbl="events", object_id=e2.id),
            self.Tagging(name="Good", tbl="events", object_id=e2.id),
            )
        self.refresh(m1, m2, e1, e2)
        self.assertEquals((2,1,2,3), tuple(o.tag_count for o in (m1,m2,e1,e2)))

class TestUserpics(testbase.TestBase):
    def setUp(self):
        super(TestUserpics, self).setUp()
        users = self.users = Table('users', self.meta,
            Column('id',  Integer, primary_key=True),
            Column('pic_count', Integer),
            Column('name', String(50)),
            )
        userpics = self.userpics = Table('userpics', self.meta,
            Column('uid', Integer, primary_key=True),
            Column('name', String(50), primary_key=True),
            Column('state', String(50)),
            )
        self.meta.create_all()

        class Userpic(self.EasyInit):
            pass
        class User(self.EasyInit):
            pass
        self.Userpic = Userpic
        self.User = User

        self.userpic_mapper = mapper(Userpic, userpics,
            extension = self.aggregator_class(
                a.Count(users.c.pic_count, (users.c.id == userpics.c.uid) & (userpics.c.state == "normal")),
                ))
        self.user_mapper = mapper(User, users)

    def testInsert(self):
        john = self.User(name="John")
        jeremy = self.User(name="Jeremy")
        self.save(john, jeremy)
        self.save(
            self.Userpic(uid=john.id, name="Happy", state="normal"),
            self.Userpic(uid=john.id, name="Sad", state="normal"),
            self.Userpic(uid=john.id, name="Working", state="normal"),
            self.Userpic(uid=jeremy.id, name="Happy", state="normal"),
            self.Userpic(uid=jeremy.id, name="Sad", state="deleted"),
            self.Userpic(uid=jeremy.id, name="Chatting", state="deleted"),
            )
        self.refresh(john, jeremy)
        self.assertEquals((3, 1), (john.pic_count, jeremy.pic_count))

class TestBlog(testbase.TestBase):
    def setUp(self):
        super(TestBlog, self).setUp()
        stats = self.stats = Table('stats', self.meta,
            Column( 'date', Date, primary_key=True),
            Column( 'posts_so_far', Integer),
            )
        blog = self.blog = Table('blog',  self.meta,
            Column( 'id',  Integer, primary_key=True),
            Column( 'date',  Date, index=True),
            Column( 'text', String),
            )
        self.meta.create_all()

        class BlogEntry(self.EasyInit):
            pass
        class StatRow(self.EasyInit):
            pass
        self.BlogEntry = BlogEntry
        self.StatRow = StatRow

        self.blog_mapper = mapper(BlogEntry, blog,
            extension=self.aggregator_class(
                a.Count(stats.c.posts_so_far, stats.c.date >= blog.c.date),
            ))
        self.stats_mapper = mapper(StatRow, stats)

    def testInsert(self):
        from datetime import date
        self.save(
            self.StatRow(date=date(2001,01,01)),
            self.StatRow(date=date(2001,01,02)),
            self.StatRow(date=date(2001,01,03)),
            )
        self.save(
            self.BlogEntry(date=date(2001,01,01), text="I've born"),
            self.BlogEntry(date=date(2001,01,02), text="I can speak"),
            self.BlogEntry(date=date(2001,01,02), text="Wow, I can walk too!"),
            self.BlogEntry(date=date(2001,01,03), text="I'm not human :)"),
            )
        self.session.clear()
        d1 = self.session.query(self.StatRow).get(date(2001,01,01))
        d2 = self.session.query(self.StatRow).get(date(2001,01,02))
        d3 = self.session.query(self.StatRow).get(date(2001,01,03))
        self.assertEquals((1,3,4), (d1.posts_so_far, d2.posts_so_far, d3.posts_so_far))


class SimpleTest4(SimpleTest3, testbase.TestAccurateMixin):
    pass
class TagsPerMovie2(TagsPerMovie, testbase.TestAccurateMixin):
    pass
class TestUserpics2(TestUserpics, testbase.TestAccurateMixin):
    pass
class TestBlog2(TestBlog, testbase.TestAccurateMixin):
    pass

if __name__ == '__main__':
    unittest.main()
