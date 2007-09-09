#$Id$

import unittest
from aggregator.convert_expr import *
from sqlalchemy import MetaData, select, and_, Table, Column, Integer, String, Numeric, Date, func
try:
    from sqlalchemy.sql.compiler import DefaultCompiler
    _concat = '||'
    _v03 = False
except ImportError:
    from sqlalchemy.ansisql import ANSICompiler as DefaultCompiler  #SA0.3
    _concat = '+'
    _v03 = True

class T_mark( unittest.TestCase):

    def Count( me, target, expr, source_tbl =None, corresp_src_cols ={} ):
        me.res = res = {}
        for mext in (False,True):
            res[mext] = Converter.apply( expr, inside_mapperext=mext,
                    target_tbl= target.table,
                    source_tbl= source_tbl,
                    corresp_src_cols= corresp_src_cols
                )

    def setUp( me):
        me.m = MetaData()
        #hack for better visibility
        def bp( me,bindparam):
            return (bindparam.value is None and 'BindParam('+bindparam.key or 'const('+repr(bindparam.value) )+')'
        me.old_bp = DefaultCompiler._truncate_bindparam
        DefaultCompiler._truncate_bindparam = bp

    def tearDown( me):
        DefaultCompiler._truncate_bindparam = me.old_bp

    Source = staticmethod( Source)
    Target = staticmethod( Target)

    def _checkClause( me, result, expect_expr, expect_attrs ):
        expr, attrs = result
        me.assertEquals( str(expr).replace(' \n', '\n'), expect_expr)
        me.assertEquals( attrs, expect_attrs)

    def check( me, mapper, recalc):
        me._checkClause( me.res[ False], *recalc)
        me._checkClause( me.res[ True],  *mapper)

    def test1_count_tags_per_movie( me):
        '''count tags per movie
                testing: source,target, subclause4recalconly, and, ==
        a.Count( movies.c.count, and_( tags.c.table == "movies", tags.c.object_id == movies.c.id))
        ##onrecalc:
        UPDATE movies SET tag_cnt = ( SELECT COUNT(*) FROM tags
        WHERE tags.table = "movies" AND tags.object_id = :cur_movie__id)
        WHERE movies.id = :cur_movie__id
        ##atomic:
        UPDATE movies SET tag_cnt = tag_cnt+1
        WHERE movies.id = :cur_movie__id
        '''
        tags   = Table( 'tags',   me.m, Column( 'oid', Integer), Column( 'tabl',  String),  Column( 'tag', String), )
        movies = Table( 'movies', me.m, Column( 'id',  Integer), Column( 'count', Integer), Column( 'name', String) )
        me.Count( movies.c.count, and_(
            SourceRecalcOnly( tags.c.tabl == "movies"),
            me.Source( tags.c.oid) == me.Target( movies.c.id, corresp_src=tags.c.oid)
        ), source_tbl=tags, corresp_src_cols= { movies.c.id: tags.c.oid })

        me.check( mapper= (':const(True) AND :BindParam(oid) = movies.id',
                          ['tabl', 'oid']),
                  recalc= ("tags.tabl = :const('movies') AND tags.oid = :BindParam(oid)",
                          ['oid'])
            )

    def test2_count_userpics_per_user( me):
        '''count userpics per user
                testing: source,target, const, and, ==; #fkey possible
        a.Count( users.c.userpic_cnt, and_( users.c.uid == userpics.c.uid, userpics.c.state == "normal")
        ##onrecalc:
        UPDATE users SET userpic_cnt = ( SELECT COUNT(*) FROM userpics
        WHERE :uid = userpics.uid AND userpics.state = "normal")
        WHERE users.uid = :uid AND :state = "normal"
        ##atomic:
        UPDATE users SET userpic_cnt = userpic_cnt+1
        WHERE users.uid = :uid AND :state = "normal"
        '''
        users   = Table( 'users',    me.m, Column( 'id',  Integer), Column( 'count', Integer), Column( 'name', String), )
        userpics= Table( 'userpics', me.m, Column( 'uid', Integer), Column( 'state', String), )
        me.Count( users.c.count, and_(
            me.Target( users.c.id, corresp_src=userpics.c.uid) == me.Source( userpics.c.uid), #fkey
            me.Source( userpics.c.state) == "normal"
        ), source_tbl=userpics, corresp_src_cols= { users.c.id: userpics.c.uid })

        me.check( recalc= (":BindParam(uid) = userpics.uid AND userpics.state = :const('normal')",
                          ['uid']),
                  mapper= ("users.id = :BindParam(uid) AND :BindParam(state) = :const('normal')",
                          ['uid','state'])
            )

    def test3_count_posts_before_date( me):
        '''count posts before date
                testing: source,target-not-replaceable, >=
        a.Count( stats.c.posts_so_far, stats.c.date >= blog.c.date)
        ## onrecalc:
        UPDATE stats SET stats.posts_so_far = (SELECT COUNT(*) FROM blog
        WHERE stats.c.date >= blog.c.date)
        WHERE stats.c.date >= :blog_date
        ## atomic:
        UPDATE stats SET stats.posts_so_far = stats.posts_so_far + 1
        WHERE stats.c.date >= :blog_date
        '''
        stats = Table( 'stats', me.m, Column( 'date1', Date), Column( 'posts_so_far', Integer), )
        blog  = Table( 'blog',  me.m, Column( 'date',  Date), Column( 'text', String), )
        me.Count( stats.c.posts_so_far,
            me.Target( stats.c.date1, corresp_src=None) >= me.Source( blog.c.date),
            source_tbl= blog, corresp_src_cols= { stats.c.date1: None })

        me.check( mapper= ('stats.date1 >= :BindParam(date)',
                          ['date']),
                  recalc= ('stats.date1 >= blog.date',
                          [])
            )


    def test4_balance_trans_via_prev_balance_date_subselect( me):
        '''sum balances of transactions per account/date-range; prev.balance.date as startdate
                testing: source,target-not-replaceable, and, <=, subselect
        update balance set total = (select sum(trans.money)
        where trans.account like balance.account+'%'
              AND trans.date <= balance.finaldate
              AND trans.date > (SELECT MAX(finaldate) FROM balance as b WHERE b.finaldate < balance.finaldate)
        )
        where srctrans.account like balance.account+'%'
        AND srctrans.date <= balance.finaldate
        AND srctrans.date > (SELECT MAX(finaldate) FROM balance as b WHERE b.finaldate < balance.finaldate)
            #the subselect is to get previous_balance.finaldate.
        Needs fix if no previous_balance!
        '''
        trans  =Table( 'trans',   me.m, Column( 'date', Date),      Column( 'account', String), Column( 'money', Numeric) )
        balance=Table( 'balance', me.m, Column( 'finaldate', Date), Column( 'account', String), Column( 'total', Numeric) )
        b = balance.alias('b')
        sprev = select( [ func.max( b.c.finaldate)],
                    b.c.finaldate < balance.c.finaldate
                )
        q = sprev.correlate( balance)   #non-generative in 0.3, hence separated
        if not _v03: sprev = q          #but generative in 0.4, hence overwrite

        me.Count( balance.c.total, and_(
            me.Source( trans.c.account).startswith( balance.c.account),
            me.Source( trans.c.date) <= balance.c.finaldate,
                        trans.c.date > func.coalesce( sprev,0 )
        ), source_tbl=trans)

        me.check( mapper= ('''\
:BindParam(account) LIKE balance.account '''+_concat+''' :const('%') \
AND :BindParam(date) <= balance.finaldate \
AND :BindParam(date) > coalesce((SELECT max(b.finaldate)
FROM balance AS b
WHERE b.finaldate < balance.finaldate), :const(0))''',
                          ['account','date']),
                  recalc= ('''\
trans.account LIKE balance.account '''+_concat+''' :const('%') \
AND trans.date <= balance.finaldate \
AND trans.date > coalesce((SELECT max(b.finaldate)
FROM balance AS b
WHERE b.finaldate < balance.finaldate), :const(0))''',
                          [])
            )

    def test5_balance_trans_via_prev_balance_date_subselect( me):
        '''sum balances of transactions per account/date-range; with separate startdate
                testing: source,target-not-replaceable, and, <=
        update balance set total = (select sum(trans.money)
        where trans.account like balance.account+'%'
              AND trans.date <= balance.finaldate AND trans.date >  balance.startdate
        )
        where srctrans.account like balance.account+'%'
        AND srctrans.date <= balance.finaldate AND srctrans.date >  balance.startdate
        ## startdate === previous balance.finaldate
        '''
        trans  =Table( 'trans',   me.m, Column( 'date', Date),      Column( 'account', String), Column( 'money', Numeric) )
        balance=Table( 'balance', me.m, Column( 'finaldate', Date), Column( 'account', String), Column( 'total', Numeric), Column('startdate', Date) )
        me.Count( balance.c.total, and_(
            me.Source( trans.c.account).startswith( balance.c.account),
            me.Source( trans.c.date) <= balance.c.finaldate,
                        trans.c.date > balance.c.startdate
        ), source_tbl=trans)

        me.check( mapper= ('''\
:BindParam(account) LIKE balance.account '''+_concat+''' :const('%') \
AND :BindParam(date) <= balance.finaldate \
AND :BindParam(date) > balance.startdate''',
                          ['account', 'date']),
                  recalc= ('''\
trans.account LIKE balance.account '''+_concat+''' :const('%') \
AND trans.date <= balance.finaldate \
AND trans.date > balance.startdate''',
                          [])
            )


class T_table_guess( T_mark):
    Source = Table = staticmethod( lambda x,**k: x)

if __name__ == '__main__':
    unittest.main()
# vim:ts=4:sw=4:expandtab
