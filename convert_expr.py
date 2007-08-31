#$Id$
# -*- coding: cp1251 -*-

'''Converts a sqlalchemy expression for usage in aggregator's recalc- and
mapperext-update- level filtering.

The 'corresp_src_col' notion of some target column is used to obtain the name
of an attribute in the source instance which value is to be put (via bindparm)
instead of that column in the expression. This is not usualy needed - see note below.

Usage:
    new_expr,src_attrs4mapper = Converter.apply( input_expr, inside_mapperext=boolean, ..)
src_attrs4mapper is list of src-instance's attribute names to give as bindparams
to specify which is what, in the input_expr, either of these can be used:
 - wrap columns in the expression with Source() and/or Target( corresp_src=)
    markers, These have higher priority than table-based guessing;
 - if source_tbl and/or target_tbl is specified, applies table-based guessing of
    respectively source/target columns; needs the global corresp_src_cols.
in either case a global corresp_src_cols dict{ target_col:src_col } may be specified.
To replace a subexpressoin with True for mapperext-filter, wrap it with SourceRecalcOnly()

Note: The corresp_src_cols is needed in internal onrecalc select, but as it is
correlated to the external, any matching Target columns will be as of external
one, which would have correct values.
Hence no need to replace it with some value, hence no need to mark it at all.
Hence the default Target( corresp_src_col ) is now None, and target-table guessing
will mark as Target only if corresp_src_col is specified in the dict.



'''
_no_more = '''
To avoid some target column being replaced with corresp_src_col, do one of:
    - dont use target_tbl guessing AND dont mark it as Target
    - mark it as Target with corresp_src=None
    - specify entry in overall corresp_src_cols for that column to be None
'''

import sqlalchemy
import sqlalchemy.sql.util

class _ColumnMarker( object):
    def __str__(me):
        return me.__class__.__name__+'#' + me.col.table.name+'.'+me.col.name
    def get_corresponding_attribute( me):
        corresp_attr_name = me.corresp_src_col.name
        #proper way is: look it up in the mapper.properties...
        return sqlalchemy.bindparam( corresp_attr_name, type_= me.corresp_src_col.type), corresp_attr_name

    #ret_col_inside_mapperext = ..
    def get( me, inside_mapperext, **k):
        if me.ret_col_inside_mapperext == inside_mapperext or me.corresp_src_col is None:
            return me.col, None
        return me.get_corresponding_attribute( **k)


class _Source( _ColumnMarker):
    def __init__( me, col):
        me.col = col
    corresp_src_col = property( lambda me: me.col)
    ret_col_inside_mapperext = False

class _Target( _ColumnMarker):
    def __init__( me, col, corresp_src =None): #'otherside'):
        me.col = col
        me.corresp_src_col = corresp_src
    ret_col_inside_mapperext = True

class Converter( sqlalchemy.sql.util.AbstractClauseProcessor):
    def __init__( me, inside_mapperext =False, target_tbl =None, source_tbl =None, corresp_src_cols ={}):
        me.inside_mapperext = inside_mapperext
        me.target_tbl = target_tbl
        me.source_tbl = source_tbl
        me.src_attrs4mapper = []
        me.corresp_src_cols = corresp_src_cols

    def convert_element( me, e):
        if getattr( e, 'SourceRecalcOnly', None) and me.inside_mapperext:
            return sqlalchemy.literal( True)

        if isinstance( e, sqlalchemy.Column):
            try:
                mark = e.mark
            except:
                mark = None
                if me.target_tbl and e.table == me.target_tbl:
                    corresp_src= me.corresp_src_cols.get( e, None)
                    if corresp_src: mark = _Target( e, corresp_src)
                elif me.source_tbl and e.table == me.source_tbl:
                    mark = _Source( e)
            if mark:
                assert isinstance( mark, _ColumnMarker)
                col,src_attrs4mapper = mark.get( me.inside_mapperext)
                if src_attrs4mapper and src_attrs4mapper not in me.src_attrs4mapper:
                    me.src_attrs4mapper.append( src_attrs4mapper)
                return col
        return None

    @classmethod
    def apply( klas, expr, **k):
        c = klas(**k)
        expr = c.traverse( expr, clone=True)
        return expr, c.src_attrs4mapper

def Source( col, **k):
    col.mark = _Source( col,**k)
    return col
def Target( col, **k):
    col.mark = _Target( col,**k)
    return col
def SourceRecalcOnly( expr):
    expr.SourceRecalcOnly = True
    return expr

##############

if __name__ == '__main__':

    from sqlalchemy import MetaData, select, and_, Table, Column, Integer, String, Numeric, Date, func
    import sys, StringIO
    class IO2( StringIO.StringIO):
        stdout = None
#        stdout = sys.stdout
        def write( me, *a,**k):
            if me.stdout: me.stdout.write( *a,**k)
            return StringIO.StringIO.write( me,*a,**k)
    import unittest

    def Count( target, expr, source_tbl =None, guess_target_tbl =True, corresp_src_cols ={}, out =None):
        if not out:
            out = sys.stdout
            print '==============='
        print >>out, '---', expr
        for mext in (False,True):
            print >>out, mext and 'mapper' or 'recalc',
            e,src_attrs4mapper = Converter.apply( expr, inside_mapperext=mext,
                    target_tbl= guess_target_tbl and target.table or None,
                    source_tbl= source_tbl,
                    corresp_src_cols= corresp_src_cols
                )
            print >>out, '  >>', e
            print >>out, '   src_attr', src_attrs4mapper
        return out


    class T_mark( unittest.TestCase):
        def setUp(me):
            me.m = MetaData()
            me.out = IO2()
        def check( me, expect):
            result = me.out.getvalue().strip().replace(' \n','\n')
            expect = expect.strip()
            if result != expect:
                import difflib
                diff = 1 and difflib.unified_diff or difflib.context_diff
                for l in diff( expect.splitlines(1), result.splitlines(1), 'expect', 'result'):
                    print l,
            me.assertEquals( result, expect.strip() )

        Source = staticmethod( Source)
        Target = staticmethod( Target)

        def test1_count_tags_per_movie( me):
            if 0: print '''
count tags per movie: source,target, subclause4recalconly, and, ==
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
            Count( movies.c.count, and_(
                SourceRecalcOnly( tags.c.tabl == "movies"),
                me.Source( tags.c.oid) == movies.c.id
            ), source_tbl=tags, out=me.out )
            me.check( expect = '''
--- tags.tabl = :const('movies') AND tags.oid = movies.id
recalc   >> tags.tabl = :const('movies') AND tags.oid = movies.id
   src_attr []
mapper   >> :const(True) AND :BindParam(oid) = movies.id
   src_attr ['tabl', 'oid']
''')
#was this, but internal select is correlated to external/target, hence no need to touch movies.id
#recalc   >> tags.tabl = :const('movies') AND tags.oid = :BindParam(oid)
#   src_attr ['oid']

        def test2_count_userpics_per_user(me):
            if 0: print '''
count userpics per user: source,target, const, and, ==; #fkey possible
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
            Count( users.c.count, and_(
                users.c.id == me.Source( userpics.c.uid), #fkey
                me.Source( userpics.c.state) == "normal"
            ), source_tbl=userpics, out=me.out)
            me.check( expect = '''
--- users.id = userpics.uid AND userpics.state = :const('normal')
recalc   >> users.id = userpics.uid AND userpics.state = :const('normal')
   src_attr []
mapper   >> users.id = :BindParam(uid) AND :BindParam(state) = :const('normal')
   src_attr ['uid', 'state']
''')
#was this, but internal select is correlated to external/target, hence no need to touch movies.id
#recalc   >> :BindParam(uid) = userpics.uid AND userpics.state = :const('normal')
#   src_attr ['uid']

        def test3_count_posts_before_date( me):
            if 0: print '''
count posts before date in blog: source,target-not-replaceable, >=
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
            Count( stats.c.posts_so_far,
                stats.c.date1 >= me.Source( blog.c.date),
                source_tbl= blog, out=me.out )
            me.check( expect = '''
--- stats.date1 >= blog.date
recalc   >> stats.date1 >= blog.date
   src_attr []
mapper   >> stats.date1 >= :BindParam(date)
   src_attr ['date']
''')


        def test4_balance_trans_via_prev_balance_date_subselect( me):
            if 0: print '''
sum balances of transactions per account/date-range; use prev.balance.date as startdate: source,target-not-replaceable, and, <=, subselect
update balance set total = (select sum(trans.money)
    where trans.account like balance.account+'%'
          AND trans.date <= balance.finaldate
          AND trans.date > (SELECT MAX(finaldate) FROM balance as b WHERE b.finaldate < balance.finaldate)
    )
where srctrans.account like balance.account+'%'
    AND srctrans.date <= balance.finaldate
    AND srctrans.date > (SELECT MAX(finaldate) FROM balance as b WHERE b.finaldate < balance.finaldate)
        #the subselect is to get previous_balance.finaldate. Needs fix if no previous_balance!
'''
            trans  =Table( 'trans',   me.m, Column( 'date', Date),     Column( 'account', String), Column( 'money', Numeric) )
            balance=Table( 'balance', me.m, Column('finaldate', Date), Column( 'account', String), Column( 'total', Numeric) )
            b = balance.alias('b')
            Count( balance.c.total, and_(
                me.Source( trans.c.account).startswith( balance.c.account),
                me.Source( trans.c.date) <= balance.c.finaldate,
                        trans.c.date  <= select( [ func.max( b.c.finaldate)],
                                                   b.c.finaldate < balance.c.finaldate
                                               ).correlate( balance)
            ), source_tbl=trans, out=me.out )
            me.check( expect = '''
--- trans.account LIKE balance.account || :const('%') AND trans.date <= balance.finaldate AND trans.date <= (SELECT max(b.finaldate)
FROM balance AS b
WHERE b.finaldate < balance.finaldate)
recalc   >> trans.account LIKE balance.account || :const('%') AND trans.date <= balance.finaldate AND trans.date <= (SELECT max(b.finaldate)
FROM balance AS b
WHERE b.finaldate < balance.finaldate)
   src_attr []
mapper   >> :BindParam(account) LIKE balance.account || :const('%') AND :BindParam(date) <= balance.finaldate AND :BindParam(date) <= (SELECT max(b.finaldate)
FROM balance AS b
WHERE b.finaldate < balance.finaldate)
   src_attr ['account', 'date']
''')

        def test5_balance_trans_via_prev_balance_date_subselect( me):
            if 0: print '''
sum balances of transactions per account/date-range; with separate startdate: source,target-not-replaceable, and, <=
update balance set total = (select sum(trans.money)
    where trans.account like balance.account+'%'
          AND trans.date <= balance.finaldate AND trans.date >  balance.startdate
    )
where srctrans.account like balance.account+'%'
    AND srctrans.date <= balance.finaldate AND srctrans.date >  balance.startdate
## startdate === previous balance.finaldate
'''
            trans  =Table( 'trans',   me.m, Column( 'date', Date),     Column( 'account', String), Column( 'money', Numeric) )
            balance=Table( 'balance', me.m, Column('finaldate', Date), Column( 'account', String), Column( 'total', Numeric), Column('startdate', Date) )
            Count( balance.c.total, and_(
                me.Source( trans.c.account).startswith( balance.c.account),
                me.Source( trans.c.date) <= balance.c.finaldate,
                        trans.c.date  <= balance.c.startdate
            ), source_tbl=trans, out=me.out )
            me.check( expect = '''
--- trans.account LIKE balance.account || :const('%') AND trans.date <= balance.finaldate AND trans.date <= balance.startdate
recalc   >> trans.account LIKE balance.account || :const('%') AND trans.date <= balance.finaldate AND trans.date <= balance.startdate
   src_attr []
mapper   >> :BindParam(account) LIKE balance.account || :const('%') AND :BindParam(date) <= balance.finaldate AND :BindParam(date) <= balance.startdate
   src_attr ['account', 'date']
''')


    class T_table_guess( T_mark):
        Source = Table = staticmethod( lambda x,**k: x)


    #hack for better visibility
    def bp( self,bindparam):
        return (bindparam.value is None and 'BindParam('+bindparam.key or 'const('+repr(bindparam.value) )+')'
    from sqlalchemy.sql.compiler import DefaultCompiler
    DefaultCompiler._truncate_bindparam = bp

#    test( Source, Target)
#    def fake(x, **k): return x
#    test( fake, fake)
    unittest.main()
# vim:ts=4:sw=4:expandtab
