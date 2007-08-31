#$Id$
# -*- coding: cp1251 -*-

case1 = '''
a.Count( movies.c.count, and_(
 SourceRecalcOnly( tags.c.table == "movies"),
 Source( tags.c.object_id) == Target( movies.c.id, corresp_src=tags.c.object_id)
)
    ##onrecalc:
UPDATE movies SET tag_cnt = ( SELECT COUNT(*) FROM tags
 WHERE tags.table = "movies" AND tags.object_id = :cur_movie__id)
WHERE movies.id = :cur_movie__id
    ##atomic:
UPDATE movies SET tag_cnt = tag_cnt+1
WHERE movies.id = :cur_movie__id
'''

case2 = '''
a.Count(users.c.userpic_cnt, and_(
 Target(users.c.uid, corresp_src=userpics.c.uid) == Source(userpics.c.uid), #fkey
 Source(userpics.c.state) == "normal"
)
    ##onrecalc:
UPDATE users SET userpic_cnt = ( SELECT COUNT(*) FROM userpics
 WHERE :uid = userpics.uid AND userpics.state = "normal")
WHERE users.uid = :uid AND :state = "normal"
    ##atomic:
UPDATE users SET userpic_cnt = userpic_cnt+1
WHERE users.uid = :uid AND :state = "normal"
'''


import sqlalchemy
import sqlalchemy.sql.util

class _ColumnMarker( object):
    def __new__( klas, col,**k):
        col.mark = mark = object.__new__( klas, col,**k)
        mark.__init__( col, **k)    #will not be auto-called - see __new__ docs
        return col
    def __init__( me, col):
        me.col = col
    def __str__(me):
        return me.__class__.__name__+'#' + me.col.table.name+'.'+me.col.name
    def get_corresponding_attribute( me):
        corresp_attr_name = me.corresp_src_col.name
        #proper way is: look it up in the mapper.properties...
        return sqlalchemy.bindparam( corresp_attr_name), corresp_attr_name

    #ret_col_inside_mapperext = ..
    def get( me, inside_mapperext, **k):
        if me.ret_col_inside_mapperext == inside_mapperext or me.corresp_src_col is None:
            return me.col, None
        return me.get_corresponding_attribute( **k)

class Source( _ColumnMarker):
    corresp_src_col = property( lambda me: me.col)
    ret_col_inside_mapperext = False

class Target( _ColumnMarker):
    def __init__( me, col, corresp_src ='otherside'):
        me.col = col
        me.corresp_src_col = corresp_src
    ret_col_inside_mapperext = True

class Converter( sqlalchemy.sql.util.AbstractClauseProcessor):
    def __init__( me, inside_mapperext =False, target_tbl =None, source_tbl =None): # cur_src_instance
        me.inside_mapperext = inside_mapperext
        me.target_tbl = target_tbl
        me.source_tbl = source_tbl
        me.src_attrs4mapper = []
    def convert_element( me, e):
        if getattr( e, 'SourceRecalcOnly', None) and me.inside_mapperext:
            return sqlalchemy.literal( True)

        if isinstance( e, sqlalchemy.Column):
            try:
                mark = e.mark
            except:
                if me.target_tbl and e.table == me.target_tbl:
                    mark = Target( e)
                elif me.source_tbl and e.table == me.source_tbl:
                    mark = Source( e)
                else: mark = None
            if mark:
                assert isinstance( mark, _ColumnMarker)
                col,src_attrs4mapper = mark.get( me.inside_mapperext)
                if src_attrs4mapper:
                    me.src_attrs4mapper.append( src_attrs4mapper)
                return col
        return None

    @classmethod
    def apply( klas, expr, **k):
        c = klas(**k)
        expr = c.traverse( expr, clone=True)
        return expr, c.src_attrs4mapper

def SourceRecalcOnly(x):
    x.SourceRecalcOnly = True
    return x

if __name__ == '__main__':
    from sqlalchemy import *
#    import aggregator as a
#    aa = []
    def Count( target, expr):
        print '---', expr
        for mext in (False,True):
            print mext and 'mapper' or 'recalc',
            #print '<<', expr
            e,src_attrs4mapper = Converter.apply( expr, inside_mapperext=mext)
            print '  >>', e
            print '   src_attr', src_attrs4mapper
        print '==============='
#        aa.append( a.Count( target, expr) )

    m = MetaData() # 'sqlite:///' )

    #hack for better visibility
    def bp( self,bindparam):
        return (bindparam.value is None and 'BindParam('+bindparam.key or 'const('+repr(bindparam.value) )+')'
    from sqlalchemy.sql.compiler import DefaultCompiler
    DefaultCompiler._truncate_bindparam = bp

    if 10:
        print case1
        tags   = Table( 'tags',   m, Column( 'oid', Integer), Column( 'tabl',  String),  Column( 'tag', String), )
        movies = Table( 'movies', m, Column( 'id',  Integer), Column( 'count', Integer), Column( 'name', String) )
        Count( movies.c.count, and_(
            SourceRecalcOnly( tags.c.tabl == "movies"),
            Source( tags.c.oid) == Target( movies.c.id, corresp_src=tags.c.oid)
        ) )

    if 10:
        print case2
        users   = Table( 'users',    m, Column( 'id',  Integer), Column( 'count', Integer), Column( 'name', String), )
        userpics= Table( 'userpics', m, Column( 'uid', Integer), Column( 'state', String), )
        Count( users.c.count, and_(
            Target( users.c.id, corresp_src=userpics.c.uid) == Source( userpics.c.uid), #fkey
            Source( userpics.c.state) == "normal"
        ) )

    if 10:
        print '''
a.Count( stats.c.posts_so_far, stats.c.date >= blog.c.date)
    ## onrecalc:
UPDATE stats SET stats.posts_so_far = (SELECT COUNT(*) FROM blog
 WHERE stats.c.date >= blog.c.date)
WHERE stats.c.date >= :blog_date
    ## atomic:
UPDATE stats SET stats.posts_so_far = stats.posts_so_far + 1
WHERE stats.c.date >= :blog_date

'''
        stats = Table( 'stats', m, Column( 'date1', Date), Column( 'posts_so_far', Integer), )
        blog  = Table( 'blog',  m, Column( 'date',  Date), Column( 'text', String), )
        Count( stats.c.posts_so_far,
            Target( stats.c.date1, corresp_src=None) >= Source( blog.c.date)
        )

#    m.create_all()

# vim:ts=4:sw=4:expandtab
