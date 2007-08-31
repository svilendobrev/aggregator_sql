#$Id$
# -*- coding: cp1251 -*-

case1 = '''
a.Count( movies.c.count, and_(
 SourceRecalcOnly( tags.c.table == "movies"),
 Source( tags.c.object_id) == Target( movies.c.id, corresp_src=tags.c.object_id)
)

onrecalc:
UPDATE movies SET tag_cnt = ( SELECT COUNT(*) FROM tags
 WHERE tags.table = "movies" AND tags.object_id = :cur_movie__id)
WHERE movies.id = :cur_movie__id

atomic:
UPDATE movies SET tag_cnt = tag_cnt+1
WHERE movies.id = :cur_movie__id
'''

case2 = '''
a.Count(users.c.userpic_cnt, and_(
 Target(users.c.uid, corresp_src=userpics.c.uid) == Source(userpics.c.uid), #fkey
 Source(userpics.c.state) == "normal"
)

onrecalc:
UPDATE users SET userpic_cnt = ( SELECT COUNT(*) FROM userpics
 WHERE :uid = userpics.uid AND userpics.state = "normal")
WHERE users.uid = :uid AND :state = "normal"

atomic:
UPDATE users SET userpic_cnt = userpic_cnt+1
WHERE users.uid = :uid AND :state = "normal"
'''


import sqlalchemy
import sqlalchemy.sql.util

class _ColumnMarker( object):
    def __new__( klas, col,**k):
        col.mark = mark = object.__new__( klas, col,**k)
        mark.__init__( col, **k)    #will not be called - see __new__ docs
        return col
    def __init__( me, col):
        me.col = col
    def get( me, inside_mapperext =True):
        raise NotImplementedError
    def __str__(me):
        return me.__class__.__name__+'#' + me.col.table.name+'.'+me.col.name
    def get_corresponding_attribute( me): #, cur_src_instance):
        corresp_attr_name = me.corresp_src_col.name
        #proper way is: look it up in the mapper.properties...
        return sqlalchemy.bindparam( corresp_attr_name)    #type?uniq?
    def _get( me, docol, **k):
        if docol: return me.col
        return me.get_corresponding_attribute( **k)

class Source( _ColumnMarker):
    corresp_src_col = property( lambda me: me.col)
    def get( me, inside_mapperext =True, **k):
        return me._get( docol=not inside_mapperext, **k)

class Target( _ColumnMarker):
    def __init__( me, col, corresp_src ='otherside'):
        me.col = col
        me.corresp_src_col = corresp_src
    def get( me, inside_mapperext =True, **k):
        return me._get( docol=inside_mapperext, **k)

class Converter( sqlalchemy.sql.util.AbstractClauseProcessor):
    def __init__( me, inside_mapperext =False, target_tbl =None, source_tbl =None): # cur_src_instance
        me.inside_mapperext = inside_mapperext
        me.target_tbl = target_tbl
        me.source_tbl = source_tbl
    def convert_element( me, e):
#        print e, type(e), id(e)
        if isinstance( e, sqlalchemy.Column):
            try:
                mark = e.mark
#                print mark
            except:
                if me.target_tbl and e.table == me.target_tbl:
                    mark = Target( e)
                elif me.source_tbl and e.table == me.source_tbl:
                    mark = Source( e)
                else: mark = None
            if mark:
                assert isinstance( mark, _ColumnMarker)
                return mark.get( me.inside_mapperext) #, cur_src_instance=me.cur_src_instance
        return None

    @classmethod
    def apply( klas, expr, **k):
        expr = klas(**k).traverse( expr, clone=True)
        return expr

def SourceRecalcOnly(x): return x

if __name__ == '__main__':
    from sqlalchemy import *
#    import aggregator as a
#    aa = []
    def Count( target, expr):
        print '---', expr
        for mext in (False,True):
            print mext and 'mapper' or 'recalc',
            #print '<<', expr
            e = Converter.apply( expr, inside_mapperext=mext)
            print '       >>', e
        print '==============='
#        aa.append( a.Count( target, expr) )

    m = MetaData( 'sqlite:///' )

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
    m.create_all()

# vim:ts=4:sw=4:expandtab
