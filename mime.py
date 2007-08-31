#$Id$
# -*- coding: cp1251 -*-

'''
a.Count( movies.c.count, and_(
 tags.c.table == "movies",
 SourceLink(tags.c.object_id) == Target( movies.c.id)
)

onrecalc/accurate + mapperext:
UPDATE movies SET tag_cnt = ( SELECT COUNT(*) FROM tags
 WHERE tags.table = "movies" AND tags.object_id = :movie_id)
WHERE movies.id = :movie_id

quick/atomic update + mapperext:
UPDATE movies SET tag_cnt = tag_cnt+1
WHERE movies.id = :movie_id

--------------------
a.Count(users.c.userpic_cnt, and_(
 Target(users.c.uid) == SourceLink(userpics.c.uid), #fkey
 SourcePlain(userpics.c.state) == "normal"
)

onrecalc/accurate + mapperext:
UPDATE users SET userpic_cnt = ( SELECT COUNT(*) FROM userpics
 WHERE :uid = userpics.uid AND userpics.state = "normal")
WHERE users.uid = :uid

quick/atomic update + mapperext:
UPDATE users SET userpic_cnt = userpic_cnt+1
WHERE users.uid = :uid AND :state = "normal"

Target(col):
 def get(cur_src_instance, inside_mapperext):
  if inside_mapperext: return col
  return value-of-corresponding-src-attribute( cur_src_instance)
#value-of-corresponding-src-attribute ??? whichone? its target here..
#check the peer-one marked as Source()? doable..

Source(col):
 def get(cur_src_instance, inside_mapperext):
  if not inside_mapperext: return col
  return value-of-corresp-src-attribute( cur_src_inst)
#ok, it is src here

'''

class _ColumnMimer( sql.expression.ColumnElement):
    def __init__( me, col):
        me.col = col
    def get( me, cur_src_instance, inside_mapperext =True):
        raise NotImplementedError
    #... plus all the colElement.miming here.. urgh

class Target( _ColumnMimer):
    def __init__( me, col):
        me.col = col
        me.parent = None
    def get( me, cur_src_instance, inside_mapperext =True):
        if inside_mapperext: return col
        return me.parent.source.get_corresponding_attribute( cur_src_instance)

class SourcePlain( _ColumnMimer):
    def get( me, cur_src_instance, inside_mapperext =True):
        if not inside_mapperext: return col
        return me.get_corresponding_attribute( cur_src_instance)
    def get_corresponding_attribute( me, cur_src_instance):
        corresp_attr_name = me.col.name     #proper way is: lookup in the mapper.properties...
        return getattr( cur_src_instance, corresp_attr_name)

class SourceLink( SourcePlain): pass

class ParentLinker( sql.util.NoColumnVisitor):
    def visit_column( me, col):
        if isinstance( col, SourceLink): me.source = col
        elif isinstance( col, Target): col.parent = me
    @classmethod
    def linkparent( klas, expr):
        klas().traverse( expr)

class Visitor( sql.util.AbstractClauseProcessor):
    def __init__( me, cur_src_instance, inside_mapperext =False):
        me.cur_src_instance = cur_src_instance
        me.inside_mapperext = inside_mapperext
    def convert_element( me, col):
        if isinstance( col, _ColumnMimer):
            return col.get( me.cur_src_instance, me.inside_mapperext)
        return None

# vim:ts=4:sw=4:expandtab
