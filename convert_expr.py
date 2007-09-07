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

import sqlalchemy
try:
    from sqlalchemy.sql.util import AbstractClauseProcessor
except ImportError:
    from sqlalchemy.sql_util import AbstractClauseProcessor #SA0.3

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
    def __init__( me, col, corresp_src =None):
        me.col = col
        me.corresp_src_col = corresp_src
    ret_col_inside_mapperext = True

class Converter( AbstractClauseProcessor):
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
        expr = c.traverse( expr, clone=True)    #sa0.3->copy_container etc..
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

# vim:ts=4:sw=4:expandtab
