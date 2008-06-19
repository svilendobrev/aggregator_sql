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

_v03 = False
_bindparam = sqlalchemy.bindparam
try:
    from sqlalchemy.sql.util import AbstractClauseProcessor as _ClauseProcessor #<v4335
    def traverse_clone( c, e): return c.traverse( e, clone=True)
except ImportError, e:
    try:
        from sqlalchemy.sql.visitors import ReplacingCloningVisitor as _ClauseProcessor #SA0.5
        def traverse_clone( c, e): return c.traverse( e)
    except ImportError, e:
        try:
            from sqlalchemy.sql.visitors import ClauseVisitor as _ClauseProcessor #>=v4335, SA0.4
            def traverse_clone( c, e): return c.traverse( e, clone=True)
        except ImportError, e:
            print e
            from sqlalchemy.sql_util import AbstractClauseProcessor as _ClauseProcessor #SA0.3
            _v03 = True
            def _bindparam( *a, **kargs):
                return sqlalchemy.bindparam( type=kargs.pop('type_',None), *a, **kargs)

_pfx = 'agcnv_'    #needed to distinguish SA.bindparams and our own bindparams

class _ColumnMarker( object):
    def __str__(self):
        return self.__class__.__name__+'#' + self.col.table.name+'.'+self.col.name
    def get_corresponding_attribute( self):
        corresp_attr_name = self.corresp_src_col.name
        #proper way is: look it up in the mapper.properties...
        return _bindparam( _pfx+corresp_attr_name, type_= self.corresp_src_col.type), corresp_attr_name

    #ret_col_inside_mapperext = ..
    def get( self, inside_mapperext, **k):
        if self.ret_col_inside_mapperext == inside_mapperext or self.corresp_src_col is None:
            return self.col, None
        return self.get_corresponding_attribute( **k)

class _Source( _ColumnMarker):
    def __init__( self, col):
        self.col = self.corresp_src_col = col
    ret_col_inside_mapperext = False

class _Target( _ColumnMarker):
    def __init__( self, col, corresp_src =None):
        self.col = col
        self.corresp_src_col = corresp_src
    ret_col_inside_mapperext = True


class Converter( _ClauseProcessor):
    _pfx = _pfx
    def __init__( self, inside_mapperext =False, target_tbl =None, source_tbl =None, corresp_src_cols ={}):
        self.inside_mapperext = inside_mapperext
        self.target_tbl = target_tbl
        self.source_tbl = source_tbl
        self.src_attrs4mapper = []
        self.corresp_src_cols = corresp_src_cols
        _ClauseProcessor.__init__( self)

    mark_only = False
    def convert_element( self, e):
        if getattr( e, '_ag_recalconly', None) and self.inside_mapperext:
            if self.mark_only: return None      #>=r3727 anything replaced stops traversing inside that thing
            return sqlalchemy.literal( True)

        if isinstance( e, sqlalchemy.Column):
            mark = getattr( e, '_ag_mark', None)
            if mark is None:
                if self.target_tbl and e.table == self.target_tbl:
                    corresp_src= self.corresp_src_cols.get( e, None)
                    if corresp_src: mark = _Target( e, corresp_src)
                elif self.source_tbl and e.table == self.source_tbl:
                    mark = _Source( e)
            if mark:
                assert isinstance( mark, _ColumnMarker)
                col,src_attrs4mapper = mark.get( self.inside_mapperext)
                if src_attrs4mapper and src_attrs4mapper not in self.src_attrs4mapper:
                    self.src_attrs4mapper.append( src_attrs4mapper)
                if not self.mark_only:
                    return col
        return None
    replace = before_clone = convert_element

    @classmethod
    def apply( klas, expr, **k):
        c = klas(**k)
        if 1:   #>=r3727 anything replaced stops traversing inside that (original) thing
            c.mark_only = True
            traverse_clone( c, expr)
        c.mark_only = False
        expr = traverse_clone( c, expr)
        return expr, c.src_attrs4mapper

if _v03:
    def _copymarkers( self, *a,**k):
        newobj = self.old_copy_container(*a,**k)
        for a in ('_ag_mark', '_ag_recalconly'):
            m = getattr( self, a, None)
            if m is not None: setattr( newobj, a, m)
        return newobj
    for _klas in [ sqlalchemy.sql._UnaryExpression, sqlalchemy.sql._BinaryExpression,]:
        cc = _klas.old_copy_container = _klas.copy_container
        _klas.copy_container = _copymarkers

    _Converter = Converter
    class Converter( _Converter):
        @classmethod
        def apply( klas, expr, **k):
            c = klas(**k)
            expr = c.traverse( expr.copy_container() )    #sa0.3->copy_container etc..
            return expr, c.src_attrs4mapper

def Source( col, **k):
    col._ag_mark = _Source( col,**k)
    return col
def Target( col, **k):
    col._ag_mark = _Target( col,**k)
    return col
def SourceRecalcOnly( expr):
    expr._ag_recalconly = True
    return expr

# vim:ts=4:sw=4:expandtab
