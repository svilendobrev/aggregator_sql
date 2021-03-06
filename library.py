#$Id$

from aggregation import _Aggregation, _Agg_1Target_1Source, Func, _func_ifnull, case
from sqlalchemy import func
import operator

class Count( _Agg_1Target_1Source):
    """Count aggregation

    Special case, no real source column needed (issues count(*) which matches
    all corresponding rows. But column can be specified, then it will count
    non-null values.

    XXX Need support of latter in atomic updates
    """
    def __init__( self, target, filter_expr=None, source=None):
        _Agg_1Target_1Source.__init__( self, target, source=source, filter_expr=filter_expr)
    def setup_fkey( self, key, grouping_attribute):
        if self.source is None: self.source = key.parent
        _Agg_1Target_1Source.setup_fkey( self, key, grouping_attribute)

    _sqlfunc4column = func.count
    def sqlfunc4column( self, arg):
        if not self.source: arg = '*'
        return self._sqlfunc4column( arg)
    def oninsert( self, func_checker, instance):
        return self.target_or_0( func_checker) + 1
    def ondelete( self, func_checker, instance):
        return self.target_or_0( func_checker) - 1
    def onupdate( self, func_checker, instance):
        return ()


class Sum( _Agg_1Target_1Source):
    _sqlfunc4column = func.sum
    def oninsert( self, func_checker, instance):
        return self.target_or_0( func_checker) + self.value( instance)
    def ondelete( self, func_checker, instance):
        return self.target_or_0( func_checker) - self.oldv( instance)
    def onupdate( self, func_checker, instance):
        return self.target_or_0( func_checker) - self.oldv( instance) + self.value( instance)

_func_if = Func( name= 'if',
                 replacement_expr= lambda a,b,c, **kignore: case( [(a, b)], else_=c)
            )

class Max( _Agg_1Target_1Source):
    _sqlfunc4column = func.max

    def _func_as_expr( a,b, **kargs):
        return _func_if( (a == None) | (a < b), b, a, **kargs)
        #return _func_if( (a == None) | (b != None) & (a < b), b, a, **kargs)

    _sqlfunc4args = Func( 'max', _func_as_expr)
    def sqlfunc4args( self, column, value, **kargs):
        'must be max(ifnull(a,b),ifnull(b,a)) but assume value is never None'
        assert value is not None
        return _Agg_1Target_1Source.sqlfunc4args( self,
            _func_ifnull( column, value, **kargs),
            value, #_func_ifnull( column, value, **kargs),
            **kargs)

    _comparator4updins = operator.ge
    def oninsert( self, func_checker, instance):
        return self.sqlfunc4args( self.target, self.value( instance),
                        func_checker= func_checker,
                        type= self.target.type )
    def onupdate( self, func_checker, instance):
        if self._comparator4updins( self.value( instance), self.oldv( instance)):
            return self.oninsert( func_checker, instance)
        return self.onrecalc( func_checker, instance, old=False)
    def ondelete( self, func_checker, instance):
        return self.onrecalc( func_checker, instance, old=True)
        #XXX is recalc needed only if curvalue==maxvalue, else nothing ?
        #e.g. if self.oldv( instance) == current_target_value: then onrecalc()
        #but no way to gt current_target_value...


class Min( Max):
    _sqlfunc4column = func.min
    def _func_as_expr( a,b, **kargs):
        return _func_if( (a == None) | (a > b), b, a, **kargs)
        #return _func_if( (a == None) | (b != None) & (a > b), b, a, **kargs)
    _sqlfunc4args = Func( 'min', _func_as_expr)
    _comparator4updins = operator.le


def AverageSimple( target, source, target_count, filter_expr =None):
    return Sum( target, source, filter_expr=filter_expr), Count( target_count, filter_expr=filter_expr)

class Average( _Aggregation):
    """Average aggregation
    example of 1-source 2-target aggregation - does not calculate a single value!
    DIY, maybe a property( lambda self: self.sumname/self.countname ) -
    see make_property_getter() method.

    Does not do more than adding 2 separate aggregations (AverageSimple),
    but may save some comparisons. Whether this is worth...

    source - Column object which value will be aggregated
    target - Column object where to store sum of aggregation
    target_count - Column object where to store count of aggregation

    This same thing with Accurate mapping-method needs only one column -
    the average value - and no properties.
    """
    def __init__( self, target, source, target_count):
        self.sum = Sum( target, source)
        self.count = Count( target_count)
        assert target.table is target_count.table

    def make_property_getter( self):
        sumname = self.sum.target.name
        cntname = self.count.target.name
        return property( lambda o: getattr( o, sumname) / getattr( o, cntname))

    def setup_fkey( self, key, grouping_attribute):
        self.sum.setup_fkey( key, grouping_attribute)
        self.count.setup_fkey( key, grouping_attribute)
    target_table = property( lambda self: self.sum.target.table)

    def _combined( self, action, *a,**k):
        r = getattr( self.sum, action)( *a,**k)
        r.update( getattr( self.count, action)( *a,**k) )
        return r
    def oninsert( self, *a,**k):
        return self._combined( 'oninsert', *a,**k)
    def ondelete( self, *a,**k):
        return self._combined( 'ondelete', *a,**k)
    def onupdate( self, *a,**k):
        return self._combined( 'onupdate', *a,**k)
    def onrecalc( self, *a,**k):
        return self._combined( 'onrecalc', *a,**k)

class Average1( _Agg_1Target_1Source):
    """Average aggregation, always accurate = full sqlfunc
    source - Column object which value will be aggregated
    target - Column object where to store value of aggregation
    """
    _sqlfunc4column = func.avg
    oninsert = ondelete = onupdate = _Agg_1Target_1Source.onrecalc

# vim:ts=4:sw=4:expandtab
