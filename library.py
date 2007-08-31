#$Id$

from aggregation import _Aggregation, _Agg_1Target_1Source, _func_ifnull
from sqlalchemy import func
import operator

class Count( _Agg_1Target_1Source):
    'special case, no real source column needed - just source table, and any column in it'
    def __init__( self, target):
        _Aggregation.__init__( self)
        self.target = target
    source = property( lambda self: self.key.parent)
    _sqlfunc = func.count
    def oninsert( self, func_checker, instance):
        return self._target_expr + 1
    def ondelete( self, func_checker, instance):
        return self._target_expr - 1
    def onupdate( self, func_checker, instance):
        return ()


class Sum( _Agg_1Target_1Source):
    _sqlfunc = func.sum
    def oninsert( self, func_checker, instance):
        return self._target_expr + self.value( instance)
    def ondelete( self, func_checker, instance):
        return self._target_expr - self.oldv( instance)
    def onupdate( self, func_checker, instance):
        return self._target_expr - self.oldv( instance) + self.value( instance)

_func_if = getattr( func, 'if')

class Max( _Agg_1Target_1Source):
    _sqlfunc_name = 'max'
    _sqlfunc = func.max
    @staticmethod
    def _substitute_func( a,b):
        return _func_if( (a == None) | (a < b), b, a)
    _comparator4updins = operator.ge

    def _agg_func( self, func_checker, a, b):
        if func_checker( self._sqlfunc_name):
            return self._sqlfunc( _func_ifnull(a,b), b)
        else:
            return self._substitute_func( a,b)

    def oninsert( self, func_checker, instance):
        return self._agg_func( func_checker, self.target, self.value( instance))
    def onupdate( self, func_checker, instance):
        if self._comparator4updins( self.value( instance), self.oldv( instance)):
            return self.oninsert( func_checker, instance)
        else:
            return self.onrecalc( func_checker, instance, False)
    def ondelete( self, func_checker, instance):
        return self.onrecalc( func_checker, instance, True)
        #XXX is recalc needed only if curvalue==maxvalue, else nothing ?
        #e.g. if self.oldv( instance) == current_target_value: then onrecalc()
        #but no way to gt current_target_value...


class Min( Max):
    _sqlfunc_name = 'min'
    _sqlfunc = func.min
    @staticmethod
    def _substitute_func( a,b):
        return _func_if( (a == None) | (a > b), b, a)
    _comparator4updins = operator.le


def AverageSimple( target, source, target_count):
    return Sum( target, source), Count( target_count)

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

    def setup( self, key, grouping_attribute):
        self.sum.setup( key, grouping_attribute)
        self.count.setup( key, grouping_attribute)
    table = property( lambda self: self.sum.target.table)

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
    _sqlfunc = func.avg
    oninsert = ondelete = onupdate = _Agg_1Target_1Source.onrecalc

# vim:ts=4:sw=4:expandtab
