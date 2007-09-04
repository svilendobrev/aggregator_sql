#$Id$

from aggregation import _Aggregation, _Agg_1Target_1Source, _func_ifnull
from sqlalchemy import func
import operator

class Count( _Agg_1Target_1Source):
    """Count aggregation

    Special case, no real source column needed (issues count(*) which matches
    all corresponding rows. But column can be specified, then it will count
    non-null values.

    XXX Need support of latter in atomic updates
    """
    def __init__( me, target, filter_expr=None, source=None):
        _Agg_1Target_1Source.__init__( me, target, source=source, filter_expr=filter_expr)
    def setup_fkey( me, key, grouping_attribute):
        if me.source is None: me.source = key.parent
        _Agg_1Target_1Source.setup_fkey( me, key, grouping_attribute)

    _sqlfunc_ = func.count
    def _sqlfunc( me, arg):
        if not me.source: arg = '*'
        return me._sqlfunc_( arg)
    def oninsert( me, func_checker, instance):
        return me._target_expr + 1
    def ondelete( me, func_checker, instance):
        return me._target_expr - 1
    def onupdate( me, func_checker, instance):
        return ()


class Sum( _Agg_1Target_1Source):
    _sqlfunc = func.sum
    def oninsert( me, func_checker, instance):
        return me._target_expr + me.value( instance)
    def ondelete( me, func_checker, instance):
        return me._target_expr - me.oldv( instance)
    def onupdate( me, func_checker, instance):
        return me._target_expr - me.oldv( instance) + me.value( instance)

_func_if = getattr( func, 'if')

class Max( _Agg_1Target_1Source):
    _sqlfunc_name = 'max'
    _sqlfunc = func.max
    @staticmethod
    def _substitute_func( a,b):
        return _func_if( (a == None) | (a < b), b, a)
    _comparator4updins = operator.ge

    def _agg_func( me, func_checker, a, b):
        if func_checker( me._sqlfunc_name):
            return me._sqlfunc( _func_ifnull(a,b), b)
        else:
            return me._substitute_func( a,b)

    def oninsert( me, func_checker, instance):
        return me._agg_func( func_checker, me.target, me.value( instance))
    def onupdate( me, func_checker, instance):
        if me._comparator4updins( me.value( instance), me.oldv( instance)):
            return me.oninsert( func_checker, instance)
        else:
            return me.onrecalc( func_checker, instance, False)
    def ondelete( me, func_checker, instance):
        return me.onrecalc( func_checker, instance, True)
        #XXX is recalc needed only if curvalue==maxvalue, else nothing ?
        #e.g. if me.oldv( instance) == current_target_value: then onrecalc()
        #but no way to gt current_target_value...


class Min( Max):
    _sqlfunc_name = 'min'
    _sqlfunc = func.min
    @staticmethod
    def _substitute_func( a,b):
        return _func_if( (a == None) | (a > b), b, a)
    _comparator4updins = operator.le


def AverageSimple( target, source, target_count, filter_expr =None):
    return Sum( target, source, filter_expr=filter_expr), Count( target_count, filter_expr=filter_expr)

class Average( _Aggregation):
    """Average aggregation
    example of 1-source 2-target aggregation - does not calculate a single value!
    DIY, maybe a property( lambda me: me.sumname/me.countname ) -
    see make_property_getter() method.

    Does not do more than adding 2 separate aggregations (AverageSimple),
    but may save some comparisons. Whether this is worth...

    source - Column object which value will be aggregated
    target - Column object where to store sum of aggregation
    target_count - Column object where to store count of aggregation

    This same thing with Accurate mapping-method needs only one column -
    the average value - and no properties.
    """
    def __init__( me, target, source, target_count):
        me.sum = Sum( target, source)
        me.count = Count( target_count)
        assert target.table is target_count.table

    def make_property_getter( me):
        sumname = me.sum.target.name
        cntname = me.count.target.name
        return property( lambda o: getattr( o, sumname) / getattr( o, cntname))

    def setup_fkey( me, key, grouping_attribute):
        me.sum.setup_fkey( key, grouping_attribute)
        me.count.setup_fkey( key, grouping_attribute)
    target_table = property( lambda me: me.sum.target.table)

    def _combined( me, action, *a,**k):
        r = getattr( me.sum, action)( *a,**k)
        r.update( getattr( me.count, action)( *a,**k) )
        return r
    def oninsert( me, *a,**k):
        return me._combined( 'oninsert', *a,**k)
    def ondelete( me, *a,**k):
        return me._combined( 'ondelete', *a,**k)
    def onupdate( me, *a,**k):
        return me._combined( 'onupdate', *a,**k)
    def onrecalc( me, *a,**k):
        return me._combined( 'onrecalc', *a,**k)

class Average1( _Agg_1Target_1Source):
    """Average aggregation, always accurate = full sqlfunc
    source - Column object which value will be aggregated
    target - Column object where to store value of aggregation
    """
    _sqlfunc = func.avg
    oninsert = ondelete = onupdate = _Agg_1Target_1Source.onrecalc

# vim:ts=4:sw=4:expandtab
