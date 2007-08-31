#$Id$

'''80% remade by sdobrev.
it was:
Name: SQLAlchemyAggregator
Version: 0.1.2.dev-r779
Summary: SQLAlchemy's mapper extension which can automatically track changes in
         mapped instances and calculate aggregations based on them
Home-page: http://www.mr-pc.kiev.ua/en/projects/SQLAlchemyAggregator
Author: Paul Colomiets
Author-email: pc@gafol.net
'''

from sqlalchemy.orm import MapperExtension, EXT_CONTINUE
from sqlalchemy import func, select

_func_if = getattr( func, 'if')
_func_ifnull = func.ifnull
_func_max = func.max
_func_min = func.min

class _Aggregation( object):
    """Base class for aggregations. Some assumptions:
    - all target columns must be in same table (!)
    - event-methods (oninsert etc) have following interface for return result:
      -- a dict of target-column names/values
      -- or () for no change
      -- anything else is assumed a value, associated with self.target.name
"""
    def __init__( self):
        self.grouping_attribute = None
        self.key = None         #grouping-filter
    table = None

    def setup( self, key, grouping_attribute):
        self.key = key
        self.grouping_attribute = grouping_attribute

    @staticmethod
    def _orig( instance, attribute):
        """Returns original value of instance attribute
        Currently Raises KeyError if no original state exists (is this ok?)
        """
        return instance._sa_attr_state['original'].data[attribute]

    def _get_grouping_attribute( self, instance, old):
        """Return old or new value of the grouping_attribute based on `old` parameter
        """
        if old:
            return self._orig( instance, self.grouping_attribute)
        else:
            return getattr( instance, self.grouping_attribute)

    def onrecalc_old( self, aggregator, instance):
        return self.onrecalc( aggregator, instance, True)

###################

class _Agg_1Target_1Source( _Aggregation):
    def __init__( self, target, source):
        """aggregation of single source-column into single target-column

        target - Column object where to store value of aggregation
        source - Column object which value will be aggregated
        """
        _Aggregation.__init__( self)
        self.target = target
        self.source = source

    table = property( lambda self: self.target.table)
    _target_expr = property( lambda self: _func_ifnull( self.target, 0) )
    def _filter_expr( self, instance, old): return self.key.parent == self._get_grouping_attribute( instance, old)

    def value( self, instance): return getattr( instance, self.source.name)
    def oldv(  self, instance): return self._orig( instance, self.source.name)

    _sqlfunc = None
    def onrecalc( self, aggregator, instance, old =False):
        return select( [self._sqlfunc( self.source)], self._filter_expr( instance, old) )


class Count( _Agg_1Target_1Source):
    'special case, no real source column needed - just source table, and any column in it'
    def __init__( self, target):
        _Aggregation.__init__( self)
        self.target = target
    source = property( lambda self: self.key.parent)
    _sqlfunc = func.count
    def oninsert( self, aggregator, instance):
        return self._target_expr + 1
    def ondelete( self, aggregator, instance):
        return self._target_expr - 1
    def onupdate( self, aggregator, instance):
        return ()


class Sum( _Agg_1Target_1Source):
    _sqlfunc = func.sum
    def oninsert( self, aggregator, instance):
        return self._target_expr + self.value( instance)
    def ondelete( self, aggregator, instance):
        return self._target_expr - self.oldv( instance)
    def onupdate( self, aggregator, instance):
        return self._target_expr - self.oldv( instance) + self.value( instance)

import operator
class Max( _Agg_1Target_1Source):
    _sqlfunc = func.max
    _comparator4updins = operator.ge
    _aggregator4insert = 'max'
    def oninsert( self, aggregator, instance):
        return getattr( aggregator, self._aggregator4insert)( self.target, self.value( instance))
    def onupdate( self, aggregator, instance):
        if self._comparator4updins( self.value( instance), self.oldv( instance)):
            return self.oninsert( aggregator, instance)
        else:
            return self.onrecalc( aggregator, instance, False)
    def ondelete( self, aggregator, instance):
        return self.onrecalc( aggregator, instance, True)
        #XXX is recalc needed only if curvalue==maxvalue, else nothing ?
        #e.g. if self.oldv( instance) == current_target_value: then onrecalc()


class Min( Max):
    _sqlfunc = func.min
    _comparator4updins = operator.le
    _aggregator4insert = 'min'


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


################

class Quick( MapperExtension):
    """Mapper extension which maintains aggregations

    Quick extension does maximum it can without using aggregated queries,
    e.g. `cnt = cnt + 1`  instead of `cnt = (select count(*) from...)`
    see Accurate for those
    XXX Quick vs Accurate vs None may have to be switched at runtime ?
    e.g. mass updates may need one Accurate at the end
    """
    _insert_method = 'oninsert'
    _delete_method = 'ondelete'

    def __init__( self, *aggregations):
        """
        *aggregations
            _Aggregation-subclassed instances, specifying aggregations to maintain for this mapper
        """
        groups = {}     #group by target table... does order matter?
        for ag in aggregations:
            assert isinstance( ag, _Aggregation)
            groups.setdefault( ag.table,[]).append( ag)
        self.aggregations = groups

    def instrument_class( self, mapper, class_):
        self.local_table = table = mapper.local_table
        for (othertable, aggs) in self.aggregations.iteritems():
            for k in table.foreign_keys:
                if k.references( othertable):
                    break
            else:
                raise NotImplementedError( "No foreign key defined for pair %s %s" % (table, othertable))
            try:
                if mapper.properties[k.parent.name] != k.parent:
                    # Field is aliased somewhere
                    for (attrname, column) in mapper.properties.items():
                        if column is k.parent: # "==" works not as expected
                            grouping_attribute = attrname
                            break
                    else:
                        raise NotImplementedError( "Can't find property %s" % k.parent.name)
            except KeyError:
                grouping_attribute = k.parent.name
            for a in aggs:
                a.setup( k, grouping_attribute)
        return super( Quick, self).instrument_class( mapper, class_)

    def _make_updates( self, instance, action):
        for (table, fields) in self.aggregations.iteritems():
            self._make_change1( table, fields, instance, action)
        return EXT_CONTINUE

    def _make_change1( self, table, fields, instance, action, org_value_getter =getattr):
        updates = {}
        for f in fields:
            u = getattr( f, action)( self, instance)
            if isinstance( u, dict):
                updates.update( u)
            elif u is not ():
                updates[ f.target.name ] = u
        if updates:
            anyfield = fields[0]    # They all have same 'key' and 'grouping_attribute' attributes
            update_condition = anyfield.key.column == org_value_getter( instance, anyfield.grouping_attribute)
            table.update( update_condition, values=updates ).execute()

    def after_insert( self, mapper, connection, instance):
        """called after an object instance has been INSERTed"""
        return self._make_updates( instance, self._insert_method)

    def after_delete( self, mapper, connection, instance):
        """called after an object instance is DELETEed"""
        return self._make_updates( instance, self._delete_method)

    def after_update( self, mapper, connection, instance):
        """called after an object instance is UPDATEed"""
        for (table, fields) in self.aggregations.iteritems():
            grouping_attribute = fields[0].grouping_attribute
            if getattr( instance, grouping_attribute) == _Aggregation._orig( instance, grouping_attribute):
                self._make_change1( table, fields, instance, 'onupdate')
            else:
                self._make_change1( table, fields, instance,  self._delete_method, _Aggregation._orig)
                self._make_change1( table, fields, instance,  self._insert_method, getattr)
        return EXT_CONTINUE

    def max( self, a, b):
        if self.local_table.metadata.bind.url.drivername == 'mysql':
            return _func_if( (a == None) | (a < b), b, a)
        else:
            return _func_max( _func_ifnull(a,b), b)

    def min( self, a, b):
        if self.local_table.metadata.bind.url.drivername == 'mysql':
            return _func_if( (a == None) | (a > b), b, a)
        else:
            return _func_min( _func_ifnull(a,b), b)

class Accurate( Quick):
    """Mapper extension which maintains aggregations

    Accurate extension does all calculations using aggregated
    query at every update of related fields
    """
    _insert_method = 'onrecalc'
    _delete_method = 'onrecalc_old'

# vim:ts=4:sw=4:expandtab
