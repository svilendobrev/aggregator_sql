#$Id$

'''
now 90% remade by svilen_dobrev@sourceforge.net

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

_func_ifnull = func.ifnull

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



################

class Quick( MapperExtension):
    """Mapper extension which maintains aggregations

    Quick does maximum it can without using aggregated queries,
    e.g. `cnt = cnt + 1`  instead of `cnt = (select count(*) from...)`
    see Accurate for those

    XXX Quick vs Accurate vs None may have to be switched at runtime ?
    e.g. mass updates may need one Accurate at the end
    """
    _insert_method = 'oninsert'
    _delete_method = 'ondelete'

    def __init__( self, *aggregations):
        """ *aggregations - _Aggregation-subclassed instances, to be maintained for this mapper
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

    def db_supports( self, funcname):
        if self.local_table.metadata.bind.url.drivername == 'mysql':
            return funcname not in ('max','min')
        return True


class Accurate( Quick):
    """Mapper extension which maintains aggregations
    Accurate does all calculations using aggregated
    query at every update of related fields
    """
    _insert_method = 'onrecalc'
    _delete_method = 'onrecalc_old'

# vim:ts=4:sw=4:expandtab
