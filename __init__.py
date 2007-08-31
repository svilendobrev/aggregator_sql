from sqlalchemy.orm.mapper import MapperExtension, EXT_CONTINUE
from sqlalchemy import func, select
from warnings import warn
import sys
from weakref import proxy

if_func = getattr(func, 'if')

class _Aggregation(object):
    """Base class for aggregations"""
    def __init__(self, target):
        """Aggregation constructor
        
        target
            Field object which specifies where to store
            value of aggregation
        """
        self.target = target
    
    table = property(lambda self: self.target.table)
    
    @staticmethod
    def _orig(instance, attribute):
        """Returns original value of instance attribute
        
        Currently returns:
            instance._sa_attr_state['original'].data[attribute]
        
        Raises KeyError if no original exists (may be
        should be fixed in future)
        """
        return instance._sa_attr_state['original'].data[attribute]

    @classmethod
    def _instanceattr(C, instance, attribute, old):
        """Return old or new value of the attribute based on `old` parameter
        
        """
        if old:
            return C._orig(instance, attribute)
        else:
            return getattr(instance, attribute)
    
    def onrecalc_old(self, aggregator, instance):
        return self.onrecalc(aggregator, instance, True)

class Count(_Aggregation):

    def oninsert(self, aggregator, instance):
        return {self.target.name: func.ifnull(self.target, 0) + 1}

    def ondelete(self, aggregator, instance):
        return {self.target.name: func.ifnull(self.target, 0) - 1}

    def onrecalc(self, aggregator, instance, old=False):
        return {self.target.name: select([func.count(self.key.parent)],
            self.key.parent == self._instanceattr(instance, self.attribute, old))}
    
    def onupdate(self, aggregator, instance):
        return {}

class Sum(_Aggregation):
    def __init__(self, target, source):
        """Sum aggregation constructor
        
        target
            Field object which specifies where to store
            value of aggregation
        source
            Field object which value will be summed
        """
        super(Sum, self).__init__(target)
        self.source = source

    def oninsert(self, aggregator, instance):
        return {self.target.name: func.ifnull(self.target, 0) + getattr(instance, self.source.name)}

    def ondelete(self, aggregator, instance):
        return {self.target.name: func.ifnull(self.target, 0) - self._orig(instance, self.source.name)}

    def onupdate(self, aggregator, instance):
        return {self.target.name:
            func.ifnull(self.target, 0)
                - self._orig(instance, self.source.name)
                + getattr(instance, self.source.name)}

    def onrecalc(self, aggregator, instance, old=False):
        return {self.target.name: select([func.sum(self.source)],
            self.key.parent == self._instanceattr(instance, self.attribute, old))}

class Max(_Aggregation):
    def __init__(self, target, source):
        """Max aggregation constructor
        
        target
            Field object which specifies where to store
            value of aggregation
        source
            Field object which value will be candidate for
            maximum
        """
        super(Max, self).__init__(target)
        self.source = source

    def oninsert(self, aggregator, instance):
        return { self.target.name: aggregator.max(self.target, getattr(instance, self.source.name)) }
        
    def onrecalc(self, aggregator, instance, old=False):
        return {self.target.name: select([func.max(self.source)],
            self.key.parent == self._instanceattr(instance, self.attribute, old))}
    
    def onupdate(self, aggregator, instance):
        if getattr(instance, self.source.name) >= self._orig(instance, self.source.name):
            return self.oninsert(aggregator, instance)
        else:
            return self.onrecalc(aggregator, instance, False)

    def ondelete(self, aggregator, instance):
        return self.onrecalc(aggregator, instance, True)


class Min(_Aggregation):
    def __init__(self, target, source):
        """Max aggregation constructor
        
        target
            Field object which specifies where to store
            value of aggregation
        source
            Field object which value will be candidate for
            minimum
        """
        super(Min, self).__init__(target)
        self.source = source

    def oninsert(self, aggregator, instance):
        return { self.target.name: aggregator.min(self.target, getattr(instance, self.source.name)) }

    def onrecalc(self, aggregator, instance, old=False):
        return {self.target.name: select([func.min(self.source)],
            self.key.parent == self._instanceattr(instance, self.attribute, old))}

    def onupdate(self, aggregator, instance):
        if getattr(instance, self.source.name) <= self._orig(instance, self.source.name):
            return self.oninsert(aggregator, instance)
        else:
            return self.onrecalc(aggregator, instance, False)

    def ondelete(self, aggregator, instance):
        return self.onrecalc(aggregator, instance, True)


classes = {
    'max': Max,
    'min': Min,
    'count': Count,
    }

class Quick(MapperExtension):
    """Mapper extension which maintains aggregations
    
    Quick extension does maximum it can't without
    aggregated queries, e.g. `cnt = cnt + 1`  instead
    of `cnt = (select count(*) from...)`
    """
    _insert_method = 'oninsert'
    _delete_method = 'ondelete'
    def __init__(self, *aggregations):
        """Initialization method
        
        *aggregations
            Aggregation subclasses which specify what
            type of aggregations must be maintained
        table
            table which holds instances
        """
        groups = {}
        for ag in aggregations:
            groups.setdefault(ag.table,[]).append(ag)
        self.aggregations = groups

    def instrument_class(self, mapper, class_):
        self.mapper = proxy(mapper) # to avoid GC cycles
        self.attributes = {}
        table = mapper.local_table
        for (othertable, aggs) in self.aggregations.items():
            for k in table.foreign_keys:
                if k.references(othertable):
                    break
            else:
                raise NotImplementedError("No foreign key defined for pair %s %s" % (table, othertable))
            try:
                if mapper.properties[k.parent.name] != k.parent:
                    # Field is aliased somewhere
                    for (attrname, column) in mapper.properties.items():
                        if column is k.parent: # "==" works not as expected
                            attribute = attrname
                            break
                    else:
                        raise NotImplementedError("Can't find property %s" % k.parent.name)
            except KeyError:
                attribute = k.parent.name
            for a in aggs:
                a.key = k
                a.attribute = attribute
        return super(Quick, self).instrument_class(mapper, class_)
        
    def _make_updates(self, instance, action):
        for (table, fields) in self.aggregations.items():
            anyfield = fields[0] # They all have same 'key' and 'attribute' attributes
            update_condition = anyfield.key.column == getattr(instance, anyfield.attribute)
            updates = {}
            for f in fields:
                updates.update(getattr(f, action)(self, instance))
            table.update(update_condition, values=updates).execute()
        return EXT_CONTINUE

    def after_insert(self, mapper, connection, instance):
        """called after an object instance has been INSERTed"""
        return self._make_updates(instance, self._insert_method)

    def after_delete(self, mapper, connection, instance):
        """called after an object instance is DELETEed"""
        return self._make_updates(instance, self._delete_method)
    
    def after_update(self, mapper, connection, instance):
        """called after an object instance is UPDATEed"""
        for (table, fields) in self.aggregations.items():
            anyfield = fields[0]
            if getattr(instance, anyfield.attribute) == instance._sa_attr_state['original'].data[anyfield.attribute]:
                update_condition = anyfield.key.column == getattr(instance, anyfield.attribute)
                updates = {}
                for f in fields:
                    updates.update(f.onupdate(self, instance))
                table.update(update_condition, values=updates).execute()
            else:
                condition_delete = anyfield.key.column == instance._sa_attr_state['original'].data[anyfield.attribute]
                condition_insert = anyfield.key.column == getattr(instance, anyfield.attribute)
                updates_delete = {}
                updates_insert = {}
                for f in fields:
                    updates_delete.update(getattr(f, self._delete_method)(self, instance))
                    updates_insert.update(getattr(f, self._insert_method)(self, instance))
                table.update(condition_delete, values=updates_delete).execute()
                table.update(condition_insert, values=updates_insert).execute()
    
    def max(self, a, b):
        if self.mapper.local_table.metadata.bind.url.drivername == 'mysql':
            return if_func((a == None)|(a < b), b, a)
        else:
            return func.max(func.ifnull(a,b-1), b)

    def min(self, a, b):
        if self.mapper.local_table.metadata.bind.url.drivername == 'mysql':
            return if_func((a == None)|(a > b), b, a)
        else:
            return func.min(func.ifnull(a,b+1), b)

class Accurate(Quick):
    """Mapper extension which maintains aggregations
    
    Accurate extension does all calculations using aggregated
    query at every update of related fields
    """
    _insert_method = 'onrecalc'
    _delete_method = 'onrecalc_old'
