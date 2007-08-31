#$Id$

'''
Name: SQLAlchemyAggregator
Version: 0.1.2.dev-r779
Summary: SQLAlchemy's mapper extension which can automatically track changes in
         mapped instances and calculate aggregations based on them
Home-page: http://www.mr-pc.kiev.ua/en/projects/SQLAlchemyAggregator
Author: Paul Colomiets
Author-email: pc@gafol.net


now 90% remade by svilen_dobrev@sourceforge.net
'''

from sqlalchemy.orm import MapperExtension, EXT_CONTINUE
from sqlalchemy import func, select, bindparam
_func_ifnull = func.ifnull
if 0*'test: repeatability and less noise':
    import sqlalchemy, logging
    dict = sqlalchemy.util.OrderedDict
    format ='* SA: %(levelname)s %(message)s'
    logging.basicConfig( format= format, stream= logging.sys.stdout)
    sqlalchemy.logging.default_enabled= True    #else, default_logging() will setFormatter...

class _Aggregation( object):
    """Base class for aggregations. Some assumptions:
    - all target columns must be in same table (!)
    - event-methods (see below - oninsert etc) has this interface/rules for return result:
      -- () for no change
      -- tuple (result, bindings-dict), result is then checked wih next rules
      -- a dict of target-column names/values
      -- anything else is assumed a value, associated with self.target.name

public virtual methods/attributes - must be overloaded:
    target_table = None

    def oninsert( self, func_checker, instance):
    def ondelete( self, func_checker, instance):
    def onupdate( self, func_checker, instance):
    def onrecalc( self, func_checker, instance, old =False):
     func_checker( funcname) will return True if the func is supported by db

    _filter_expr = None
    def filter_expr( self, instance, old):
        raise NotImplementedError
        return filter_condition
            #either with var-bindparams, or const-bindparams (getattr from instance)
"""

    @staticmethod
    def _orig( instance, attribute):
        """Returns original value of instance attribute;
        Raises KeyError if no original state exists
        """
        return instance._sa_attr_state['original'].data[attribute]

    @staticmethod
    def _get_current_or_orig( instance, attribute, old):
        """Return old or new value of the attribute, according to `old` parameter
        """
        if old: return _Aggregation._orig( instance, attribute)
        return getattr( instance, attribute)

    def onrecalc_old( self, func_checker, instance):
        return self.onrecalc( func_checker, instance, True)

###################
FKEY_NEW = 1

class _Agg_1Target_1Source( _Aggregation):
    def __init__( self, target, source):
        """aggregation of single source-column into single target-column
        target - Column object where to store value of aggregation
        source - Column object which value will be aggregated
        """
        _Aggregation.__init__( self)
        self.target = target
        self.source = source

    target_table = property( lambda self: self.target.table)

    _target_expr = property( lambda self: _func_ifnull( self.target, 0) )

    def value( self, instance): return getattr( instance, self.source.name)
    def oldv(  self, instance): return self._orig( instance, self.source.name)

    _filter4recalc = None
    _filter4mapper = None
    def get_filter_and_bindings( self, (fexpr,bindings), instance, old):
        'return either with var-bindparams, or const-bound-bindparams (value= getattr(instance))'
        if callable( fexpr): fexpr = fexpr( instance, old)
        vbindings = dict( (k,self._get_current_or_orig( instance, k, old)) for k in bindings)
        return fexpr, vbindings
    #def _get_bindings( self, bindings, instance, old):
    #    return dict( (k,self._get_current_or_orig( instance, k, old)) for k in bindings)
    def _same_binding_values( self, bindings, instance):
        _orig = self._orig
        for k in bindings:
            if _orig( instance, k) != getattr( instance, k):
                return False
        return True

    _sqlfunc = None     #do overload
    def onrecalc( self, func_checker, instance, old =False):
        fexpr,vbindings = self.get_filter_and_bindings( self._filter4recalc, instance, old)
        return select( [self._sqlfunc( self.source) ], fexpr ), vbindings

    def setup_fkey( self, key, grouping_attribute):
        'used as fallback if no other filters are setup'
        if FKEY_NEW:
            self._filter4recalc = (
                    (key.parent == bindparam( grouping_attribute)),
                    ( grouping_attribute, )
                )
            self._filter4mapper = (
                    (key.column == bindparam( grouping_attribute)),
                    ( grouping_attribute, )
                )
            #the getattr(instance, name, old) part is done in aggregator/mapperext
        else:
            self.grouping_attribute = grouping_attribute
            self.key = key
            self._filter4recalc = self._filter4recalc4foreignkey, ()
            self._filter4mapper = self._filter4mapper4foreignkey, ()
    def _filter4recalc4foreignkey( self, instance, old):
        return self.key.parent == self._get_grouping_attribute( instance, old)
    def _filter4mapper4foreignkey( self, instance, old):
        return self.key.column == self._get_grouping_attribute( instance, old)
    def _get_grouping_attribute( self, instance, old):
        return self._get_current_or_orig( instance, self.grouping_attribute, old)





################

class Quick( MapperExtension):
    """Mapper extension which maintains aggregations.

    Quick does maximum it can without using aggregated queries,
    e.g. `cnt = cnt + 1`  instead of `cnt = (select count(*) from...)`
    see Accurate for those

    XXX Quick vs Accurate vs None may have to be switched at runtime ?
    e.g. mass updates may need one Accurate at the end
    """
    _insert_method = 'oninsert'
    _delete_method = 'ondelete'

    def __init__( self, *aggregations):
        """ *aggregations - _Aggregation-subclass instances, to be maintained for this mapper
        """
        self.off = False
        self.aggregations_by_table = groups = dict()

        #here combined by target table
        for ag in aggregations:
            assert isinstance( ag, _Aggregation)
            groups.setdefault( ag.target_table, [] ).append( ag)

    def instrument_class( self, mapper, class_):
        self.local_table = table = mapper.local_table
        self.aggregations = groups = dict()     #combined by table,filter
        for (target_table, aggs) in self.aggregations_by_table.iteritems():
            for a in aggs:
                if a._filter4mapper is None:
                    fkey, src_attribute = self.find_fkey( table, target_table, mapper)
                    a.setup_fkey( fkey, src_attribute)
                    groups.setdefault( (target_table, fkey), [] ).append( a)    #not a._filter_expr
                else:
                    groups.setdefault( (target_table, a._filter4mapper), [] ).append( a)
                #here re-combined by target_table+filter
                #later, for ags on same key, only ags[0]._filter* is used
        return super( Quick, self).instrument_class( mapper, class_)

    def find_fkey( self, table, target_table, mapper):
        for k in table.foreign_keys:
            #pick first one - maybe fail if there are more
            if k.references( target_table):
                break
        else:
            raise NotImplementedError( "No foreign key defined for pair %s %s" % (table, target_table))

        try:
            if mapper.properties[ k.parent.name] != k.parent:
                # Field is aliased somewhere
                for (attrname, column) in mapper.properties.iteritems():
                    if column is k.parent: # "==" works not as expected
                        grouping_attribute = attrname
                        break
                else:
                    raise NotImplementedError( "Can't find property %s" % k.parent.name)
        except KeyError:
            grouping_attribute = k.parent.name

        return k, grouping_attribute

    def _make_updates( self, instance, action):
        if not self.off:
            for aggs in self.aggregations.itervalues():
                self._make_change1( aggs, instance, action)
        return EXT_CONTINUE

    def _make_change1( self, aggs, instance, action, old =False):
        updates = dict()
        bindings = dict()
        func_checker = self._db_supports
        for a in aggs:
            u = getattr( a, action)( func_checker, instance)

            if u is (): continue
            if isinstance( u,tuple) and len(u)==2 and isinstance( u[1],dict):
                expr,vbindings = u
                u = expr
                bindings.update( vbindings)

            if isinstance( u, dict): updates.update( u)
            else: updates[ a.target.name ] = u

        if updates:
            ag = aggs[0]    # They all have same table/filters
            fexpr,vbindings = ag.get_filter_and_bindings( ag._filter4mapper, instance, old)
            bindings.update( vbindings)
            ag.target_table.update( fexpr, values=updates ).execute( **bindings)

    def after_insert( self, mapper, connection, instance):
        """called after an object instance has been INSERTed"""
        return self._make_updates( instance, self._insert_method)

    def after_delete( self, mapper, connection, instance):
        """called after an object instance is DELETEed"""
        return self._make_updates( instance, self._delete_method)

    def after_update( self, mapper, connection, instance):
        """called after an object instance is UPDATEed"""
        if not self.off:
            for aggs in self.aggregations.itervalues():
                ag = aggs[0]    # They all have same table/filters
                if FKEY_NEW:
                    bindings = ag._filter4mapper[1]
                    same = ag._same_binding_values( bindings, instance)
                else:
                    grouping_attribute = ag.grouping_attribute
                    same = getattr( instance, grouping_attribute) == _Aggregation._orig( instance, grouping_attribute)

                if same:
                    self._make_change1( aggs, instance, 'onupdate')
                else:
                    self._make_change1( aggs, instance,  self._delete_method, old=True)
                    self._make_change1( aggs, instance,  self._insert_method)
        return EXT_CONTINUE

    def _db_supports( self, funcname):
        'called back by aggregation-calculators'
        if self.local_table.metadata.bind.url.drivername == 'mysql':
            return funcname not in ('max','min')
        return True


class Accurate( Quick):
    """Mapper extension which maintains aggregations.
    Accurate does all calculations using aggregated
    query at every update of related fields
    """
    _insert_method = 'onrecalc'
    _delete_method = 'onrecalc_old'

# vim:ts=4:sw=4:expandtab
