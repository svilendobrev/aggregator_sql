#$Id$

'''
Name: SQLAlchemyAggregator
Summary: SQLAlchemy's mapper extension which can automatically track changes in
         mapped instances and calculate aggregations based on them
Home-page: http://www.mr-pc.kiev.ua/en/projects/SQLAlchemyAggregator
Authors: Paul Colomiets <pc@gafol.net>, Svilen Dobrev <svilen_dobrev@sourceforge.net>
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
      -- anything else is assumed a value, associated with me.target.name

public virtual methods/attributes - must be overloaded:
    target_table = None

    def oninsert( me, func_checker, instance):
    def ondelete( me, func_checker, instance):
    def onupdate( me, func_checker, instance):
    def onrecalc( me, func_checker, instance, old =False):
     func_checker( funcname) will return True if the func is supported by db

    _filter_expr = None
    def filter_expr( me, instance, old):
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

    def onrecalc_old( me, func_checker, instance):
        return me.onrecalc( func_checker, instance, True)

###################
FKEY_NEW = 1

class _Agg_1Target_1Source( _Aggregation):
    def __init__( me, target, source, filter_expr =None, corresp_src_cols ={}):
        """aggregation of single source-column into single target-column
        target - Column object where to store value of aggregation
        source - Column object which value will be aggregated
        """
        me.target = target
        me.source = source
        if filter_expr:
            from convert_expr import Converter
            for filterattr_name, ismapperext in dict( _filter4recalc=False, _filter4mapper=True).iteritems():
                res = Converter.apply( filter_expr,
                        inside_mapperext= ismapperext,
                        target_tbl= target.table,
                        source_tbl= source and source.table or None,
                        corresp_src_cols= corresp_src_cols
                    )
                setattr( me, filterattr_name, res)

            #used for comparison when combining with other aggregations
            me._filter_expr = filter_expr


    target_table = property( lambda me: me.target.table)

    _target_expr = property( lambda me: _func_ifnull( me.target, 0) )

    def value( me, instance): return getattr( instance, me.source.name)
    def oldv(  me, instance): return me._orig( instance, me.source.name)

    _filter_expr = None
    _filter4recalc = None
    _filter4mapper = None
    def get_filter_and_bindings( me, (fexpr,bindings), instance, old):
        'return either with var-bindparams, or const-bound-bindparams (value= getattr(instance))'
        if callable( fexpr): fexpr = fexpr( instance, old)
        vbindings = dict( (k,me._get_current_or_orig( instance, k, old)) for k in bindings)
        return fexpr, vbindings
    #def _get_bindings( me, bindings, instance, old):
    #    return dict( (k,me._get_current_or_orig( instance, k, old)) for k in bindings)
    def _same_binding_values( me, bindings, instance):
        _orig = me._orig
        for k in bindings:
            if _orig( instance, k) != getattr( instance, k):
                return False
        return True

    _sqlfunc = None     #do overload
    def onrecalc( me, func_checker, instance, old =False):
        fexpr,vbindings = me.get_filter_and_bindings( me._filter4recalc, instance, old)
        return select( [me._sqlfunc( me.source) ], fexpr ), vbindings

    def setup_fkey( me, key, grouping_attribute):
        'used as fallback if no other filters are setup'
        me._filter4recalc = (
                (key.parent == bindparam( grouping_attribute)),
                ( grouping_attribute, )
            )
        me._filter4mapper = (
                (key.column == bindparam( grouping_attribute)),
                ( grouping_attribute, )
            )
        #the getattr(instance, name, old) part is done in aggregator/mapperext

    if not FKEY_NEW:
        def setup_fkey( me, key, grouping_attribute):
            me.grouping_attribute = grouping_attribute
            me.key = key
            me._filter4recalc = me._filter4recalc4foreignkey, ()
            me._filter4mapper = me._filter4mapper4foreignkey, ()
        def _filter4recalc4foreignkey( me, instance, old):
            return me.key.parent == me._get_grouping_attribute( instance, old)
        def _filter4mapper4foreignkey( me, instance, old):
            return me.key.column == me._get_grouping_attribute( instance, old)
        def _get_grouping_attribute( me, instance, old):
            return me._get_current_or_orig( instance, me.grouping_attribute, old)



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

    def __init__( me, *aggregations):
        """ *aggregations - _Aggregation-subclass instances, to be maintained for this mapper
        """
        me.off = False
        me.aggregations_by_table = groups = dict()

        #here combined by target table
        for ag in aggregations:
            assert isinstance( ag, _Aggregation)
            groups.setdefault( ag.target_table, [] ).append( ag)

    def instrument_class( me, mapper, class_):
        me.local_table = table = mapper.local_table
        me.aggregations = groups = dict()     #combined by table,filter
        for (target_table, aggs) in me.aggregations_by_table.iteritems():
            for a in aggs:
                if a._filter_expr is None:
                    fkey, src_attribute = me.find_fkey( table, target_table, mapper)
                    a.setup_fkey( fkey, src_attribute)
                    groups.setdefault( (target_table, fkey), [] ).append( a)    #not a._filter_expr
                else:
                    groups.setdefault( (target_table, a._filter_expr), [] ).append( a)
                #here re-combined by target_table+filter
                #later, for ags on same key, only ags[0]._filter* is used
        return super( Quick, me).instrument_class( mapper, class_)

    def find_fkey( me, table, target_table, mapper):
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

    def _make_updates( me, instance, action):
        if not me.off:
            for aggs in me.aggregations.itervalues():
                me._make_change1( aggs, instance, action)
        return EXT_CONTINUE

    def _make_change1( me, aggs, instance, action, old =False):
        updates = dict()
        bindings = dict()
        func_checker = me._db_supports
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

    def after_insert( me, mapper, connection, instance):
        """called after an object instance has been INSERTed"""
        return me._make_updates( instance, me._insert_method)

    def after_delete( me, mapper, connection, instance):
        """called after an object instance is DELETEed"""
        return me._make_updates( instance, me._delete_method)

    def after_update( me, mapper, connection, instance):
        """called after an object instance is UPDATEed"""
        if not me.off:
            for aggs in me.aggregations.itervalues():
                ag = aggs[0]    # They all have same table/filters
                if FKEY_NEW:
                    bindings = ag._filter4mapper[1]
                    same = ag._same_binding_values( bindings, instance)
                else:
                    grouping_attribute = ag.grouping_attribute
                    same = getattr( instance, grouping_attribute) == _Aggregation._orig( instance, grouping_attribute)

                if same:
                    me._make_change1( aggs, instance, 'onupdate')
                else:
                    me._make_change1( aggs, instance,  me._delete_method, old=True)
                    me._make_change1( aggs, instance,  me._insert_method)
        return EXT_CONTINUE

    def _db_supports( me, funcname):
        'called back by aggregation-calculators'
        if me.local_table.metadata.bind.url.drivername == 'mysql':
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
