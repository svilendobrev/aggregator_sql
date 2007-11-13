#$Id$

'''
Name: SQLAlchemyAggregator
Summary: SQLAlchemy's mapper xtension which can automatically track changes in
         mapped instances and calculate aggregations based on them
Home-page: http://www.mr-pc.kiev.ua/en/projects/SQLAlchemyAggregator
Authors: Paul Colomiets <pc@gafol.net>, Svilen Dobrev <svilen_dobrev@sourceforge.net>
'''

from sqlalchemy.orm import MapperExtension
try:
    from sqlalchemy.orm import EXT_CONTINUE
    _v03 = False
except ImportError:
    from sqlalchemy.orm import EXT_PASS as EXT_CONTINUE     #SA0.3
    _v03 = True

import sqlalchemy.orm.attributes
import warnings

#XXX no such thing as ifnull XXX - use coalesce, case, whatever
from sqlalchemy import func, select, bindparam, case
_func_ifnull = func.coalesce
#_func_ifnull = func.ifnull
#def _func_ifnull( a,b): return case( [(a==None, b)],else_=a)

if 0*'test: repeatability and less noise':
    import sqlalchemy, logging
    dict = sqlalchemy.util.OrderedDict
    format ='* SA: %(levelname)s %(message)s'
    logging.basicConfig( format= format, stream= logging.sys.stdout)
    sqlalchemy.logging.default_enabled= True    #else, default_logging() will setFormatter...



class _Aggregation( object):
    """Base class for aggregations. Some assumptions:
    - all target columns must be in same table (!)
    - event-methods (see below - oninsert etc) have this interface/rules for return result:
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

    filter_expr = ... #either with var-bindparams, or const-bindparams (getattr from instance)
"""

    if hasattr( sqlalchemy.orm.attributes, 'InstanceState'):    #>v3463
        @staticmethod
        def _orig( instance, attribute):
            """Returns original value of instance attribute;
            Raises KeyError if no original state exists
            """
            return instance._state.committed_state[ attribute]
    else:
        @staticmethod
        def _orig( instance, attribute):
            return instance._sa_attr_state[ 'original'].data[ attribute]


    @staticmethod
    def _get_current_or_orig( instance, attribute, old):
        """Return old or new value of the attribute, according to `old` parameter
        """
        if old: return _Aggregation._orig( instance, attribute)
        return getattr( instance, attribute)

    def onrecalc_old( self, func_checker, instance):
        return self.onrecalc( func_checker, instance, True)

###################
from convert_expr import Converter

class _Agg_1Target_1Source( _Aggregation):
    def __init__( self, target, source, filter_expr =None, corresp_src_cols ={}):
        """aggregation of single source-column into single target-column
        target - column where to store value of aggregation
        source - column which value will be aggregated
        both columns can be either sql.Column()s or respective class.instrumentedAttribute
        """
        if isinstance( source, sqlalchemy.orm.attributes.InstrumentedAttribute):
            source = source.property.columns[0]
        if isinstance( target, sqlalchemy.orm.attributes.InstrumentedAttribute):
            target = target.property.columns[0]
        self.target = target
        self.source = source
        self.filter_expr = filter_expr #also used for comparison when combining with other aggregations
        self.corresp_src_cols = corresp_src_cols

    def _initialize( self, default_table =None):
        if self.filter_expr:
            kargs = dict( expr= self.filter_expr,
                        target_tbl= self.target.table,
                        source_tbl= self.source and self.source.table or default_table, #None,
                        corresp_src_cols= self.corresp_src_cols,
                    )
            self._filter4recalc = Converter.apply( inside_mapperext= False, **kargs)
            self._filter4mapper = Converter.apply( inside_mapperext= True, **kargs)

    target_table = property( lambda self: self.target.table)
    if _v03:
        _target_expr = property(
            lambda self: _func_ifnull( self.target, 0, type= self.target.type ) )
    else:
        _target_expr = property(
            lambda self: _func_ifnull( self.target, 0, type_= self.target.type ) )

    def value( self, instance): return getattr( instance, self.source.name)
    def oldv(  self, instance): return self._orig( instance, self.source.name)

    filter_expr = None
    _filter4recalc = None
    _filter4mapper = None
    def get_filter_and_bindings( self, (fexpr,bindings), instance, old):
        'return either with var-bindparams, or const-bound-bindparams (value= getattr(instance))'
        if callable( fexpr): fexpr = fexpr( instance, old)
        vbindings = dict( (k,self._get_current_or_orig( instance, k, old)) for k in bindings)
        return fexpr, vbindings
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
        self._filter4recalc = (
                (key.parent == bindparam( Converter._pfx+ grouping_attribute)),
                ( grouping_attribute, )
            )
        self._filter4mapper = (
                (key.column == bindparam( Converter._pfx+ grouping_attribute)),
                ( grouping_attribute, )
            )
        #the getattr(instance, name, old) part is done in aggregator/mapperext



################
import sqlalchemy.orm

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

    def __init__( self, *aggregations, **kargs):
        """ *aggregations - _Aggregation-subclass instances, to be maintained for this mapper
        """
        self.off = False
        self.aggregations_by_table = groups = dict()

        #here combined by target table, then target column
        for ag in aggregations:
            assert isinstance( ag, _Aggregation)
            groups.setdefault( ag.target_table, [] ).append( ag)

        mapper = kargs.get( 'mapper') or kargs.get( 'class_')
        if mapper:  #autohook and setup
            if not isinstance( mapper, sqlalchemy.orm.Mapper):
                mapper = sqlalchemy.orm.class_mapper( mapper)
            self._setup( mapper)
            if not mapper.extension or self not in mapper.extension:
                mapper.extension.append( self)

    def _setup( self, mapper):
        self.local_table = table = mapper.local_table
        self.aggregations = groups = dict()     #combined by table,filter
        for (target_table, aggs) in self.aggregations_by_table.iteritems():
            used_columns = set()
            for a in aggs:
                a._initialize( table)
                if a.filter_expr is None:
                    fkey, src_attribute = self.find_fkey( table, target_table, mapper)
                    a.setup_fkey( fkey, src_attribute)
                    groups.setdefault( (target_table, fkey), [] ).append( a)    #not a.filter_expr
                else:
                    groups.setdefault( (target_table, a.filter_expr), [] ).append( a)
                #here re-combined by target_table+filter
                #later, for ags on same key, only ags[0]._filter* is used
                target = a.target
                if target in used_columns:
                    warnings.warn( Warning( 'Aggregator: target column '+ str(target) + ' has more than one aggregator; full recalc will be wrong' ))
                    # XXX this case may need another sub-classification, by target table.column;
                    # as having >1 agg on same column, onrecalc must somehow bundle them all
                    # into one super-select / single update. This may not be 100% automatic,
                    # the way of bundling should be pre-specified - is it a+b or a/b or..
                used_columns.add( target)
        #suicide
        self._setup = lambda *a,**k: None

    def find_fkey( self, table, target_table, mapper):
        for k in table.foreign_keys:
            #pick first one - maybe fail if there are more
            if k.references( target_table):
                break
        else:
            raise NotImplementedError( "No foreign key defined for pair %s %s" % (table, target_table))

        #print 'LLLook for ', k.parent, k.parent.name
        grouping_attribute = None
        # Field maybe aliased somewhere
        for propcol in mapper.iterate_properties:
            if isinstance( propcol, sqlalchemy.orm.ColumnProperty):
                if propcol.columns[0] is k.parent:     # "==" works not as expected
                    grouping_attribute = propcol.key
                    return k, grouping_attribute

        colname = k.parent.name
        raise RuntimeError( "Can't find property for %(colname)r / foreignkey %(k)r" % locals() )


    def _make_updates( self, instance, connection, action):
        if not self.off:
            for aggs in self.aggregations.itervalues():
                self._make_change1( aggs, instance, connection, action)

    def _make_change1( self, aggs, instance, connection, action, old =False):
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
            if Converter._pfx:
                bindings = dict( (Converter._pfx+k,v) for k,v in bindings.items() )
            if 0:
                print 'UUUUUUUU'
                for k,v in updates.items(): print k,v
                print bindings
#            ag.target_table.update( fexpr, values=updates ).execute( **bindings)   #own transaction+commit
            connection.execute( ag.target_table.update( fexpr, values=updates ), **bindings)    #part of overall transaction

    def _db_supports( self, funcname):
        'called back by aggregation-calculators'
        if self.local_table.metadata.bind.url.drivername == 'mysql':
            return funcname not in ('max','min')
        return True

    #mapperExtension protocol - these are called after the instance is ins/upd/del-eted
    def after_insert( self, mapper, connection, instance):
        self._setup( mapper)
        self._make_updates( instance, connection, self._insert_method)
        return self._after_all( mapper, connection, instance)
    def after_delete( self, mapper, connection, instance):
        self._setup( mapper)
        self._make_updates( instance, connection, self._delete_method)
        return self._after_all( mapper, connection, instance)
    def after_update( self, mapper, connection, instance):
        self._setup( mapper)
        if not self.off:
            for aggs in self.aggregations.itervalues():
                ag = aggs[0]    # They all have same table/filters
                #XXX BUT there will be conflict onrecalc if same column in several ag's
                bindings = ag._filter4mapper[1]
                same = ag._same_binding_values( bindings, instance)

                if same:
                    self._make_change1( aggs, instance, connection, 'onupdate')
                else:
                    self._make_change1( aggs, instance, connection,  self._delete_method, old=True)
                    self._make_change1( aggs, instance, connection,  self._insert_method)
        return self._after_all( mapper, connection, instance)

    auto_expire_refs = ()
    def _after_all( self, mapper, connection, instance):
        if 10:
            session = sqlalchemy.orm.object_session( instance)
            for name in self.auto_expire_refs:
                g = getattr( instance, name, None)
                if g is not None:
                    print 'EXPIRING', type(g), g.db_id, g.kolichestvo
                    session.expire( g)
                    #session.refresh( g)
        return EXT_CONTINUE

class Accurate( Quick):
    """Mapper extension which maintains aggregations.
    Accurate does all calculations using aggregated
    query at every update of related fields
    """
    _insert_method = 'onrecalc'
    _delete_method = 'onrecalc_old'

# vim:ts=4:sw=4:expandtab
