#$Id$


#example: plain double-side accounting:
#   Store .total_dt/.total_kt sums DocItem.value respective to DocItem.store_dt/store_kt

#this is the only one that works, it is independent from dbcook:
def make_aggregators():
    '''the expressions (klas.attr_id etc) below work only AFTER mappers are made.
    call this after dbcook.builder() is done (or after SAdb.bind() if dbcook.SAdb used)
    '''
    import dbcook.misc.aggregator as a
    a.Quick(
        a.Sum( Store.total_dt, DocItem.value, Store.db_id == DocItem.store_dt_id ),
        a.Sum( Store.total_kt, DocItem.value, Store.db_id == DocItem.store_kt_id ),
        class_= DocItem,
        auto_expire_refs = [ 'store_dt', 'store_kt' ],
    )

##########
#possible dbcook syntaxes
# all these may need some support from within dbcook, and are not nice enough
class Store:
    pass
    #...
    if 0*1:
        @classmethod
        def DBcook_aggregators( klas):
            import dbcook.misc.aggregator as a
            return [
                a.Sum( klas.total_dt, DocItem.value, klas == DocItem.store_dt ),
                a.Sum( klas.total_kt, DocItem.value, klas == DocItem.store_kt  ),
            ]
            # translate filter-expr
            # group by class=source.klas; a.Quick( all for class1, class_=class1)

    if 0*2:
        import dbcook.misc.aggregator as a
        DBcook_aggregators = {
                total_dt: (a.Sum, DocItem.value, lambda self, src =DocItem: self == src.store_dt ),
                total_kt: (a.Sum, DocItem.value, lambda self, src =DocItem: self == src.store_kt ),
            }
            # translate filter-expr
            # group by class=source.klas; a.Quick( all for class1, class_=class1)

    if 0*3:
        total_dt= Aggregator( Decimal( default_value= 0),
                        Aggregator.Sum, DocItem.value,
                        #filter= (klas == DocItem.store_dt),
                        filter= lambda self, src =DocItem: self == src.store_dt,
                        auto_expire= DocItem.store_dt,
                    )
        total_kt= Decimal( default_value= 0)
        Aggregator.Sum( total_kt, DocItem.value,
                        #filter= (klas == DocItem.store_kt),
                        filter= lambda self, src =DocItem: self == src.store_kt,
                        auto_expire= DocItem.store_kt,
                    )
            # translate filter-expr
            # group by class=source.klas; a.Quick( all for class1, class_=class1)

# vim:ts=4:sw=4:expandtab
