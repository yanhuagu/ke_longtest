with avg_tmp as (
    select
        avg(c_acctbal) as avg_acctbal
    from
         customer
    where
        c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
)

select
    cntrycode,
    count(1) as numcust,
    sum(c_acctbal) as totacctbal
from (
    select
        substring(c_phone, 1, 2) as cntrycode,
        c_acctbal
    from 
         customer left join  v_orders on c_custkey = o_custkey, avg_tmp
    where 
        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
        and c_acctbal > avg_acctbal
        and o_orderkey is null
) t
group by
    cntrycode
order by
    cntrycode;
