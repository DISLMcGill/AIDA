from aida.aida import *;

config = __import__('TPCHconfig-AIDA')

TABLE_INVOLVED = {
    'q01': ['lineitem'],
    'q02': ['part', 'partsupp', 'supplier', 'nation', 'region'],
    'q03': ['customer', 'orders', 'lineitem'],
    'q04': ['lineitem', 'orders'],
    'q05': ['customer', 'orders', 'lineitem', 'supplier', 'nation', 'region'],
    'q06': ['lineitem'],
    'q07': ['supplier', 'lineitem', 'orders', 'customer', 'nation'],
    'q08': ['part', 'supplier', 'lineitem', 'orders', 'customer', 'nation', 'region'],
    'q09': ['part', 'supplier', 'lineitem', 'partsupp', 'orders', 'nation'],
    'q10': ['customer', 'orders', 'lineitem', 'nation'],
    'q11': ['partsupp', 'supplier', 'nation'],
    'q12': ['orders', 'lineitem'],
    'q13': ['customer', 'orders'],
    'q14': ['lineitem', 'part'],
    'q15': ['supplier', 'lineitem'],
    'q16': ['supplier', 'part', 'partsupp'],
    'q17': ['lineitem', 'part'],
    'q18': ['customer', 'orders', 'lineitem'],
    'q19': ['lineitem', 'part'],
    'q20': ['supplier', 'nation', 'part', 'lineitem', 'partsupp'],
    'q21': [],
    'q22': ['orders', 'customer']
}

def q01(db):
    """
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day (3)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

    :param db:
    :return:
    """
    lineitem = db.lineitem;
    l = lineitem.filter(Q('l_shipdate', DATE('1998-09-02'), CMP.LTE));
    l = l.project(('l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice'
                   , {F('l_extendedprice') * (1 - F('l_discount')): 'disc_price'}
                   , {F('l_extendedprice') * (1 - F('l_discount')) * (1 + F('l_tax')): 'charge'}
                   , 'l_quantity'
                   , 'l_extendedprice'
                   , 'l_discount'
                   ));
    l = l.aggregate(
        ('l_returnflag', 'l_linestatus', {SUM('l_quantity'): 'sum_qty'}, {SUM('l_extendedprice'): 'sum_base_price'}
         , {SUM('disc_price'): 'sum_disc_price'}
         , {SUM('charge'): 'sum_charge'}
         , {AVG('l_quantity'): 'avg_qty'}
         , {AVG('l_extendedprice'): 'avg_price'}
         , {AVG('l_discount'): 'avg_disc'}
         , {COUNT('*'): 'count_order'})
        , ('l_returnflag', 'l_linestatus'));
    l = l.order(('l_returnflag', 'l_linestatus'));

    return l;


#TODO: Replace head() by limit()
def q02(db):
    """
select
	s_acctbal,
        s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 15
	and p_type like '%BRASS'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'EUROPE'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;

-- rewritten to avoid corelated query

select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  part,
  supplier,
  partsupp,
  nation,
  region,
	 (
    select
		  ps_partkey as i_partkey,
      min(ps_supplycost) as min_supplycost
    from
      partsupp,
      supplier,
      nation,
      region
    where
      s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
		group by ps_partkey
  ) costtbl
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
	and ps_partkey = i_partkey
  and ps_supplycost = min_supplycost
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 100;

    :param db:
    :return:
    """
    p  = db.part.filter(Q('p_size', C(15)), Q('p_type', C('%BRASS'), CMP.LIKE));
    ps = db.partsupp;
    s  = db.supplier;
    n  = db.nation;
    r  = db.region.filter(Q('r_name', C('EUROPE')));

    j  = ps.join(s, ('ps_suppkey',), ('s_suppkey',), COL.ALL, COL.ALL);
    j  = j.join(n, ('s_nationkey',), ('n_nationkey',), COL.ALL, COL.ALL);
    j  = j.join(r, ('n_regionkey',), ('r_regionkey',), COL.ALL, COL.ALL);

    ti = j.aggregate(({'ps_partkey':'i_partkey'},{MIN('ps_supplycost'):'min_supply_cost'}), ('ps_partkey',));

    t  = j.join(p, ('ps_partkey',), ('p_partkey',), COL.ALL, COL.ALL );
    t  = t.join(ti, ('ps_partkey', 'ps_supplycost'), ('i_partkey', 'min_supply_cost'), COL.ALL, COL.ALL );
    t  = t.project(('s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment'));
    t  = t.order(('s_acctbal#desc', 'n_name', 's_name', 'p_partkey'));
    t  = t.head(100);

    return t;



#TODO: Replace head() by limit()
def q03(db):
    """
 select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from
  customer,
  orders,
  lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc,
  o_orderdate
limit 10;

    :param db:
    :return:
    """
    c = db.customer.filter(Q('c_mktsegment', C('BUILDING')));
    o = db.orders.filter(Q('o_orderdate', DATE('1995-03-15'), CMP.LT));
    l = db.lineitem.filter(Q('l_shipdate', DATE('1995-03-15'), CMP.GT)).project(('l_orderkey', {F('l_extendedprice')*(1-F('l_discount')):'rev'}));

    t = c.join(o, ('c_custkey',), ('o_custkey',), COL.ALL, COL.ALL);
    t = t.join(l, ('o_orderkey',), ('l_orderkey',), COL.ALL, COL.ALL);
    t = t.aggregate(('l_orderkey',{SUM('rev'):'revenue'}, 'o_orderdate', 'o_shippriority'),('l_orderkey', 'o_orderdate', 'o_shippriority'));
    t = t.order(('revenue#desc', 'o_orderdate'));
    t = t.head(10);

    return t;


def q04(db):
    """
select
  o_orderpriority,
  count(*) as order_count
from
  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
    select
      *
    from
      lineitem
    where
      l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
  )
group by
  o_orderpriority
order by
  o_orderpriority;

-- rewritten to avoid corelated query

select
  o_orderpriority,
  count(*) as order_count
from
  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and o_orderkey IN (
    select
      l_orderkey
    from
      lineitem
    where
      l_commitdate < l_receiptdate
  )
group by
  o_orderpriority
order by
  o_orderpriority;

    :param db:
    :return:
    """

    l = db.lineitem.filter(Q('l_commitdate', 'l_receiptdate', CMP.LT)).project('l_orderkey');
    o = db.orders.filter(Q('o_orderdate', DATE('1993-07-01'), CMP.GTE), Q('o_orderdate', DATE('1993-10-01'), CMP.LT), Q('o_orderkey', l, CMP.IN));

    t = o.aggregate(('o_orderpriority', {COUNT('*'):'order_count'}), ('o_orderpriority',));
    t = t.order('o_orderpriority');

    return t;


def q05(db):
    """
select
  n_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue
from
  customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '1' year
group by
  n_name
order by
  revenue desc;

    :param db:
    :return:
    """
    c = db.customer;
    o = db.orders.filter(Q('o_orderdate', DATE('1994-01-01'), CMP.GTE), Q('o_orderdate', DATE('1995-01-01'), CMP.LT));
    l = db.lineitem;
    s = db.supplier;
    n = db.nation;
    r = db.region.filter(Q('r_name', C('ASIA')));

    t = c.join(o, ('c_custkey',),('o_custkey',), COL.ALL, COL.ALL);
    t = t.join(l, ('o_orderkey',), ('l_orderkey',), COL.ALL, COL.ALL);
    t = t.join(s, ('l_suppkey', 'c_nationkey'), ('s_suppkey', 's_nationkey'), COL.ALL, COL.ALL);
    t = t.join(n, ('s_nationkey',), ('n_nationkey',), COL.ALL, COL.ALL);
    t = t.join(r, ('n_regionkey',), ('r_regionkey',), COL.ALL, COL.ALL);
    t = t.project(('n_name', {F('l_extendedprice')*(1-F('l_discount')):'rev'}))
    t = t.aggregate(('n_name', {SUM('rev'):'revenue'}), ('n_name',));
    t = t.order('revenue#desc');

    return t;



def q06(db):
    """
select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

    :param db:
    :return:
    """

    l = db.lineitem.filter(  Q('l_shipdate', DATE('1994-01-01'), CMP.GTE)
                           , Q('l_shipdate', DATE('1995-01-01'), CMP.LT)
                           , Q('l_discount', C(0.05), CMP.GTE)
                           , Q('l_discount', C(0.07), CMP.LTE)
                           , Q('l_quantity', C(24), CMP.LT)
                          ).project(({F('l_extendedprice')*F('l_discount'):'rev'},)
                          ).aggregate(({SUM('rev'):'revenue'})) ;
    return l;



def q07(db):
    """
select
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) as revenue
from
  (
    select
      n1.n_name as supp_nation,
      n2.n_name as cust_nation,
      extract(year from l_shipdate) as l_year,
      l_extendedprice * (1 - l_discount) as volume
    from
      supplier,
      lineitem,
      orders,
      customer,
      nation n1,
      nation n2
    where
      s_suppkey = l_suppkey
      and o_orderkey = l_orderkey
      and c_custkey = o_custkey
      and s_nationkey = n1.n_nationkey
      and c_nationkey = n2.n_nationkey
      and (
        (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
        or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
      )
      and l_shipdate between date '1995-01-01' and date '1996-12-31'
  ) as shipping
group by
  supp_nation,
  cust_nation,
  l_year
order by
  supp_nation,
  cust_nation,
  l_year;

    :param db:
    :return:
    """
    s  = db.supplier;
    l  = db.lineitem.filter(Q('l_shipdate',DATE('1995-01-01'),CMP.GTE), Q('l_shipdate',DATE('1996-12-31'),CMP.LTE)) \
                    .project(('l_suppkey', 'l_orderkey', {EXTRACT('l_shipdate',EXTRACT.OP.YEAR):'l_year'}, {F('l_extendedprice')*(1-F('l_discount')):'volume'}));
    o  = db.orders;
    c  = db.customer;
    n  = db.nation;

    ns = n.project(({'n_name':'supp_nation'}, {'n_nationkey':'ns_nationkey'}));
    nc = n.project(({'n_name':'cust_nation'}, {'n_nationkey':'nc_nationkey'}));

    t  = s.join(l, ('s_suppkey',), ('l_suppkey',), COL.ALL, COL.ALL );
    t  = t.join(o, ('l_orderkey',), ('o_orderkey',), COL.ALL, COL.ALL);
    t  = t.join(c, ('o_custkey',), ('c_custkey',), COL.ALL, COL.ALL);
    t  = t.join(ns, ('s_nationkey',), ('ns_nationkey',), COL.ALL, COL.ALL);
    t  = t.join(nc, ('c_nationkey',), ('nc_nationkey',), COL.ALL, COL.ALL);
    t  = t.filter( (Q('supp_nation',C('FRANCE')) & Q('cust_nation',C('GERMANY'))) | (Q('supp_nation',C('GERMANY')) & Q('cust_nation',C('FRANCE'))) );
    t  = t.aggregate(('supp_nation','cust_nation','l_year', {SUM('volume'):'revenue'}), ('supp_nation','cust_nation','l_year'));
    t  = t.order(('supp_nation', 'cust_nation', 'l_year'));

    return t;



def q08(db):
    """
select
  o_year,
  sum(case
    when nation = 'BRAZIL' then volume
    else 0
  end) / sum(volume) as mkt_share
from
  (
    select
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) as volume,
      n2.n_name as nation
    from
      part,
      supplier,
      lineitem,
      orders,
      customer,
      nation n1,
      nation n2,
      region
    where
      p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'AMERICA'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between date '1995-01-01' and date '1996-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'
  ) as all_nations
group by
  o_year
order by
  o_year;

    :param db:
    :return:
    """
    p  = db.part.filter(Q('p_type', C('ECONOMY ANODIZED STEEL')));
    s  = db.supplier;
    l  = db.lineitem;
    o  = db.orders.filter(Q('o_orderdate',DATE('1995-01-01'),CMP.GTE), Q('o_orderdate',DATE('1996-12-31'),CMP.LTE));
    c  = db.customer;
    n  = db.nation;
    r  = db.region.filter(Q('r_name', C('AMERICA')));

    n1 = n.project(({'n_name':'n1_name'}, {'n_nationkey':'n1_nationkey'}, {'n_regionkey':'n1_regionkey'}));
    n2 = n.project(({'n_name':'n2_name'}, {'n_nationkey':'n2_nationkey'}, {'n_regionkey':'n2_regionkey'}));

    t  = l.join(p, ('l_partkey',), ('p_partkey',), COL.ALL, COL.ALL);
    t  = t.join(s, ('l_suppkey',), ('s_suppkey',), COL.ALL, COL.ALL);
    t  = t.join(o, ('l_orderkey',), ('o_orderkey',), COL.ALL, COL.ALL);
    t  = t.join(c, ('o_custkey',), ('c_custkey',), COL.ALL, COL.ALL);
    t  = t.join(n1,('c_nationkey',), ('n1_nationkey',), COL.ALL, COL.ALL);
    t  = t.join(r, ('n1_regionkey',), ('r_regionkey',), COL.ALL, COL.ALL);
    t  = t.join(n2,('s_nationkey',), ('n2_nationkey',), COL.ALL, COL.ALL);

    t  = t.project(({EXTRACT('o_orderdate', EXTRACT.OP.YEAR):'o_year'}
                    ,{CASE( ((Q('n2_name', C('BRAZIL')),F('l_extendedprice')*(1 - F('l_discount'))), ), C(0)):'volume1'}
                    ,{F('l_extendedprice')*(1 - F('l_discount')):'volume'}, {'n2_name':'nation'}));
    t  = t.aggregate(('o_year', {SUM('volume1'):'sum_v1'}, {SUM('volume'):'sum_v'}), ('o_year',));
    t  = t.project(('o_year', {F('sum_v1')/F('sum_v'):'mkt_share'} ));
    t  = t.order(('o_year',));

    return t;



def q09(db):
    """
select
  nation,
  o_year,
  sum(amount) as sum_profit
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
      part,
      supplier,
      lineitem,
      partsupp,
      orders,
      nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

    :param db:
    :return:
    """
    p  = db.part.filter(Q('p_name',C('%green%'), CMP.LIKE));
    s  = db.supplier;
    l  = db.lineitem;
    ps = db.partsupp;
    o  = db.orders;
    n  = db.nation;

    t  = s.join(l, ('s_suppkey',), ('l_suppkey',), COL.ALL, COL.ALL);
    t  = t.join(ps, ('l_suppkey', 'l_partkey'), ('ps_suppkey', 'ps_partkey'), COL.ALL, COL.ALL );
    t  = t.join(p, ('l_partkey',), ('p_partkey',), COL.ALL, COL.ALL);
    t  = t.join(o, ('l_orderkey',), ('o_orderkey',), COL.ALL, COL.ALL);
    t  = t.join(n, ('s_nationkey',), ('n_nationkey',), COL.ALL, COL.ALL);
    t  = t.project(({'n_name':'nation'}, {EXTRACT('o_orderdate',EXTRACT.OP.YEAR):'o_year'}, {F('l_extendedprice')*(1-F('l_discount')) - F('ps_supplycost')*F('l_quantity'):'amount'} ));
    t  = t.aggregate(('nation', 'o_year', {SUM('amount'):'sum_profit'}), ('nation', 'o_year'));
    t  = t.order(('nation', 'o_year#desc'));

    return t;


def q10(db):
    """
select
  c_custkey,
  c_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
from
  customer,
  orders,
  lineitem,
  nation
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01'
  and o_orderdate < date '1993-10-01' + interval '3' month
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
order by
  revenue desc
limit 20;

    :param db:
    :return:
    """

    c = db.customer;
    o = db.orders.filter(Q('o_orderdate', DATE('1993-10-01'), CMP.GTE), Q('o_orderdate', DATE('1994-01-01'), CMP.LT));
    l = db.lineitem.filter(Q('l_returnflag', C('R'))).project(('l_orderkey', {F('l_extendedprice')*(1-F('l_discount')):'rev'}));
    n = db.nation;

    t = c.join(o, ('c_custkey',), ('o_custkey',), COL.ALL, COL.ALL);
    t = t.join(l, ('o_orderkey',), ('l_orderkey',), COL.ALL, COL.ALL);
    t = t.join(n, ('c_nationkey',), ('n_nationkey',), COL.ALL, COL.ALL);
    t = t.aggregate(('c_custkey', 'c_name', {SUM('rev'):'revenue'}, 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment'),
                    ('c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment'));
    t = t.order(('revenue#desc',));
    t = t.head(20);

    return t;



def q11(db):
    """
select
  ps_partkey,
  sum(ps_supplycost * ps_availqty) as value
from
  partsupp,
  supplier,
  nation
where
  ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
  ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
      select
        sum(ps_supplycost * ps_availqty) * 0.0100000000
      --                                     ^^^^^^^^^^^^
      -- The above constant needs to be adjusted according
      -- to the scale factor (SF): constant = 0.0001 / SF.
      from
        partsupp,
        supplier,
        nation
      where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    )
order by
  value desc;

    :param db:
    :return:
    """
    ps = db.partsupp;
    s  = db.supplier;
    n  = db.nation.filter(Q('n_name', C('GERMANY')));

    j  = ps.join(s, ('ps_suppkey',), ('s_suppkey',), COL.ALL, COL.ALL);
    j  = j.join(n, ('s_nationkey',), ('n_nationkey',), COL.ALL, COL.ALL);

    ti = j.project(({F('ps_supplycost')*F('ps_availqty'):'totsupcost'},));
    ti = ti.aggregate(({SUM('totsupcost'):'sum_totsupcost'},));
    ti = ti.project(({F('sum_totsupcost')*0.0001/config.SF :'sumtotsupcost'},));

    t  = j.project(('ps_partkey', {F('ps_supplycost')*F('ps_availqty'):'val'}))
    t  = t.aggregate(('ps_partkey', {SUM('val'):'value'}), ('ps_partkey',));
    t  = t.filter(Q('value', ti, CMP.GT));
    t  = t.order(('value#desc',));

    return t;



def q12(db):
    """
select
  l_shipmode,
  sum(case
    when o_orderpriority = '1-URGENT'
      or o_orderpriority = '2-HIGH'
      then 1
    else 0
  end) as high_line_count,
  sum(case
    when o_orderpriority <> '1-URGENT'
      and o_orderpriority <> '2-HIGH'
      then 1
    else 0
  end) as low_line_count
from
  orders,
  lineitem
where
  o_orderkey = l_orderkey
  and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1994-01-01' + interval '1' year
group by
  l_shipmode
order by
  l_shipmode;

    :param db:
    :return:
    """

    o  = db.orders;
    l  = db.lineitem.filter(Q('l_shipmode', ('MAIL', 'SHIP'), CMP.IN), Q('l_commitdate', 'l_receiptdate', CMP.LT), Q('l_shipdate', 'l_commitdate', CMP.LT)
                           ,Q('l_receiptdate', DATE('1994-01-01'), CMP.GTE), Q('l_receiptdate', DATE('1995-01-01'), CMP.LT));

    t  = l.join(o, ('l_orderkey',), ('o_orderkey',), COL.ALL, COL.ALL);
    t  = t.project(('l_shipmode', {CASE(  ( (Q('o_orderpriority',('1-URGENT', '2-HIGH'),CMP.IN), 1) ,),  C(0)):'hl_count'}
                                , {CASE(  ( (Q('o_orderpriority',('1-URGENT', '2-HIGH'),CMP.NOTIN), 1) ,),  C(0)):'ll_count'}));

    t  = t.aggregate(('l_shipmode', {SUM('hl_count'):'high_line_count'}, {SUM('ll_count'):'low_line_count'}), ('l_shipmode',))
    t  = t.order(('l_shipmode',));
    return t;



def q13(db):
    """
select
  c_count,
  count(*) as custdist
from
  (
    select
      c_custkey,
      count(o_orderkey)
    from
      customer left outer join orders on
        c_custkey = o_custkey
        and o_comment not like '%special%requests%'
    group by
      c_custkey
  ) as c_orders (c_custkey, c_count)
group by
  c_count
order by
  custdist desc,
  c_count desc;

    :param db:
    :return:
    """

    c = db.customer;
    o = db.orders.filter(Q('o_comment', C('%special%requests%'), CMP.NOTLIKE));
    t = c.join(o, ('c_custkey',), ('o_custkey',), COL.ALL, COL.ALL, JOIN.LEFT);
    t = t.aggregate(('c_custkey', {COUNT('o_orderkey'):'c_count'}), ('c_custkey',));
    t = t.aggregate(('c_count', {COUNT('*'):'custdist'}), ('c_count',));
    t = t.order(('custdist#desc', 'c_count#desc'));

    return t;



def q14(db):
    """
select
  100.00 * sum(case
    when p_type like 'PROMO%'
      then l_extendedprice * (1 - l_discount)
    else 0
  end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
  lineitem,
  part
where
  l_partkey = p_partkey
  and l_shipdate >= date '1995-09-01'
  and l_shipdate < date '1995-09-01' + interval '1' month;

    :param dbc:
    :return:
    """

    l  = db.lineitem.filter(Q('l_shipdate', DATE('1995-09-01'), CMP.GTE), Q('l_shipdate', DATE('1995-10-01'), CMP.LT));
    p  = db.part;

    t  = l.join(p, ('l_partkey',), ('p_partkey',), COL.ALL, COL.ALL);
    t  = t.project((  {CASE(( (Q('p_type', C('PROMO%'), CMP.LIKE) ,F('l_extendedprice')*(1 - F('l_discount')))  ,),0):'revenue1'},  { F('l_extendedprice')*(1 - F('l_discount')):'revenue2'} ));
    t  = t.aggregate(({SUM('revenue1'):'sum_revenue1'}, {SUM('revenue2'):'sum_revenue2'}));
    t  = t.project(({100.00*F('sum_revenue1')/F('sum_revenue2'):'promo_revenue'},))

    return t;


def q15(db):
    """

create view revenue0 (supplier_no, total_revenue) as
  select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
  from
    lineitem
  where
    l_shipdate >= date '1996-01-01'
    and l_shipdate < date '1996-01-01' + interval '3' month
  group by
    l_suppkey;


select
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
from
  supplier,
  revenue0
where
  s_suppkey = supplier_no
  and total_revenue = (
    select
      max(total_revenue)
    from
      revenue0
  )
order by
  s_suppkey;

drop view revenue0;

--- we will replace the use of a view with the equivalent query.

    :param db:
    :return:
    """

    s  = db.supplier;
    l  = db.lineitem.filter(Q('l_shipdate', DATE('1996-01-01'), CMP.GTE), Q('l_shipdate', DATE('1996-04-01'), CMP.LT)) \
                    .project(({'l_suppkey':'supplier_no'}, {F('l_extendedprice')*(1 - F('l_discount')):'rev'})) \
                    .aggregate(('supplier_no',{SUM('rev'):'total_revenue'}), ('supplier_no',));

    ti = l.aggregate((MAX('total_revenue'),));

    t  = s.join(l, ('s_suppkey',), ('supplier_no',), COL.ALL, COL.ALL);
    t  = t.filter(Q('total_revenue', ti));
    t  = t.project(('s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue'));
    t  = t.order(('s_suppkey',));

    return t;



def q16(db):
    """
select
  p_brand,
  p_type,
  p_size,
  count(distinct ps_suppkey) as supplier_cnt
from
  partsupp,
  part
where
  p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
    select
      s_suppkey
    from
      supplier
    where
      s_comment like '%Customer%Complaints%'
  )
group by
  p_brand,
  p_type,
  p_size
order by
  supplier_cnt desc,
  p_brand,
  p_type,
  p_size;

    :param db:
    :return:
    """

    s  = db.supplier.filter(Q('s_comment', C('%Customer%Complaints%'), CMP.LIKE)).project(('s_suppkey'));
    p  = db.part.filter(Q('p_brand', C('Brand#45'), CMP.NE), Q('p_type', C('MEDIUM POLISHED%'), CMP.NOTLIKE), Q('p_size', (49, 14, 23, 45, 19, 3, 36, 9), CMP.IN));
    ps = db.partsupp.filter(Q('ps_suppkey', s, CMP.NOTIN));

    t  = p.join(ps, ('p_partkey',), ('ps_partkey',), COL.ALL, COL.ALL);
    t  = t.aggregate(('p_brand', 'p_type', 'p_size', {COUNT('ps_suppkey', distinct=True):'supplier_cnt'}), ('p_brand', 'p_type', 'p_size'));
    t  = t.order(('supplier_cnt#desc', 'p_brand', 'p_type', 'p_size'));

    return t;



def q17(db):
    """
select
  sum(l_extendedprice) / 7.0 as avg_yearly
from
  lineitem,
  part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      lineitem
    where
      l_partkey = p_partkey
  );

-- Rewritten to avoid using a corelated query

select
  sum(l_extendedprice) / 7.0 as avg_yearly
from
  lineitem,
  part,
(
    select p_partkey as i_partkey, 0.2 * avg(l_quantity) as  avg_qty
    from
      lineitem,
			part
    where
      l_partkey = p_partkey
	group by p_partkey
)x
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
	and p_partkey = i_partkey
  and l_quantity < avg_qty;

    :param db:
    :return:
    """
    l  = db.lineitem;
    p  = db.part;

    ti = l.join(p, ('l_partkey',), ('p_partkey',), COL.ALL, COL.ALL);
    t  = ti;
    ti = ti.aggregate(({'p_partkey':'i_partkey'}, {AVG('l_quantity'):'avg_q'}), ('p_partkey',));
    ti = ti.project(('i_partkey', {0.2*F('avg_q'):'avg_qty'}));

    t  = t.filter(Q('p_brand', C('Brand#23')), Q('p_container', C('MED BOX')));
    t  = t.join(ti, ('p_partkey',), ('i_partkey',), COL.ALL, COL.ALL);
    t  = t.filter(Q('l_quantity', 'avg_qty', CMP.LT));
    t  = t.aggregate(({SUM('l_extendedprice'):'tot_price'},)).project(({F('tot_price')/7.0:'avg_yearly'}));

    return t;



def q18(db):
    """
select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  customer,
  orders,
  lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      lineitem
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate
limit 100;

    :param db:
    :return:
    """
    c  = db.customer;
    o  = db.orders;
    l  = db.lineitem;

    ti = l.aggregate(('l_orderkey', {SUM('l_quantity'):'qty'}), ('l_orderkey',)).filter(Q('qty', C(300), CMP.GT)).project(('l_orderkey',));

    t = c.join(o, ('c_custkey',), ('o_custkey',), COL.ALL, COL.ALL);
    t = t.join(l, ('o_orderkey',), ('l_orderkey',), COL.ALL, COL.ALL);
    t = t.filter(Q('o_orderkey', ti, CMP.IN));
    t = t.aggregate(('c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice', SUM('l_quantity')), ('c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'));
    t = t.order(('o_totalprice#desc', 'o_orderdate'));
    t = t.head(100);

    return t;



def q19(db):
    """
select
  sum(l_extendedprice* (1 - l_discount)) as revenue
from
  lineitem,
  part
where
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#12'
    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l_quantity >= 1 and l_quantity <= 1 + 10
    and p_size between 1 and 5
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l_quantity >= 10 and l_quantity <= 10 + 10
    and p_size between 1 and 10
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#34'
    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l_quantity >= 20 and l_quantity <= 20 + 10
    and p_size between 1 and 15
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  );

    :param db:
    :return:
    """
    l = db.lineitem;
    p = db.part;
    l = l.filter(Q('l_shipmode', ('AIR', 'AIR REG'), CMP.IN )
                        & Q('l_shipinstruct', C('DELIVER IN PERSON')))
    p = p.filter(Q('p_size', C(1), CMP.GTE))
    t = l.join(p, 'l_partkey', 'p_partkey', COL.ALL, COL.ALL);
    t = t.filter(
                    (
                        Q('p_partkey', 'l_partkey')
                        & Q('p_brand', C('Brand#12'))
                        & Q('p_container', ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'), CMP.IN)
                        & Q('l_quantity', C(1), CMP.GTE) & Q('l_quantity', C(11), CMP.LTE)
                        & Q('p_size', C(5), CMP.LTE)
                    ) |
                    (
                        Q('p_partkey', 'l_partkey')
                        & Q('p_brand', C('Brand#23'))
                        & Q('p_container', ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'), CMP.IN)
                        & Q('l_quantity', C(10), CMP.GTE) & Q('l_quantity', C(20), CMP.LTE)
                        & Q('p_size', C(10), CMP.LTE)
                    ) |
                    (
                        Q('p_partkey', 'l_partkey')
                        & Q('p_brand', C('Brand#34'))
                        & Q('p_container', ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'), CMP.IN)
                        & Q('l_quantity', C(20), CMP.GTE) & Q('l_quantity', C(30), CMP.LTE)
                        & Q('p_size', C(15), CMP.LTE)
                    )
                );
    t = t.project(({F('l_extendedprice')*(1 - F('l_discount')):'rev'},));
    t = t.aggregate(({SUM('rev'):'revenue'},));

    return t;




def q20(db):
    """
select
  s_name,
  s_address
from
  supplier,
  nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
      partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          part
        where
          p_name like 'forest%'
      )
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
          lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

-- rewritten to avoid corelated query

select
  s_name,
  s_address
from
  supplier,
  nation
where
  s_suppkey in
	(
		select
			ps_suppkey
		from
			partsupp,
      (
        select
					l_partkey,
					l_suppkey,
          0.5 * sum(l_quantity) as totqty
        from
          lineitem
        where
          l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
        group by
					l_partkey,
					l_suppkey
      )l
		where ps_partkey = l_partkey
			and ps_suppkey = l_suppkey
			and ps_availqty > totqty
			and ps_partkey in
			(
        select
          p_partkey
        from
          part
        where
          p_name like 'forest%'
      )
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

    :param db:
    :return:
    """

    s  = db.supplier;
    n  = db.nation.filter(Q('n_name', C('CANADA')));
    p  = db.part.filter(Q('p_name', C('forest%'), CMP.LIKE)).project(('p_partkey',));
    l  = db.lineitem.filter(Q('l_shipdate',DATE('1994-01-01'),CMP.GTE), Q('l_shipdate',DATE('1995-01-01'),CMP.LT)) \
                    .aggregate(('l_partkey', 'l_suppkey', {SUM('l_quantity'):'sumqty'}), ('l_partkey', 'l_suppkey')) \
                    .project(('l_partkey', 'l_suppkey', {0.5*F('sumqty'):'totqty'} ));
    ps = db.partsupp;

    ti = ps.join(l, ('ps_partkey', 'ps_suppkey'), ('l_partkey', 'l_suppkey'), COL.ALL, COL.ALL);
    ti = ti.filter(Q('ps_availqty', 'totqty', CMP.GT), Q('ps_partkey', p, CMP.IN));
    ti = ti.project(('ps_suppkey',));

    t  = s.join(n, ('s_nationkey',), ('n_nationkey',), COL.ALL, COL.ALL);
    t  = t.filter(Q('s_suppkey', ti, CMP.IN));
    t  = t.project(('s_name', 's_address'));
    t  = t.order(('s_name',));

    return t;



#TODO : Implementation - pending multiple coulumn IN/NOT IN support
def q21(db):
    """
select
  s_name,
  count(*) as numwait
from
  supplier,
  lineitem l1,
  orders,
  nation
where
  s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select
      *
    from
      lineitem l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select
      *
    from
      lineitem l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s_nationkey = n_nationkey
  and n_name = 'SAUDI ARABIA'
group by
  s_name
order by
  numwait desc,
  s_name
limit 100;

-- rewritten to avoid corelated query

select
  s_name,
  count(*) as numwait
from
  supplier,
  lineitem l1,
  orders,
  nation
where
  s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
	and (l1.l_orderkey, l1.l_suppkey) in
	(
	  select t1.l_orderkey, t1.l_suppkey
		from lineitem t1, lineitem t2
		where t1.l_orderkey = t2.l_orderkey
		  and t1.l_suppkey <> t2.l_suppkey
		--group by t1.l_orderkey, t1.l_suppkey
	)
	and (l1.l_orderkey, l1.l_suppkey) not in
	(
	  select t1.l_orderkey, t1.l_suppkey
		from lineitem t1, lineitem t2
		where t1.l_orderkey = t2.l_orderkey
		  and t1.l_suppkey <> t2.l_suppkey
			and t2.l_receiptdate > t2.l_commitdate
		--group by t1.l_orderkey, t1.l_suppkey
	)
  and s_nationkey = n_nationkey
  and n_name = 'SAUDI ARABIA'
group by
  s_name
order by
  numwait desc,
  s_name
limit 100;


    :param db:
    :return:
    """
    raise NotImplementedError("Query 21 of TPC-H is not implemented");



def q22(db):
    """
select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substring(c_phone from 1 for 2) as cntrycode,
      c_acctbal
    from
      customer
    where
      substring(c_phone from 1 for 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substring(c_phone from 1 for 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )
      and not exists (
        select
          *
        from
          orders
        where
          o_custkey = c_custkey
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

-- rewritten to avoid corelated query

select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substring(c_phone from 1 for 2) as cntrycode,
      c_acctbal
    from
      customer
    where
      substring(c_phone from 1 for 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substring(c_phone from 1 for 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )
      and c_custkey not in (
        select
          o_custkey
        from
          orders
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;


    :param db:
    :return:
    """

    o  = db.orders.project(('o_custkey',));
    c  = db.customer.project(('c_acctbal', {SUBSTRING('c_phone', 1, 2):'cntrycode'}, 'c_custkey')).filter(Q('cntrycode',('13', '31', '23', '29', '30', '18', '17'), CMP.IN));

    ti = c.filter(Q('c_acctbal', 0.0, CMP.GT)).aggregate(({AVG('c_acctbal'):'avg_acctbal'},));
    t  = c.filter(Q('c_acctbal', ti, CMP.GT), Q('c_custkey', o, CMP.NOTIN));
    t  = t.aggregate(('cntrycode', {COUNT('*'):'numcust'}, {SUM('c_acctbal'):'totacctbal'}), ('cntrycode',));
    t  = t.order(('cntrycode',));

    return t;
