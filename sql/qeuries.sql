SELECT 
    c.c_mktsegment AS customer_segment,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue
FROM retail_staging.lineitem l
JOIN retail_staging.orders o ON l.l_orderkey = o.o_orderkey
JOIN retail_staging.customer c ON o.o_custkey = c.c_custkey
GROUP BY c.c_mktsegment
ORDER BY net_revenue DESC
LIMIT 5;