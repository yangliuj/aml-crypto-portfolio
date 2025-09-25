with tx as (select * from read_csv('data/raw/tx_sample.csv', AUTO_DETECT=TRUE)),
inflow as (
  select to_addr, count(distinct from_addr) as src_cnt, sum(amount) as sum_in,
         min(block_time) as tmin, max(block_time) as tmax
  from tx group by to_addr
)
select o.tx_hash as suspicious_tx, i.to_addr as aggregator_addr, i.src_cnt, i.sum_in, o.block_time as out_time, o.amount as out_amt
from inflow i join tx o on lower(o.from_addr)=lower(i.to_addr)
where i.src_cnt >= 2 and o.amount >= i.sum_in * 0.8
  and o.block_time between i.tmin and i.tmax
order by out_time;