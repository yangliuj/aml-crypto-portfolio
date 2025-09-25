with tx as (select * from read_csv('data/raw/tx_sample.csv', AUTO_DETECT=TRUE)),
     address_tag as (select * from read_csv('data/raw/address_tags.csv', AUTO_DETECT=TRUE))
select t.tx_hash, t.chain, t.block_time, t.from_addr, t.to_addr, t.asset, t.amount,
       at.tag_type, at.source, at.confidence
from tx t
join address_tag at on lower(at.address)=lower(t.to_addr)
where at.tag_type in ('SANCTIONS','MIXER') and at.confidence >= 0.80
order by t.block_time;