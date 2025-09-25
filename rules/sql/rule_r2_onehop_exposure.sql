with tx as (select * from read_csv('data/raw/tx_sample.csv', AUTO_DETECT=TRUE)),
     address_tag as (select * from read_csv('data/raw/address_tags.csv', AUTO_DETECT=TRUE)),
high_risk as (select lower(address) as address from address_tag
              where tag_type in ('SANCTIONS','MIXER') and confidence >= 0.80),
onehop as (
  select child.tx_hash as alerted_tx, child.block_time, child.from_addr as child_from, child.to_addr as child_to,
         case when lower(parent.from_addr) in (select address from high_risk) then 1 else 0 end as parent_from_highrisk,
         case when lower(parent.to_addr)   in (select address from high_risk) then 1 else 0 end as parent_to_highrisk
  from tx child
  left join tx parent on lower(parent.to_addr)=lower(child.from_addr)
)
select * from onehop where parent_from_highrisk=1 or parent_to_highrisk=1 order by block_time;