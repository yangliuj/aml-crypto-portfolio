"""
Run KYT rules and generate simple evidence pages without Jupyter/VSCode.

Usage:
  1) pip install duckdb pandas
  2) python scripts\run_replay.py

Outputs:
  - prints R1/R3 results to console
  - writes evidence md files to reports/STR_cases/
"""
import json, datetime, duckdb, pandas as pd, hashlib, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "raw"
REPORT_DIR = ROOT / "reports" / "STR_cases"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# Load data
tx = pd.read_csv(DATA / "tx_sample.csv")
tags = pd.read_csv(DATA / "address_tags.csv")

# Snapshot hashes (for audit replay)
def sha256_path(p):
    with open(p, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

SNAP = {
    "tx_sample.csv": sha256_path(DATA / "tx_sample.csv"),
    "address_tags.csv": sha256_path(DATA / "address_tags.csv"),
    "generated_at": datetime.datetime.utcnow().isoformat() + "Z"
}

# Rules (R1: direct sanctions/mixer; R3: fan-in -> single out)
con = duckdb.connect(database=":memory:")
con.register("tx", tx)
con.register("address_tag", tags)

r1 = con.execute("""
select t.tx_hash, t.chain, t.block_time, t.from_addr, t.to_addr, t.asset, t.amount,
       at.tag_type, at.source, at.confidence
from tx t
join address_tag at on lower(at.address)=lower(t.to_addr)
where at.tag_type in ('SANCTIONS','MIXER') and at.confidence >= 0.80
order by t.block_time
""").fetchdf()

r3 = con.execute("""
with inflow as (
  select to_addr, count(distinct from_addr) as src_cnt, sum(amount) as sum_in,
         min(block_time) as tmin, max(block_time) as tmax
  from tx group by to_addr
)
select o.tx_hash as suspicious_tx, i.to_addr as aggregator_addr, i.src_cnt, i.sum_in, o.block_time as out_time, o.amount as out_amt
from inflow i join tx o on lower(o.from_addr)=lower(i.to_addr)
where i.src_cnt >= 2 and o.amount >= i.sum_in * 0.8
  and o.block_time between i.tmin and i.tmax
order by out_time
""").fetchdf()

print("=== R1 Direct High-Risk (SANCTIONS/MIXER) ===")
print(r1.to_string(index=False))
print("\n=== R3 Fan-in -> Out (structuring) ===")
print(r3.to_string(index=False))

def write_evidence(tx_hash, rule_code):
    md = f"""# 可疑交易一页纸
- 交易哈希：{tx_hash}
- 告警规则：{rule_code}
- 数据快照哈希：{json.dumps(SNAP, ensure_ascii=False)}
- 生成时间：{datetime.datetime.utcnow().isoformat()}Z
"""
    (REPORT_DIR / f"{rule_code}_{tx_hash}.md").write_text(md, encoding="utf-8")

for txh in r1["tx_hash"].tolist():
    write_evidence(txh, "R1")
for txh in r3["suspicious_tx"].tolist():
    write_evidence(txh, "R3")

print(f"\nEvidence pages written to: {REPORT_DIR}")
