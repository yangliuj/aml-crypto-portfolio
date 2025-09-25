import datetime, json, hashlib, duckdb, pandas as pd, pathlib

RULES_VERSION = "v1.0"

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "raw"
OUT  = ROOT / "reports" / "STR_cases"
OUT.mkdir(parents=True, exist_ok=True)

def sha256_path(p: pathlib.Path) -> str:
    with open(p, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

# snapshot hashes for audit replay
SNAP = {
    "tx_sample.csv": sha256_path(DATA / "tx_sample.csv"),
    "address_tags.csv": sha256_path(DATA / "address_tags.csv"),
}

# Load data
tx   = pd.read_csv(DATA / "tx_sample.csv")
tags = pd.read_csv(DATA / "address_tags.csv")

con = duckdb.connect(database=":memory:")
con.register("tx", tx)
con.register("address_tag", tags)

# ---------- R1: Direct SANCTIONS/MIXER ----------
r1 = con.execute("""
select
  t.tx_hash, t.chain, t.block_time, t.asset, t.amount,
  t.from_addr, t.to_addr,
  ag.tag_type as cp_tag_type,
  ag."source" as cp_tag_source,
  ag.confidence as cp_tag_conf
from tx t
join address_tag ag on lower(ag.address)=lower(t.to_addr)
where ag.tag_type in ('SANCTIONS','MIXER') and ag.confidence >= 0.80
order by t.block_time
""").fetchdf()

# ---------- R3: Fan-in -> Single Out (structuring) ----------
r3 = con.execute("""
with inflow as (
  select to_addr, count(distinct from_addr) as src_cnt, sum(amount) as sum_in,
         min(block_time) as tmin, max(block_time) as tmax
  from tx group by to_addr
),
outflow as (
  select o.tx_hash, o.chain, o.block_time, o.asset, o.amount,
         o.from_addr, o.to_addr
  from tx o
)
select
  of.tx_hash          as suspicious_tx,
  of.chain,
  of.block_time,
  of.asset,
  of.amount           as out_amt,
  i.to_addr           as aggregator_addr,
  of.from_addr        as out_from,
  of.to_addr          as out_to,
  i.src_cnt,
  i.sum_in,
  tag_to.tag_type     as cp_tag_type,
  tag_to."source"     as cp_tag_source,
  tag_to.confidence   as cp_tag_conf
from inflow i
join outflow of on lower(of.from_addr)=lower(i.to_addr)
left join address_tag tag_to on lower(tag_to.address)=lower(of.to_addr)
where i.src_cnt >= 2
  and of.amount >= i.sum_in * 0.8
  and of.block_time between i.tmin and i.tmax
order by of.block_time
""").fetchdf()

print("=== R1 Direct High-Risk (SANCTIONS/MIXER) ===")
print(r1.to_string(index=False))
print("\n=== R3 Fan-in -> Out (structuring) ===")
print(r3.to_string(index=False))

def md_escape(x):
    return "" if x is None else str(x)

def pretty(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)

def write(name: str, body: str):
    (OUT / name).write_text(body, encoding="utf-8")

now = datetime.datetime.utcnow().isoformat() + "Z"

# ----- Evidence pages (English, rich fields + raw record + hashes) -----
for row in r1.to_dict(orient="records"):
    body = (
        f"# Suspicious Transaction (One-Pager)\n"
        f"- **Rule / Version**: R1 (Direct SANCTIONS/MIXER) / {RULES_VERSION}\n"
        f"- **Tx hash**: {row['tx_hash']}\n"
        f"- **Chain**: {row['chain']}  \n"
        f"- **Block time (UTC)**: {row['block_time']}  \n"
        f"- **Asset / Amount**: {row['asset']} / {row['amount']}  \n"
        f"- **From → To**: {row['from_addr']} → {row['to_addr']}  \n"
        f"- **Counterparty tag**: {md_escape(row['cp_tag_type'])} "
        f"(source={md_escape(row['cp_tag_source'])}, conf={md_escape(row['cp_tag_conf'])})  \n"
        f"- **Generated at (UTC)**: {now}\n"
        f"- **Data snapshot (SHA256)**: tx_sample.csv={SNAP['tx_sample.csv']}, address_tags.csv={SNAP['address_tags.csv']}\n"
        f"\n## Raw record\n```json\n{pretty(row)}\n```\n"
    )
    write(f"R1_{row['tx_hash']}.md", body)

for row in r3.to_dict(orient="records"):
    body = (
        f"# Suspicious Transaction (One-Pager)\n"
        f"- **Rule / Version**: R3 (Fan-in → Single Out) / {RULES_VERSION}\n"
        f"- **Tx hash**: {row['suspicious_tx']}\n"
        f"- **Chain**: {row['chain']}  \n"
        f"- **Block time (UTC)**: {row['block_time']}  \n"
        f"- **Asset / Amount**: {row['asset']} / {row['out_amt']}  \n"
        f"- **From → To**: {row['out_from']} → {row['out_to']}  \n"
        f"- **Aggregator addr**: {row['aggregator_addr']}  \n"
        f"- **Fan-in sources / Sum-in**: {row['src_cnt']} / {row['sum_in']}  \n"
        f"- **Counterparty tag (out_to)**: {md_escape(row['cp_tag_type'])} "
        f"(source={md_escape(row['cp_tag_source'])}, conf={md_escape(row['cp_tag_conf'])})  \n"
        f"- **Generated at (UTC)**: {now}\n"
        f"- **Data snapshot (SHA256)**: tx_sample.csv={SNAP['tx_sample.csv']}, address_tags.csv={SNAP['address_tags.csv']}\n"
        f"\n## Raw record\n```json\n{pretty(row)}\n```\n"
    )
    write(f"R3_{row['suspicious_tx']}.md", body)

print(f"\nEvidence pages written to: {OUT}")
