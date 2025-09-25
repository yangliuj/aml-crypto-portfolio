# AML Crypto Portfolio (KYT 作品集起步包)

包含三个可复现小项目：
1) **KYT 规则库 & 案件包（SQL）**
2) **Travel Rule 前置校验（JSON Schema + Postman）**
3) **Audit Replay 复跑原型（Notebook）**

## 10分钟复现
1. 安装：`pip install duckdb pandas`
2. 数据：已在 `data/raw/` 提供示例 CSV
3. 运行：
   - SQL 规则：`rules/sql/*.sql`
   - 复跑：打开 `notebooks/replay.ipynb` 运行所有单元；证据页将生成到 `reports/STR_cases/`

> 示例仅用于教学，不含真实 PII。
