# 隐私预算记账本（privacy-budget-ledger）

一套可在本地直接运行的隐私预算（privacy budget）记账与查询服务，围绕
**分析者 × 数据集** 两个维度管理额度，支持以唯一查询标识做幂等消耗、
一次查询跨多个数据集的组合扣减、并发争抢下的强一致以及服务重启后的状态保留。

- 语言/运行时：Python 3.14 + FastAPI + SQLite（标准库 `sqlite3`，无额外服务依赖）
- 存储模型：**只追加事件账本**（append-only event log），没有可被覆盖的"当前余额"

---

## 1. 目录结构

```
ledger/
  errors.py    # 可区分的领域错误码（失败原因分类）
  models.py    # 领域模型：DatasetCharge / ChargeResult / ConsumeResult / BalanceView
  store.py     # SQLite 只追加事件账本 + 单写事务 + 启动完整性校验
  service.py   # 应用服务：参数校验、鉴权顺序、幂等判定、结构化日志
  api.py       # FastAPI HTTP 接口与错误响应映射
main.py        # 本地启动入口
scripts/demo.py# 不依赖 HTTP 的功能演示
tests/         # 单测：错误分类 / 幂等 / 多数据集 / 并发 / 持久化防倒退 / HTTP
```

---

## 2. 预算口径

- 额度按 `(analyst_id, dataset_id)` 二元组授权，字段为 `limit`（上限）。
- `consumed`（已消耗）**只由 `events` 表中的成功扣减事件求和得到**，
  账本中不存在可就地修改的余额字段。
- 余额：`balance = limit − consumed`；`consumed` 任何时刻都不得超过 `limit`。
- 一次查询可携带多个扣减项 `charges: [{dataset_id, cost}, ...]`，
  本次总消耗为各项之和；同一次请求内同一数据集只允许出现一次（需自行合并）。
- 授权上限允许**不变或上调**；**下调上限被视为账本倒退，直接拒绝**
  （`ledger_rollback_detected`）。
- 每个扣减项 `cost` 必须是正的有限数（>0，不接受 NaN/Inf/0/负数）。

---

## 3. 幂等规则

- 每个请求必须带全局唯一的 `query_id`；以
  `UNIQUE(query_id, dataset_id)` 约束保证同一查询的每个数据集只会有一条事件。
- 服务端处理顺序（全部在同一个写事务内）：
  1. 分析者存在 → 2. 每个数据集存在 → 3. 每个数据集已被该分析者授权
  → 4. 查 `query_id` 是否已存在成功事件
  → 5. 事务内复核每个数据集余额是否充足
  → 6. 一次性写入全部事件并提交。
- **重复提交**：`(query_id, 数据集集合, 每项 cost, 所属分析者)` 完全一致时，
  不再扣减，直接重放首次的成功结论（响应 `status=idempotent_replay`，
  返回的每项"扣减前/后余额"与首次完全一致）。
- **标识冲突**：`query_id` 已存在但扣减内容或分析者不一致 →
  `409 query_conflict`，不产生任何扣减。
- 失败的请求不写入事件，因此**不会占用 `query_id`**；同一 `query_id`
  用合法参数重试可以成功。

## 4. 并发与事务规则

- 进程内所有数据库访问由一把可重入锁串行化，写事务使用
  `BEGIN IMMEDIATE`；多数据集组合扣减在**同一个事务**内完成，
  任一步失败立即 `ROLLBACK`：要么全部扣成功，要么整体不生效，
  不可能留下"只扣了一部分"的中间状态。
- 余额充足性在事务内、插入前复核，因此并发争抢同一份额度时，
  累计 `consumed` 不会越过 `limit`（详见 `tests/test_concurrency.py`）。
- SQLite 数据库文件默认开启 WAL；默认库路径 `data/budget_ledger.db`，
  可用环境变量 `BUDGET_LEDGER_DB` 覆盖。
- **重启持久化与防倒退**：重新打开库时会按 `seq` 顺序回放全部事件并校验：
  - 事件序号必须从 1 连续无缺口（删除事件 → 检出缺口）；
  - 每个事件必须能找到对应授权；
  - 回放累计消耗不得超过授权上限（篡改上限 → 检出越限）。

  任一校验失败即抛 `ledger_rollback_detected`，拒绝在可疑账本上继续服务，
  而不是从某个中间状态继续记账。

---

## 5. 失败原因分类（可区分错误码）

所有错误响应统一为：

```json
{"error": {"code": "<machine_readable>", "message": "…", "details": { … }}}
```

| code | HTTP | 触发条件 | 是否产生扣减 |
|---|---|---|---|
| `analyst_not_found` | 404 | 分析者不存在 | 否 |
| `dataset_not_found` | 404 | 数据集不存在（多数据集中任一不存在即整体失败） | 否 |
| `unauthorized_dataset` | 403 | 分析者未被授权访问该数据集（越权） | 否 |
| `budget_insufficient` | 409 | 任一数据集余额不足（组合扣减整体回滚） | 否 |
| `query_conflict` | 409 | `query_id` 已存在但扣减内容/分析者不一致 | 否 |
| `invalid_parameter` | 422 | 空标识、空 charges、cost 非正/非有限数、同请求内数据集重复、body 校验失败 | 否 |
| `ledger_rollback_detected` | 500 | 事件序列缺口/回放越限/试图下调上限等账本倒退 | 服务拒绝继续记账 |
| `query_not_found` | 404 | 查询不存在的 `query_id` 明细（`GET /queries/{id}`） | — |

---

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/analysts` | `{"id": "a1"}` 注册分析者 |
| POST | `/datasets` | `{"id": "d1"}` 注册数据集 |
| PUT | `/analysts/{analyst_id}/datasets/{dataset_id}/grant` | `{"limit": 10.0}` 授权/上调 |
| GET | `/analysts/{analyst_id}/datasets/{dataset_id}/balance` | 查询单个额度 |
| GET | `/analysts/{analyst_id}/balances` | 查询该分析者所有授权额度 |
| POST | `/consume` | 幂等消耗（支持多数据集） |
| GET | `/queries/{query_id}` | 按查询标识回放成功结论 |
| GET | `/health` | 健康检查 |

`POST /consume` 请求示例：

```json
{
  "query_id": "q-20260928-001",
  "analyst_id": "a1",
  "charges": [
    {"dataset_id": "d1", "cost": 4.0},
    {"dataset_id": "d2", "cost": 2.0}
  ]
}
```

成功响应（首次 `accepted`；重复提交为 `idempotent_replay`，内容一致）：

```json
{
  "query_id": "q-20260928-001",
  "analyst_id": "a1",
  "status": "accepted",
  "total_charged": 6.0,
  "items": [
    {"dataset_id": "d1", "charged": 4.0, "balance_before": 10.0, "balance_after": 6.0},
    {"dataset_id": "d2", "charged": 2.0, "balance_before": 5.0,  "balance_after": 3.0}
  ]
}
```

---

## 7. 本地验证方法

前置：已安装 [uv](https://docs.astral.sh/uv/)。

```bash
# 1) 安装依赖（含 pytest / httpx）
uv sync

# 2) 跑全部单测（日志会实时打印 查询标识 / 扣减前后余额 / 判定依据）
uv run pytest -q

# 3) 不依赖 HTTP 的功能演示：授权、幂等、越权、不足、冲突、组合回滚、重启保留
uv run python scripts/demo.py

# 4) 启动 HTTP 服务（默认 http://127.0.0.1:8000，库文件 data/budget_ledger.db）
uv run python main.py
#   交互文档：http://127.0.0.1:8000/docs
```

curl 快速体验：

```bash
curl -s -X POST localhost:8000/analysts -H 'content-type: application/json' -d '{"id":"a1"}'
curl -s -X POST localhost:8000/datasets -H 'content-type: application/json' -d '{"id":"d1"}'
curl -s -X PUT  localhost:8000/analysts/a1/datasets/d1/grant -H 'content-type: application/json' -d '{"limit":10}'
curl -s -X POST localhost:8000/consume -H 'content-type: application/json' \
  -d '{"query_id":"q-1","analyst_id":"a1","charges":[{"dataset_id":"d1","cost":4}]}'
# 再发一次同样的请求，status 变为 idempotent_replay，余额不变
curl -s localhost:8000/analysts/a1/datasets/d1/balance
```

### 测试覆盖说明

- `tests/test_service_errors.py`：5 类业务失败原因可区分、失败零扣减、拒绝下调上限；
- `tests/test_idempotency.py`：同 `query_id` 多次提交只扣一次且结论一致、可追溯；
- `tests/test_multi_dataset.py`：跨数据集全成功/整体回滚、混合查询后口径稳定；
- `tests/test_concurrency.py`：40 线程争抢不越限、25 线程并发同标识只扣一次、
  8 线程组合扣减无部分状态、60 线程混合压力结果稳定；
- `tests/test_persistence.py`：重启后额度与幂等记录保留、删事件/篡改上限被判定为倒退；
- `tests/test_api.py`：HTTP 端到端错误码、幂等响应、统一错误信封。

测试日志统一由 `privacy_budget_ledger` logger 输出，关键字段：
`query_id`、`analyst_id`、`dataset_id`、`扣减前余额`、`本次扣减`、
`扣减后余额`、`判定依据`。
