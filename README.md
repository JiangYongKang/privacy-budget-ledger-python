# 隐私预算记账本（privacy-budget-ledger）

一套可在本地直接运行的隐私预算（privacy budget）记账与查询服务，围绕
**分析者 × 数据集** 两个维度管理额度，支持以唯一查询标识做幂等消耗、
一次查询跨多个数据集的组合扣减、并发争抢下的强一致、服务重启后的状态保留，
以及**先预占、后结算（reserve → settle/release）**的占位式记账。

- 语言/运行时：Python 3.14 + FastAPI + SQLite（标准库 `sqlite3`，无额外服务依赖）
- 存储模型：**只追加事件账本**（append-only event log），没有可被覆盖的"当前余额"

---

## 1. 目录结构

```
ledger/
  errors.py    # 可区分的领域错误码（失败原因分类，含预占相关 4 类）
  models.py    # 领域模型：DatasetCharge / ConsumeResult / BalanceView
               #             ReservationCharge / ReservationResult / SettleResult / ReleaseResult
  store.py     # SQLite 只追加事件账本 + 预占表 + 单写事务 + 启动完整性校验
  service.py   # 应用服务：参数校验、鉴权顺序、幂等判定、TTL 配置、结构化日志
  api.py       # FastAPI HTTP 接口与错误响应映射
main.py        # 本地启动入口
scripts/demo.py# 不依赖 HTTP 的功能演示
tests/         # 单测：错误分类 / 幂等 / 多数据集 / 并发 / 持久化防倒退 / HTTP
               #       预占结算归还 / 预占持久化 / 预占并发混跑 / 预占 HTTP
```

---

## 2. 预算口径

- 额度按 `(analyst_id, dataset_id)` 二元组授权，字段为 `limit`（上限）。
- `consumed`（已消耗）**只由 `events` 表中的成功扣减事件求和得到**，
  账本中不存在可就地修改的余额字段。
- 余额：`balance = limit − consumed`；`consumed` 任何时刻都不得超过 `limit`。
- **预占口径（与直接扣减同一本账）**：
  - `reserved`：该额度上所有**未过期、未终结**预占（`held`）的金额之和；
  - `available = limit − consumed − reserved`：真正还能被新请求（预占或直接扣减）
    拿走的额度；
  - 任何时刻都必须满足 **`consumed + reserved ≤ limit`**。
    预占与直接扣减混着并发发的时候，也守这条线（两者在同一把写锁、
    同一个事务口径下复核）。
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

## 5. 预占 / 结算 / 归还（先占住，后结账）

适用于"任务跑之前不知道确切用量，先把额度占住，跑完按实际用量结账"的场景。

### 5.1 生命周期

```
POST /reservations  ──held(占住,计入 reserved)──┐
                                                ├─ POST /reservations/settle  → settled（实扣入账，多占退回）
                                                ├─ POST /reservations/release → released（整笔归还）
                                                └─ 到达 expires_at            → expired（自动失效，额度退回）
```

- **预占 reserve**：携带全局唯一的 `reservation_id`、分析者与一个/多个
  `{dataset_id, amount}`。预占成功后该笔额度进入 `held`，立即计入 `reserved`，
  别的请求（包括直接 `/consume`）抢不走。跨多数据集时与组合扣减一样：
  **要么全占上，要么整体不生效**，不存在只占一半。
- **结算 settle**：任务跑完后按各数据集**实际用量**结算。
  - 实际用量 `cost` 必须 `0 ≤ cost ≤ 该数据集预占额`；
  - 实扣部分写入只追加事件（`events.ref_type='settle'`），计入 `consumed`；
  - **多占的部分 `reserved − cost` 立即退回**，重新变为 `available`；
  - 实际用量为 0 也允许：不产生扣减事件，预占整笔释放。
- **归还 release**：任务不做了，整笔归还，全部占用释放，无任何扣减。
- 预占**有效期内**的占用，与已扣减额一起构成"不能再超"的部分，
  口径即第 2 节的 `consumed + reserved ≤ limit`。

### 5.2 有效期（TTL）与过期

- 创建预占时可传 `ttl_seconds`；不传则用服务默认值
  （环境变量 `BUDGET_RESERVATION_TTL_SECONDS`，默认 **900 秒**）。
  本地验证时把它调小（如 `BUDGET_RESERVATION_TTL_SECONDS=2`）即可快速看到过期。
- 到 `expires_at` 仍未结算/归还的预占**自动失效**：状态变 `expired`，
  占用额度退回，不需要调用方补偿。
- 失效采用**懒处理 + 显式清理**双保险：
  - 任何读取/预占/结算/归还/余额计算遇到已到期的 `held` 都会先把它翻成 `expired`；
  - 也可主动调 `POST /reservations/sweep-expired` 批量清理；
  - **服务重启打开库时会先 sweep 一次**再做完整性校验。
- **过期后再拿它来结算会被明确拒绝**（`409 reservation_expired`），
  而不是照常扣账；过期后归还返回与自动释放一致的结论（幂等）。

### 5.3 幂等与冲突（与原 query_id 规则一致）

- `reservation_id` 唯一。预占/结算/归还都可安全重复提交：
  - 内容一致的重复预占 → 重放首次结论（含相同的 `expires_at`、占用前后可用额），
    不重复占用；
  - 同一笔预占重复结算（实际用量完全一致）→ 重放首次结算结论，不重复扣账；
  - 重复归还 → 只生效一次，退回金额与首次一致。
- 同一 `reservation_id` 提交**不同内容**（金额不同、数据集集合不同）或由
  **不同分析者**提交 → `409 reservation_conflict`，不动任何额度。
- 已 `settled` 不能再 release、已 `released/expired` 不能再 settle
  → `409 reservation_closed` / `409 reservation_expired`。
- 不存在的 `reservation_id` → `404 reservation_not_found`。
- 结算数据集必须是预占集合的子集、用量不得超过预占额；越界 →
  `reservation_conflict` 或 `invalid_parameter`，预占保持不变。

### 5.4 与原扣减 / 幂等 / 并发规则如何共存

- **同一本账**：预占不单独建账。直接扣减在事务内复核的是
  `consumed + reserved + 本次 ≤ limit`；预占复核的是同一口径。
  因此两类请求混着发、并发发，都不会越限。
- **同一把锁 / 同一种事务**：预占、结算、归还、直接扣减都在
  `RLock + BEGIN IMMEDIATE` 的单写事务内完成，跨数据集写多张表，
  任一步失败整体 `ROLLBACK`。
- **同一种持久化与防倒退**：预占主表/明细表与事件表一起落库；
  重开库时除校验事件序列外，还校验预占明细完整、`held` 未过期、
  结算金额自洽、以及 `consumed + 有效预占 ≤ limit`。
  重启不会丢失有效期内的占用，也不会凭空多出额度。
- **各自独立的标识命名空间**：`query_id` 与 `reservation_id` 互不影响；
  结算产生的事件内部使用 `settle:<reservation_id>:<dataset_id>` 标识，
  不会与普通 `consume` 的 `query_id` 冲突。

---

## 6. 失败原因分类（可区分错误码）

所有错误响应统一为：

```json
{"error": {"code": "<machine_readable>", "message": "…", "details": { … }}}
```

| code | HTTP | 触发条件 | 是否产生扣减/占用 |
|---|---|---|---|
| `analyst_not_found` | 404 | 分析者不存在 | 否 |
| `dataset_not_found` | 404 | 数据集不存在（多数据集中任一不存在即整体失败） | 否 |
| `unauthorized_dataset` | 403 | 分析者未被授权访问该数据集（越权） | 否 |
| `budget_insufficient` | 409 | 任一数据集可用额度不足（扣减/预占整体回滚） | 否 |
| `query_conflict` | 409 | `query_id` 已存在但扣减内容/分析者不一致 | 否 |
| `reservation_not_found` | 404 | 预占标识不存在 | 否 |
| `reservation_expired` | 409 | 预占已过期自动释放，仍拿来结算/复用 | 否（占用已退回） |
| `reservation_closed` | 409 | 预占已结算/已归还，又执行互斥操作 | 否 |
| `reservation_conflict` | 409 | 同一 `reservation_id` 内容/分析者不一致，或结算项越界 | 否 |
| `invalid_parameter` | 422 | 空标识、空 charges、cost/amount 非法、数据集重复、ttl 非正、body 校验失败 | 否 |
| `ledger_rollback_detected` | 500 | 事件序列缺口/回放越限/预占状态不自洽/试图下调上限等账本倒退 | 服务拒绝继续记账 |
| `query_not_found` | 404 | 查询不存在的 `query_id` 明细（`GET /queries/{id}`） | — |

> 说明：`budget_insufficient.details` 同时给出新旧两种口径字段——
> `balance = limit − consumed`（旧）以及
> `consumed / reserved / available`（新），便于排障与兼容。

---

## 7. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/analysts` | `{"id": "a1"}` 注册分析者 |
| POST | `/datasets` | `{"id": "d1"}` 注册数据集 |
| PUT | `/analysts/{analyst_id}/datasets/{dataset_id}/grant` | `{"limit": 10.0}` 授权/上调 |
| GET | `/analysts/{analyst_id}/datasets/{dataset_id}/balance` | 查询单个额度（含 `reserved`/`available`） |
| GET | `/analysts/{analyst_id}/balances` | 查询该分析者所有授权额度 |
| POST | `/consume` | 幂等直接扣减（支持多数据集） |
| GET | `/queries/{query_id}` | 按查询标识回放成功结论 |
| POST | `/reservations` | 预占额度（支持多数据集，可带 `ttl_seconds`） |
| POST | `/reservations/settle` | 按实际用量结算（多占退回） |
| POST | `/reservations/release` | 整笔归还预占 |
| POST | `/reservations/sweep-expired` | 主动清理已到期预占，返回被失效的 id 列表 |
| GET | `/reservations/{reservation_id}` | 查询预占当前状态（held/settled/released/expired） |
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

预占 → 结算 / 归还示例：

```bash
# 1) 预占：d1 占 6、d2 占 3，有效期 300 秒（不传 ttl_seconds 则用默认 TTL）
curl -s -X POST localhost:8000/reservations -H 'content-type: application/json' -d '{
  "reservation_id": "rsv-20261008-001",
  "analyst_id": "a1",
  "ttl_seconds": 300,
  "charges": [{"dataset_id": "d1", "amount": 6}, {"dataset_id": "d2", "amount": 3}]
}'
# -> status=held, expires_at=..., items 里含每个数据集占用前后的 available

# 此时余额口径：d1 consumed=0 reserved=6 available=4
curl -s localhost:8000/analysts/a1/datasets/d1/balance

# 2) 跑完按实际用量结算：d1 实际 4（退回 2），d2 实际 3（退回 0）
curl -s -X POST localhost:8000/reservations/settle -H 'content-type: application/json' -d '{
  "reservation_id": "rsv-20261008-001",
  "analyst_id": "a1",
  "charges": [{"dataset_id": "d1", "amount": 4}, {"dataset_id": "d2", "amount": 3}]
}'
# -> status=settled, total_charged=7, total_refunded=2
# 同样的结算请求再发一次 -> status=idempotent_replay，不重复扣账

# 任务不做了则整笔归还（与结算二选一）：
curl -s -X POST localhost:8000/reservations/release -H 'content-type: application/json' -d '{
  "reservation_id": "rsv-20261008-002", "analyst_id": "a1"
}'
```

---

## 8. 本地验证方法

前置：已安装 [uv](https://docs.astral.sh/uv/)。

```bash
# 1) 安装依赖（含 pytest / httpx）
uv sync

# 2) 跑全部单测（日志会实时打印 标识 / 前后额度 / 判定依据）
uv run pytest -q

# 3) 不依赖 HTTP 的功能演示：授权、幂等、越权、不足、冲突、组合回滚、重启保留
uv run python scripts/demo.py

# 4) 启动 HTTP 服务（默认 http://127.0.0.1:8000，库文件 data/budget_ledger.db）
#    把预占默认有效期调短到 2 秒，方便快速验证过期自动释放：
BUDGET_RESERVATION_TTL_SECONDS=2 uv run python main.py
#   交互文档：http://127.0.0.1:8000/docs
```

快速验证"先占后结 / 过期释放"（配合上面的 2 秒 TTL）：

```bash
# 占住 6，随后立即看余额：reserved=6 available=4
curl -s -X POST localhost:8000/reservations -H 'content-type: application/json' \
  -d '{"reservation_id":"r-ttl","analyst_id":"a1","charges":[{"dataset_id":"d1","amount":6}]}'
curl -s localhost:8000/analysts/a1/datasets/d1/balance

# 等 3 秒后：sweep 列出 r-ttl，余额恢复 available=10
curl -s -X POST localhost:8000/reservations/sweep-expired
curl -s localhost:8000/analysts/a1/datasets/d1/balance
# 再拿它结算 -> 409 reservation_expired，而不是照常扣账
curl -s -X POST localhost:8000/reservations/settle -H 'content-type: application/json' \
  -d '{"reservation_id":"r-ttl","analyst_id":"a1","charges":[{"dataset_id":"d1","amount":1}]}'
```

重启持久化验证：默认使用文件库，预占（`held`）后直接 Ctrl-C 停服再启动，
有效期内的预占仍在、`reserved` 不丢；若在 TTL 之后重启，打开库即自动 sweep，
额度退回且结算被拒。

### 测试覆盖说明

- `tests/test_service_errors.py`：5 类业务失败原因可区分、失败零扣减、拒绝下调上限；
- `tests/test_idempotency.py`：同 `query_id` 多次提交只扣一次且结论一致、可追溯；
- `tests/test_multi_dataset.py`：跨数据集全成功/整体回滚、混合查询后口径稳定；
- `tests/test_concurrency.py`：40 线程争抢不越限、25 线程并发同标识只扣一次、
  8 线程组合扣减无部分状态、60 线程混合压力结果稳定；
- `tests/test_persistence.py`：重启后额度与幂等记录保留、删事件/篡改上限被判定为倒退；
- `tests/test_api.py`：HTTP 端到端错误码、幂等响应、统一错误信封；
- `tests/test_reservations.py`：预占后结算多占退回（含 0 用量全额退）、
  预占后整笔归还、到期自动释放且拒绝结算（懒失效 + 显式 sweep）、
  预占/结算/归还重复提交只生效一次且结论一致、同标识不同内容/不同分析者冲突、
  跨数据集组合预占全占或整体回滚、预占沿用原有错误分类、
  以及预占与直接扣减共用 `consumed + reserved ≤ limit` 口径；
- `tests/test_reservation_persistence.py`：有效期内重启占用不丢且可结算、
  过期后重启自动退额且拒结算、结算记录重启保留不重扣、已归还预占重启不复活；
- `tests/test_reservation_concurrency.py`：60 线程（30 直接扣 + 30 预占）
  混打不越限、预占占住后并发直接扣减受限且结算退回恢复可用、
  25 线程并发同一 `reservation_id` 只占一份；
- `tests/test_reservation_api.py`：预占/结算/归还/过期的 HTTP 状态码、
  幂等响应、非法 body 信封与零占用。

测试日志统一由 `privacy_budget_ledger` logger 输出，关键字段：
`query_id` / `reservation_id`、`analyst_id`、`dataset_id`、
`扣减前余额 / 占用前可用`、`本次扣减 / 本次占用`、`实扣 / 退回`、
`扣减后余额 / 占用后可用`、`到期(expires_at)`、`判定依据`，
可从日志直接看出每次结论是首次生效、幂等重放、整体回滚还是过期拒绝。
